
#!/usr/bin/env python3
import argparse
from collections import defaultdict, deque
import pandas as pd

def counts_from_long(long_df):
    return long_df.groupby(['Line','Course']).size().reset_index(name='StudentCount')

def build_offerings(long_df):
    counts = counts_from_long(long_df)
    wide = counts.pivot(index='Course', columns='Line', values='StudentCount')
    course_to_lines = {course: [ln for ln, ct in row.dropna().items() if ct > 0] for course, row in wide.iterrows()}
    return wide, course_to_lines

def build_student_schedule(long_df):
    sched = defaultdict(dict)
    for _, r in long_df.iterrows():
        sched[r['Code']][r['Line']] = r['Course']
    return sched

def compute_imbalance(wide):
    rows = []
    for course, row in wide.iterrows():
        vals = row.dropna()
        nz = vals[vals>0]
        appears_in = int((vals>0).sum())
        if appears_in < 2:
            continue
        if len(nz) >= 2:
            rng = float(nz.max()-nz.min())
            rows.append({'Course': course, 'Range': rng})
    imb = pd.DataFrame(rows).sort_values(['Range','Course'], ascending=[False, True]).reset_index(drop=True)
    return imb

def plan_student_chain(student, course, from_ln, to_ln, sched, offerings, depth=2):
    if to_ln not in sched[student]:
        return [(course, from_ln, to_ln)]
    if depth == 0:
        return None
    blocking_course = sched[student][to_ln]
    for alt_ln in offerings.get(blocking_course, []):
        if alt_ln == to_ln or alt_ln in sched[student]:
            continue
        return [(blocking_course, to_ln, alt_ln), (course, from_ln, to_ln)]
    if depth > 1:
        for alt_ln in offerings.get(blocking_course, []):
            if alt_ln == to_ln:
                continue
            if alt_ln not in sched[student]:
                continue
            c2 = sched[student][alt_ln]
            for alt2 in offerings.get(c2, []):
                if alt2 in sched[student] or alt2 == alt_ln:
                    continue
                return [(c2, alt_ln, alt2), (blocking_course, to_ln, alt_ln), (course, from_ln, to_ln)]
    return None

def compute_multi_move_plan_constrained(long_df, max_rounds=100, max_moves_per_student=3):
    sched = build_student_schedule(long_df)
    wide, offerings = build_offerings(long_df)
    moves = []
    improved = True
    rounds = 0
    moved_sc = set()
    student_move_counts = defaultdict(int)
    while improved and rounds < max_rounds:
        improved = False
        rounds += 1
        counts = counts_from_long(long_df)
        wide = counts.pivot(index='Course', columns='Line', values='StudentCount')
        for course, row in wide.iterrows():
            offered = [ln for ln, ct in row.dropna().items() if ct > 0]
            if len(offered) < 2:
                continue
            curr = {ln: int(row[ln]) for ln in offered}
            total = sum(curr.values())
            n = len(offered)
            base = total // n
            remainder = total % n
            lines_sorted_asc = sorted(offered, key=lambda ln: curr[ln])
            target = {ln: base for ln in offered}
            for ln in lines_sorted_asc[:remainder]:
                target[ln] = base + 1
            surplus = [ln for ln in offered if curr[ln] > target[ln]]
            deficit = [ln for ln in offered if curr[ln] < target[ln]]
            if not surplus or not deficit:
                continue
            for to_ln in deficit:
                need = target[to_ln] - curr[to_ln]
                if need <= 0:
                    continue
                for from_ln in surplus:
                    give = curr[from_ln] - target[from_ln]
                    if give <= 0:
                        continue
                    candidates = long_df[(long_df['Course']==course) & (long_df['Line']==from_ln)]['Code'].tolist()
                    moved_local = False
                    for student in candidates:
                        if student_move_counts[student] >= max_moves_per_student:
                            continue
                        if (student, course) in moved_sc:
                            continue
                        chain = plan_student_chain(student, course, from_ln, to_ln, sched, offerings, depth=2)
                        if chain is None:
                            continue
                        proposed_courses = [c for (c, _, _) in chain]
                        if any((student, c) in moved_sc for c in proposed_courses):
                            continue
                        if student_move_counts[student] + len(chain) > max_moves_per_student:
                            continue
                        valid = True
                        for c, src_ln, dst_ln in chain:
                            if sched[student].get(src_ln) != c or dst_ln in sched[student]:
                                valid = False
                                break
                        if not valid:
                            continue
                        for c, src_ln, dst_ln in chain:
                            sched[student].pop(src_ln, None)
                            sched[student][dst_ln] = c
                            mask = (long_df['Code']==student) & (long_df['Course']==c) & (long_df['Line']==src_ln)
                            long_df.loc[mask, 'Line'] = dst_ln
                            moves.append({'StudentCode': student, 'Course': c, 'FromLine': src_ln, 'ToLine': dst_ln})
                            moved_sc.add((student, c))
                            student_move_counts[student] += 1
                        improved = True
                        moved_local = True
                        break
                    if moved_local:
                        break
                if improved:
                    break
            if improved:
                break
    return pd.DataFrame(moves), long_df

def main():
    ap = argparse.ArgumentParser(description="Line Balance Reports with optional multi-step planner.")
    ap.add_argument("--input", "-i", required=True)
    ap.add_argument("--outdir", "-o", default="./out")
    ap.add_argument("--multi-move", action="store_true", help="Enable multi-step per-student move planner with safeguards")
    ap.add_argument("--max-moves-per-student", type=int, default=3)
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    alloc_cols = [c for c in df.columns if str(c).startswith('AL')]
    long0 = df.melt(id_vars=[c for c in df.columns if c not in alloc_cols], value_vars=alloc_cols, var_name='Line', value_name='Class')
    long0 = long0.dropna(subset=['Class']).copy()
    long0['Course'] = long0['Class'].astype(str).str[:5]

    wide0, _ = build_offerings(long0)
    imb = compute_imbalance(wide0)

    if args.multi_move:
        moves, long_after = compute_multi_move_plan_constrained(long0.copy(), max_rounds=200, max_moves_per_student=args.max_moves_per_student)
    else:
        moves = pd.DataFrame(columns=['StudentCode','Course','FromLine','ToLine'])
        long_after = long0.copy()

    before = long0.groupby(['Course','Line']).size().reset_index(name='Before')
    after = long_after.groupby(['Course','Line']).size().reset_index(name='After')
    impact = pd.merge(before, after, on=['Course','Line'], how='outer').fillna(0)
    impact['Before'] = impact['Before'].astype(int)
    impact['After'] = impact['After'].astype(int)
    impact['Change'] = impact['After'] - impact['Before']
    impact = impact.sort_values(['Course','Line']).reset_index(drop=True)

    os.makedirs(args.outdir, exist_ok=True)
    imb.to_csv(os.path.join(args.outdir, "imbalanced_courses.csv"), index=False)
    moves.to_csv(os.path.join(args.outdir, "move_suggestions.csv"), index=False)
    impact.to_csv(os.path.join(args.outdir, "before_after_impact.csv"), index=False)

    # Optional Excel
    try:
        with pd.ExcelWriter(os.path.join(args.outdir, "line_balance_reports.xlsx"), engine="openpyxl") as xlw:
            imb.to_excel(xlw, sheet_name="ImbalancedCourses", index=False)
            moves.to_excel(xlw, sheet_name="MoveSuggestions", index=False)
            impact.to_excel(xlw, sheet_name="BeforeAfterImpact", index=False)
    except Exception as e:
        print("Excel output skipped:", e)

if __name__ == "__main__":
    import os
    main()
