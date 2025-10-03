
import streamlit as st
import pandas as pd
from collections import defaultdict, deque

st.set_page_config(page_title="Line Balance Reports", layout="wide")
st.title("📊 Line Balance Reports (Courses × Lines) — v2")

uploaded = st.file_uploader("Upload Student Allocations CSV (must include 'Code' and AL1..)", type=["csv"])

def melt_long(df):
    alloc_cols = [c for c in df.columns if str(c).startswith("AL")]
    long = df.melt(
        id_vars=[c for c in df.columns if c not in alloc_cols],
        value_vars=alloc_cols,
        var_name="Line",
        value_name="Class",
    )
    long = long.dropna(subset=["Class"]).copy()
    long["Course"] = long["Class"].astype(str).str[:5]
    return long

def counts_from_long(long_df):
    return long_df.groupby(["Line","Course"]).size().reset_index(name="StudentCount")

def build_offerings(long_df):
    counts = counts_from_long(long_df)
    wide = counts.pivot(index="Course", columns="Line", values="StudentCount")
    course_to_lines = {course: [ln for ln, ct in row.dropna().items() if ct > 0] for course, row in wide.iterrows()}
    return wide, course_to_lines

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
            rows.append({"Course": course, "Range": rng})
    return pd.DataFrame(rows).sort_values(["Range","Course"], ascending=[False, True]).reset_index(drop=True)

def build_student_schedule(long_df):
    sched = defaultdict(dict)
    for _, r in long_df.iterrows():
        sched[r["Code"]][r["Line"]] = r["Course"]
    return sched

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
        wide = counts.pivot(index="Course", columns="Line", values="StudentCount")
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
                    candidates = long_df[(long_df["Course"]==course) & (long_df["Line"]==from_ln)]["Code"].tolist()
                    moved_local = False
                    for student in candidates:
                        if student_move_counts[student] >= st.session_state.max_moves_per_student:
                            continue
                        if (student, course) in moved_sc:
                            continue
                        chain = plan_student_chain(student, course, from_ln, to_ln, sched, offerings, depth=2)
                        if chain is None:
                            continue
                        proposed_courses = [c for (c, _, _) in chain]
                        if any((student, c) in moved_sc for c in proposed_courses):
                            continue
                        if student_move_counts[student] + len(chain) > st.session_state.max_moves_per_student:
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
                            mask = (long_df["Code"]==student) & (long_df["Course"]==c) & (long_df["Line"]==src_ln)
                            long_df.loc[mask, "Line"] = dst_ln
                            moves.append({"StudentCode": student, "Course": c, "FromLine": src_ln, "ToLine": dst_ln})
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

if uploaded is not None:
    df = pd.read_csv(uploaded)
    long = melt_long(df)
    counts = counts_from_long(long)
    wide, _ = build_offerings(long)
    imbalance = compute_imbalance(wide)

    st.subheader("1) Imbalanced Courses")
    st.dataframe(imbalance)

    st.subheader("2) Move Suggestions")
    enable_multi = st.toggle("Enable multi-step per-student planner (with safeguards)", value=True)
    st.session_state.max_moves_per_student = st.number_input("Max moves per student", min_value=1, max_value=10, value=3, step=1)
    if enable_multi:
        moves, long_after = compute_multi_move_plan_constrained(long.copy(), max_rounds=200, max_moves_per_student=st.session_state.max_moves_per_student)
    else:
        moves = pd.DataFrame(columns=["StudentCode","Course","FromLine","ToLine"])
        long_after = long.copy()
    st.dataframe(moves)

    st.subheader("3) Before / After Impact")
    before = long.groupby(["Course","Line"]).size().reset_index(name="Before")
    after = long_after.groupby(["Course","Line"]).size().reset_index(name="After")
    impact = pd.merge(before, after, on=["Course","Line"], how="outer").fillna(0)
    impact["Before"] = impact["Before"].astype(int)
    impact["After"] = impact["After"].astype(int)
    impact["Change"] = impact["After"] - impact["Before"]
    impact = impact.sort_values(["Course","Line"]).reset_index(drop=True)
    st.dataframe(impact)

    def df_to_csv(df):
        return df.to_csv(index=False).encode("utf-8")

    c1, c2, c3 = st.columns(3)
    with c1:
        st.download_button("Download Imbalanced Courses (CSV)", data=df_to_csv(imbalance), file_name="imbalanced_courses.csv")
    with c2:
        st.download_button("Download Move Suggestions (CSV)", data=df_to_csv(moves), file_name="move_suggestions.csv")
    with c3:
        st.download_button("Download Before-After Impact (CSV)", data=df_to_csv(impact), file_name="before_after_impact.csv")
else:
    st.info("Upload a CSV to start.")
