
#!/usr/bin/env python3
"""
Line Balance Report Generator

Generates three reports from a student allocations export (CSV):
1) Imbalanced courses (ignoring zeros, requiring appearances in >= min_lines)
2) Student-by-student move suggestions to balance courses across lines (same course only)
3) Before/After impact of the suggested moves on each course/line

Usage:
    python line_balance_report.py --input "StudentAllocations-Lines-Export (15).csv" --outdir ./out

Requirements:
    - Python 3.8+
    - pandas, numpy, openpyxl (for Excel export)

Notes:
    - "Course" is derived from the first five characters of the class code (e.g., "12ENG").
    - Lines are identified by columns whose names start with "AL" (e.g., AL1..AL6).
    - Move suggestions re-distribute students from surplus lines to deficit lines of the SAME course only.
"""

import argparse
import os
import sys
from collections import deque, defaultdict
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


def load_allocations(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Identify allocation columns (lines)
    alloc_cols = [c for c in df.columns if str(c).startswith("AL")]
    if not alloc_cols:
        raise ValueError("No line columns found. Expected columns that start with 'AL' (e.g., AL1, AL2...).")
    # Long format
    long = df.melt(
        id_vars=[c for c in df.columns if c not in alloc_cols],
        value_vars=alloc_cols,
        var_name="Line",
        value_name="Class"
    )
    long = long.dropna(subset=["Class"]).copy()
    # Derive course (first 5 chars)
    long["Course"] = long["Class"].astype(str).str[:5]
    # Ensure we have a student identifier column named 'Code'
    if "Code" not in long.columns:
        raise ValueError("Expected a 'Code' column to identify students.")
    return long, alloc_cols


def count_by_course_line(long: pd.DataFrame) -> pd.DataFrame:
    counts = long.groupby(["Line", "Course"]).size().reset_index(name="StudentCount")
    return counts


def compute_imbalance(counts: pd.DataFrame, ignore_zeros: bool = True, min_lines: int = 2) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
        wide_counts: pivot table (Course x Line) with counts (NaN when absent)
        imbalance: DataFrame with columns [Course, Range, Max, Min, OfferingLines, AppearsIn]
    """
    wide = counts.pivot(index="Course", columns="Line", values="StudentCount")
    # Prepare nonzero stats
    rows = []
    for course, row in wide.iterrows():
        vals = row.dropna()
        if ignore_zeros:
            nz = vals[vals > 0]
        else:
            nz = vals.fillna(0)
        appears_in = int((vals > 0).sum())
        if ignore_zeros and appears_in < min_lines:
            # skip courses that don't appear in at least min_lines lines
            continue
        if ignore_zeros:
            if len(nz) == 0:
                continue
            if len(nz) == 1:
                rng = 0
                mx = nz.max()
                mn = nz.min()
            else:
                rng = float(nz.max() - nz.min())
                mx = float(nz.max())
                mn = float(nz.min())
        else:
            rng = float(nz.max() - nz.min())
            mx = float(nz.max())
            mn = float(nz.min())
        rows.append({
            "Course": course,
            "Range": rng,
            "Max": mx,
            "Min": mn,
            "OfferingLines": ",".join([c for c, v in vals.items() if v > 0]),
            "AppearsIn": appears_in
        })
    imbalance = pd.DataFrame(rows).sort_values(["Range", "Course"], ascending=[False, True]).reset_index(drop=True)
    return wide, imbalance


def build_course_line_students(long: pd.DataFrame) -> Dict[Tuple[str, str], List[str]]:
    """
    Returns mapping: (Course, Line) -> [student codes]
    """
    d = long.groupby(["Course", "Line"])["Code"].apply(list).to_dict()
    return d


def compute_balancing_moves_for_course(course: str, wide_counts: pd.DataFrame, course_line_students: Dict[Tuple[str, str], List[str]]) -> List[Dict]:
    """
    Balances a single course across its offering lines:
    - Determine target (nearly equal) counts across lines where it is currently offered (non-zero only)
    - Produce student-by-student move suggestions from surplus to deficit lines
    """
    if course not in wide_counts.index:
        return []

    line_counts = wide_counts.loc[course].dropna()
    offering_lines = [ln for ln, ct in line_counts.items() if ct > 0]
    if len(offering_lines) < 2:
        return []

    # Current counts
    curr = {ln: int(line_counts[ln]) for ln in offering_lines}
    total = sum(curr.values())
    n = len(offering_lines)

    base = total // n
    remainder = total % n

    # Fair target: sort by current ascending; first 'remainder' get base+1
    lines_sorted_asc = sorted(offering_lines, key=lambda ln: curr[ln])
    target = {ln: base for ln in offering_lines}
    for ln in lines_sorted_asc[:remainder]:
        target[ln] = base + 1

    surplus = {ln: curr[ln] - target[ln] for ln in offering_lines if curr[ln] > target[ln]}
    deficit = {ln: target[ln] - curr[ln] for ln in offering_lines if curr[ln] < target[ln]}
    if sum(surplus.values()) == 0:
        return []

    # Build queues of students to move from surplus lines
    surplus_queues = {
        ln: deque(course_line_students.get((course, ln), [])[:surplus[ln]])
        for ln in surplus
    }

    # Distribute greedily into deficits
    moves = []
    for to_ln, need in deficit.items():
        remaining = need
        for from_ln in list(surplus.keys()):
            if surplus[from_ln] <= 0 or remaining <= 0:
                continue
            take = min(surplus[from_ln], remaining)
            for _ in range(take):
                if surplus_queues[from_ln]:
                    student = surplus_queues[from_ln].popleft()
                    moves.append({
                        "Course": course,
                        "FromLine": from_ln,
                        "ToLine": to_ln,
                        "StudentCode": student
                    })
                    surplus[from_ln] -= 1
                    remaining -= 1
            if remaining == 0:
                break
    return moves


def compute_all_moves(long: pd.DataFrame, wide_counts: pd.DataFrame, imbalance: pd.DataFrame, top_only: int = 0) -> pd.DataFrame:
    """
    Generate moves for all courses listed in `imbalance`.
    If top_only > 0, restrict to the top N most imbalanced courses.
    """
    course_line_students = build_course_line_students(long)
    courses = imbalance["Course"].tolist()
    if top_only > 0:
        courses = courses[:top_only]
    all_moves: List[Dict] = []
    for course in courses:
        all_moves.extend(compute_balancing_moves_for_course(course, wide_counts, course_line_students))
    moves_df = pd.DataFrame(all_moves, columns=["Course", "FromLine", "ToLine", "StudentCode"])
    return moves_df


def apply_moves_and_impact(long: pd.DataFrame, moves: pd.DataFrame) -> pd.DataFrame:
    """Apply moves to the long dataframe and compute before/after/changes per course/line."""
    before_counts = long.groupby(["Course", "Line"]).size().reset_index(name="Before")

    if moves.empty:
        after_counts = before_counts.rename(columns={"Before": "After"}).copy()
    else:
        long_after = long.copy()
        # For each move, change the Line value for that student's row for that course
        for _, r in moves.iterrows():
            mask = (
                (long_after["Code"] == r["StudentCode"]) &
                (long_after["Course"] == r["Course"]) &
                (long_after["Line"] == r["FromLine"])
            )
            long_after.loc[mask, "Line"] = r["ToLine"]
        after_counts = long_after.groupby(["Course", "Line"]).size().reset_index(name="After")

    impact = pd.merge(before_counts, after_counts, on=["Course", "Line"], how="outer").fillna(0)
    impact["Before"] = impact["Before"].astype(int)
    impact["After"] = impact["After"].astype(int)
    impact["Change"] = impact["After"] - impact["Before"]
    impact = impact.sort_values(["Course", "Line"]).reset_index(drop=True)
    return impact


def write_outputs(
    outdir: str,
    wide_counts: pd.DataFrame,
    imbalance: pd.DataFrame,
    moves: pd.DataFrame,
    impact: pd.DataFrame,
    write_excel: bool = True
) -> Dict[str, str]:
    os.makedirs(outdir, exist_ok=True)
    paths = {}
    # CSVs
    wide_path = os.path.join(outdir, "counts_by_course_line.csv")
    imb_path = os.path.join(outdir, "imbalanced_courses.csv")
    moves_path = os.path.join(outdir, "move_suggestions.csv")
    impact_path = os.path.join(outdir, "before_after_impact.csv")

    wide_counts.to_csv(wide_path)
    imbalance.to_csv(imb_path, index=False)
    moves.to_csv(moves_path, index=False)
    impact.to_csv(impact_path, index=False)

    paths["counts_by_course_line"] = wide_path
    paths["imbalanced_courses"] = imb_path
    paths["move_suggestions"] = moves_path
    paths["before_after_impact"] = impact_path

    # Optional Excel workbook
    if write_excel:
        xlsx_path = os.path.join(outdir, "line_balance_reports.xlsx")
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as xlw:
            wide_counts.to_excel(xlw, sheet_name="CountsByCourseLine")
            imbalance.to_excel(xlw, sheet_name="ImbalancedCourses", index=False)
            moves.to_excel(xlw, sheet_name="MoveSuggestions", index=False)
            impact.to_excel(xlw, sheet_name="BeforeAfterImpact", index=False)
        paths["excel_workbook"] = xlsx_path

    return paths


def main():
    parser = argparse.ArgumentParser(description="Generate line balance reports from student allocations CSV.")
    parser.add_argument("--input", "-i", required=True, help="Path to the StudentAllocations CSV.")
    parser.add_argument("--outdir", "-o", default="./out", help="Output directory (default: ./out).")
    parser.add_argument("--min-lines", type=int, default=2, help="Minimum lines a course must appear in to be considered imbalanced (default: 2).")
    parser.add_argument("--ignore-zeros", action="store_true", default=True, help="Ignore zeros when computing imbalance (default: True).")
    parser.add_argument("--top-only", type=int, default=0, help="If > 0, only generate moves for top N imbalanced courses.")
    parser.add_argument("--no-excel", action="store_true", help="Disable Excel workbook output.")
    args = parser.parse_args()

    long, alloc_cols = load_allocations(args.input)
    counts = count_by_course_line(long)
    wide_counts, imbalance = compute_imbalance(counts, ignore_zeros=args.ignore_zeros, min_lines=args.min_lines)
    moves = compute_all_moves(long, wide_counts, imbalance, top_only=args.top_only)
    impact = apply_moves_and_impact(long, moves)

    paths = write_outputs(
        args.outdir,
        wide_counts,
        imbalance,
        moves,
        impact,
        write_excel=(not args.no_excel)
    )

    print("Reports generated:")
    for k, v in paths.items():
        print(f"- {k}: {v}")


if __name__ == "__main__":
    main()
