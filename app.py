
import streamlit as st
import pandas as pd
import numpy as np
from collections import deque

st.set_page_config(page_title="Line Balance Reports", layout="wide")

st.title("📊 Line Balance Reports (Courses × Lines)")
st.markdown("""
Upload your **Student Allocations CSV** (with columns like `Code`, `AL1`..`AL6`).
This app will:
1. Highlight **imbalanced courses** (ignoring zero lines; requires course in ≥2 lines),
2. Generate **student-by-student move suggestions** (same course, between existing lines),
3. Show the **Before / After / Change** impact per course/line.
""")

uploaded = st.file_uploader("Upload CSV", type=["csv"])

def melt_long(df):
    alloc_cols = [c for c in df.columns if str(c).startswith("AL")]
    if "Code" not in df.columns:
        st.error("CSV must include a 'Code' column for unique student identifiers.")
        st.stop()
    long = df.melt(
        id_vars=[c for c in df.columns if c not in alloc_cols],
        value_vars=alloc_cols,
        var_name="Line",
        value_name="Class"
    )
    long = long.dropna(subset=["Class"]).copy()
    long["Course"] = long["Class"].astype(str).str[:5]
    return long, alloc_cols

@st.cache_data
def compute_counts(long):
    return long.groupby(["Line","Course"]).size().reset_index(name="StudentCount")

def compute_imbalance(counts, ignore_zeros=True, min_lines=2):
    wide = counts.pivot(index="Course", columns="Line", values="StudentCount")
    rows = []
    for course, row in wide.iterrows():
        vals = row.dropna()
        nz = vals[vals > 0] if ignore_zeros else vals.fillna(0)
        appears_in = int((vals > 0).sum())
        if ignore_zeros and appears_in < min_lines:
            continue
        if len(nz) == 0:
            continue
        rng = float(nz.max() - nz.min()) if len(nz) >= 2 else 0.0
        rows.append({
            "Course": course,
            "Range": rng,
            "Max": float(nz.max()),
            "Min": float(nz.min()),
            "AppearsIn": appears_in,
            "OfferingLines": ",".join([c for c, v in vals.items() if v > 0])
        })
    imb = pd.DataFrame(rows).sort_values(["Range","Course"], ascending=[False, True]).reset_index(drop=True)
    return wide, imb

def build_cls(long):
    return long.groupby(["Course","Line"])["Code"].apply(list).to_dict()

def compute_moves_for_course(course, wide_counts, cls):
    if course not in wide_counts.index:
        return []
    line_counts = wide_counts.loc[course].dropna()
    offering = [ln for ln, ct in line_counts.items() if ct > 0]
    if len(offering) < 2:
        return []
    curr = {ln: int(line_counts[ln]) for ln in offering}
    total = sum(curr.values())
    n = len(offering)
    base = total // n
    remainder = total % n
    lines_sorted_asc = sorted(offering, key=lambda ln: curr[ln])
    target = {ln: base for ln in offering}
    for ln in lines_sorted_asc[:remainder]:
        target[ln] = base + 1
    surplus = {ln: curr[ln] - target[ln] for ln in offering if curr[ln] > target[ln]}
    deficit = {ln: target[ln] - curr[ln] for ln in offering if curr[ln] < target[ln]}
    if sum(surplus.values()) == 0:
        return []
    s_queues = {ln: deque(cls.get((course, ln), [])[:surplus[ln]]) for ln in surplus}
    moves = []
    for to_ln, need in deficit.items():
        remaining = need
        for from_ln in list(surplus.keys()):
            if surplus[from_ln] <= 0 or remaining <= 0:
                continue
            take = min(surplus[from_ln], remaining)
            for _ in range(take):
                if s_queues[from_ln]:
                    student = s_queues[from_ln].popleft()
                    moves.append({"Course": course, "FromLine": from_ln, "ToLine": to_ln, "StudentCode": student})
                    surplus[from_ln] -= 1
                    remaining -= 1
            if remaining == 0:
                break
    return moves

def compute_all_moves(long, wide_counts, imbalance, top_only=0):
    cls = build_cls(long)
    courses = imbalance["Course"].tolist()
    if top_only and top_only > 0:
        courses = courses[:top_only]
    all_moves = []
    for c in courses:
        all_moves.extend(compute_moves_for_course(c, wide_counts, cls))
    return pd.DataFrame(all_moves, columns=["Course","FromLine","ToLine","StudentCode"])

def apply_moves_and_impact(long, moves):
    before = long.groupby(["Course","Line"]).size().reset_index(name="Before")
    if moves is None or moves.empty:
        after = before.rename(columns={"Before":"After"}).copy()
    else:
        la = long.copy()
        for _, r in moves.iterrows():
            mask = (la["Code"]==r["StudentCode"]) & (la["Course"]==r["Course"]) & (la["Line"]==r["FromLine"])
            la.loc[mask,"Line"] = r["ToLine"]
        after = la.groupby(["Course","Line"]).size().reset_index(name="After")
    impact = pd.merge(before, after, on=["Course","Line"], how="outer").fillna(0)
    impact["Before"] = impact["Before"].astype(int)
    impact["After"] = impact["After"].astype(int)
    impact["Change"] = impact["After"] - impact["Before"]
    return impact.sort_values(["Course","Line"]).reset_index(drop=True)

if uploaded is not None:
    df = pd.read_csv(uploaded)
    st.success("CSV loaded.")
    long, alloc_cols = melt_long(df)
    counts = compute_counts(long)
    wide, imbalance = compute_imbalance(counts, ignore_zeros=True, min_lines=2)

    st.subheader("1) Imbalanced Courses")
    st.dataframe(imbalance)

    st.subheader("2) Suggested Student Moves (same course, between existing lines)")
    top_only = st.number_input("Limit to top N most imbalanced courses (0 = all)", min_value=0, value=0, step=1)
    moves = compute_all_moves(long, wide, imbalance, top_only=int(top_only))
    st.dataframe(moves)

    st.subheader("3) Before / After Impact")
    impact = apply_moves_and_impact(long, moves)
    st.dataframe(impact)

    # Downloads
    def df_to_csv_bytes(dframe):
        return dframe.to_csv(index=False).encode("utf-8")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.download_button("Download Imbalanced Courses (CSV)", data=df_to_csv_bytes(imbalance), file_name="imbalanced_courses.csv", mime="text/csv")
    with col2:
        st.download_button("Download Move Suggestions (CSV)", data=df_to_csv_bytes(moves), file_name="move_suggestions.csv", mime="text/csv")
    with col3:
        st.download_button("Download Before-After Impact (CSV)", data=df_to_csv_bytes(impact), file_name="before_after_impact.csv", mime="text/csv")

    st.info("Tip: Use the 'top N' control to focus moves on the biggest imbalances.")
else:
    st.info("Upload a CSV to get started.")
