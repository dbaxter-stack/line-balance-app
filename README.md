# Line Balance Reports (Courses × Lines) — v5.2

Features:
- **Section-aware moves**: preserves multiple classes per line (assigns to least-filled section)
- **Multi-step planner with safeguards**: one class per student per line, per-student cap
- **Word report**: Quick Summary, Courses Still Unbalanced (RangeAfter>3), Per-course Range Summary (only positive ranges; zeros ignored)
- **Streamlit** download for the Word report

## Run locally
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
streamlit run app.py
```

## CLI
```bash
python line_balance_report.py   --input "StudentAllocations-Lines-Export (15).csv"   --outdir ./out   --multi-move   --max-moves-per-student 3
```
