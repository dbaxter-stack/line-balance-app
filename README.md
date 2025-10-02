# Line Balance Reports (Courses × Lines)

This project analyzes student allocations to identify **imbalanced courses**, proposes **student-by-student move suggestions** (between existing lines for the *same* course), and shows the **before/after impact** per course and line.

## 📦 What's here

- `app.py` — Streamlit app for interactive upload and reporting.
- `line_balance_report.py` — CLI script to generate CSV/Excel reports offline.
- `requirements.txt` — Python dependencies.
- `.github/workflows/python-ci.yml` — simple CI to lint and run a smoke test.
- `.gitignore` — standard Python ignores.

## 🧪 Expected CSV Format

- Must include a unique student identifier column named **`Code`**.
- Timetable lines are columns starting with **`AL`** (e.g., `AL1`..`AL6`).
- Class codes are strings like `12ENG1`; the **first five characters** form the course (e.g., `12ENG`).

## ▶️ Run locally

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Streamlit app
streamlit run app.py

# CLI reports
python line_balance_report.py --input "StudentAllocations-Lines-Export (15).csv" --outdir ./out
```

## ☁️ Deploy on Streamlit Community Cloud

1. Push this folder to a **public GitHub repo**.
2. Go to **share.streamlit.io** and connect your repo.
3. Set the app entrypoint to `app.py` and the Python version to 3.10+.
4. Streamlit will auto-install `requirements.txt` and run the app.

## 🛠️ CLI Options

- `--min-lines 2` — require courses to appear in ≥2 lines.
- `--ignore-zeros` — ignore lines where course is absent when measuring imbalance (default True).
- `--top-only N` — only generate moves for top N most imbalanced courses.
- `--no-excel` — skip Excel workbook output.

## 🔒 Notes & Assumptions

- Students are moved only **between lines that already offer the same course**.
- Move selection within a surplus line is by student code order (deterministic, simple).

---

Made for Compass Education scheduling analysis.
