# Fraud Graph Engine — Phase 3 Setup

## Files
```
fraud_engine.py   ← Modular scoring engine (run independently or imported)
app.py            ← Streamlit dashboard (Phase 3)
```

## Install dependencies
```bash
pip install streamlit pydeck pandas networkx
```

## Run the dashboard
```bash
streamlit run app.py
```
Opens at: http://localhost:8501

---

## What the map shows

| Layer | Description |
|---|---|
| 🔵 Large circle | Fraud ring cluster boundary (semi-transparent) |
| 🔴🟠🟡🟢 Dot | Individual account pin, sized by risk score |
| ↗ Arc | Transaction flow arrow between accounts |
| Label | Account holder name |

## Sidebar controls
- Filter by risk tier (BLOCK / SUSPICIOUS / WATCH / CLEAN)
- Toggle map layers on/off
- Filter by city
- Click any account in the dropdown to see full score breakdown

## Score formula
```
own_score   = Σ (flag_weight × flag_fired)
final_score = own_score + (0.30 × avg_score_of_neighbours)

Thresholds: CLEAN 0-30 | WATCH 31-60 | SUSPICIOUS 61-90 | BLOCK 91+
```
