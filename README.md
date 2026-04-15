# Energy & Sustainability Dashboard
### Can Economic Growth and Carbon Reduction Happen Together?

> **An end-to-end BI project** — KPI design → real data collection → pipeline engineering → interactive dashboard.

🔴 **Live Demo** → [gunehee.github.io/BI_energy-sustainability](https://gunehee.github.io/BI_energy-sustainability/)  
🔴 **Dashboard** → (index.html)
👤 **Author** → [GunHee Lee](https://github.com/Gunehee)

## Project Summary

| | |
|---|---|
| **Core Question** | Can a country simultaneously achieve economic growth and reduce carbon intensity? |
| **KPIs** | CO₂ per capita (t) · Renewable energy share (%) · GDP per capita PPP (USD) |
| **Scope** | 40 countries · 2000–2022 |
| **Data Sources** | Our World in Data (OWID GitHub) · World Bank REST API |
| **Stack** | Python (pandas, requests) · HTML / CSS / JavaScript · Chart.js · GitHub Pages |
| **Completed** | December 2025 |

---

## Data Collection

### Source 1 — Our World in Data (CO₂ + Renewables)

| Item | Detail |
|------|--------|
| URL | `https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv` |
| Access | Direct CSV via `requests.get()` — no API key required |
| Columns used | `co2_per_capita` · `renewables_share_energy` |
| License | CC BY 4.0 |

### Source 2 — World Bank REST API (GDP)

| Item | Detail |
|------|--------|
| Endpoint | `https://api.worldbank.org/v2/country/{iso3}/indicator/NY.GDP.PCAP.PP.KD` |
| Access | JSON REST API — no auth required |
| Metric | GDP per capita, PPP (constant 2017 international USD) |
| License | CC BY 4.0 |

### Pipeline Flow

```
Our World in Data GitHub (CSV)
  └─► requests.get(OWID_URL)
        └─► pandas read_csv(StringIO(raw))
              └─► filter columns: co2_per_capita, renewables_share_energy
                    └─► fill_missing_years()  ──────────────────────────┐
                                                                         │
World Bank REST API (JSON, per country)                                  ├──► merge (inner join)
  └─► requests.get(WB_API_URL)                                          │         ↓
        └─► parse data[1] records list                                   │    validate_dataset()
              └─► extract gdp_per_capita                                 │         ↓
                    └─► fill_missing_years()  ──────────────────────────┘    classify_decoupling()
                                                                                    ↓
                                                                          cleaned_data.json
                                                                                    ↓
                                                                           index.html (dashboard)
```

### Reproduce from scratch

```bash
pip install pandas requests
python data_pipeline.py       # downloads real data, writes cleaned_data.json
open index.html               # dashboard auto-loads cleaned_data.json
```

Downloads are cached in `.cache/` — subsequent runs take under 1 second.

---

## Methodology

### Step 1 — KPIs defined before data collection

KPIs were fixed and documented *before* any data was collected, preventing post-hoc selection bias.

| KPI | Why this metric | Documented limitation |
|-----|----------------|----------------------|
| CO₂ per capita (t) | Controls for population; isolates structural change | Production-based only — excludes offshored emissions |
| Renewable share (%) | Measures energy transition, not just outcome | Includes large hydro (contested definition) |
| GDP per capita PPP | Removes FX + inflation distortion | Does not capture inequality |

### Step 2 — Decoupling classification (2010 → 2022)

**Why 2010:** 2008 financial crisis caused temporary emission drops unrelated to structural change. 2010 captures real energy transition effects.

| Status | Definition | n |
|--------|-----------|---|
| Strong Decoupling | GDP >+10% AND CO₂/cap >−10% | 16 |
| Weak Decoupling | GDP >+10% AND CO₂/cap within ±10% | 9 |
| No Decoupling | GDP >+10% AND CO₂/cap >+10% | 9 |
| Low Growth | GDP <+10% | 4 |
| Negative | GDP declined >5% | 2 |

Threshold sensitivity analysis (±5%, ±15%) showed consistent results — strong decouplers remain stable across all variations.

---

## Key Findings

1. **16 / 40 countries achieved strong decoupling** — concentrated in EU with sustained renewable investment
2. **Denmark: GDP +25%, CO₂/cap −50.5%** — wind energy 22% → 50% over 12 years
3. **UK: CO₂/cap −40.2%** — active coal phase-out + offshore wind expansion
4. **China: "No Decoupling" but CO₂/GDP intensity fell ~40%** — relative vs absolute decoupling distinction matters
5. **Renewables >30% is the strongest structural predictor** of decoupling across the dataset

---

## Documented Limitations

| Limitation | Next step |
|-----------|-----------|
| Production-based CO₂ only (offshoring invisible) | Add consumption-based CO₂ (OWID has this) |
| No sector breakdown | Add IEA sector data |
| Correlation ≠ causation | Granger causality test or panel fixed-effects regression |
| No uncertainty bands | Add confidence intervals per country |
| Interpolated mid-years not visually distinguished | Mark interpolated vs actual data points |

---

## File Structure

```
BI_energy-sustainability/
├── README.md                ← this file
├── index.html               ← interactive dashboard (GitHub Pages entry point)
├── data_pipeline.py         ← data collection & processing (Python)
├── cleaned_data.json        ← processed dataset (generated by pipeline)
└── .cache/                  ← local download cache (add to .gitignore)
```

---

*Data: Our World in Data (CC BY 4.0) · World Bank Open Data (CC BY 4.0)*  
*GunHee Lee · github.com/Gunehee · Dec 2025*
