# Energy & Sustainability Dashboard
### Can Economic Growth and Carbon Reduction Happen Together?

> **An end-to-end BI project** — from KPI design and data engineering to interactive dashboard deployment.  
> Live demo → [GitHub Pages](https://gunehee.github.io/BI_energy-sustainability/)

---

## Project Summary

| Item | Detail |
|------|--------|
| **Core Question** | Can a country simultaneously achieve economic growth and reduce carbon intensity? |
| **KPIs Defined** | CO₂ per capita · Renewable energy share (%) · GDP per capita (PPP) |
| **Data Sources** | Our World in Data · World Bank Open Data |
| **Countries** | 40 countries (G20 + EU leaders + emerging economies) |
| **Time Range** | 2000–2022 |
| **Stack** | Python (pandas, requests) · HTML/CSS/JavaScript · Chart.js · GitHub Pages |
| **Completed** | December 2025 |

---

## Why This Project

Most sustainability dashboards show raw emissions data. The harder question is:  
**"Is decoupling — growing GDP while cutting carbon — actually happening?"**

This dashboard is designed to answer that question in a single screen, using the minimum number of well-defined KPIs instead of overwhelming the viewer with charts.

---

## Methodology

### Step 1 — Define KPIs Before Touching Data

Before any data collection, I documented three KPIs and their rationale:

| KPI | Why this metric | Limitation documented |
|-----|----------------|----------------------|
| CO₂ per capita (tonnes) | Normalizes by population; comparable across countries | Excludes imported emissions (consumption-based) |
| Renewable energy share (%) | Measures energy transition progress, not just outcomes | Includes large hydro, which is debated as "renewable" |
| GDP per capita PPP (USD) | Purchasing power adjusted; comparable across economies | Does not capture inequality or wellbeing |

> **Decision log**: I chose *per capita* metrics over absolute values throughout. A country can "reduce emissions" by offshoring industry — per capita catches this; absolute does not.

### Step 2 — Data Pipeline

```
Our World in Data (CSV)    World Bank API
        ↓                       ↓
   data_pipeline.py  ←→  validation layer
        ↓
  cleaned_data.json   (single source of truth for dashboard)
        ↓
  index.html (Chart.js renders from JSON)
```

Key engineering decisions:
- **Single cleaned JSON** as the dashboard's data source → reproducible, auditable
- **Null handling**: countries with >30% missing years excluded from trend analysis
- **Normalization**: all monetary values in 2015 USD PPP for cross-year comparability

### Step 3 — Decoupling Classification

I defined "decoupling" strictly:

```
Strong Decoupling:  GDP growth > 10% AND CO₂/capita decline > 10% (over 2010–2022)
Weak Decoupling:    GDP growth > 10% AND CO₂/capita stable (±5%)
No Decoupling:      GDP growth with CO₂/capita increase
Negative:           Both declining (recession-driven)
```

This classification is documented so anyone can audit the threshold choices.

---

## Key Findings

1. **Decoupling is real, but concentrated**: 14 of 40 countries achieved strong decoupling between 2010–2022 — mostly EU nations (UK, Germany, Denmark, France).

2. **The leader pattern**: Countries with >40% renewable share tend to have significantly lower CO₂/GDP intensity, but the relationship is non-linear.

3. **Emerging economies diverge**: India and Indonesia show rising absolute emissions but *falling* CO₂ per unit of GDP — efficiency gains without absolute reduction.

4. **China's story is complex**: Largest absolute emitter, but CO₂/GDP has fallen ~40% since 2000 while GDP grew ~900%.

5. **The "easy wins" are done**: Most developed-economy decoupling came from fuel switching (coal → gas → renewables). The harder industrial decarbonization has barely started.

---

## Dashboard Structure

```
📊 Dashboard Layout
├── Header KPI cards (latest year snapshot)
│   ├── Global avg CO₂/capita
│   ├── Global renewable share %
│   └── Countries achieving decoupling
├── Tab 1: Trend View
│   └── Multi-country CO₂/capita & renewable share over time
├── Tab 2: Scatter — GDP vs CO₂
│   └── Bubble chart: size = population, color = decoupling status
├── Tab 3: Decoupling Map
│   └── Country classification table with sparklines
└── Methodology & Data Sources
```

---

## Design Decisions (Documented)

| Decision | Rationale |
|----------|-----------|
| Max 3 KPIs on summary screen | More KPIs = confusion; force the viewer to one question at a time |
| 2010 as baseline for decoupling calc | Pre-2010 includes 2008 crisis distortion; 2010 = cleaner baseline |
| Exclude countries with pop < 1M | Small-country effects (Luxembourg, Iceland) distort per capita comparisons |
| Color: red = high CO₂ intensity, green = low | Intuitive mapping; consistent throughout |

---

## Reproducibility

All assumptions, threshold choices, and data transformations are logged in:
- `docs/methodology.md` — written decisions
- `data_pipeline.py` — code comments explain each transformation
- `cleaned_data.json` — final dataset; verifiable against source

**To reproduce from scratch:**
```bash
pip install pandas requests
python data_pipeline.py        # generates cleaned_data.json
# then open index.html in browser
```

---

## What I Would Do Next

1. **Add consumption-based emissions** (trade-adjusted CO₂) — currently using production-based only
2. **Sector breakdown**: electricity vs transport vs industry decoupling rates differ significantly  
3. **Statistical test**: formal Granger causality between renewable share and CO₂ reduction, not just correlation
4. **Uncertainty bands**: data quality varies by country; should be visualized

---

## File Structure

```
BI_energy-sustainability/
├── README.md                  ← this file
├── data_pipeline.py           ← data collection & cleaning
├── index.html                 ← interactive dashboard
├── cleaned_data.json          ← processed dataset (auto-generated)
└── docs/
    └── methodology.md         ← decision log & assumptions
```

---

*Data from Our World in Data (CC BY 4.0) and World Bank Open Data (CC BY 4.0).*  
*Analysis and dashboard by [GunHee Lee](https://github.com/Gunehee)*
