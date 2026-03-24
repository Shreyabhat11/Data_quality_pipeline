# 📊 Power BI Dashboard — Step-by-Step Build Guide
## Data Quality & Pipeline Monitoring Dashboard

---

## 🔌 Step 0: Connect Your Data Sources

1. Open Power BI Desktop → **Get Data → Text/CSV**
2. Load these 4 files from the `outputs/` folder:
   - `data_quality_report.csv`
   - `schema_issues.csv`
   - `anomaly_flags.csv`
   - `health_scores.csv`
3. In **Power Query Editor**:
   - Ensure `null_percentage`, `mean`, `std`, `health_score` are **Decimal Number**
   - Ensure `dataset_date` is **Text** (Day 1, Day 2, Day 3)
   - Close & Apply

---

## 📐 DAX Measures (Create in Model View → New Measure)

```dax
-- Total Datasets
Total Datasets = DISTINCTCOUNT(data_quality_report[dataset_name])

-- Average Null %
Avg Null % = AVERAGE(data_quality_report[null_percentage])

-- Overall Health Score
Avg Health Score = AVERAGE(health_scores[health_score])

-- Total Schema Issues
Total Schema Issues = COUNTROWS(schema_issues)

-- Total Anomalies
Total Anomalies = COUNTROWS(anomaly_flags)

-- High Severity Anomalies
High Severity Count = 
    CALCULATE(
        COUNTROWS(anomaly_flags),
        anomaly_flags[severity] = "HIGH"
    )

-- Health Score Color (used for conditional formatting)
Health Score Color = 
    SWITCH(
        TRUE(),
        SELECTEDVALUE(health_scores[grade]) = "A", "#27AE60",
        SELECTEDVALUE(health_scores[grade]) = "B", "#2ECC71",
        SELECTEDVALUE(health_scores[grade]) = "C", "#F39C12",
        SELECTEDVALUE(health_scores[grade]) = "D", "#E67E22",
        "#E74C3C"   -- F grade
    )

-- Null Delta (change from previous day) - calculated column
-- Add in Power Query or as calculated column:
Null Pct Delta = 
    VAR CurrentNull = data_quality_report[null_percentage]
    VAR CurrentCol  = data_quality_report[column_name]
    VAR CurrentDay  = data_quality_report[dataset_date]
    RETURN CurrentNull   -- extend with EARLIER() for full delta logic

-- Data Freshness Status
Freshness Status = 
    IF([Total Datasets] >= 3, "✅ Up to Date", "⚠️ Check Pipeline")
```

---

## 📄 PAGE 1: Data Health Overview

**Purpose:** Executive summary — instant pipeline status at a glance

### Layout (1280×720 canvas):

```
┌─────────────────────────────────────────────────────────────┐
│  🏷️ Title: "Data Quality Dashboard — Pipeline Monitor"       │
├──────────┬──────────┬──────────┬──────────┬─────────────────┤
│  KPI 1   │  KPI 2   │  KPI 3   │  KPI 4   │   KPI 5         │
│ Datasets │ Avg Null │ Health   │ Schema   │ Anomaly Flags   │
│   3      │  10.7%   │  68.4    │ Issues:3 │  🚨  3 Flags    │
├──────────┴──────────┴──────────┴──────────┴─────────────────┤
│                                                               │
│   BAR CHART: Health Score by Dataset                          │
│   (orders_day_1=100, orders_day_2=49.2, orders_day_3=56.1)  │
│                                                               │
├─────────────────────────────┬─────────────────────────────────┤
│  DONUT: Severity Breakdown  │  TABLE: Latest Health Scores    │
│  HIGH / MEDIUM / LOW        │  Dataset | Score | Grade        │
└─────────────────────────────┴─────────────────────────────────┘
```

### Visuals:

| Visual | Type | Fields |
|--------|------|--------|
| KPI Cards (×5) | Card | Total Datasets, Avg Null %, Avg Health Score, Total Schema Issues, Total Anomalies |
| Health Score Bar | Clustered Bar Chart | X: dataset_name, Y: health_score, Color: grade |
| Severity Donut | Donut Chart | Legend: severity (from anomaly_flags), Values: count |
| Health Table | Table | dataset_name, health_score, grade, avg_null_pct, duplicate_pct |

**Formatting:**
- Background: `#1A1A2E` (dark navy)
- Card fill: `#16213E`
- Accent: `#0F3460` / `#E94560`
- Font: Segoe UI, white text
- Health score bar: conditional color — green (A/B), amber (C), red (D/F)

---

## 📄 PAGE 2: Null Trends

**Purpose:** Track null percentage changes column-by-column over pipeline runs

### Layout:

```
┌─────────────────────────────────────────────────────────────┐
│ Title: "Null Value Trends — Column Breakdown"                │
│ Slicer: [Column Name ▼]                                     │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│   LINE CHART: Null % over Time                              │
│   X: dataset_date  |  Y: null_percentage  |  Lines: column  │
│                                                              │
├────────────────────┬────────────────────────────────────────┤
│  STACKED BAR:      │  HEATMAP TABLE:                         │
│  Null count by     │  Column × Day → null_percentage         │
│  column per day    │  (conditional format: white→red)        │
└────────────────────┴────────────────────────────────────────┘
```

### Visuals:

| Visual | Type | Fields |
|--------|------|--------|
| Null Trend Line | Line Chart | X: dataset_date, Y: null_percentage, Legend: column_name |
| Null Count Bar | Stacked Bar | X: dataset_name, Y: null_count, Legend: column_name |
| Null Heatmap | Matrix | Rows: column_name, Cols: dataset_date, Values: null_percentage |
| Column Slicer | Slicer | column_name |

**Conditional Formatting (Heatmap):**
- Rules: 0% = White → 50%+ = Red (#E74C3C)
- Format: Background color, font auto-contrast

---

## 📄 PAGE 3: Schema Drift

**Purpose:** Show which columns were added, removed, or changed in type

### Layout:

```
┌─────────────────────────────────────────────────────────────┐
│ Title: "Schema Drift — Column Change Log"                    │
├──────────────────┬──────────────────────────────────────────┤
│  KPI: Total      │  KPI: HIGH Issues │  KPI: MEDIUM Issues  │
│  Schema Issues   │        2          │         1            │
├──────────────────┴──────────────────────────────────────────┤
│                                                              │
│   TABLE: Full Schema Issues Log                             │
│   Cols: Dataset | Column | Issue Type | Severity | Detail   │
│   (Conditional format: HIGH=red bg, MEDIUM=amber)           │
├─────────────────────────────────────────────────────────────┤
│   BAR: Issue Count by Type (NEW_COLUMN / MISSING / DTYPE)   │
└─────────────────────────────────────────────────────────────┘
```

### Visuals:

| Visual | Type | Fields |
|--------|------|--------|
| Schema Table | Table | dataset_name, column_name, issue_type, severity, detail |
| Issue Type Bar | Clustered Bar | X: issue_type, Y: Count, Color: severity |
| KPI Cards | Card | Count filtered by severity |

**Conditional Format on Table (severity column):**
- HIGH → background `#E74C3C`, font white
- MEDIUM → background `#F39C12`, font white
- LOW → background `#27AE60`, font white

---

## 📄 PAGE 4: Distribution Monitoring

**Purpose:** Detect statistical drift in numeric columns (mean/std changes)

### Layout:

```
┌─────────────────────────────────────────────────────────────┐
│ Title: "Distribution Monitoring — Numeric Column Stats"      │
│ Slicer: [Column Name ▼]                                     │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│   LINE CHART: Mean over Time                                │
│   X: dataset_date  |  Y: mean  |  Error bars from std       │
│                                                              │
├────────────────────────────────┬────────────────────────────┤
│  LINE CHART: Std Dev over Time │  TABLE: Stats Summary       │
│  X: dataset_date  Y: std       │  day | mean | std | min/max │
└────────────────────────────────┴────────────────────────────┘
```

### Visuals:

| Visual | Type | Fields |
|--------|------|--------|
| Mean Trend | Line Chart | X: dataset_date, Y: mean, Legend: column_name |
| Std Dev Trend | Line Chart | X: dataset_date, Y: std |
| Stats Table | Table | dataset_date, column_name, mean, median, std, min, max |
| Column Slicer | Slicer | column_name (numeric only) |

**Note:** Add reference line at Day 1 mean to visualize drift threshold

---

## 📄 PAGE 5: Alerts Panel

**Purpose:** Consolidated view of all anomaly flags with severity and context

### Layout:

```
┌─────────────────────────────────────────────────────────────┐
│ Title: "🚨 Anomaly Alerts — Pipeline Issues"                 │
├─────────┬───────────┬───────────────────────────────────────┤
│ HIGH: 1 │ MEDIUM: 2 │  Slicer: [All | HIGH | MEDIUM]        │
├─────────┴───────────┴───────────────────────────────────────┤
│                                                              │
│   TABLE: Anomaly Flags                                      │
│   Dataset | Column | Type | Prev | Curr | Delta | Severity  │
│   (Row highlight: HIGH=red, MEDIUM=amber)                   │
│                                                              │
├─────────────────────────────────────────────────────────────┤
│   BAR: Anomaly Count by Type    │  GAUGE: Avg Health Score  │
│   (NULL_SPIKE, DIST_DRIFT, etc.)│  Min:0 Max:100 Target:80  │
└─────────────────────────────────────────────────────────────┘
```

### Visuals:

| Visual | Type | Fields |
|--------|------|--------|
| Alerts Table | Table | dataset_name, column_name, anomaly_type, prev_value, curr_value, delta, severity, message |
| Anomaly Type Bar | Bar Chart | X: anomaly_type, Y: Count, Color: severity |
| Health Gauge | Gauge | Value: Avg Health Score, Min: 0, Max: 100, Target: 80 |
| Severity Slicer | Slicer | severity |
| KPI Cards | Card | High Severity Count, Total Anomalies |

---

## 🎨 Global Formatting & Theme

### Color Palette:
```
Primary Dark:    #1A1A2E
Card BG:         #16213E
Accent Blue:     #0F3460
Alert Red:       #E74C3C
Amber:           #F39C12
Success Green:   #27AE60
Text:            #FFFFFF / #ECF0F1
Grid lines:      #2C3E50
```

### Apply Theme:
1. View → Themes → Customize current theme
2. Paste the above colors in the respective slots
3. Set default font: Segoe UI, 11pt, white

### Navigation:
- Add a **Navigator** panel on the left side using bookmark + button shapes
- 5 buttons: Overview | Null Trends | Schema | Distribution | Alerts
- Use bookmarks + "Action → Bookmark" for page navigation

---

## ✅ Publishing Checklist

- [ ] All 5 pages created with correct visuals
- [ ] DAX measures created and tested
- [ ] Slicers cross-filter all visuals on same page
- [ ] Conditional formatting applied on severity columns
- [ ] Navigation buttons work via bookmarks
- [ ] Report saved as `.pbix`
- [ ] Published to Power BI Service (optional)
- [ ] Screenshot taken for portfolio/README
