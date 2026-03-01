# ⚽ Football Data Warehouse

A end-to-end Data Warehouse project built on football data from the top 5 European leagues, sourced from [Understat](https://understat.com/) via Kaggle.

📁 **Dataset:** [Football Database — Kaggle](https://www.kaggle.com/datasets/technika148/football-database)

---

## 🏗️ Architecture

The project follows a **3-layer Medallion Architecture**:

```
Sources (CSV) → Bronze Layer → Silver Layer → Gold Layer
```

| Layer | Type | Purpose |
|---|---|---|
| **Bronze** | Tables | Raw data, loaded as-is |
| **Silver** | Tables | Cleaned & standardized data |
| **Gold** | Views | Business-ready, aggregated data |

---

## 🛠️ Tools & Technologies

- **Database:** Microsoft SQL Server
- **Language:** T-SQL
- **Data Modeling:** draw.io
- **Source Data:** Kaggle / Understat

---

## 📂 Project Structure

```
├── bronze/          # Raw data loading stored procedures
├── silver/          # Data cleaning stored procedures
├── gold/            # Business views (Star Schema)
├── data_catalog/    # Data catalog documentation
└── diagrams/        # Data model & architecture diagrams
```

---

## 📊 Data Model (Star Schema)

The Gold layer follows a **Galaxy Schema** with 4 fact tables sharing common dimensions.

**Dimensions:**
- `dim_players` — Player info + main position
- `dim_teams` — Team info
- `dim_leagues` — League info + Understat notation
- `dim_seasons` — Season reference with formatted names

**Facts:**
- `fact_games` — Match results and goals
- `fact_playerstats` — Aggregated player stats per season per league
- `fact_teamstats` — Aggregated team performance per season per league
- `fact_leaguetable` — Full league standings with points

---

## 📋 Data Coverage

- **Leagues:** EPL, La Liga, Bundesliga, Serie A, Ligue 1
- **Seasons:** 2014/2015 → 2019/2020
- **Tables:** Appearances, Games, Shots, Players, Teams, Leagues, Team Stats

---

## 🔍 Key Business Questions Answered

- Who are the top scorers and assisters per league and season?
- Which teams have the most wins, goals, and clean sheets?
- What is the full league standings table per season?
- How did team performance change across seasons?

---

## 📖 Data Catalog

See [DATA_CATALOG.md](./DATA_CATALOG.md) for full column descriptions and business definitions.

---

## 🚀 How to Run

1. Run `bronze/load_bronze.sql` to load raw CSV data
2. Run `silver/load_silver.sql` to clean and standardize data
3. Run `gold/create_gold_views.sql` to create business views
