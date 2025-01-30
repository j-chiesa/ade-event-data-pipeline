# Amsterdam Dance Event (ADE) Historical Data Pipeline

&#x20;&#x20;

A robust data engineering project to collect, process, and analyze historical event data from the Amsterdam Dance Event (ADE) - Europe's premier electronic music festival.

## 📖 Project Overview

This project constructs a high-quality historical dataset from ADE events through two automated ETL pipelines:

1. **Web Scraping Pipeline**: Collects raw event data from [DJGuide.nl](https://www.djguide.nl)
2. **Data Transformation Pipeline**: Cleans, enriches, and validates the data for analytical use

**Current Coverage**: 2005-2009\
**Target Coverage**: 2005-2024 (Continually expanding)\
**Status**: MVP Released - Actively Developed

## ✨ Key Features

- 🕸️ Selenium-based web scraper with anti-blocking mechanisms
- 🧹 Advanced data cleansing with PySpark transformations
- ✅ AWS Glue Data Quality Rulesets for validation
- 📊 Power BI analytics dashboard for quality monitoring
- ☁️ AWS S3 cloud storage integration
- 📆 Temporal analysis capabilities through event timelines

---

## ⚙️ ETL Pipeline Architecture

### Pipeline 1: Data Acquisition

#### Core components:

- Headless Firefox browser with Selenium
- BeautifulSoup HTML parsing
- Cookie consent handling
- AWS S3 integration (s3://ade-data-bucket)
- Error-resistant scraping logic\
  **Output**: Raw CSV files stored in `s3://ade-data-bucket/ade_event_data_raw/`

### Pipeline 2: Data Transformation

#### Key transformations:

- Temporal feature engineering (event duration calculation)
- Geospatial validation (Amsterdam coordinates filter)
- Price normalization (Presale/Door price extraction)
- Array-based processing for genres/lineups
- Venue standardization through window functions
- Type casting and null handling\
  **Output**: Cleaned Parquet files in `s3://ade-data-bucket/ade_event_data_clean/`

## 🔍 Data Quality Assurance

### Ruleset 1: Accuracy Checks

```python
Rules = [
    ColumnValues "Latitude" between 52.28-52.50,
    ColumnValues "Longitude" between 4.63-5.08,
    PricePresale >= 0,
    Capacity > 0,
    EventDurationMin >= 0,
    Edition >= 2005
]
```

### Ruleset 2: Completeness Checks

```python
Rules = [
    Completeness "EventName" > 99%,
    Completeness "VenueName" > 95%,
    Completeness "Latitude" > 90%,
    ColumnLength "Lineup" > 0
]
```

**Validation**: Automated AWS Glue Data Quality checks with results visualized in Power BI

## 📈 Analytics & Visualization

### Power BI Dashboard Features:

- **Data Quality Scorecards**
- **Temporal Coverage Analysis**
- **Venue Capacity Distribution**
- **Price Evolution Trends**
- **Genre Popularity Heatmaps**
- **Data Completeness Reports**

---

## 🚧 Project Roadmap

### Current Phase

- MVP with 2005-2009 data validated
- Basic quality rules implemented
- Initial dashboard deployment
