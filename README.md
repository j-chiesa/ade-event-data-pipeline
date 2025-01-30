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
    # Validate latitude and longitude is within valid global range
    ColumnValues "Latitude" between -90 and 90 where "Latitude is not null",
    ColumnValues "Longitude" between -180 and 180 where "Longitude is not null",
    # Ensure prices are non-negative
    ColumnValues "PricePresale" >= 0 where "PricePresale is not null",
    ColumnValues "PriceDoor" >= 0 where "PriceDoor is not null",
    # Capacity must be a positive integer
    ColumnValues "Capacity" > 0 where "Capacity is not null",
    # Event duration must be non-negative
    ColumnValues "EventDurationMin" >= 0 where "EventDurationMin is not null",
    # Edition year must be grater to first edition
    ColumnValues "Edition" >= 2005,
    # Validate coordinates are within Amsterdam's metropolitan area
    ColumnValues "Latitude" between 52.28 and 52.50 where "Latitude is not null",  
    ColumnValues "Longitude" between 4.63 and 5.08 where "Longitude is not null"
]
```

### Ruleset 2: Completeness Checks

```python
Rules = [
    # Critical fields must have > 99% completeness
    Completeness "EventName" > 0.99,
    Completeness "StartDate" > 0.99,
    Completeness "EndDate" > 0.99,
    
    # Venue and address fields must have > 95% completeness
    Completeness "VenueName" > 0.95,
    Completeness "Address" > 0.95,
    
    # Coordinates must have > 90% completeness
    Completeness "Latitude" > 0.9,
    Completeness "Longitude" > 0.9,
    
    # Edition field must have > 99% completeness
    Completeness "Edition" > 0.99,
    
    # At least one price field (Presale or Door) must be present
    Completeness "PricePresale" > 0.98,
    Completeness "PriceDoor" > 0.98,
    
    # Lineup and Genre arrays must not be empty
    ColumnLength "Lineup" > 0,
    ColumnLength "Genre" > 0
]
```

**Validation**: Automated AWS Glue Data Quality checks with results visualized in Power BI

## 📈 Analytics & Visualization

### Power BI Dashboard Features:



---

## 🚧 Project Roadmap

### Current Phase

- MVP with 2005-2009 data validated
- Basic quality rules implemented
- Initial dashboard deployment
