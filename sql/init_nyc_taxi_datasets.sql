-- =====================================================
-- Dataset Initialization Script
-- Project: NYC-Green-Taxi-Data-Lakehouse
-- Domain: NYC Taxi
-- Purpose: Create Bronze, Silver, and gold datasets
-- =====================================================

-- -------------------------------
-- Bronze Dataset
-- -------------------------------
CREATE SCHEMA IF NOT EXISTS `{{project_id}}.nyc_taxi_bronze`
OPTIONS (
  description = "Bronze layer for raw NYC taxi ingestion data"
);

-- -------------------------------
-- Silver Dataset
-- -------------------------------
CREATE SCHEMA IF NOT EXISTS `{{project_id}}.nyc_taxi_silver`
OPTIONS (
  description = "Silver layer for cleansed, standardized, and enriched NYC taxi data"
);


-- -------------------------------
-- Gold Dataset
-- -------------------------------
CREATE SCHEMA IF NOT EXISTS `{{project_id}}.nyc_taxi_gold`
OPTIONS (
  description = "Gold layer for standardized and aggregated NYC taxi data"
);
