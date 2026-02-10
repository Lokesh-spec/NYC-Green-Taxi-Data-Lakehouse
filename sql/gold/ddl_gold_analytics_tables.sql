-- =====================================================
-- Project: NYC-Green-Taxi-Data-Warehouse
-- Layer: Gold
-- Purpose:
--   Create analytics-ready star schema tables
--   (Dimensions + Fact)
-- =====================================================

-- -----------------------------------------------------
-- Dimension: Locations
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS
  `{{project_id}}.{{gold_dataset_id}}.dim_locations` (
    location_key STRING,
    location_id INT64,
    borough STRING,
    zone STRING,
    service_zone STRING
  )
OPTIONS (
  description = "Location dimension derived from taxi zone lookup"
);

-- -----------------------------------------------------
-- Dimension: Payments
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS
  `{{project_id}}.{{gold_dataset_id}}.dim_payments` (
    payment_key STRING,
    payment_type STRING
  )
OPTIONS (
  description = "Payment type dimension for taxi trips"
);

-- -----------------------------------------------------
-- Dimension: Date
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS
  `{{project_id}}.{{gold_dataset_id}}.dim_date` (
    date_key INT64,
    full_date DATE,
    year INT64,
    quarter INT64,
    month INT64,
    month_name STRING,
    week_of_year INT64,
    day_of_month INT64,
    day_of_week INT64,
    day_name STRING,
    is_weekend BOOL
  )
PARTITION BY full_date
CLUSTER BY year, month
OPTIONS (
  description = "Date dimension table for time-based analytics"
);

-- -----------------------------------------------------
-- Fact: Trips
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS
  `{{project_id}}.{{gold_dataset_id}}.fact_trips` (
    trip_key STRING,
    pickup_date_key INT64,
    dropoff_date_key INT64,
    pickup_location_key STRING,
    dropoff_location_key STRING,
    payment_type_key STRING,
    passenger_count INT64,
    trip_distance FLOAT64,
    fare_amount FLOAT64,
    extra FLOAT64,
    mta_tax FLOAT64,
    tip_amount FLOAT64,
    tolls_amount FLOAT64,
    improvement_surcharge FLOAT64,
    congestion_surcharge FLOAT64,
    total_amount FLOAT64,
    trip_duration_seconds INT64,
    ingestion_ts TIMESTAMP,
    ingestion_date DATE
  )
PARTITION BY ingestion_date
CLUSTER BY pickup_location_key, dropoff_location_key
OPTIONS (
  description = "Fact table containing NYC Green Taxi trip measures"
);


-- -----------------------------------------------------
-- Fact: Fact zone daily metrics
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `glass-chemist-483110-u0.nyc_taxi_gold.fact_zone_daily_metrics` (
    date_key INT64,
    full_date DATE,
    location_key STRING,
    location_role STRING,
    total_fare_amount FLOAT64,
    total_tip_amount FLOAT64,
    total_amount FLOAT64,
    total_trips INT64,
    average_trip_duration FLOAT64,
    average_trip_distance FLOAT64
);

-- -----------------------------------------------------
-- Fact: Fact zone monthly metrics
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `glass-chemist-483110-u0.nyc_taxi_gold.fact_zone_monthly_metrics` (
    year INT64,
    month INT64,
    month_start_date DATE,

    location_key STRING,
    location_role STRING,

    total_fare_amount FLOAT64,
    total_tip_amount FLOAT64,
    total_amount FLOAT64,
    total_trips INT64,

    average_trip_duration FLOAT64,
    average_trip_distance FLOAT64
);
