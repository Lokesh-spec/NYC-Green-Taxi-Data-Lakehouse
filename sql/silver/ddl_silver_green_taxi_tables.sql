-- =====================================================
-- Project: NYC-Green-Taxi-Data-Warehouse
-- Layer: Silver
-- Purpose: Create cleansed & standardized tables
-- =====================================================

-- -----------------------------------------------------
-- Silver: Green Taxi Trip Data
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS
 `{{project_id}}.{{silver_dataset_id}}.green_tripdata` (
  trip_id STRING,

  vendor_id INT64,

  pickup_datetime DATETIME,
  dropoff_datetime DATETIME,

  store_and_fwd_flag STRING,
  rate_code STRING,

  pickup_location_id INT64,
  dropoff_location_id INT64,

  passenger_count INT64,
  passenger_count_status STRING,

  trip_distance FLOAT64,

  fare_amount FLOAT64,
  extra FLOAT64,
  mta_tax FLOAT64,
  tip_amount FLOAT64,
  tolls_amount FLOAT64,
  improvement_surcharge FLOAT64,
  congestion_surcharge FLOAT64,
  cbd_congestion_fee FLOAT64,
  total_amount FLOAT64,

  payment_type STRING,
  trip_type STRING,

  ingestion_ts TIMESTAMP,
  ingestion_date DATE
)
PARTITION BY ingestion_date
CLUSTER BY pickup_location_id, dropoff_location_id
OPTIONS (
  description = "Silver layer table containing cleaned and standardized NYC Green Taxi trip data"
);

-- -----------------------------------------------------
-- Silver: Taxi Zone Lookup
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS
 `{{project_id}}.{{silver_dataset_id}}.taxi_zone_lookup` (
  location_id INT64,
  borough STRING,
  zone STRING,
  service_zone STRING
)
OPTIONS (
  description = "Silver layer taxi zone lookup table for enriching pickup and dropoff locations"
);
