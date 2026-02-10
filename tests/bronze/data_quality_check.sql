-- =====================================================
-- Project: NYC-Green-Taxi-Data-Warehouse
-- Layer: Bronze
-- Script: bronze_data_quality_checks.sql
-- Purpose: Run basic data quality checks on Bronze layer tables
-- =====================================================

-- 1 Vendor ID Check (No Nulls)
SELECT 
    COUNT(*) AS total_rows,
    COUNT(VendorID) AS non_null_vendor_count,
    COUNT(*) - COUNT(VendorID) AS null_vendor_count
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.green_tripdata`;

-- 2 Pickup/Dropoff Timestamps
SELECT *
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.green_tripdata`
WHERE lpep_pickup_datetime IS NULL
   OR lpep_dropoff_datetime IS NULL
   OR lpep_dropoff_datetime < lpep_pickup_datetime;

-- 3 Store and Forward Flag
SELECT DISTINCT store_and_fwd_flag
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.green_tripdata`;

-- 4 RatecodeID
SELECT DISTINCT RatecodeID
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.green_tripdata`;

-- 5 Pickup and Dropoff Location IDs
SELECT *
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.green_tripdata`
WHERE PULocationID IS NULL OR DOLocationID IS NULL;

-- 6 Passenger Count
-- All distinct values
SELECT DISTINCT passenger_count
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.green_tripdata`;

-- Invalid passenger counts
SELECT *
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.green_tripdata`
WHERE passenger_count <= 0;

-- 7 Taxi Zone Lookup Table Checks
-- Null LocationIDs
SELECT *
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.taxi_zone_lookup`
WHERE LocationID IS NULL;

-- Trim issues in Borough
SELECT *
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.taxi_zone_lookup`
WHERE TRIM(Borough) != Borough;

-- Trim issues in Zone
SELECT *
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.taxi_zone_lookup`
WHERE TRIM(Zone) != Zone;

-- Trim issues in service_zone
SELECT *
FROM `glass-chemist-483110-u0.nyc_taxi_bronze.taxi_zone_lookup`
WHERE TRIM(service_zone) != service_zone;
