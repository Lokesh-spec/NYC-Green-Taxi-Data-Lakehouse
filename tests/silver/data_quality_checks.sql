-- =====================================================
-- Project: NYC-Green-Taxi-Data-Warehouse
-- Layer: Silver
-- Script: silver_data_quality_checks.sql
-- Purpose: Run basic data quality checks on Silver layer tables
-- =====================================================

-- 1 Null trip_id check
SELECT COUNT(*) AS null_trip_id_count
FROM `glass-chemist-483110-u0.nyc_taxi_silver.green_tripdata`
WHERE trip_id IS NULL;

-- 2 Pickup datetime must exist
SELECT COUNT(*) AS null_pickup_count
FROM `glass-chemist-483110-u0.nyc_taxi_silver.green_tripdata`
WHERE pickup_datetime IS NULL;

-- 3 Dropoff datetime should be after pickup
SELECT COUNT(*) AS invalid_dropoff_count
FROM `glass-chemist-483110-u0.nyc_taxi_silver.green_tripdata`
WHERE dropoff_datetime < pickup_datetime;

-- 4 Invalid passenger count
SELECT COUNT(*) AS invalid_passenger_count
FROM `glass-chemist-483110-u0.nyc_taxi_silver.green_tripdata`
WHERE passenger_count < 0;

-- 5 Fare sanity check
SELECT COUNT(*) AS negative_fare_count
FROM `glass-chemist-483110-u0.nyc_taxi_silver.green_tripdata`
WHERE total_amount < 0;

-- 6 Duplicate trip_id check
SELECT trip_id, COUNT(*) AS cnt
FROM `glass-chemist-483110-u0.nyc_taxi_silver.green_tripdata`
GROUP BY trip_id
HAVING COUNT(*) > 1;

-- 7 Accepted payment_type values
SELECT DISTINCT payment_type
FROM `glass-chemist-483110-u0.nyc_taxi_silver.green_tripdata`
WHERE payment_type NOT IN ('Credit card','Cash','No charge','Dispute','Unknown');

-- 8 Taxi Zone Lookup: No duplicate location IDs
SELECT location_id, COUNT(*) AS cnt
FROM `glass-chemist-483110-u0.nyc_taxi_silver.taxi_zone_lookup`
GROUP BY location_id
HAVING COUNT(*) > 1;

-- 9 Taxi Zone Lookup: borough should not be null
SELECT COUNT(*) AS null_borough_count
FROM `glass-chemist-483110-u0.nyc_taxi_silver.taxi_zone_lookup`
WHERE borough IS NULL;
