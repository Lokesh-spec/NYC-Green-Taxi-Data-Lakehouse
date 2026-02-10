-- ================================
-- NYC Taxi Gold DQ Checks Script
-- ================================

-- 1 Dimension Tables: Unique Key Checks

-- dim_dates: Check if date_key is unique
SELECT date_key, COUNT(*) AS cnt
FROM `glass-chemist-483110-u0.nyc_taxi_gold.dim_dates`
GROUP BY date_key
HAVING COUNT(*) > 1;

-- dim_locations: Check if location_key is unique
SELECT location_key, COUNT(*) AS cnt
FROM `glass-chemist-483110-u0.nyc_taxi_gold.dim_locations`
GROUP BY location_key
HAVING COUNT(*) > 1;


-- 2 Fact Table: No Missing Foreign Keys

SELECT COUNT(*) AS missing_fk_count
FROM `glass-chemist-483110-u0.nyc_taxi_gold.fact_trips`
WHERE pickup_location_key IS NULL
   OR dropoff_location_key IS NULL
   OR date_key IS NULL;


-- 3 Fact Table Grain: 1 trip → 1 row

SELECT trip_key, COUNT(*) AS cnt
FROM `glass-chemist-483110-u0.nyc_taxi_gold.fact_trips`
GROUP BY trip_key
HAVING COUNT(*) > 1;


-- 4 Fact Table: Trip Duration Must Be Positive

SELECT COUNT(*) AS negative_duration_count
FROM `glass-chemist-483110-u0.nyc_taxi_gold.fact_trips`
WHERE trip_duration_seconds < 0;

