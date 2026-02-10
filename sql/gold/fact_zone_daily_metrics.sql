-- =====================================================
-- Gold Layer
-- Fact: Zone Daily Metrics
-- Purpose:
--   Incrementally aggregate trip-level data to daily
--   metrics per zone (pickup and dropoff)
-- =====================================================

MERGE INTO `{{params.project_id}}.{{params.gold_dataset}}.fact_zone_daily_metrics` AS target
USING (

  -- 1. Identify trips to process
  WITH
    base_trips AS (
      SELECT
        ft.*,
        d.full_date
      FROM `{{params.project_id}}.{{params.gold_dataset}}.fact_trips` ft
      JOIN `{{params.project_id}}.{{params.gold_dataset}}.dim_dates` d
        ON d.date_key = ft.pickup_date_key
      WHERE
        d.full_date > COALESCE(
          (
            SELECT MAX(full_date)
            FROM `{{params.project_id}}.{{params.gold_dataset}}.fact_zone_daily_metrics`
          ),
          DATE '2024-01-01')
    )

  -- 2. Aggregate pickup + dropoff
  SELECT
    date_key,
    full_date,
    location_key,
    location_role,
    total_fare_amount,
    total_tip_amount,
    total_amount,
    total_trips,
    average_trip_duration,
    average_trip_distance
  FROM
    (

      -- Pickup
      SELECT
        pickup_date_key AS date_key,
        full_date,
        pickup_location_key AS location_key,
        'pickup' AS location_role,
        SUM(fare_amount) AS total_fare_amount,
        SUM(tip_amount) AS total_tip_amount,
        SUM(total_amount) AS total_amount,
        COUNT(*) AS total_trips,
        ROUND(AVG(trip_duration_seconds), 2) AS average_trip_duration,
        ROUND(AVG(trip_distance), 2) AS average_trip_distance
      FROM base_trips
      GROUP BY pickup_date_key, full_date, pickup_location_key
      UNION ALL

      -- Dropoff
      SELECT
        dropoff_date_key AS date_key,
        full_date,
        dropoff_location_key AS location_key,
        'dropoff' AS location_role,
        SUM(fare_amount),
        SUM(tip_amount),
        SUM(total_amount),
        COUNT(*),
        ROUND(AVG(trip_duration_seconds), 2),
        ROUND(AVG(trip_distance), 2)
      FROM base_trips
      GROUP BY dropoff_date_key, full_date, dropoff_location_key
    )
) AS source
ON
  target.date_key = source.date_key
  AND target.location_key = source.location_key
  AND target.location_role
    = source.location_role
      WHEN MATCHED THEN UPDATE
SET
  total_fare_amount = source.total_fare_amount,
  total_tip_amount = source.total_tip_amount,
  total_amount = source.total_amount,
  total_trips = source.total_trips,
  average_trip_duration = source.average_trip_duration,
  average_trip_distance = source.average_trip_distance
    WHEN NOT MATCHED
      THEN
        INSERT(
          date_key,
          full_date,
          location_key,
          location_role,
          total_fare_amount,
          total_tip_amount,
          total_amount,
          total_trips,
          average_trip_duration,
          average_trip_distance)
          VALUES(
            source.date_key,
            source.full_date,
            source.location_key,
            source.location_role,
            source.total_fare_amount,
            source.total_tip_amount,
            source.total_amount,
            source.total_trips,
            source.average_trip_duration,
            source.average_trip_distance);
