-- =====================================================
-- Gold Layer
-- Fact: Zone Monthly Metrics
-- Purpose:
--   Incrementally aggregate daily zone metrics
--   and upsert them into monthly fact table
-- =====================================================

MERGE INTO `{{params.project_id}}.{{params.gold_dataset}}.fact_zone_monthly_metrics` AS target
USING (

  -- 1. Aggregate daily metrics to monthly level
  SELECT
    dd.year,
    dd.month,
    DATE_TRUNC(dd.full_date, MONTH) AS month_start_date,
    fzdm.location_key,
    fzdm.location_role,

    -- Monthly totals
    SUM(fzdm.total_fare_amount) AS total_fare_amount,
    SUM(fzdm.total_tip_amount) AS total_tip_amount,
    SUM(fzdm.total_amount) AS total_amount,
    SUM(fzdm.total_trips) AS total_trips,

    -- Weighted averages
    ROUND(
      SUM(fzdm.average_trip_duration * fzdm.total_trips)
        / NULLIF(SUM(fzdm.total_trips), 0),
      2) AS average_trip_duration,
    ROUND(
      SUM(fzdm.average_trip_distance * fzdm.total_trips)
        / NULLIF(SUM(fzdm.total_trips), 0),
      2) AS average_trip_distance
  FROM `{{params.project_id}}.{{params.gold_dataset}}.fact_zone_daily_metrics` fzdm
  JOIN `{{params.project_id}}.{{params.gold_dataset}}.dim_dates` dd
    ON fzdm.date_key = dd.date_key

  -- 2. Incremental filter (reprocess last month safely)
  WHERE
    DATE_TRUNC(dd.full_date, MONTH)
    >= COALESCE(
      (
        SELECT DATE_SUB(MAX(month_start_date), INTERVAL 1 MONTH)
        FROM `{{params.project_id}}.{{params.gold_dataset}}.fact_zone_monthly_metrics`
      ),
      DATE '2024-01-01')
  GROUP BY
    dd.year,
    dd.month,
    month_start_date,
    fzdm.location_key,
    fzdm.location_role
) AS source

-- 3. Natural key for monthly grain
ON
  target.year = source.year
  AND target.month = source.month
  AND target.location_key = source.location_key
  AND target.location_role
    = source.location_role

      -- 4. Update existing month/location rows
      WHEN MATCHED
        THEN
          UPDATE
SET
  total_fare_amount = source.total_fare_amount,
  total_tip_amount = source.total_tip_amount,
  total_amount = source.total_amount,
  total_trips = source.total_trips,
  average_trip_duration = source.average_trip_duration,
  average_trip_distance = source.average_trip_distance

    -- 5. Insert new month/location rows
    WHEN NOT MATCHED
      THEN
        INSERT(
          year,
          month,
          month_start_date,
          location_key,
          location_role,
          total_fare_amount,
          total_tip_amount,
          total_amount,
          total_trips,
          average_trip_duration,
          average_trip_distance)
          VALUES(
            source.year,
            source.month,
            source.month_start_date,
            source.location_key,
            source.location_role,
            source.total_fare_amount,
            source.total_tip_amount,
            source.total_amount,
            source.total_trips,
            source.average_trip_duration,
            source.average_trip_distance);
