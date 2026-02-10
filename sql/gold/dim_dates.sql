-- =====================================================
-- Dimension: dim_dates
-- Source   : green_tripdata
-- Purpose  : Insert missing calendar dates only
-- =====================================================

INSERT INTO `{{params.project_id}}.{{params.gold_dataset}}.dim_dates`
(
  date_key,
  full_date,
  year,
  quarter,
  month,
  month_name,
  week_of_year,
  day_of_month,
  day_of_week,
  day_name,
  is_weekend
)
WITH dates AS (
  SELECT DISTINCT DATE(pickup_datetime) AS full_date
  FROM `{{params.project_id}}.{{params.silver_dataset}}.green_tripdata`
  WHERE pickup_datetime IS NOT NULL
  UNION DISTINCT
  SELECT DISTINCT DATE(dropoff_datetime) AS full_date
  FROM `{{params.project_id}}.{{params.silver_dataset}}.green_tripdata`
  WHERE dropoff_datetime IS NOT NULL
)
SELECT
  CAST(FORMAT_DATE('%Y%m%d', d.full_date) AS INT64) AS date_key,
  d.full_date,
  EXTRACT(YEAR FROM d.full_date) AS year,
  EXTRACT(QUARTER FROM d.full_date) AS quarter,
  EXTRACT(MONTH FROM d.full_date) AS month,
  FORMAT_DATE('%B', d.full_date) AS month_name,
  EXTRACT(WEEK FROM d.full_date) AS week_of_year,
  EXTRACT(DAY FROM d.full_date) AS day_of_month,
  EXTRACT(DAYOFWEEK FROM d.full_date) AS day_of_week,
  FORMAT_DATE('%A', d.full_date) AS day_name,
  EXTRACT(DAYOFWEEK FROM d.full_date) IN (1, 7) AS is_weekend
FROM dates d
LEFT JOIN `{{params.project_id}}.{{params.gold_dataset}}.dim_dates` g
  ON d.full_date = g.full_date
WHERE g.full_date IS NULL;
