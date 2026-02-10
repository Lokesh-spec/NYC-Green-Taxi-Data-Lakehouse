-- =========================================================
-- Script: Load dim_locations from taxi_zone_lookup
-- Purpose: Insert new locations only (no duplicates)
-- =========================================================

-- Optional: declare variables if you plan to parameterize later
-- DECLARE project_id STRING DEFAULT 'my_project';
-- DECLARE silver_dataset_id STRING DEFAULT 'silver';
-- DECLARE gold_dataset_id STRING DEFAULT 'gold';

INSERT INTO `{{params.project_id}}.{{params.gold_dataset}}.dim_locations`
(
  location_key,
  location_id,
  borough,
  zone,
  service_zone
)
SELECT
  CAST(FARM_FINGERPRINT(CAST(src.location_id AS STRING)) AS STRING) AS location_key,
  CAST(src.location_id AS INT64) AS location_id,
  src.Borough AS borough,
  src.Zone AS zone,
  src.service_zone
FROM `{{params.project_id}}.{{params.silver_dataset}}.taxi_zone_lookup` src
LEFT JOIN `{{params.project_id}}.{{params.gold_dataset}}.dim_locations` tgt
  ON src.location_id = tgt.location_id
WHERE tgt.location_id IS NULL;
