-- =====================================================
-- Script   : load_taxi_zone_lookup_bronze_to_silver.sql
-- Layer    : Bronze → Silver
-- Table    : silver.taxi_zone_lookup
-- Load     : Incremental insert (new records only)
-- =====================================================

INSERT INTO `{{params.project_id}}.{{params.silver_dataset}}.taxi_zone_lookup`
  (
    location_id,
    borough,
    zone,
    service_zone
  )
SELECT DISTINCT
  b.LocationID AS location_id,
  TRIM(b.Borough) AS borough,
  TRIM(b.Zone) AS zone,
  TRIM(b.service_zone) AS service_zone
FROM `{{params.project_id}}.{{params.bronze_dataset}}.taxi_zone_lookup` b
LEFT JOIN `{{params.project_id}}.{{params.silver_dataset}}.taxi_zone_lookup` s
  ON b.LocationID = s.location_id
WHERE s.location_id IS NULL;
