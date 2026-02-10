-- =====================================================
-- Dimension: dim_payments
-- Source   : green_tripdata
-- Purpose  : Insert new payment types only
-- =====================================================

INSERT INTO `{{params.project_id}}.{{params.gold_dataset}}.dim_payments`
(
  payment_key,
  payment_type
)
SELECT
  CAST(FARM_FINGERPRINT(payment_type) AS STRING) AS payment_key,
  payment_type
FROM (
  SELECT DISTINCT s.payment_type
  FROM `{{params.project_id}}.{{params.silver_dataset}}.green_tripdata` s
  LEFT JOIN `{{params.project_id}}.{{params.gold_dataset}}.dim_payments` d
    ON s.payment_type = d.payment_type
  WHERE d.payment_type IS NULL
    AND s.payment_type IS NOT NULL
);

