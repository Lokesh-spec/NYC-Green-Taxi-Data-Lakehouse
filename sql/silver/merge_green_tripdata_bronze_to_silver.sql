-- =====================================================
-- Script   : merge_green_tripdata_bronze_to_silver.sql
-- Layer    : Bronze → Silver
-- Table    : silver.green_tripdata
-- Load     : Incremental MERGE
-- Purpose  : Clean, normalize, and deduplicate Green Taxi trips
-- =====================================================

MERGE INTO `{{params.project_id}}.{{params.silver_dataset}}.green_tripdata` st
USING (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      trip_id,
      VendorID AS vendor_id,
      DATETIME(lpep_pickup_datetime) AS pickup_datetime,
      DATETIME(lpep_dropoff_datetime) AS dropoff_datetime,

      CASE
        WHEN LOWER(store_and_fwd_flag) = 'y' THEN 'Yes'
        WHEN LOWER(store_and_fwd_flag) = 'n' THEN 'No'
        ELSE 'Unknown'
      END AS store_and_fwd_flag,

      CASE
        WHEN RatecodeID = 1 THEN 'Standard rate'
        WHEN RatecodeID = 2 THEN 'JFK'
        WHEN RatecodeID = 3 THEN 'Newark'
        WHEN RatecodeID = 4 THEN 'Nassau or Westchester'
        WHEN RatecodeID = 5 THEN 'Negotiated fare'
        WHEN RatecodeID = 6 THEN 'Group ride'
        ELSE 'Unknown'
      END AS rate_code,

      PULocationID AS pickup_location_id,
      DOLocationID AS dropoff_location_id,

      CASE
        WHEN SAFE_CAST(passenger_count AS INT64) <= 0 THEN NULL
        ELSE SAFE_CAST(passenger_count AS INT64)
      END AS passenger_count,

      CASE
        WHEN passenger_count IS NULL THEN 'missing'
        WHEN SAFE_CAST(passenger_count AS INT64) IS NULL THEN 'invalid'
        WHEN SAFE_CAST(passenger_count AS INT64) = 0 THEN 'zero_reported'
        WHEN SAFE_CAST(passenger_count AS INT64) BETWEEN 1 AND 6 THEN 'valid'
        ELSE 'out_of_range'
      END AS passenger_count_status,

      CASE
        WHEN trip_distance < 0 THEN NULL
        ELSE trip_distance
      END AS trip_distance,

      fare_amount,
      extra,
      mta_tax,
      tip_amount,
      tolls_amount,
      improvement_surcharge,

      CASE WHEN congestion_surcharge < 0 THEN NULL ELSE congestion_surcharge END AS congestion_surcharge,
      CASE WHEN cbd_congestion_fee < 0 THEN NULL ELSE cbd_congestion_fee END AS cbd_congestion_fee,
      CASE WHEN total_amount < 0 THEN NULL ELSE total_amount END AS total_amount,

      CASE
        WHEN payment_type = 1 THEN 'Credit card'
        WHEN payment_type = 2 THEN 'Cash'
        WHEN payment_type = 3 THEN 'No charge'
        WHEN payment_type = 4 THEN 'Dispute'
        ELSE 'Unknown'
      END AS payment_type,

      CASE
        WHEN trip_type = 1 THEN 'Street-hail'
        WHEN trip_type = 2 THEN 'Dispatch'
        ELSE 'Unknown'
      END AS trip_type,

      ingestion_ts,
      DATE(ingestion_ts) AS ingestion_date,

      ROW_NUMBER() OVER (
        PARTITION BY trip_id
        ORDER BY ingestion_ts DESC
      ) AS rn

    FROM `{{params.project_id}}.{{params.bronze_dataset}}.green_tripdata`
    WHERE
      NOT (payment_type IN (1, 2) AND total_amount < 0)
      AND lpep_dropoff_datetime >= lpep_pickup_datetime
      AND ingestion_ts >= (
        SELECT COALESCE(MAX(ingestion_ts), TIMESTAMP('1970-01-01'))
        FROM `{{params.project_id}}.{{params.silver_dataset}}.green_tripdata`
      )
  )
  WHERE rn = 1
) bt
ON bt.trip_id = st.trip_id
    WHEN MATCHED
      THEN
        UPDATE
SET
  vendor_id = bt.vendor_id,
  pickup_datetime = bt.pickup_datetime,
  dropoff_datetime = bt.dropoff_datetime,
  store_and_fwd_flag = bt.store_and_fwd_flag,
  rate_code = bt.rate_code,
  pickup_location_id = bt.pickup_location_id,
  dropoff_location_id = bt.dropoff_location_id,
  passenger_count = bt.passenger_count,
  passenger_count_status = bt.passenger_count_status,
  trip_distance = bt.trip_distance,
  fare_amount = bt.fare_amount,
  extra = bt.extra,
  mta_tax = bt.mta_tax,
  tip_amount = bt.tip_amount,
  tolls_amount = bt.tolls_amount,
  improvement_surcharge = bt.improvement_surcharge,
  congestion_surcharge = bt.congestion_surcharge,
  cbd_congestion_fee = bt.cbd_congestion_fee,
  total_amount = bt.total_amount,
  payment_type = bt.payment_type,
  trip_type = bt.trip_type,
  ingestion_ts = bt.ingestion_ts,
  ingestion_date = bt.ingestion_date
    WHEN NOT MATCHED
      THEN
        INSERT(
          trip_id,
          vendor_id,
          pickup_datetime,
          dropoff_datetime,
          store_and_fwd_flag,
          rate_code,
          pickup_location_id,
          dropoff_location_id,
          passenger_count,
          passenger_count_status,
          trip_distance,
          fare_amount,
          extra,
          mta_tax,
          tip_amount,
          tolls_amount,
          improvement_surcharge,
          congestion_surcharge,
          total_amount,
          payment_type,
          trip_type,
          ingestion_ts,
          ingestion_date)
          VALUES(
            bt.trip_id,
            bt.vendor_id,
            bt.pickup_datetime,
            bt.dropoff_datetime,
            bt.store_and_fwd_flag,
            bt.rate_code,
            bt.pickup_location_id,
            bt.dropoff_location_id,
            bt.passenger_count,
            bt.passenger_count_status,
            bt.trip_distance,
            bt.fare_amount,
            bt.extra,
            bt.mta_tax,
            bt.tip_amount,
            bt.tolls_amount,
            bt.improvement_surcharge,
            bt.congestion_surcharge,
            bt.total_amount,
            bt.payment_type,
            bt.trip_type,
            bt.ingestion_ts,
            bt.ingestion_date);
