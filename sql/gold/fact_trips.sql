-- =====================================================
-- Fact: fact_trips
-- Load Type : Incremental MERGE (latest record wins)
-- Source    : silver.green_tripdata
-- Grain     : One row per trip_id (latest ingestion_ts)
-- =====================================================

MERGE INTO `{{params.project_id}}.{{params.gold_dataset}}.fact_trips` gt
USING (
  WITH
    ranked_trips AS (
      SELECT
        t.trip_id AS trip_key,
        dpu.date_key AS pickup_date_key,
        ddo.date_key AS dropoff_date_key,
        pu.location_key AS pickup_location_key,
        dl.location_key AS dropoff_location_key,
        dp.payment_key AS payment_type_key,
        t.passenger_count,
        t.trip_distance,
        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.improvement_surcharge,
        t.congestion_surcharge,
        t.total_amount,
        TIMESTAMP_DIFF(
          t.dropoff_datetime,
          t.pickup_datetime,
          SECOND) AS trip_duration_seconds,
        t.ingestion_ts,
        t.ingestion_date,
        ROW_NUMBER()
          OVER (
            PARTITION BY t.trip_id
            ORDER BY t.ingestion_ts DESC
          ) AS rn
      FROM `{{params.project_id}}.{{params.silver_dataset}}.green_tripdata` t
      LEFT JOIN `{{params.project_id}}.{{params.gold_dataset}}.dim_dates` dpu
        ON dpu.full_date = DATE(t.pickup_datetime)
      LEFT JOIN `{{params.project_id}}.{{params.gold_dataset}}.dim_dates` ddo
        ON ddo.full_date = DATE(t.dropoff_datetime)
      LEFT JOIN `{{params.project_id}}.{{params.gold_dataset}}.dim_locations` pu
        ON pu.location_id = t.pickup_location_id
      LEFT JOIN `{{params.project_id}}.{{params.gold_dataset}}.dim_locations` dl
        ON dl.location_id = t.dropoff_location_id
      LEFT JOIN `{{params.project_id}}.{{params.gold_dataset}}.dim_payments` dp
        ON dp.payment_type = t.payment_type
      WHERE
        t.ingestion_ts >= (
          SELECT
            COALESCE(
              MAX(ingestion_ts),
              TIMESTAMP('1970-01-01'))
          FROM `{{params.project_id}}.{{params.gold_dataset}}.fact_trips`
        )
    )
  SELECT *
  FROM ranked_trips
  WHERE rn = 1
) st
ON
  gt.trip_key = st.trip_key
    WHEN MATCHED
      THEN
        UPDATE
SET
  pickup_date_key = st.pickup_date_key,
  dropoff_date_key = st.dropoff_date_key,
  pickup_location_key = st.pickup_location_key,
  dropoff_location_key = st.dropoff_location_key,
  payment_type_key = st.payment_type_key,
  passenger_count = st.passenger_count,
  trip_distance = st.trip_distance,
  fare_amount = st.fare_amount,
  extra = st.extra,
  mta_tax = st.mta_tax,
  tip_amount = st.tip_amount,
  tolls_amount = st.tolls_amount,
  improvement_surcharge = st.improvement_surcharge,
  congestion_surcharge = st.congestion_surcharge,
  total_amount = st.total_amount,
  trip_duration_seconds = st.trip_duration_seconds,
  ingestion_ts = st.ingestion_ts,
  ingestion_date = st.ingestion_date
    WHEN NOT MATCHED
      THEN
        INSERT(
          trip_key,
          pickup_date_key,
          dropoff_date_key,
          pickup_location_key,
          dropoff_location_key,
          payment_type_key,
          passenger_count,
          trip_distance,
          fare_amount,
          extra,
          mta_tax,
          tip_amount,
          tolls_amount,
          improvement_surcharge,
          congestion_surcharge,
          total_amount,
          trip_duration_seconds,
          ingestion_ts,
          ingestion_date)
          VALUES(
            st.trip_key,
            st.pickup_date_key,
            st.dropoff_date_key,
            st.pickup_location_key,
            st.dropoff_location_key,
            st.payment_type_key,
            st.passenger_count,
            st.trip_distance,
            st.fare_amount,
            st.extra,
            st.mta_tax,
            st.tip_amount,
            st.tolls_amount,
            st.improvement_surcharge,
            st.congestion_surcharge,
            st.total_amount,
            st.trip_duration_seconds,
            st.ingestion_ts,
            st.ingestion_date);
