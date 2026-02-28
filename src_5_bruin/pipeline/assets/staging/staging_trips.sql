/* @bruin
name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips_raw
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: create+replace

columns:
  - name: vendor_id
    type: integer
    checks:
      - name: not_null
  - name: pickup_datetime
    type: timestamp
    nullable: false
    checks:
      - name: not_null
  - name: payment_type
    type: integer
@bruin */

SELECT
  t.vendor_id,
  t.pickup_datetime,
  t.dropoff_datetime,
  t.passenger_count,
  t.trip_distance,
  t.payment_type,
  pl.payment_type_name,
  t.total_amount,
  t.extracted_at
FROM ingestion.trips_raw t
LEFT JOIN ingestion.payment_lookup pl
  ON t.payment_type = pl.payment_type_id
WHERE t.pickup_datetime >= '{{ start_datetime }}'
  AND t.pickup_datetime < '{{ end_datetime }}'