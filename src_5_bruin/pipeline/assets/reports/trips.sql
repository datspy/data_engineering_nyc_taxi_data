/* @bruin
name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: create+replace

columns:
  - name: pickup_date
    type: date
    primary_key: true
  - name: payment_type_name
    type: string
    primary_key: true
  - name: trip_count
    type: bigint
    checks:
      - name: non_negative
  - name: total_amount_sum
    type: double
    checks:
      - name: non_negative
@bruin */

SELECT
  CAST(pickup_datetime AS DATE) AS pickup_date,
  COALESCE(payment_type_name, 'unknown') AS payment_type_name,
  COUNT(*) AS trip_count,
  SUM(COALESCE(total_amount, 0)) AS total_amount_sum
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 1, 2