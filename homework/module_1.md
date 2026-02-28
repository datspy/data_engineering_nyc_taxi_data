# Module 1 Homework: Engineering Data Pipeline (Docker & Postgres)

## Setup

```python
from sqlalchemy import create_engine
import pandas as pd

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
env_file = os.path.join(parent_dir, ".env")
load_dotenv(os.path.join(env_file))

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
postgres_engine = fr"postgresql://{user}:{password}@localhost:5432/nyc_taxi"
engine = create_engine(postgres_engine)
```

## Question 3. Counting short trips

```python
sql_query = """
SELECT count(*) AS total_trips
FROM green_taxi_data
WHERE pickup_datetime >= '2025-11-01'
  AND pickup_datetime < '2025-12-01'
  AND trip_distance_miles <= 1
"""
with engine.connect() as conn, conn.begin():
    data = pd.read_sql_query(sql_query, conn)
data
```

| total_trips |
| --- |
| 8007 |

## Question 4. Longest trip for each day

```python
sql_query = """
SELECT date(pickup_datetime) AS trip_date, trip_distance_miles
FROM green_taxi_data
WHERE trip_distance_miles <= 100
ORDER BY trip_distance_miles DESC
LIMIT 1
"""
with engine.connect() as conn, conn.begin():
    data = pd.read_sql_query(sql_query, conn)
data
```

| trip_date  | trip_distance_miles |
| --- | --- |
| 2025-11-14 | 88.03 |

## Question 5. Biggest pickup zone

```python
sql_query = """
WITH base AS (
    SELECT pickup_location_id, sum(total_amount) AS all_trips_amount
    FROM green_taxi_data
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 1
)
SELECT tz.zone, b.pickup_location_id, b.all_trips_amount
FROM base b
LEFT JOIN taxi_zone_lookup tz
  ON b.pickup_location_id = tz.location_id
"""
with engine.connect() as conn, conn.begin():
    data = pd.read_sql_query(sql_query, conn)
data
```

| zone | pickup_location_id | all_trips_amount |
| --- | --- | --- |
| East Harlem North | 74 | 257684.7 |

## Question 6. Largest tip

```python
sql_query = """
WITH base AS (
    SELECT dropoff_location_id, max(tip_amount) AS max_tip_amount
    FROM green_taxi_data
    WHERE pickup_location_id = 74
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 1
)
SELECT tz.zone, b.dropoff_location_id, b.max_tip_amount
FROM base b
LEFT JOIN taxi_zone_lookup tz
  ON b.dropoff_location_id = tz.location_id
"""
with engine.connect() as conn, conn.begin():
    data = pd.read_sql_query(sql_query, conn)
data
```

| zone | dropoff_location_id | max_tip_amount |
| --- | --- | --- |
| Yorkville West | 263 | 81.89 |
