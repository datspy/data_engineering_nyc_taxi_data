### Homework SQL


```python
from sqlalchemy import create_engine
import pandas as pd
engine = create_engine('postgresql://pguser:pgpassword@localhost:5432/nyc_taxi')
```

#### Question 3. Counting short trips


```python
sql_query = '''
SELECT count(*) as total_trips 
from green_taxi_data
where pickup_datetime>='2025-11-01'
and pickup_datetime<'2025-12-01'
and trip_distance_miles<=1
'''
with engine.connect() as conn, conn.begin():
    data = pd.read_sql_query(sql_query, conn)
data
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>total_trips</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>8007</td>
    </tr>
  </tbody>
</table>
</div>



#### Question 4. Longest trip for each day


```python
sql_query = '''
SELECT date(pickup_datetime) as trip_date, trip_distance_miles
from green_taxi_data
where trip_distance_miles<=100
order by trip_distance_miles desc
limit 1
'''
with engine.connect() as conn, conn.begin():
    data = pd.read_sql_query(sql_query, conn)
data
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>trip_date</th>
      <th>trip_distance_miles</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2025-11-14</td>
      <td>88.03</td>
    </tr>
  </tbody>
</table>
</div>



#### Question 5. Biggest pickup zone


```python
sql_query = '''
with base as (
select pickup_location_id, sum(total_amount) as all_trips_amount
from green_taxi_data
group by 1
order by 2 desc
limit 1)
select tz.zone, b.pickup_location_id, b.all_trips_amount
from base b
left join taxi_zone_lookup tz
on b.pickup_location_id=tz.location_id
'''
with engine.connect() as conn, conn.begin():
    data = pd.read_sql_query(sql_query, conn)
data
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>zone</th>
      <th>pickup_location_id</th>
      <th>all_trips_amount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>East Harlem North</td>
      <td>74</td>
      <td>257684.7</td>
    </tr>
  </tbody>
</table>
</div>



#### Question 6. Largest tip


```python
sql_query = '''
with base as (
select dropoff_location_id, max(tip_amount) as max_tip_amount
from green_taxi_data
where pickup_location_id=74
group by 1
order by 2 desc
limit 1)
select tz.zone, b.dropoff_location_id, b.max_tip_amount
from base b
left join taxi_zone_lookup tz
on b.dropoff_location_id=tz.location_id
'''
with engine.connect() as conn, conn.begin():
    data = pd.read_sql_query(sql_query, conn)
data
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>zone</th>
      <th>dropoff_location_id</th>
      <th>max_tip_amount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Yorkville West</td>
      <td>263</td>
      <td>81.89</td>
    </tr>
  </tbody>
</table>
</div>