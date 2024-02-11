```
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp-2024-413415.ny_taxi_zurich.green_taxi_2022_external`
OPTIONS (
format = 'parquet',
uris = ['gs://week_3_bq_training/green_taxi_2022.parquet']
)
```

```
SELECT 
count(*) 
FROM `zoomcamp-2024-413415.ny_taxi_zurich.green_taxi_2022`
```

```
SELECT 
distinct PULocationID
FROM `zoomcamp-2024-413415.ny_taxi_zurich.green_taxi_2022`
```

```
SELECT 
count(*) 
FROM `zoomcamp-2024-413415.ny_taxi_zurich.green_taxi_2022`
where fare_amount=0
```

```
CREATE OR REPLACE TABLE zoomcamp-2024-413415.ny_taxi_zurich.green_taxi_2022_p_q
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM zoomcamp-2024-413415.ny_taxi_zurich.green_taxi_2022
```

```
select
distinct PULocationID
from `zoomcamp-2024-413415.ny_taxi_zurich.green_taxi_2022`
where DATE(TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64))) between '2022-06-01' and '2022-06-30'
```