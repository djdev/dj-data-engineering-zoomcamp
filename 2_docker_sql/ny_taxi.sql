SELECT COUNT(tip_amount) FROM yellow_taxi_data;

SELECT COUNT(*) FROM yellow_taxi_data
WHERE date(tpep_pickup_datetime)='2021-01-18' and date(tpep_dropoff_datetime)='2021-01-18';

SELECT * FROM yellow_taxi_data ORDER BY date(tpep_dropoff_datetime) ASC