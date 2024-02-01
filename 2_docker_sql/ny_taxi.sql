-- Question 3. Count records
SELECT COUNT(*) FROM public.yellow_taxi_data
WHERE date(tpep_pickup_datetime)='2021-01-18' and date(tpep_dropoff_datetime)='2021-01-18';


-- Question 4. Longest trip for each day (result ignore column with NULL values)
SELECT * FROM (
	SELECT MAX(trip_distance) AS M, date(tpep_pickup_datetime) AS pickup_time
    FROM yellow_taxi_data
	WHERE "VendorID" IS NOT NULL
    GROUP BY pickup_time
) max_t1 INNER JOIN yellow_taxi_data
ON yellow_taxi_data.tpep_pickup_datetime = max_t1.pickup_time
WHERE yellow_taxi_data."VendorID" IS NOT NULL
ORDER BY M DESC

SELECT * FROM public.yellow_taxi_data 
WHERE date(tpep_pickup_datetime)='2021-01-12' AND "VendorID" IS NOT NULL
ORDER BY trip_distance DESC


-- Question 5. Three biggest pick up Boroughs
SELECT z."Zone", SUM(total_amount) AS sum_total 
FROM yellow_taxi_data
INNER JOIN zones z
ON yellow_taxi_data."PULocationID" = z."LocationID"
WHERE date(tpep_pickup_datetime)='2021-01-18' AND service_zone IS NOT Null
GROUP BY z."Zone"
ORDER BY sum_total DESC

-- Question 6. Largest tip
SELECT z2."Zone", max(tip_amount) AS max_tip
FROM yellow_taxi_data
INNER JOIN zones z
ON yellow_taxi_data."PULocationID" = z."LocationID"
INNER JOIN zones z2
ON yellow_taxi_data."DOLocationID" = z2."LocationID"
WHERE z."Zone"='Astoria'
GROUP BY z2."Zone"
ORDER BY max_tip DESC
LIMIT 1