SELECT origin, destination, AVG(price) AS avg_price
FROM flight_prices
GROUP BY origin, destination;





SELECT depart_date, AVG(price) AS avg_price
FROM flight_prices
GROUP BY depart_date
ORDER BY depart_date;





SELECT 
    EXTRACT(DAY FROM depart_date - fetched_at) AS days_before_departure,
    AVG(price) AS avg_price
FROM flight_prices
GROUP BY days_before_departure
ORDER BY avg_price ASC;
