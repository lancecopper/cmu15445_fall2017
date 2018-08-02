SELECT zip, count(case_id) AS num FROM parties WHERE zip!="" AND lower(city)="maryland"
GROUP BY zip ORDER BY num DESC LIMIT 3;

SELECT * FROM parties WHERE lower(city)="maryland";
