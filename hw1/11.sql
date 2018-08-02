WITH
interested_attorneys(name, num)
    AS (SELECT name, count(case_id) AS num FROM attorneys WHERE name!="" GROUP BY name),
f_cases(name, case_id)
    AS (SELECT attorneys.name, case_id FROM attorneys
        INNER JOIN interested_attorneys ON attorneys.name=interested_attorneys.name
		WHERE num>100),
all_cases(name, case_id, disposition)
    AS (SELECT name, cases.case_id, disposition FROM f_cases
        INNER JOIN cases ON cases.case_id=f_cases.case_id),
won_cases(name, case_id)
    AS (SELECT name, case_id FROM all_cases WHERE disposition="Not Guilty"),
case_statistic(name, total_num)
    AS (SELECT name, count(case_id) FROM all_cases GROUP BY name),
won_statistic(name, won_num)
    AS (SELECT name, count(case_id) FROM won_cases GROUP BY name)
SELECT case_statistic.name, total_num, (won_num * 1.0 / total_num) AS win_rate
FROM case_statistic, won_statistic
ON case_statistic.name=won_statistic.name
ORDER BY win_rate DESC
LIMIT 7;
