SELECT "county " || violation_county, count(case_id) AS num 
FROM 
(SELECT violation_county, cases.case_id FROM cases INNER JOIN charges ON cases.case_id=charges.case_id WHERE description LIKE "%reckless%" AND violation_county != "") 
GROUP BY violation_county 
ORDER BY num DESC, violation_county ASC 
LIMIT 3;



