WITH 
aa_cases(case_id, disposition) AS (SELECT cases.case_id, disposition FROM cases, parties
ON cases.case_id=parties.case_id WHERE disposition IN ("Guilty", "Not Guilty") AND race="African American"),
c_cases(case_id, disposition) AS (SELECT cases.case_id, disposition FROM cases, parties
ON cases.case_id=parties.case_id WHERE disposition IN ("Guilty", "Not Guilty") AND race="Caucasian")
SELECT "African American", disposition, count(case_id) * 100.0 / (SELECT count(case_id) from aa_cases) FROM aa_cases GROUP BY disposition
UNION ALL
SELECT "Cautisian", disposition, count(case_id) * 100.0 / (SELECT count(case_id) from c_cases) FROM c_cases GROUP BY disposition;

