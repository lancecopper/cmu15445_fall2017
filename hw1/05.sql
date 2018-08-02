SELECT count(case_id) as num, substr(filing_date, 1, 3) || "0s" AS decade FROM documents WHERE filing_date != "" GROUP BY decade ORDER BY num DESC, decade ASC LIMIT 3;



