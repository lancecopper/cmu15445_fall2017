sql1:
SELECT filing_year, avg(age) FROM charges INNER JOIN (SELECT parties.case_id, strftime('%Y', filing_date) AS filing_year, (strftime('%Y.%m%d', filing_date) - strftime('%Y.%m%d', dob)) AS age FROM parties INNER JOIN cases ON parties.case_id=cases.case_id WHERE filing_date!="" AND parties.type="Defendant" AND name!="" AND dob!="" AND age>0 AND age<100) AS raw_agetable ON charges.case_id=raw_agetable.case_id WHERE charges.disposition='Guilty' GROUP BY filing_year ORDER BY filing_year DESC LIMIT 5;


sql2(using CTE):
WITH
f_cases(case_id, filing_date) AS (SELECT case_id, filing_date from cases WHERE filing_date!=""),
f_parties(case_id, dob) AS (SELECT case_id, dob FROM parties WHERE type="Defendant" AND name!="" AND dob!=""),
f_charges(case_id) AS (SELECT case_id FROM charges WHERE disposition='Guilty'),
interest_parties(case_id, dob) AS (SELECT f_parties.case_id, dob FROM f_parties, f_charges
								   ON f_parties.case_id=f_charges.case_id),
raw_data(filing_year, age) AS
    (SELECT strftime('%Y', filing_date) AS filing_year,
            (strftime('%Y.%m%d', filing_date) - strftime('%Y.%m%d', dob)) AS age
     FROM f_cases, interest_parties ON f_cases.case_id=interest_parties.case_id)
SELECT filing_year, avg(age) FROM raw_data
WHERE age>0 AND age<100
GROUP BY filing_year
ORDER BY filing_year DESC
LIMIT 5;


requirement:
Look at the disposition to pick only Guilty parties (charges.disposition = 'Guilty'). 
Ensure that you only fetch the cases whose filing date is not empty. 
Also, ensure that you only fetch the parties who are defendants (parties.type = Defendant), 
whose name is not empty, 
whose date of birth is not empty, 
and whose computed age is greater than 0 and less than 100 years.
List the tuples in descending order with respect to the filing year and only display 5 tuples.



