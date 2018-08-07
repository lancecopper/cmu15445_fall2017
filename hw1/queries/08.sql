--- Average age of guilty criminals over time

with filing_year_table as
(
select cases.case_id, strftime('%Y', filing_date) as filing_year
from cases, charges
where cases.case_id = charges.case_id
and charges.disposition = 'Guilty'
and cases.filing_date <> ''
),
age_table as
(
select cases.case_id,
strftime('%Y.%m%d', cases.filing_date) - strftime('%Y.%m%d', parties.dob) as age from cases, parties
where cases.case_id = parties.case_id
and parties.type = 'Defendant'
and parties.name <> ''
and parties.dob is not null
and age > 0
and age < 100
)
select filing_year_table.filing_year, avg(age_table.age) as average_age
from cases, filing_year_table, age_table
where cases.case_id = filing_year_table.case_id
and cases.case_id = age_table.case_id
group by filing_year_table.filing_year
order by filing_year desc
limit 5
;
