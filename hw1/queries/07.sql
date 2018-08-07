--- List the top 3 parties who have been
--- charged in the most number of distinct counties.

select parties.name, count(distinct(cases.violation_county)) as cnt 
from cases, parties
where cases.case_id = parties.case_id
and parties.type = 'Defendant'
and parties.name <> '' group by parties.name order by cnt desc
limit 3
;
