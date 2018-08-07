--- Count the number of cases related to reckless endangerment in each county: 
--- Print county name and number of cases
--- Sort by number of cases (descending),
--- and break ties by county name (ascending),
--- and report only the top 3 counties.

select cases.violation_county, count(cases.case_id) as cnt 
from cases, charges
where cases.case_id = charges.case_id
and cases.violation_county <> ''
and charges.description like '%RECKLESS%' group by cases.violation_county
order by cnt desc, cases.violation_county asc limit 3
;
