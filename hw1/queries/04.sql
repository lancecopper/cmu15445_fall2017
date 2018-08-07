--- Cases filed in the 1950s

select case_id, filing_date
from cases
where filing_date >= '1950-01-01' and filing_date < '1960-01-01' 
order by filing_date
limit 3
;
