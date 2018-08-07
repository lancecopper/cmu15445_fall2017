--- Top zip codes with most criminals

select parties.zip, count(parties.case_id) as case_count 
from parties, charges
where parties.case_id = charges.case_id
and parties.zip <> ''
group by parties.zip
order by case_count desc 
limit 3
;
