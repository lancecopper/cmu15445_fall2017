--- In which 3 decades did the most number of cases get filed?

select count(case_id) as case_count, 
substr(filing_date, 1, 3) || '0s' as decade from cases
where filing_date <> ''
group by decade
order by case_count desc 
limit 3
;

