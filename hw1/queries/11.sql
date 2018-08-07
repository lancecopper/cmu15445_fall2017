--- Attorney performance

with attorney_table as
(
select charges.case_id as case_id, disposition, name from charges, attorneys
where charges.case_id = attorneys.case_id
and name <> ''
),
aggregate_table as
(
select count(case_id) as total_count, name
from attorney_table
group by name
),
success_table as
(
select count(case_id) as success_count, name
from attorney_table
where disposition = 'Not Guilty'
group by name
)
select aggregate_table.name, total_count, (success_count * 100.0/total_count) as success_percent 
from aggregate_table, success_table
where aggregate_table.name = success_table.name and total_count > 100
order by success_percent desc, total_count desc
limit 5
;
