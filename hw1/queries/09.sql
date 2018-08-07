--- Disposition by race

with disposition_table as
(
select race, charges.disposition, parties.case_id as case_id
from charges, parties
where charges.case_id = parties.case_id
and race <> ''
and disposition in ('Guilty', 'Not Guilty')
and race in ('African American', 'Caucasian')
),
disposition_aggregate_table as
(
select disposition, count(case_id) as case_count
from disposition_table
group by disposition
),
race_aggregate_table as
(
select race, disposition, count(case_id) as case_count
from disposition_table
group by race, disposition
)
select race, race_aggregate_table.disposition,
(race_aggregate_table.case_count * 100.0) / disposition_aggregate_table.case_count 
from race_aggregate_table, disposition_aggregate_table
where race_aggregate_table.disposition = disposition_aggregate_table.disposition
;
