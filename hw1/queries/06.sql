--- Fraction of cases closed statistically

select statistically_closed_cases.cnt * 100.0 / all_cases.cnt as percentage from
(
select count(case_id) as cnt
from cases
where status = 'Case Closed Statistically' )
as statistically_closed_cases,
(
select count(case_id) as cnt
from cases
)
as all_cases
;
