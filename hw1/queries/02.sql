--- Count the number of cases related to repeated phone calls.

select count(case_id)
from charges
where description like '%PHONE%' ;
