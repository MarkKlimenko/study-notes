-- Adding and Subtracting Days, Months, and Years
select hiredate - interval '5 day'   as hd_minus_5D,
       hiredate + interval '5 day'   as hd_plus_5D,
       hiredate - interval '5 month' as hd_minus_5M,
       hiredate + interval '5 month' as hd_plus_5M,
       hiredate - interval '5 year'  as hd_minus_5Y,
       hiredate + interval '5 year'  as hd_plus_5Y
from emp
where deptno = 10;

-- Determining the Number of Days Between Two Dates
select ward_hd - allen_hd
from (select hiredate as ward_hd
      from emp
      where ename = 'WARD') x,
     (select hiredate as allen_hd
      from emp
      where ename = 'ALLEN') y;
