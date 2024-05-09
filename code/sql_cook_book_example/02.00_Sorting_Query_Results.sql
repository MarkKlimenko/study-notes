-- Returning Query Results in a Specified Order
select ename, job, sal
from emp
where deptno = 10
order by sal asc;

-- Sorting by Multiple Fields
select empno, deptno, sal, ename, job
from emp
order by deptno, sal desc;

-- Sorting by Substrings
select ename, job, right(job, 2)
from emp
order by right(job, 2);

-- Sorting Mixed Alphanumeric Data
create view V
as
select ename || ' ' || deptno as data
from emp;

select *
from V;

select data,
       replace(data, replace(translate(data, '0123456789', '##########'), '#', ''), '') nums,
       replace(translate(data, '0123456789', '##########'), '#', '')                    chars
from V;

-- ORDER BY DEPTNO
select *
from v
order by replace(data, replace(translate(data, '0123456789', '##########'), '#', ''), '');

-- ORDER BY ENAME
select *
from v
order by replace(translate(data, '0123456789', '##########'), '#', '');

-- Dealing with Nulls When Sorting

select ename, sal, comm
from emp
order by 3;

select ename, sal, comm
from emp
order by 3 desc;


select ename, sal, comm
from (select ename,
             sal,
             comm,
             case when comm is null then 0 else 1 end as is_null
      from emp) x
order by is_null desc, comm desc;

-- or
select ename, sal, comm
from emp
order by 3 desc nulls last;

-- Sorting on a Data-Dependent Key

select ename, sal, job, comm
from emp
order by case
             when job = 'SALESMAN' then comm
             else sal
             end;

-- or
select ename,
       sal,
       job,
       comm,
       case when job = 'SALESMAN' then comm else sal end as ordered
from emp
order by 5;
