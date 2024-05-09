-- Stacking One Rowset atop Another UNION ALL
select ename as ename_and_dname, deptno
from emp
where deptno = 10
union all
select '----------', null
from t1
union all
select dname, deptno
from dept;

-- Combining Related Rows
select e.ename, d.loc
from emp e,
     dept d
where e.deptno = d.deptno
  and e.deptno = 10;

select e.ename, d.loc
from emp e
         join dept d on e.deptno = d.deptno
where e.deptno = 10;

-- Finding Rows in Common Between Two Tables
drop view if exists v;
create or replace view V
as
select ename, job, sal
from emp
where job = 'CLERK';

select *
from V;

select e.empno, e.ename, e.job, e.sal, e.deptno
from emp e
         join V
              on (e.ename = v.ename
                  and e.job = v.job
                  and e.sal = v.sal);

-- Retrieving Values from One Table That Do Not Exist in Another
select deptno
from dept
except
select deptno
from emp;

select deptno
from dept
where deptno not in (select deptno from emp);

-- Retrieving Rows from One Table That Do Not Correspond to Rows in Another
select d.*
from dept d
         left join emp e on (d.deptno = e.deptno)
where e.deptno is null;

-- Adding Joins to a Query Without Interfering with Other Joins
create table emp_bonus
(
    EMPNO    int,
    RECEIVED date,
    TYPE     int
);

insert into emp_bonus(EMPNO, RECEIVED, TYPE)
values (7369, '14-MAR-2005', 1),
       (7900, '14-MAR-2005', 2),
       (7788, '14-MAR-2005', 3);

select *
from emp_bonus;

select e.ename, d.loc, eb.received
from emp e
         join dept d
              on (e.deptno = d.deptno)
         left join emp_bonus eb
                   on (e.empno = eb.empno)
order by 2;

-- Performing Joins When Using Aggregates
truncate table emp_bonus;

insert into emp_bonus (EMPNO, RECEIVED, TYPE)
values (7934, '17-MAR-2005', 1),
       (7934, '15-FEB-2005', 2),
       (7839, '15-FEB-2005', 3),
       (7782, '15-FEB-2005', 1);

select *
from emp_bonus;

select *
from emp;

select e.empno,
       e.ename,
       e.sal,
       e.deptno,
       e.sal * case
                   when eb.type = 1 then .1
                   when eb.type = 2 then .2
                   else .3
           end as bonus
from emp e
         join emp_bonus eb
              on e.empno = eb.empno
                  and e.deptno = 10;

-- wrong join

select deptno,
       sum(sal)   as total_sal,
       sum(bonus) as total_bonus
from (select e.empno,
             e.ename,
             e.sal,
             e.deptno,
             e.sal * case
                         when eb.type = 1 then .1
                         when eb.type = 2 then .2
                         else .3
                 end as bonus
      from emp e,
           emp_bonus eb
      where e.empno = eb.empno
        and e.deptno = 10) x
group by deptno;

-- sum(sal) is not equals
select sum(sal)
from emp
where deptno = 10;

-- Using NULLs in Operations and Comparisons
-- For
-- example, you want to find all employees in EMP whose commission (COMM) is less
-- than the commission of employee WARD. Employees with a NULL commission
-- should be included as well.

-- without NULL
select ename, comm
from emp
where comm < (select comm
                           from emp
                           where ename = 'WARD');

-- with NULL
select ename, comm
from emp
where coalesce(comm, 0) < (select comm
                           from emp
                           where ename = 'WARD');
