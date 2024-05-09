-- Computing an Average
select avg(sal) as avg_sal
from emp;

select deptno, avg(sal) as avg_sal
from emp
group by deptno;

-- Finding the Min/Max Value in a Column
select deptno, min(sal) as min_sal, max(sal) as max_sal
from emp
group by deptno;

-- Summing the Values in a Column
select deptno, sum(sal) as total_for_dept
from emp
group by deptno;

-- Counting Rows in a Table
select deptno, count(*)
from emp
group by deptno;

-- Generating a Running Total
select ename,
       sal,
       sum(sal) over (order by sal,empno) as running_total
from emp
order by 2;

--
