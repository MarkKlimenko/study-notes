-- Retrieving All Rows and Columns from a Table
select *
from emp;

-- Retrieving All (specified) Rows and Columns from a Table
select empno,
       ename,
       job,
       sal,
       mgr,
       hiredate,
       comm,
       deptno
from emp;

-- Retrieving a Subset of Rows from a Table
select *
from emp
where deptno = 10;

-- Finding Rows That Satisfy Multiple Conditions
select *
from emp
where deptno = 10
   or comm is not null
   or sal <= 2000 and deptno = 20;

-- Providing Meaningful Names for Columns
select sal salary, comm commission
from emp;

-- Referencing an Aliased Column in the WHERE Clause (FAIL)
-- There are no ability to use alias in WHERE Clause in this query
select sal salary, comm commission
from emp
where salary < 5000;

-- Referencing an Aliased Column in the WHERE Clause
select *
from (select sal as salary, comm as commission
      from emp) x
where salary < 5000;

with temp as (select sal as salary, comm as commission
              from emp)
select *
from temp
where salary < 5000;

-- Concatenating Column Values
select ename || ' WORKS AS A ' || job as msg
from emp
where deptno = 10;

-- Using Conditional Logic in a SELECT Statement
select ename,
       sal,
       case
           when sal <= 2000 then 'UNDERPAID'
           when sal >= 4000 then 'OVERPAID'
           else 'OK'
           end as status
from emp;

-- Limiting the Number of Rows Returned
select *
from emp
limit 5;

-- Returning n Random Records from a Table
select *
from emp
order by random()
limit 5;

-- Finding Null Values
select *
from emp
where comm is null;

/*
 NULL is never equal/not equal to anything, not even itself;
 therefore, you cannot use = or != for testing whether a column is NULL.
 To determine whether a row has NULL values, you must use IS NULL.
 You can also use IS NOT NULL to find rows without a null in a given column.
*/

-- Transforming Nulls into Real Values
select coalesce(comm, 0)
from emp;

-- Searching for Patterns
select ename, job
from emp
where deptno in (10, 20)
  and (ename like '%I%' or job like '%ER');
