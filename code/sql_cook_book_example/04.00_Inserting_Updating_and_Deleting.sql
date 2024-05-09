-- Inserting a New Record
insert into dept (deptno, dname, loc)
values (1, 'A', 'B'),
       (2, 'B', 'C');

insert into dept
values (50, 'PROGRAMMING', 'BALTIMORE');

-- Inserting Default Values
create table D
(
    id integer default 0
);
insert into D (id)
values (default);

-- Copying Rows from One Table into Another
insert into dept_east (deptno, dname, loc)
select deptno, dname, loc
from dept
where loc in ('NEW YORK', 'BOSTON');

-- Copying a Table Definition
create table dept_2
as
select *
from dept
where 1 = 0;

-- Updating When Corresponding Rows Exist
update emp
set sal = sal * 1.20
where empno in (select empno from emp_bonus)

-- Updating with Values from Another Table
update emp
set sal  = ns.sal,
    comm = ns.sal / 2
from new_sal ns
where ns.deptno = emp.deptno;

-- Deleting All Records from a Table
delete
from emp;

-- Deleting Specific Records
delete
from emp
where deptno = 10;

-- Deleting Referential Integrity Violations
delete
from emp
where deptno not in (select deptno from dept);

-- Deleting Duplicate Records
delete
from dupes
where id not in (select min(id)
                 from dupes
                 group by name);

--
