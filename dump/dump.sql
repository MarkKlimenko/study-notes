-- docker exec -it go-example-postgres bash
-- psql -U postgres
create table accounts
(
    balance int
);
alter table accounts
    add acctnum int;
insert into accounts (balance, acctnum)
values (100, 11111),
       (200, 22222);

begin;


(select city, length(city) from station order by length(city) desc limit 1)
UNION
select city, length(city)
from station
order by length(city) desc
limit 1;

select distinct city
from station
where lower(left(city, 1)) in ('a', 'e', 'i', 'o', 'u')
  AND lower(right(city, 1)) in ('a', 'e', 'i', 'o', 'u');

-- ===============================
select name
from employee
order by name;

select name
from employee
where salary > 2000
  AND months < 10
order by employee_id asc;

-- select  round(sum(salary) - sum(...)) from EMPLOYEES;
select replace(CAST(salary AS varchar), '0', '')
from employees;
select ceiling(avg(salary) - avg(replace(salary, '0', '')))
from employees;

-- ===============================
select sum(population)
from city
where district = 'California';
select round(avg(population))
from city;
select sum(population)
from city
where COUNTRYCODE = 'JPN';


select sm,
       count(sm)
from (select salary * months sm from Employee) smt
group by sm
order by sm desc
limit 1;

select salary * months,
       count(1)
from Employee
group by salary * months
order by 1 desc
limit 1;


select CASE
           WHEN (a + b) <= c OR (b + c) <= a OR (c + a) <= b THEN 'Not A Triangle'
           WHEN a = b and b = c and c = a THEN 'Equilateral'
           WHEN a = b OR b = c OR c = a THEN 'Isosceles'
           WHEN a != b and b != c and c != a THEN 'Scalene'
           ELSE 'Error' END
from TRIANGLES;


-- =====
select concat(name, '(', left(occupation, 1), ')')
from occupations
order by 1 asc;

select concat('There are a total of ', count(*), ' ', lower(occupation), 's.')
from occupations
group by occupation
order by count(*), occupation;


select round(max(LAT_N), 4)
from station
where LAT_N < 137.2345;

select round(LONG_W, 4)
from station
where LAT_N < 137.2345
order by LAT_N desc
limit 1;

select round(sqrt(power(min(LAT_N) - max(LAT_N), 2) + power(min(LONG_W) - max(LONG_W), 2)), 4)
from station;


select sum(ci.population)
from city ci
         left join country co on ci.countrycode = co.code
where co.continent = 'Asia';

select continent, avg(population)
from city
         left join country on countrycode = code
group by continent;

select co.continent, floor(avg(ci.population))
from city ci
         join country co on ci.countrycode = co.code
group by co.continent;


set @count = 20;
select repeat('* ', @count := @count - 1)
from information_schema.tables
limit 20;

set @count = 0;
select repeat('* ', @count := @count + 1)
from information_schema.tables
limit 20;



select bst_outer.N,
       CASE
           WHEN P is NULL then 'Root'
           WHEN (select count(p) from bst where p = bst_outer.n) > 0 then 'Inner'
           ELSE 'Leaf'
           END
from bst bst_outer
order by bst_outer.N;

select CASE
           WHEN grade < 8 then NULL
           ELSE name
           END,
       grade,
       marks
from students
         join grades on marks >= min_mark AND marks <= max_mark
order by grade desc, name asc, marks asc;



select company_code,
       founder name

from company
         left join Lead_Manager using (company_code)
         left join Senior_Manager using (company_code)
         left join Manager using (company_code)
         left join Employee using (company_code)
order by company_code asc;


select c.company_code,
       c.founder,
       count(distinct lm.Lead_Manager_code),
       count(distinct sm.Senior_Manager_code),
       count(distinct m.Manager_code),
       count(distinct e.Employee_code)
from company c
         join Lead_Manager lm using (company_code)
         join Senior_Manager sm using (company_code)
         join Manager m using (company_code)
         join Employee e using (company_code)
group by c.company_code, c.founder
order by c.company_code asc;


select distinct c.company_code,
                c.founder,
                count(distinct lm.Lead_Manager_code),
                count(distinct sm.Senior_Manager_code),
                count(distinct m.Manager_code),
                count(distinct e.Employee_code)
from company c
         right join Lead_Manager lm using (company_code)
         right join Senior_Manager sm using (company_code)
         right join Manager m using (company_code)
         right join Employee e using (company_code)
group by c.company_code, c.founder
order by c.company_code asc;

select distinct c.company_code,
                c.founder,
                count(distinct lm.Lead_Manager_code),
                count(distinct sm.Senior_Manager_code),
                count(distinct m.Manager_code),
                count(distinct e.Employee_code)
from company c
         right join Lead_Manager lm on c.company_code = lm.company_code
         right join Senior_Manager sm on c.company_code = sm.company_code
         right join Manager m on c.company_code = m.company_code
         right join Employee e on c.company_code = e.company_code
group by c.company_code, c.founder
order by c.company_code asc;



select distinct c.company_code,
                c.founder,
                count(distinct lm.Lead_Manager_code),
                count(distinct sm.Senior_Manager_code),
                count(distinct m.Manager_code),
                count(distinct e.Employee_code)
from company c
         right join Lead_Manager lm on c.company_code = lm.company_code
         right join Senior_Manager sm on lm.Lead_Manager_code = sm.Lead_Manager_code
         right join Manager m on sm.Senior_Manager_code = m.Senior_Manager_code
         right join Employee e on m.Manager_code = e.Manager_code
group by c.company_code, c.founder
order by c.company_code asc;


-- ============================================================
select hacker_id, name
from (select ha.hacker_id hacker_id,
             ha.name      name,
             CASE
                 WHEN df.score = sb.score then true
                 ELSE false
                 END      is_passed
      from Hackers ha
               join Submissions sb on sb.hacker_id = ha.hacker_id
               join Challenges ch on ch.challenge_id = sb.challenge_id
               join Difficulty df on ch.difficulty_level = df.difficulty_level) results
where is_passed = true
group by hacker_id, name
having count(is_passed) > 1
order by count(is_passed) desc, hacker_id asc;


select ha.hacker_id, ha.name
from Hackers ha
         join Submissions sb on sb.hacker_id = ha.hacker_id
         join Challenges ch on ch.challenge_id = sb.challenge_id
         join Difficulty df on ch.difficulty_level = df.difficulty_level
where df.score = sb.score
group by ha.hacker_id, ha.name
having count(ha.hacker_id) > 1
order by count(ha.hacker_id) desc, hacker_id asc;



WITH temp (hacker_id, name, challenges_count) as (
    select hacker_id, name, count(challenge_id)
    from Hackers
             join Challenges using(hacker_id)
    group by hacker_id, name
    order by count(challenge_id) desc, hacker_id
)
select * from temp
where
        (select count(*) from temp sub where sub.challenges_count = challenges_count) <= 1
   OR challenges_count = max(challenges_count);


WITH temp (hacker_id, name, challenges_count) as (
    select hacker_id, name, count(challenge_id)
    from Hackers
             join Challenges using(hacker_id)
    group by hacker_id, name
    order by count(challenge_id) desc, hacker_id
)
select * from temp m_temp
where
        m_temp.challenges_count = (select max(challenges_count) from temp)
   OR (select count(sub.challenges_count) from temp sub where sub.challenges_count = m_temp.challenges_count) <= 1;


select id, age, coins_needed, power,
       min(coins_needed) over(partition by age, power) as min_coins_needed
from Wands
         join Wands_Property using(code)
where is_evil = 0
order by power desc, age desc;


-- ==== select in select
WITH temp (id, age, coins_needed, power, min_coins_needed) as (
    select id, age, coins_needed, power,
           (select min(coins_needed)
            from Wands int_w
                     join Wands_Property int_wp using(code)
            where is_evil = 0 AND int_wp.age = wp.age AND int_w.power = w.power) min_coins_needed
    from Wands w
             join Wands_Property wp using(code)
    where is_evil = 0
    order by power desc, age desc
)
select id, age, coins_needed, power
from temp
where coins_needed = min_coins_needed;


-- ==== window function
WITH temp as (
    select id, age, coins_needed, power,
           min(coins_needed) over(partition by age, power) as min_coins_needed
    from Wands
             join Wands_Property using(code)
    where is_evil = 0
    order by power desc, age desc
)
select id, age, coins_needed, power
from temp
where coins_needed = min_coins_needed;


with temp as (
    select hacker_id, name, challenge_id, max(score) max_score
    from Hackers
             join Submissions using(hacker_id)
    group by hacker_id, name, challenge_id
)
select hacker_id, name, sum(max_score)
from temp
group by hacker_id, name
having sum(max_score) > 0
order by sum(max_score) desc, hacker_id asc;


CREATE OR REPLACE FUNCTION NthHighestSalary(N INT) RETURNS TABLE (Salary INT) AS $$
BEGIN

    if N - 1 < 0 then
        RETURN null;
    end if;

    RETURN QUERY (
        -- Write your PostgreSQL query statement below.
        select distinct e.salary
        from Employee e
        order by e.salary desc
        limit case
                  when N - 1 < 0 then 0
                  else 1
            end
            offset case
                       when N - 1 < 0 then 0
                       else N -1
            end
    );
END;
$$ LANGUAGE plpgsql;


select distinct p.product_id,
                coalesce((
                             select pin.new_price
                             from Products pin
                             where p.product_id = pin.product_id
                               and pin.change_date <= '2019-08-16'
                             order by pin.change_date desc
                             limit 1
                         ), 10)
                    price
from Products p;


select distinct product_id,
                FIRST_VALUE(new_price) OVER (
        PARTITION BY product_id
        where change_date <= '2019-08-16'
        ORDER BY change_date desc
    )
    price
from Products p;



with alltrn as (
    select
        to_char(trans_date, 'YYYY-MM') as month,
        country,
        count(*) trans_count,
        sum(amount) trans_total_amount
    from Transactions
    group by month, country
)
with apprtrn as (
    select
        to_char(trans_date, 'YYYY-MM') as month,
        country,
        count(*) approved_count,
        sum(amount) approved_total_amount
    from Transactions
    where state = 'approved'
    group by month, country
)

select alltrn.month, alltrn.country, trans_count, approved_count, trans_total_amount, approved_total_amount
from alltrn
join apprtrn using(month, country);
