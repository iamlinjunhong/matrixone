drop table if exists t1;
create table t1 (a int, b datetime);
insert into t1 values(1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13');
select sum(a) over(partition by a order by b range between interval 1 day preceding and interval 2 day following) from t1;

drop table if exists t1;
create table t1 (a int, b date);
insert into t1 values(1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13'), (1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13'), (1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13');
select max(a) over(order by b range between interval 1 day preceding and interval 2 day following) from t1;

drop table if exists t1;
create table t1 (a int, b time);
insert into t1 values(1, 112233), (2, 122233), (3, 132233), (1, 112233), (2, 122233), (3, 132233), (1, 112233), (2, 122233), (3, 132233);
select min(a) over(order by b range between interval 1 hour preceding and current row) from t1;

drop table if exists t1;
create table t1 (a int, b timestamp);
insert into t1 values(1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13'), (1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13'), (1, '2020-11-11'), (2, '2020-11-12'), (3, '2020-11-13');
select count(*) over(order by b range current row) from t1;

drop table if exists t1;
create table t1 (a int, b int, c int);
insert into t1 values(1, 2, 1), (3, 4, 2), (5, 6, 3), (7, 8, 4), (3, 4, 5), (3, 4, 6), (3, 4, 7);
select a, rank() over (partition by a) from t1 group by a, c;
select a, c, rank() over (partition by a order by c) from t1 group by a, c;
select a, c, rank() over (partition by a order by c) from t1 group by a, c;
select a, c, b, rank() over (partition by a, c, b) from t1;
select a,  b, rank() over (partition by a, b) from t1;
select a, c, sum(a) over (), sum(c) over () from t1;
select a, c, sum(a) over (order by c), sum(c) over (order by a) from t1;
select a, sum(b), sum(sum(b)) over (partition by a), sum(sum(b)) over (partition by c) from t1 group by a, c;
select a, sum(b), rank() over (partition by a +1), rank() over (partition by c), c from t1 group by a, c;
select a, sum(b), sum(sum(b))  over (partition by a) as o from t1 group by a, c;
select a, sum(b), cast(sum(sum(b))  over (partition by a+1 order by a+1 rows between 2  preceding and CURRENT row) as float) as o from t1 group by a, c;
select a, sum(b), sum(sum(b)) over (partition by a rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) from
t1 group by a, c;
select a, sum(a) over (partition by c order by b range BETWEEN 3 preceding and 4 following), c, b from t1;
select a, sum(a) over (order by a) from t1;
select a, rank() over (partition by a) from t1;
select a, rank() over () from t1;
select a, sum(a) over (partition by a rows current row) from t1;
select c, sum(c) over (order by c range between 1 preceding and 1 following) from t1;
select c, sum(100) over (order by c range between 1 preceding and 1 following), a, b from t1;
select c, sum(null) over (order by c range between 1 preceding and 1 following), a, b from t1;
select a, b, c, rank() over (partition by a, b order by c) from t1;
select a, c, rank() over(partition by a order by c rows current row) from t1;

drop table if exists t1;
create table t1 (a int, b decimal(7, 2));
insert into t1 values(1, 12.12), (2, 123.13), (3, 456.66), (4, 1111.34);
select a, sum(b) over (partition by a order by a) from t1;