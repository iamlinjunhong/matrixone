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
select a, row_number() over (partition by a) from t1 group by a, c;
select a, c, row_number() over (partition by a order by c) from t1 group by a, c;
select a, c, row_number() over (partition by a order by c) from t1 group by a, c;
select a, c, b, row_number() over (partition by a, c, b) from t1;
select a,  b, row_number() over (partition by a, b) from t1;
select a, sum(b), row_number() over (partition by a +1), row_number() over (partition by c), c from t1 group by a, c;
select a, row_number() over (partition by a) from t1;
select a, row_number() over () from t1;
select a, b, c, row_number() over (partition by a, b order by c) from t1;
select a, c, row_number() over(partition by a order by c rows current row) from t1;
select a, dense_rank() over (partition by a) from t1 group by a, c;
select a, c, dense_rank() over (partition by a order by c) from t1 group by a, c;
select a, c, dense_rank() over (partition by a order by c) from t1 group by a, c;
select a, c, b, dense_rank() over (partition by a, c, b) from t1;
select a,  b, dense_rank() over (partition by a, b) from t1;
select a, sum(b), dense_rank() over (partition by a +1), dense_rank() over (partition by c), c from t1 group by a, c;
select a, dense_rank() over (partition by a) from t1;
select a, dense_rank() over () from t1;
select a, b, c, dense_rank() over (partition by a, b order by c) from t1;
select a, c, dense_rank() over(partition by a order by c rows current row) from t1;
select a, c, rank() over(order by a), row_number() over(order by a), dense_rank() over(order by a) from t1;

drop table if exists t1;
create table t1 (a int, b decimal(7, 2));
insert into t1 values(1, 12.12), (2, 123.13), (3, 456.66), (4, 1111.34);
select a, sum(b) over (partition by a order by a) from t1;

drop table if exists wf01;
create table wf01(i int,j int);
insert into wf01 values(1,1);
insert into wf01 values(1,4);
insert into wf01 values(1,2);
insert into wf01 values(1,4);
select * from wf01;
select i, j, sum(i+j) over (order by j desc rows unbounded preceding) foo from wf01;
select i, j, sum(i+j) over (order by j desc rows between 2 preceding and 2 following) as foo from wf01;
select i, j, sum(i+j) over (order by j desc rows unbounded preceding) foo from wf01 order by foo;
select i, j, sum(i+j) over (order by j desc rows unbounded preceding) foo from wf01 order by foo desc;

drop table if exists wf08;
create table wf08(d decimal(10,2), date date);
insert into wf08 values (10.4, '2002-06-09');
insert into wf08 values (20.5, '2002-06-09');
insert into wf08 values (10.4, '2002-06-10');
insert into wf08 values (3,    '2002-06-09');
insert into wf08 values (40.2, '2015-08-01');
insert into wf08 values (40.2, '2002-06-09');
insert into wf08 values (5,    '2015-08-01');
select * from (select rank() over (order by d) as `rank`, d, date from wf08) alias order by `rank`, d, date;
select * from (select dense_rank() over (order by d) as `d_rank`, d, date from wf08) alias order by `d_rank`, d, date;

drop table if exists wf07;
create table wf07 (user_id integer not null, date date);
insert into wf07 values (1, '2002-06-09');
insert into wf07 values (2, '2002-06-09');
insert into wf07 values (1, '2002-06-09');
insert into wf07 values (3, '2002-06-09');
insert into wf07 values (4, '2002-06-09');
insert into wf07 values (4, '2002-06-09');
insert into wf07 values (5, '2002-06-09');
select rank() over () r from wf07;
select dense_rank() over () r from wf07;

drop table if exists wf12;
create table wf12(d double);
insert into wf12 values (1.7976931348623157e+307);
insert into wf12 values (1);
select d, sum(d) over (rows between current row and 1 following) from wf12;

drop table if exists wf06;
create table wf06 (id integer, sex char(1));
insert into wf06 values (1, 'm');
insert into wf06 values (2, 'f');
insert into wf06 values (3, 'f');
insert into wf06 values (4, 'f');
insert into wf06 values (5, 'm');
drop table if exists wf07;
create table wf07 (user_id integer not null, date date);
insert into wf07 values (1, '2002-06-09');
insert into wf07 values (2, '2002-06-09');
insert into wf07 values (1, '2002-06-09');
insert into wf07 values (3, '2002-06-09');
insert into wf07 values (4, '2002-06-09');
insert into wf07 values (4, '2002-06-09');
insert into wf07 values (5, '2002-06-09');
select id value, sum(id) over (rows unbounded preceding) from wf06 inner join wf07 on wf07.user_id = wf06.id;

drop table if exists row01;
create table row01(i int,j int);
insert into row01 values(1,1);
insert into row01 values(1,4);
insert into row01 values(1,2);
insert into row01 values(1,4);
select i, j, sum(i+j) over (order by j rows between 2 preceding and 1 preceding) foo from row01 order by foo desc;
select i, j, sum(i+j) over (order by j rows between 2 following and 1 following) foo from row01 order by foo desc;

drop table if exists test01;
create table test01(i int, j int);
insert into test01 values (1,null);
insert into test01 values (1,null);
insert into test01 values (1,1);
insert into test01 values (1,null);
insert into test01 values (1,2);
insert into test01 values (2,1);
insert into test01 values (2,2);
insert into test01 values (2,null);
insert into test01 values (2,null);
select i, j, min(j) over (partition by i order by j rows unbounded preceding) from test01;

drop table if exists double01;
create table double01(d double);
insert into double01 values (2);
insert into double01 values (2);
insert into double01 values (3);
insert into double01 values (1);
insert into double01 values (1);
insert into double01 values (1.2);
insert into double01 values (null);
insert into double01 values (null);
select d, sum(d) over (partition by d order by d), avg(d) over (order by d rows between 1 preceding and 1 following) from double01;
select d, sum(d) over (partition by d order by d), avg(d) over (order by d rows between 2 preceding and 1 following) from double01;

drop table if exists wf01;
create table wf01(d float);
insert into wf01 values (10);
insert into wf01 values (1);
insert into wf01 values (2);
insert into wf01 values (3);
insert into wf01 values (4);
insert into wf01 values (5);
insert into wf01 values (6);
insert into wf01 values (7);
insert into wf01 values (8);
insert into wf01 values (9);
select d, sum(d) over (order by d range between current row and 2 following), avg(d) over (order by d range between current row and 2 following) from wf01;
select d, sum(d) over (order by d range between 2 preceding and 2 following), avg(d) over (order by d range between current row and 2 following) from wf01;

drop table if exists dense_rank01;
create table dense_rank01 (id integer, sex char(1));
insert into dense_rank01 values (1, 'm');
insert into dense_rank01 values (2, 'f');
insert into dense_rank01 values (3, 'f');
insert into dense_rank01 values (4, 'f');
insert into dense_rank01 values (5, 'm');
select sex, id, rank() over (partition by sex order by id desc) from dense_rank01;
select sex, id, dense_rank() over (partition by sex order by id desc) from dense_rank01;