create table t1(t1v1 int, t1v2 int);
    create table t2(t2v1 int, t2v3 int);
    insert into t1 values (0, 0), (1, 1), (2, 2);
    insert into t2 values (0, 200), (1, 201), (2, 202);
select t1v1 from (select t1v1 from (select t1v1 from t1));
