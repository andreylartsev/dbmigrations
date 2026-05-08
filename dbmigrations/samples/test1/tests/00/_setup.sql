create temp table test_data (key int, val varchar(20), primary key(key)) on commit drop;
insert into  test_data values (10, 'abc'), (20, 'def');
-- select * from test_data;