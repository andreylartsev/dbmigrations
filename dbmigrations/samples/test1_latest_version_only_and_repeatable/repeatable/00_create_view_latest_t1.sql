drop view if exists latest_t1;

create view latest_t1 as 
    select max(v1) as v1 from t1;