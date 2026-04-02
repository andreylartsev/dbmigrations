drop view if exists latest_t2;

create view latest_t2 as 
    select max(created_at) as v1 from t2;
