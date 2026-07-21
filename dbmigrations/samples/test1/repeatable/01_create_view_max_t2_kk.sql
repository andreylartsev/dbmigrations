drop view if exists max_t2_kk;

create view max_t2_kk as   
    select max(kk) as kk from t2; 