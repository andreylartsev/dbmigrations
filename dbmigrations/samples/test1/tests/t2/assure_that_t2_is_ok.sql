-- test if table exists 
select 1 from t2 where 2=1;

-- test if fields within table exists and have correct type
select 
    kk = '1'::varchar, 
    created_at = now()::timestamptz
from t2 
where 2=1; 