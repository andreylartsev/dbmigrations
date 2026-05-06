SELECT 
    (SELECT max(v1) FROM test1.t1) = (SELECT v1 from test1.latest_t1); 