CREATE OR REPLACE FUNCTION fibonacci(n integer) 
RETURNS bigint AS $$
DECLARE
    a bigint := 0;
    b bigint := 1;
    temp bigint;
BEGIN
    IF n <= 0 THEN RETURN 0; END IF;
    IF n = 1 THEN RETURN 1; END IF;
    FOR i IN 2..n LOOP
        temp := a + b;
        a := b;
        b := temp;
    END LOOP;
    RETURN b;
    
END;
$$ LANGUAGE plpgsql;