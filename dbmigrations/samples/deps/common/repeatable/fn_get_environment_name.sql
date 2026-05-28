CREATE OR REPLACE FUNCTION fn_get_environment_name()
RETURNS text 
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RAISE EXCEPTION 'Environment name is undefined!';
END;
$$;