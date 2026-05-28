-- @common/fn_get_environment_name.sql

CREATE OR REPLACE FUNCTION fn_get_environment_name()
RETURNS text 
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN 'dev1';
END;
$$;