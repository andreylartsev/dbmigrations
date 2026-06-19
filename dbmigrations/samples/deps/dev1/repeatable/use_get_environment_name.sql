-- @depends_on ./fn_get_environment_name.sql

DO $$
DECLARE
    v_env VARCHAR := fn_get_environment_name();
BEGIN
    RAISE NOTICE 'Environment name is %', v_env;
END
$$;
