-- @common/global_params.sql

INSERT INTO global_params (component_name, param_name, param_value) 
VALUES 
    ('comp1', 'param1', '4'),
    ('comp1', 'param2', '5'),
    ('comp1', 'param3', '6')
ON CONFLICT (component_name, param_name)
DO UPDATE SET
    param_value = EXCLUDED.param_value;
