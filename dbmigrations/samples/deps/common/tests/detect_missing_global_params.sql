WITH required_params (component_name, param_name) AS (
    VALUES 
        ('comp1', 'param1'),
        ('comp1', 'param2'),
        ('comp1', 'param3'),  
        ('comp1', 'param4')  
)
SELECT component_name, param_name FROM required_params

EXCEPT

SELECT component_name, param_name 
    FROM global_params
;
