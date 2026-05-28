DROP TABLE IF EXISTS global_params;

CREATE TABLE global_params (
    component_name VARCHAR(100),
    param_name VARCHAR(100),
    param_value VARCHAR(100),
    PRIMARY KEY (component_name, param_name)
);

INSERT INTO global_params (component_name, param_name, param_value) 
VALUES 
    ('comp1', 'param1', '1'),
    ('comp1', 'param2', '2'),
    ('comp1', 'param3', '3');

