-- test if any of required keys are missing within the table
WITH required_keys(id) AS (
  VALUES 
  	(1),
	  (2)
)
SELECT id FROM required_keys

EXCEPT

SELECT v1 FROM t1;

-- second tests with result set
WITH required_keys(id) AS (
  VALUES 
	  (33)
)
SELECT id FROM required_keys

EXCEPT

SELECT v1 FROM t1;
