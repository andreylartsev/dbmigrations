-- test if any of required keys are missing within the table
WITH required_keys(id) AS (
	VALUES 
		(1), 
		(2)
)
SELECT id
  FROM required_keys i
  WHERE NOT EXISTS (
 	SELECT v1 FROM t1 where i.id = t1.v1);