-- test if any of required records are missing 
WITH required_keys(id) AS (
	VALUES 
		('1'), 
		('2')
)
SELECT id
  FROM required_keys i

EXCEPT

SELECT kk FROM t2; 
