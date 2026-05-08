-- test if any of required records are missing 
WITH required_keys(id) AS (
	VALUES 
		('1'), 
		('2')
)
SELECT id
  FROM required_keys i
  WHERE NOT EXISTS (
 	SELECT kk FROM t2 t where i.id = t.kk); 
