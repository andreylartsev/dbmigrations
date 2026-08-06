-- first test
SELECT
  (
    SELECT
      max(v1)
    FROM
      t1) 
  = (
    SELECT
      v1
    FROM
      latest_t1);