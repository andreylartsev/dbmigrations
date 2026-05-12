-- first test
SELECT
  (
    SELECT
      max(v1)
    FROM
      test1.t1) 
  = (
    SELECT
      v1
    FROM
      test1.latest_t1);

-- second test
SELECT
  (
    SELECT
      min(v1)
    FROM
      test1.t1)
  = (
    SELECT
      v1
    FROM
      test1.latest_t1);

