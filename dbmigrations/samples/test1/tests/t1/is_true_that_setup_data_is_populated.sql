SELECT
  EXISTS (
    SELECT
      1
    FROM
      test_data_t1
    LIMIT 1);

SELECT
  true
FROM
  test_data_t1
WHERE
  key = 10
LIMIT 1;

SELECT
  EXISTS (
    SELECT
      1
    FROM
      test_data
    LIMIT 1);

SELECT
  true
FROM
  test_data
WHERE
  key = 10
LIMIT 1;

