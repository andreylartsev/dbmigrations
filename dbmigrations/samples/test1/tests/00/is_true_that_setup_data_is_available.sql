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

