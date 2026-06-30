-- table exists and contains some records
SELECT
  EXISTS (
    SELECT
      1
    FROM
      test_data
    LIMIT 1);

-- there are record with a key = 10
SELECT
  true
FROM
  test_data
WHERE
  key = 10
LIMIT 1;

-- there are record within the table test_data_00
SELECT
  EXISTS (
    SELECT
      1
    FROM
      test_data_00
    LIMIT 1);

-- there are record with a key = 10 within the table test_data_00
SELECT
  true
FROM
  test_data_00
WHERE
  key = 10
LIMIT 1;

