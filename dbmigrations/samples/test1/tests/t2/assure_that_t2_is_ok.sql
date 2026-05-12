-- test if table exists
SELECT
  1
FROM
  t2
WHERE
  2 = 1;

-- test if fields within table exists and have correct type
SELECT
  kk = '1'::VARCHAR,
  created_at = now()::TIMESTAMPTZ
FROM
  t2
WHERE
  2 = 1;
