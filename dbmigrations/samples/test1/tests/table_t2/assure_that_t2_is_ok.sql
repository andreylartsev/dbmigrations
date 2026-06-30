-- test if table exists
SELECT 1 FROM t2 WHERE 2 = 1;

-- test if fields within table exists and have correct type
SELECT
  kk = NULL::VARCHAR,
  created_at = NULL::TIMESTAMPTZ
FROM
  t2
WHERE
  2 = 1;
