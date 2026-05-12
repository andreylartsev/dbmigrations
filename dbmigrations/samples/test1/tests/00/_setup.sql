CREATE TEMP TABLE test_data (
  key int,
  val VARCHAR(20),
  PRIMARY KEY (key)
) ON COMMIT DROP;

INSERT INTO test_data
VALUES
  (10, 'abc'),
  (20, 'def');
