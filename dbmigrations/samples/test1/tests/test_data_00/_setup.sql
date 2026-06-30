CREATE TEMP TABLE test_data_00 (
  key int,
  val VARCHAR(20),
  PRIMARY KEY (key)
) ON COMMIT DROP;

INSERT INTO test_data_00
VALUES
  (10, 'abc'),
  (20, 'def');
