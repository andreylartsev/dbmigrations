WITH required_records (v1) AS (
    VALUES 
        (6),
        (4),
        (3),
        (2),
        (1)
)
SELECT v1 FROM required_records

EXCEPT

SELECT v1 
    FROM t1
;
