-- Task 1

DROP TABLE exch_quotes_archive;
CREATE TABLE exch_quotes_archive
(
    exchange_id  int                not null,
    bond_id      int                not null,
    trading_date date               not null,
    bid          float default null null,
    ask          float default null null,
    CONSTRAINT prices_pk
        PRIMARY KEY (exchange_id, bond_id, trading_date)
) PARTITION BY KEY (exchange_id) PARTITIONS 8;

DROP PROCEDURE IF EXISTS fill_exch_quotes_archive;
CREATE PROCEDURE fill_exch_quotes_archive(IN start_date DATE)
BEGIN
    DECLARE crt_date DATE;
    DECLARE end_date DATE;
    DECLARE bond_count INT DEFAULT 200;
    DECLARE price int DEFAULT -1;
    DECLARE CONTINUE HANDLER FOR 1062
        BEGIN
            DROP TEMPORARY TABLE IF EXISTS exchanges;
            DROP TEMPORARY TABLE IF EXISTS bonds;
            DROP TABLE IF EXISTS prices;
        END;
    SET crt_date = IFNULL(start_date, CURRENT_DATE),
        end_date = IFNULL(start_date, CURRENT_DATE) - INTERVAL 62 DAY;
    CREATE TABLE prices
    (
        g_price1 float,
        g_price2 float
    ) ENGINE = memory;
    WHILE price < 100 DO
            INSERT INTO prices VALUES (price, price + 10);
            SET price = price + 1;
        END WHILE;
    WHILE price < 200 DO
            INSERT INTO prices VALUES (price, price - 10);
            SET price = price + 1;
        END WHILE;
    INSERT INTO prices
    VALUES (26, null),(null, 124),(-100, -200),(-200, -300),(-300, -400),(300, 400),(400, 200),(500, 100),(1000, 600),(1200, 700);
    CREATE TEMPORARY TABLE exchanges
    (
        exchange_id int
    ) ENGINE = memory;
    INSERT INTO exchanges VALUES (1), (4), (72), (99), (250), (399), (502), (600);
    CREATE TEMPORARY TABLE bonds
    (
        bond_id int
    ) ENGINE = memory;
    WHILE bond_count > 0
        DO
            INSERT INTO bonds VALUES (bond_count);
            SET bond_count = bond_count - 1;
        END WHILE;
    WHILE (crt_date > end_date) DO
        IF (DAYNAME(crt_date) <> 'Sunday' AND DAYNAME(crt_date) <> 'Saturday') THEN
            INSERT INTO exch_quotes_archive(exchange_id, bond_id, trading_date, bid, ask)
                (SELECT exchange_id, bond_id, crt_date, bit,
                        IF(bit IS NULL AND ask IS NULL,
                            ((SELECT (g_price2 / 100)
                                FROM prices
                                WHERE g_price2 IS NOT NULL
                                ORDER BY RAND()
                                LIMIT 1)), ask
                        ) AS ask
                    FROM (SELECT exchange_id, bond_id, crt_date,
                                 (SELECT (g_price1 / 100) FROM prices ORDER BY RAND() LIMIT 1) AS bit,
                                 (SELECT (g_price2 / 100) FROM prices ORDER BY RAND() LIMIT 1) AS ask
                            FROM (SELECT exchange_id FROM exchanges AS exclude ORDER BY RAND() LIMIT 7) AS exclude
                                 CROSS JOIN bonds
                          ) as tmp
                ) ON DUPLICATE KEY UPDATE exch_quotes_archive.exchange_id = exch_quotes_archive.exchange_id;
        END IF;
        SET crt_date = ADDDATE(crt_date, INTERVAL -1 DAY);
    END WHILE;
    DROP TEMPORARY TABLE IF EXISTS exchanges;
    DROP TEMPORARY TABLE IF EXISTS bonds;
    DROP TABLE IF EXISTS prices;
END;

CALL fill_exch_quotes_archive(NULL);



-- Task 2

WITH RECURSIVE date AS (
    SELECT 14 AS n, CURRENT_DATE AS selected_date
    UNION ALL
    SELECT n - 1, selected_date - INTERVAL 1 DAY
        FROM date
        WHERE n > 1
)
SELECT selected_date,
       bonds.bond_id,
       avg_bid,
       avg_ask
FROM date
         CROSS JOIN (SELECT DISTINCT bond_id FROM exch_quotes_archive) AS bonds
         LEFT JOIN (SELECT AVG(bid) AS avg_bid, AVG(ask) AS avg_ask, trading_date, bond_id
                        FROM exch_quotes_archive
                    GROUP BY bond_id, trading_date) AS average
                          ON (average.trading_date = date.selected_date
                         AND average.bond_id       = bonds.bond_id)
ORDER BY selected_date DESC, bond_id;

