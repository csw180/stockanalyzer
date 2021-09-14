
DROP TABLE strategy;

CREATE TABLE IF NOT EXISTS strategy (
    code VARCHAR(20),
    date CHAR(8),
    name    VARCHAR(40),
    strategy  VARCHAR(20),
    buy   BIGINT(20),
    cutsell    BIGINT(20),
    targetsell BIGINT(20),
    closed VARCHAR(1),
    last_update char(8),
    PRIMARY KEY (code, date));
    
REPLACE INTO strategy (code, date, NAME, strategy, buy, cutsell,targetsell, closed, last_update)
VALUES ('A227950','20210629','엔투텍','strategy1',2170,2240,2130,'N','20210629');

REPLACE INTO strategy (code, date, NAME, strategy, buy, cutsell,targetsell, closed, last_update)
VALUES ('A037350','20210624','성도이엔지','strategy1',6390,6610,6320,'N','20210629');

REPLACE INTO strategy (code, date, NAME, strategy, buy, cutsell,targetsell, closed, last_update)
VALUES ('A294870','20210624','HDC현대산업개발','strategy1',30450,31500,29600,'Y','20210629');
    

SELECT * FROM strategy

    

SELECT * FROM strategy


SELECT * FROM strategy WHERE name LIKE 'strategy1' AND closed ='N'