DROP TABLE IF EXISTS warehouse.bo_testing.test_hnw_bonus_rate;

CREATE TABLE IF NOT EXISTS warehouse.bo_testing.test_hnw_bonus_rate
(
	symbol			VARCHAR(255)
	, min_amount	NUMERIC
);

INSERT INTO warehouse.bo_testing.test_hnw_bonus_rate VALUES
(	'BTC', 0.15);
INSERT INTO warehouse.bo_testing.test_hnw_bonus_rate VALUES
(	'ETH', 3);
INSERT INTO warehouse.bo_testing.test_hnw_bonus_rate VALUES
(	'ZMT', 3000);
INSERT INTO warehouse.bo_testing.test_hnw_bonus_rate VALUES
(	'USDC', 3000);
INSERT INTO warehouse.bo_testing.test_hnw_bonus_rate VALUES
(	'USDT', 3000);
INSERT INTO warehouse.bo_testing.test_hnw_bonus_rate VALUES
(	'ADA', 6000);
INSERT INTO warehouse.bo_testing.test_hnw_bonus_rate VALUES
(	'XRP', 9000);
INSERT INTO warehouse.bo_testing.test_hnw_bonus_rate VALUES
(	'SOL', 100);