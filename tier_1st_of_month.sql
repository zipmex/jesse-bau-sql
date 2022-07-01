	SET timezone TO 'GMT';

DROP TABLE IF EXISTS warehouse.analytics.zmt_tier_1stofmonth;

CREATE TABLE IF NOT EXISTS warehouse.analytics.zmt_tier_1stofmonth
(
	id						SERIAL PRIMARY KEY 
	, created_at	 		TIMESTAMPTZ
	, signup_hostcountry	VARCHAR(255)
	, ap_account_id		 	INTEGER 
	, symbol 				VARCHAR(255)
	, ziplock_amount		NUMERIC
	, vip_tier				VARCHAR(255)
	, zip_tier				VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS zmt_tier_1stofmonth_idx ON warehouse.analytics.zmt_tier_1stofmonth
(created_at, ap_account_id, symbol, zip_tier);

TRUNCATE TABLE warehouse.analytics.zmt_tier_1stofmonth;

CREATE TEMP TABLE IF NOT EXISTS tmp_zmt_tier_1stofmonth AS
(
SELECT 
	w.created_at 
	, u.signup_hostcountry
	, w.ap_account_id 
	, w.symbol 
	, ziplock_amount 
	, CASE WHEN ziplock_amount >= 100 AND ziplock_amount < 1000 THEN 'vip1'
			WHEN ziplock_amount >= 1000 AND ziplock_amount < 5000 THEN 'vip2'
			WHEN ziplock_amount >= 5000 AND ziplock_amount < 20000 THEN 'vip3'
			WHEN ziplock_amount >= 20000 THEN 'vip4'
			ELSE 'no_tier' END AS vip_tier
	, CASE WHEN ziplock_amount >= 100 AND ziplock_amount < 20000 THEN 'ZipMember'
			WHEN ziplock_amount >= 20000 THEN 'ZipCrew'
			ELSE 'ZipStarter' END AS zip_tier
FROM 
	analytics.wallets_balance_eod w
	LEFT JOIN analytics.users_master u
		ON w.ap_account_id = u.ap_account_id 
WHERE 
	w.symbol = 'ZMT' 
	AND w.created_at >= '2020-12-01'
	AND w.created_at = DATE_TRUNC('month', w.created_at)
	ORDER BY 1,2
);

INSERT INTO warehouse.analytics.zmt_tier_1stofmonth ( created_at, signup_hostcountry, ap_account_id, symbol, ziplock_amount, vip_tier, zip_tier)
(
SELECT * FROM tmp_zmt_tier_1stofmonth
);

DROP TABLE IF EXISTS tmp_zmt_tier_1stofmonth