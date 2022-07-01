CREATE TABLE IF NOT EXISTS warehouse.bo_testing.sample_demo_20211118
(
	id						serial PRIMARY KEY
	, ap_account_id			integer
	, age_					integer
	, income				varchar(255)
	, months_active			integer
	, sum_trade_usd 		numeric
	, avg_aum_nonzmt_usd_amount 		numeric
	, order_count			integer
	, aum_over_trade		numeric
	, trade_over_aum		numeric
	, avg_monthly_trade		numeric
	, number_alt_held		integer
	, alt_usd_value			numeric
	, zmt_spent				numeric
	, zlaunch				numeric
	, withdraw_usd_amount	numeric
	, withdraw_count		numeric
	, median_interest_bearing			numeric
	, median_ziplock		numeric
	, median_zipup_ziplock				numeric
	, median_aum_over_trade				numeric
	, median_interest_bearing_incl_zmt	numeric
	, median_ziplock_incl_zmt			numeric
	, median_total_incl_zmt				numeric
	, median_total_aum_excl_zmt_ziplock	numeric
	, median_total_aum_excl_zmt			numeric
	, median_total_aum_incl_zmt			numeric
	, persona				varchar(255)
	, remarks				varchar(255)
);

CREATE INDEX IF NOT EXISTS sample_demo_20211118_idx ON warehouse.bo_testing.sample_demo_20211118 
(ap_account_id);

DROP TABLE IF EXISTS warehouse.bo_testing.sample_demo_20211118;

INSERT INTO bo_testing.sample_demo_20211118 (ap_account_id, age_, income, months_active, sum_trade_usd, avg_aum_nonzmt_usd_amount, order_count, aum_over_trade, trade_over_aum, avg_monthly_trade, number_alt_held, alt_usd_value, zmt_spent, zlaunch, withdraw_usd_amount, withdraw_count, median_interest_bearing, median_ziplock, median_zipup_ziplock, median_aum_over_trade, median_interest_bearing_incl_zmt, median_ziplock_incl_zmt, median_total_incl_zmt, median_total_aum_excl_zmt_ziplock, median_total_aum_excl_zmt, median_total_aum_incl_zmt, persona, remarks) VALUES (176961, null, null, null, 3748.207135, 4.841265465, null, 0.001291622, 774.2205343, null, 1.0, 3748.207135, null, null, 3693.332441, 1.0, null, null, null, null, null, null, null, 4.0, 4.0, 4.0, 'Casual Investor', 'no zipup, trades < 50000');
INSERT INTO bo_testing.sample_demo_20211118 (ap_account_id, age_, income, months_active, sum_trade_usd, avg_aum_nonzmt_usd_amount, order_count, aum_over_trade, trade_over_aum, avg_monthly_trade, number_alt_held, alt_usd_value, zmt_spent, zlaunch, withdraw_usd_amount, withdraw_count, median_interest_bearing, median_ziplock, median_zipup_ziplock, median_aum_over_trade, median_interest_bearing_incl_zmt, median_ziplock_incl_zmt, median_total_incl_zmt, median_total_aum_excl_zmt_ziplock, median_total_aum_excl_zmt, median_total_aum_incl_zmt, persona, remarks) VALUES (182520, 26.0, null, null, 3743.157477, 1.211797148, 1.0, 0.000323737, 3088.930754, null, 1.0, 3743.157477, null, null, 3770.929071, 1.0, null, null, null, null, null, null, null, 1.0, 1.0, 1.0, 'Casual Investor', 'no zipup, trades < 50000');
INSERT INTO bo_testing.sample_demo_20211118 (ap_account_id, age_, income, months_active, sum_trade_usd, avg_aum_nonzmt_usd_amount, order_count, aum_over_trade, trade_over_aum, avg_monthly_trade, number_alt_held, alt_usd_value, zmt_spent, zlaunch, withdraw_usd_amount, withdraw_count, median_interest_bearing, median_ziplock, median_zipup_ziplock, median_aum_over_trade, median_interest_bearing_incl_zmt, median_ziplock_incl_zmt, median_total_incl_zmt, median_total_aum_excl_zmt_ziplock, median_total_aum_excl_zmt, median_total_aum_incl_zmt, persona, remarks) VALUES (119483, 37.0, 'less_than_15000_baht', null, 1003.733739, 72.12318364, 8.0, 0.071854896, 13.91693612, null, 2.0, 1003.733739, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 'Beginner', 'no zipup, trades < 2000, orders < 50');
INSERT INTO bo_testing.sample_demo_20211118 (ap_account_id, age_, income, months_active, sum_trade_usd, avg_aum_nonzmt_usd_amount, order_count, aum_over_trade, trade_over_aum, avg_monthly_trade, number_alt_held, alt_usd_value, zmt_spent, zlaunch, withdraw_usd_amount, withdraw_count, median_interest_bearing, median_ziplock, median_zipup_ziplock, median_aum_over_trade, median_interest_bearing_incl_zmt, median_ziplock_incl_zmt, median_total_incl_zmt, median_total_aum_excl_zmt_ziplock, median_total_aum_excl_zmt, median_total_aum_incl_zmt, persona, remarks) VALUES (80135, 49.0, null, null, null, 1413.345991, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 1252.0, 1252.0, 1252.0, 'Beginner', 'no withdrawal, no zipup, aum<1000');

SELECT count(*) FROM warehouse.bo_testing.sample_demo_20211118 
;



SELECT
	ap_account_id
	, LOWER(pi2.email) email 
	, user_name
	, country
	, pcs_profile
FROM warehouse.bo_testing.pcs_id_20211213 pi2 
	LEFT JOIN analytics.users_master um 
		ON LOWER(pi2.email) =um.email
ORDER BY 1


DROP TABLE warehouse.bo_testing.dm_trade_asset_monthly ;


SELECT 
'ledger_v2'	service 
, max(updated_at) max_updated_at
, max(lv.inserted_at) max_inserted_at
FROM asset_manager_public.ledgers_v2 lv  
UNION ALL
SELECT 
'ledger_balances_v2'	service 
, max(updated_at) max_updated_at
, max(inserted_at) max_inserted_at
FROM asset_manager_public.ledger_balances_v2 lbv 
