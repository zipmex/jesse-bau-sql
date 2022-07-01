
WITH base AS (
	SELECT
		DISTINCT 
		d.ap_account_id 
		, d.signup_hostcountry 
		, product_type 
		, CASE WHEN sd.persona IS NULL THEN 'unknown' ELSE sd.persona END AS persona
	FROM analytics.deposit_tickets_master d
		LEFT JOIN bo_testing.sample_demo_20211118 sd 
			ON d.ap_account_id = sd.ap_account_id 
	WHERE
		d.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		AND signup_hostcountry IN ('TH','ID','AU','global')
		AND created_at >= '2021-08-01'
		AND status = 'FullyProcessed'
)	, base_group AS (
	SELECT 
		*
		, COUNT(ap_account_id) OVER(PARTITION BY ap_account_id) id_count
	FROM base
)	, user_group AS (
	SELECT
		DISTINCT 
		ap_account_id 
		, signup_hostcountry
		, CASE 	WHEN id_count = 2 THEN 'mix_depositor'
				WHEN id_count = 1 AND product_type = 'CryptoCurrency' THEN 'only_crypto'
				WHEN id_count = 1 AND product_type = 'NationalCurrency' THEN 'only_cash'
				END AS depositor_group
		, persona
	FROM base_group
)	, deposit_group AS (
	SELECT
		ap_account_id 
		, signup_hostcountry
--		, CASE WHEN product_symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH') THEN 'zipup_coin'
--					WHEN product_symbol IN ('ZMT') THEN 'ZMT'
--					WHEN product_symbol IN ('AUD','IDR','SGD','THB','VND','USD') THEN 'cash'
--					ELSE 'other'
--					END AS asset_group
		, SUM(CASE WHEN product_symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH') THEN amount_usd END) zipup_amount_usd
		, SUM(CASE WHEN product_symbol IN ('ZMT') THEN amount_usd END) zmt_amount_usd
		, SUM(CASE WHEN product_symbol IN ('AUD','IDR','SGD','THB','VND','USD') THEN amount_usd END) cash_amount_usd
		, SUM(CASE WHEN product_symbol NOT IN ('AUD','IDR','SGD','THB','VND','USD','BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH','ZMT') THEN amount_usd END) other_amount_usd
	FROM 
		analytics.deposit_tickets_master d
	WHERE
		ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		AND signup_hostcountry IN ('TH','ID','AU','global')
		AND created_at >= '2021-08-01'
		AND status = 'FullyProcessed'
	GROUP BY 1,2
)	, zipup_group AS (
	SELECT 
		u.*
		, CASE WHEN zipup_amount_usd IS NOT NULL THEN 1 ELSE 0 END AS zipup_depositor
		, CASE WHEN cash_amount_usd IS NOT NULL THEN 1 ELSE 0 END AS cash_depositor
		, CASE WHEN other_amount_usd IS NOT NULL THEN 1 ELSE 0 END AS other_depositor
	--	, CASE WHEN zmt_amount_usd IS NOT NULL THEN 1 ELSE 0 END AS zmt_depositor
		, zipup_amount_usd , cash_amount_usd , other_amount_usd
	FROM user_group u 
		LEFT JOIN deposit_group d
			ON u.ap_account_id = d.ap_account_id 
)	, mix_depositor AS (
	SELECT 
		depositor_group
		, signup_hostcountry
		, persona
		, ap_account_id 
		, COUNT(DISTINCT ap_account_id) total_depositor
		, SUM(zipup_depositor) zipup_depositor
		, SUM(cash_depositor) cash_depositor
		, SUM(other_depositor) other_depositor
		, SUM(zipup_amount_usd) zipup_deposit_usd
		, SUM(cash_amount_usd) cash_deposit_usd
		, SUM(other_amount_usd) other_deposit_usd
	FROM zipup_group
	GROUP BY 1,2,3,4
)	, aum_snapshot AS (
	SELECT 
		w.created_at 
		, w.ap_account_id 
		, signup_hostcountry 
		, CASE WHEN symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH') THEN 'zipup_coin'
				WHEN symbol IN ('ZMT') THEN 'ZMT'
				ELSE 'non_zipup' END AS asset_group
		, SUM( CASE	WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
				WHEN r.product_type = 2 THEN trade_wallet_amount * r.price
				END) trade_wallet_amount_usd
		, SUM(w.z_wallet_amount * r.price) z_wallet_amount_usd
		, SUM(w.ziplock_amount * r.price) ziplock_amount_usd
	FROM analytics.wallets_balance_eod w
		LEFT JOIN analytics.users_master u
			ON w.ap_account_id = u.ap_account_id
		LEFT JOIN analytics.rates_master r
			ON w.created_at = r.created_at 
			AND w.symbol = r.product_1_symbol 
	WHERE 
		w.created_at = '2021-11-29 00:00:00' AND w.created_at < DATE_TRUNC('day', NOW())
	--	AND ((w.created_at = DATE_TRUNC('month', w.created_at) + '1 month' - '1 day'::INTERVAL) OR (w.created_at = DATE_TRUNC('day', NOW()) - '2 day'::INTERVAL))
	--	AND w.symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH')
		AND w.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		AND signup_hostcountry IN ('TH','ID','AU','global')
	GROUP BY 1,2,3,4
)
SELECT
	a.created_at
	, a.ap_account_id
	, sd2.persona 
	, a.signup_hostcountry
	, a.asset_group
	, m.depositor_group
	, a.trade_wallet_amount_usd
	, z_wallet_amount_usd
	, ziplock_amount_usd
FROM aum_snapshot a 
	LEFT JOIN mix_depositor m 
		ON m.ap_account_id = a.ap_account_id
		AND m.signup_hostcountry = a.signup_hostcountry
	--	AND depositor_group = 'mix_depositor'
	LEFT JOIN (
		SELECT DISTINCT ap_account_id , persona FROM bo_testing.sample_demo_20211118
		) sd2
		ON a.ap_account_id = sd2.ap_account_id 
;


SELECT
	DATE_TRUNC('month', created_at) created_at 
	, signup_hostcountry 
	, product_type 
--	, product_symbol
	, CASE WHEN product_symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH') THEN 'zipup_coin'
				WHEN product_symbol IN ('ZMT') THEN 'ZMT'
				WHEN product_symbol IN ('AUD','IDR','SGD','THB','VND','USD') THEN 'cash'
				ELSE 'other'
				END AS asset_group
	, SUM(amount) deposit_amount
	, SUM(amount_usd) deposit_amount_usd
FROM analytics.deposit_tickets_master d
WHERE
	ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
	AND signup_hostcountry IN ('TH','ID','AU','global')
	AND created_at >= '2021-08-01'
	AND status = 'FullyProcessed'
GROUP BY 1,2,3,4
;




SELECT 
	service_id 
	, account_id 
	, user_id
	, ap_account_id
	, signup_hostcountry
	, product_id 
	, SUM(credit) - SUM(debit) deposit_balance
FROM 
	asset_manager_public.ledgers_v2 l
	LEFT JOIN analytics.users_master u
		ON l.account_id = u.user_id 
WHERE service_id = 'main_wallet'
	AND ref_action IN ('deposit','withdraw','release')
	AND signup_hostcountry IN ('TH','ID','AU','global')
GROUP BY 1,2,3,4,5,6