WITH base AS (
	SELECT
		DATE_TRUNC('month', t.created_at) created_at 
		, t.signup_hostcountry 
		, t.ap_account_id 
		, CASE WHEN product_1_symbol IN ('ADA','UNI','THETA','SOL','DOT','MATIC','VET','AXS','BTT','PAX','ONT','DGB','ICX','ANKR','WRX','KNC'
						,'FTT','ZRX','BAND','DOGE','ALGO','CAKE','MIOTA','NEO','KSM','XTZ','OGN','CTSI','SRM','ALPHA','BAL','BTS','JST','COTI'
						,'AAVE','BNT','BNB','WAVES','XEM','DASH','ZIL','MANA','RLC','STORJ','WTC','XVG','LSK','EGLD','FTM','RVN','UMA','HOT','1INCH'
						,'ONE','BAKE','NANO','REN','TFUEL','RUNE','HBAR','CHZ','SUSHI','GRT','SNX','ATOM','QTUM','TRX','KAVA','ANT','FIL','SLP'
						,'XVS','ZEN','LRC','IOST','EOS','BAT','CRV') 
					THEN '100coin' ELSE 'other' END AS is_100coin
		, COUNT(DISTINCT t.ap_account_id) "count_trader"
		, COUNT(DISTINCT t.order_id) "count_orders"
		, COUNT(DISTINCT t.trade_id) "count_trades"
		, SUM(t.quantity) "sum_coin_volume"
		, SUM(t.amount_usd) "sum_usd_volume" 
	FROM 
		analytics.trades_master t
		LEFT JOIN analytics.users_master u
			ON t.ap_account_id = u.ap_account_id
	WHERE 
		DATE_TRUNC('day', t.created_at) >= '2021-01-01 00:00:00' AND DATE_TRUNC('day', t.created_at) < DATE_TRUNC('day', NOW())
		AND t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		AND t.signup_hostcountry IN ('TH','ID','AU','global')
	--	AND t.ap_account_id = 143639
	GROUP BY 1,2,3,4
	ORDER BY 1
)	, base_count AS (
	SELECT 
		*
		, COUNT(ap_account_id) OVER (PARTITION BY created_at, ap_account_id) id_count
	FROM base
)	, base_group AS (
	SELECT
		*
		, CASE WHEN id_count = 2 THEN 'mix_trader'
				WHEN id_count = 1 AND is_100coin = '100coin' THEN '100coin_trader'
				WHEN id_count = 1 AND is_100coin = 'other' THEN 'other'
				ELSE 'error'
				END AS trader_group
	FROM base_count
)
SELECT
	created_at 
	, signup_hostcountry 
	, ap_account_id 
	, trader_group
	, COUNT(DISTINCT ap_account_id) trader_count
	, SUM(sum_usd_volume) sum_usd_volume
	, COUNT(DISTINCT ap_account_id)/ 1000.0 trader_count_k
	, SUM(sum_usd_volume)/ 1000000.0 sum_usd_volume_m
FROM base_group
GROUP BY 1,2,3,4
;