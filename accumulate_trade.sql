WITH accum_trader AS (

	SELECT
		DATE_TRUNC('month', t.created_at) created_at 
	--	, DATE_TRUNC('month', u.onfido_completed_at) verified_month
		, t.signup_hostcountry 
		, t.ap_account_id 
		, CASE WHEN t.product_1_symbol = 'ZMT' THEN TRUE ELSE FALSE END AS is_zmt
		, COALESCE (
			CASE WHEN u.age < 30  THEN 'below30'
				WHEN u.age >= 30 AND u.age <= 40 THEN '30-40'
				WHEN u.age >= 41 AND u.age <= 55 THEN '41-55'
				WHEN u.age >= 56 then 'over55'
			ELSE NULL
			END
			,s."age"
			) AS age_grp
		, COUNT(DISTINCT t.order_id) "count_orders"
		, COUNT(DISTINCT t.trade_id) "count_trades"
		, COUNT(DISTINCT t.execution_id) "count_executions"
		, SUM(t.quantity) "sum_coin_volume"
		, SUM(t.amount_usd) "sum_usd_volume" 
		, SUM(CASE WHEN t.side = 'Buy' THEN t.amount_usd END) buy_usd_volume
		, SUM(CASE WHEN t.side = 'Sell' THEN t.amount_usd END) sell_usd_volume
	FROM 
		analytics.trades_master t
		LEFT JOIN analytics.users_master u
			ON t.ap_account_id = u.ap_account_id
		LEFT JOIN (				
			SELECT 
				DISTINCT
				s.user_id 			
				,cast (s.survey ->> 'gender' as text) as gender		
				,cast (s.survey ->> 'age' as text) as "age"		
				,cast (s.survey ->> 'total_estimate_monthly_income' as text) as income
				, s.survey ->> 'occupation' occupation
				, s.survey ->> 'education' education
			FROM
				user_app_public.suitability_surveys s 			
			WHERE
				archived_at IS NULL --taking the latest survey submission			
			)s 
			ON s.user_id  = u.user_id
	WHERE 
		t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		AND t.signup_hostcountry IN ('TH','ID','AU','global')
		AND t.created_at >= '2021-04-01 00:00:00'
		AND t.created_at < '2021-10-01 00:00:00'
	GROUP BY 1,2,3,4,5
	ORDER BY 1
;
)
SELECT 
	DATE_TRUNC('month', t.created_at) created_at 
	, a.signup_hostcountry 
	, a.ap_account_id 
	, a.age_grp
	, t.side
	, CASE		WHEN product_1_symbol IN ('BTC') THEN 'BTC'
				WHEN product_1_symbol IN ('ETH') THEN 'ETH'
				WHEN product_1_symbol IN ('USDT', 'USDC') THEN 'usdc_usdt'
				WHEN product_1_symbol IN ('GOLD', 'LTC') THEN 'gold_ltc'
				WHEN product_1_symbol IN ('ZMT') THEN 'ZMT'
				WHEN product_1_symbol IN ('AXS') THEN 'AXS'
				WHEN product_1_symbol IN ('TOK',	'EOS',	'BTT',	'PAX',	'ONT',	'DGB',	'ICX',	'ANKR',	'WRX',	'KNC',	'XVS'
											,	'KAVA',	'OGN',	'CTSI',	'SRM',	'BAL',	'BTS',	'JST',	'COTI',	'ZEN',	'ANT'
											,	'RLC',	'STORJ',	'WTC',	'XVG',	'LSK',	'EGLD',	'FTM',	'RVN',	'UMA',	'LRC',	'FIL',	'AAVE'
											,	'TFUEL',	'RUNE',	'HBAR',	'CHZ',	'HOT',	'SUSHI',	'GRT',	'BNT',	'IOST',	'SLP')
						THEN 'batch_08_25'
				WHEN product_1_symbol IN ('MATIC',	'AAVE',	'HOT',	'SNX',	'BAT',	'FTT',	'UNI',	'1INCH',	'CHZ',	'CRV',	'ZRX',	'BNT',	'KNC')
						THEN 'batch_09_17'
				WHEN product_1_symbol IN ('ALPHA','BAND') THEN 'batch_band_alpha'
				WHEN product_1_symbol IN ('GALA','SUSHI','GRT','SLP','ATOM','LUNA','RUNE','AVAX','TRX','ALGO','XTZ')
						THEN 'batch_10_28'
				WHEN product_1_symbol IN ('ADA','BNB','SOL','DOT') THEN 'ada_bnb_sol_dot'
				ELSE 'other' END AS asset_group	
	, SUM(t.amount_usd) sum_usd_volume
FROM 
	analytics.trades_master t
	LEFT JOIN accum_trader a 
		ON t.ap_account_id = a.ap_account_id
WHERE 
	a.sell_usd_volume IS NULL 
	AND a.ap_account_id IS NOT NULL
	AND t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
GROUP BY 1,2,3,4,5,6
;




	--	, CASE WHEN product_1_symbol IN ('BTC',	'ETH',	'BNB',	'SOL',	'ADA',	'DOT',	'ATOM',	'LUNA',	'AVAX',	'ALGO',	'TRON',	'FTM',	'HBAR',	'ONE') THEN 'layer1' 
	--				WHEN product_1_symbol = 'ZMT' THEN 'ZMT' 
	--				ELSE 'other' END AS is_layer1
	/*	, CASE	WHEN product_1_symbol IN ('BTC') THEN 'BTC'
				WHEN product_1_symbol IN ('ETH') THEN 'ETH'
				WHEN product_1_symbol IN ('USDT', 'USDC') THEN 'usdc_usdt'
				WHEN product_1_symbol IN ('GOLD', 'LTC') THEN 'gold_ltc'
				WHEN product_1_symbol IN ('ZMT') THEN 'ZMT'
				WHEN symbol IN ('AXS') THEN 'AXS'
				WHEN symbol IN ('TOK',	'EOS',	'BTT',	'PAX',	'ONT',	'DGB',	'ICX',	'ANKR',	'WRX',	'KNC',	'XVS'
											,	'KAVA',	'OGN',	'CTSI',	'SRM',	'BAL',	'BTS',	'JST',	'COTI',	'ZEN',	'ANT'
											,	'RLC',	'STORJ',	'WTC',	'XVG',	'LSK',	'EGLD',	'FTM',	'RVN',	'UMA',	'LRC',	'FIL',	'AAVE'
											,	'TFUEL',	'RUNE',	'HBAR',	'CHZ',	'HOT',	'SUSHI',	'GRT',	'BNT',	'IOST',	'SLP')
						THEN 'batch_08_25'
				WHEN symbol IN ('MATIC',	'AAVE',	'HOT',	'SNX',	'BAT',	'FTT',	'UNI',	'1INCH',	'CHZ',	'CRV',	'ZRX',	'BNT',	'KNC')
						THEN 'batch_09_17'
				WHEN symbol IN ('ALPHA','BAND') THEN 'batch_band_alpha'
				WHEN t.product_1_symbol IN ('GALA','SUSHI','GRT','SLP','ATOM','LUNA','RUNE','AVAX','TRX','ALGO','XTZ')
						THEN 'batch_10_28'
				WHEN t.product_1_symbol IN ('ADA','BNB','SOL','DOT') THEN 'ada_bnb_sol_dot'
				ELSE 'other' END AS asset_group
		, CASE WHEN t.counter_party IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping) THEN FALSE ELSE TRUE END "is_organic_trade" 
		, CASE WHEN t.product_1_symbol = 'ZMT' THEN TRUE ELSE FALSE END AS is_zmt 
	*/	



SELECT
	DATE_TRUNC('month', t.created_at) created_at 
	, t.signup_hostcountry 
	, t.ap_account_id 
--	, t.product_1_symbol
--	, t.side 
	, CASE WHEN product_1_symbol IN ('BNB',	'SOL',	'ADA',	'DOT',	'ATOM',	'LUNA',	'AVAX',	'ALGO',	'TRON',	'FTM',	'HBAR',	'ONE') THEN 'layer1' 
				WHEN product_1_symbol = 'ZMT' THEN 'ZMT' 
				WHEN product_1_symbol IN ('BTC','ETH') THEN product_1_symbol 
				ELSE 'other' END AS is_layer1
	, COALESCE (
				CASE WHEN u.age < 30  THEN 'below30'
					WHEN u.age >= 30 AND u.age <= 40 THEN '30-40'
					WHEN u.age >= 41 AND u.age <= 55 THEN '41-55'
					WHEN u.age >= 56 then 'over55'
				ELSE NULL
				END
		,s."age") AS age_grp
	, COUNT(DISTINCT t.order_id) "count_orders"
	, COUNT(DISTINCT t.trade_id) "count_trades"
	, COUNT(DISTINCT t.execution_id) "count_executions"
	, SUM(t.quantity) "sum_coin_volume"
	, SUM(t.amount_usd) "sum_usd_volume" 
FROM 
	analytics.trades_master t
	LEFT JOIN analytics.users_master u
		ON t.ap_account_id = u.ap_account_id
	LEFT JOIN (				
		SELECT 
			DISTINCT
			s.user_id 			
			,cast (s.survey ->> 'gender' as text) as gender		
			,cast (s.survey ->> 'age' as text) as "age"		
			,cast (s.survey ->> 'total_estimate_monthly_income' as text) as income
			, s.survey ->> 'occupation' occupation
			, s.survey ->> 'education' education
		FROM
			user_app_public.suitability_surveys s 			
		WHERE
			archived_at IS NULL --taking the latest survey submission			
		)s 
		ON s.user_id  = u.user_id
WHERE 
	DATE_TRUNC('day', t.created_at) >= '2021-04-01 00:00:00' AND DATE_TRUNC('day', t.created_at) < '2021-10-01 00:00:00' -- DATE_TRUNC('day', NOW())
	AND t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
	AND t.signup_hostcountry IN ('TH','ID','AU','global')
GROUP BY 1,2,3,4,5
ORDER BY 1
;