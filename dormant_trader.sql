('1INCH', 'ADA', 'BNB', 'DOGE', 'AAVE',	'AFIN',	'ALGO',	'ALPHA', 'ANKR', 'ANT',	'ATOM',	'AVAX',	'AXS'
,	'BAKE',	'BAL',	'BAND',	'BAT',	'BNT',	'BTS',	'BTT',	'CAKE',	'CHZ',	'COTI',	'CRV',	'CTSI',	'DASH'
,	'DGB',	'DOT',	'EGLD',	'EOS',	'FIL',	'FTM',	'FTT', 'GALA', 'GRT', 'GOGO', 'HBAR','HOT',	'ICX', 'IOST'
, 'JST', 'KAVA', 'KNC',	'KSM',	'LRC',	'LSK', 'LUNA', 'MANA',	'MATIC', 'IOTA', 'NANO', 'NEO',	'OGN',	'ONE'
,	'ONT',	'PAX',	'QTUM',	'REN',	'RLC',	'RUNE',	'RVN',	'SLP',	'SNX',	'SOL',	'SRM',	'STORJ', 'SUSHI'
, 'TFUEL', 'THETA',	'TRX',	'UMA',	'UNI',	'VET', 'SUSHI', 'TOK', 'WAVES', 'WRX',	'WTC',	'XEM',	'XTZ'
,	'XVG',	'XVS',	'ZEN',	'ZIL',	'ZRX')



WITH temp_a AS (
	SELECT 
		ap_account_id, signup_hostcountry , created_at, trade_id 
	-- use LAG to find the previous trade time of each trade_id 
		, LAG(created_at) OVER (PARTITION BY ap_account_id ORDER BY created_at)  
	FROM 
		warehouse.analytics.trades_master tm 
	WHERE 
		ap_account_id IS NOT NULL
		AND ap_account_id NOT IN (SELECT ap_account_id FROM warehouse.mappings.users_mapping) 
		AND product_2_symbol NOT IN ('BTC','TST2','ETH') 
		AND created_at >= '2020-07-01 00:00:00'
		AND signup_hostcountry IN ('TH') -- ('TH','ID','AU','global')
	--	AND side='Buy'
	ORDER BY  ap_account_id DESC, created_at
),temp_b AS (
	SELECT
		*
	-- extract seconds from the time period between 2 trade_id and calculate hour/ day based on seconds
		, EXTRACT(EPOCH FROM (created_at - lag)) /3600/ 24 as day_difference
	-- segmenting the user based on the time difference: less than 1 day, between 1-10 days...
		, CASE 
			WHEN (EXTRACT(EPOCH FROM (created_at - lag)) /3600) < 24 THEN 'A_<_1_day'
			WHEN (EXTRACT(EPOCH FROM (created_at - lag)) /3600) < 240 THEN 'B_1-10_days'
			WHEN (EXTRACT(EPOCH FROM (created_at - lag)) /3600) < 720 THEN 'C_10_30_days'
			WHEN (EXTRACT(EPOCH FROM (created_at - lag)) /3600) < 2160 THEN 'D_30_90_days'
			WHEN (EXTRACT(EPOCH FROM (created_at - lag)) /3600) < 4320 THEN 'E_90_180_days'
			WHEN (EXTRACT(EPOCH FROM (created_at - lag)) /3600) >= 4320 THEN 'F_>_180_days'
			ELSE 'G_Single_Trade' END segment
	FROM temp_a
)--,temp_c AS (
	SELECT
		b.*
		, tm2.product_1_symbol 
	-- identify dormant users being activated by 100coin project
		, CASE WHEN product_1_symbol IN 
					('1INCH', 'ADA', 'BNB', 'DOGE', 'AAVE',	'AFIN',	'ALGO',	'ALPHA', 'ANKR', 'ANT',	'ATOM',	'AVAX',	'AXS'
					,	'BAKE',	'BAL',	'BAND',	'BAT',	'BNT',	'BTS',	'BTT',	'CAKE',	'CHZ',	'COTI',	'CRV',	'CTSI',	'DASH'
					,	'DGB',	'DOT',	'EGLD',	'EOS',	'FIL',	'FTM',	'FTT', 'GALA', 'GRT', 'GOGO', 'HBAR','HOT',	'ICX', 'IOST'
					, 'JST', 'KAVA', 'KNC',	'KSM',	'LRC',	'LSK', 'LUNA', 'MANA',	'MATIC', 'IOTA', 'NANO', 'NEO',	'OGN',	'ONE'
					,	'ONT',	'PAX',	'QTUM',	'REN',	'RLC',	'RUNE',	'RVN',	'SLP',	'SNX',	'SOL',	'SRM',	'STORJ', 'SUSHI'
					, 'TFUEL', 'THETA',	'TRX',	'UMA',	'UNI',	'VET', 'SUSHI', 'TOK', 'WAVES', 'WRX',	'WTC',	'XEM',	'XTZ'
					,	'XVG',	'XVS',	'ZEN',	'ZIL',	'ZRX')
				THEN 1 ELSE 0 END AS is_100coin_activation
		, SUM(CASE WHEN tm2.side = 'Buy' THEN tm2.amount_usd END) AS buy_vol_usd
		, SUM(CASE WHEN tm2.side = 'Sell' THEN tm2.amount_usd END) AS sell_vol_usd
	FROM temp_b b 
	-- join trade master to find trade volume of a specific trade id and account id
		LEFT JOIN analytics.trades_master tm2 
			ON b.trade_id = tm2.trade_id 
			AND b.ap_account_id = tm2.ap_account_id 
	WHERE 
		segment IN ('E_90_180_days','F_>_180_days')
		AND b.created_at >= '2021-08-01'
	GROUP BY 1,2,3,4,5,6,7,8,9
	ORDER BY 3
;
)
SELECT
	* 
FROM temp_c
