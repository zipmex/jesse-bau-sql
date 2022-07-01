/*
 * who traded BTC IN Sept
1. DO they trade again?
2. IF they don't, what DO they trade?
3. Who stop trading?

--> sending survey
*/

WITH sep_btc_trader AS (
	SELECT 
		ap_account_id 
		, signup_hostcountry 
		, product_1_symbol 
		, SUM(quantity) coin_amount
		, SUM(amount_usd) usd_amount
	FROM analytics.trades_master t
	WHERE 
		created_at >= '2021-07-01 00:00:00'
		AND created_at < '2021-08-01 00:00:00'
		AND t.ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','27443','37807','37955','38121','38260','38262','38263'
					,'40683','40706','44056','44057','44679','48948','49649','49658','49659','52018','52019','63152','161347','316078','317029','335645','496001','610371','710015','729499')
		AND t.signup_hostcountry IN ('TH','ID','AU','global')
		AND product_1_symbol = 'BTC'
	GROUP BY 1,2,3
)	, oct_repeat AS (
	SELECT 
		DATE_TRUNC('month', t.created_at) created_at 
		, s.ap_account_id 
		, t.signup_hostcountry 
		, CASE 	WHEN t.product_1_symbol = 'BTC' THEN 'BTC' 
				WHEN t.product_1_symbol IN ('ZMT') THEN 'ZMT'
				WHEN t.product_1_symbol IN ('ETH') THEN 'ETH'
				WHEN t.product_1_symbol IN ('LTC','GOLD') THEN 'ltc_gold'
				WHEN t.product_1_symbol IN ('USDC','UDST') THEN 'usdc_usdt'
				WHEN t.product_1_symbol IN ('AXS') THEN 'AXS'
				WHEN t.product_1_symbol IN ('TOK',	'EOS',	'BTT',	'PAX',	'ONT',	'DGB',	'ICX',	'ANKR',	'WRX',	'KNC',	'XVS'
										,	'KAVA',	'OGN',	'CTSI',	'SRM',	'BAL',	'BTS',	'JST',	'COTI',	'ZEN',	'ANT'
										,	'RLC',	'STORJ',	'WTC',	'XVG',	'LSK',	'EGLD',	'FTM',	'RVN',	'UMA',	'LRC',	'FIL',	'AAVE'
										,	'TFUEL',	'RUNE',	'HBAR',	'CHZ',	'HOT',	'SUSHI',	'GRT',	'BNT',	'IOST',	'SLP')
					THEN 'batch_08_25'
				WHEN t.product_1_symbol IN ('MATIC',	'AAVE',	'HOT',	'SNX',	'BAT',	'FTT',	'UNI',	'1INCH',	'CHZ',	'CRV',	'ZRX',	'BNT',	'KNC')
					THEN 'batch_09_17'
				WHEN t.product_1_symbol IN ('ALPHA','BAND') THEN 'batch_band_alpha'
				WHEN t.product_1_symbol IN ('GALA','SUSHI','GRT','SLP','ATOM','LUNA','RUNE','AVAX','TRX','ALGO','XTZ')
					THEN 'batch_10_28'
				WHEN t.product_1_symbol IN ('ADA','BNB','SOL','DOT') THEN 'ada_bnb_sol_dot'
				ELSE 'other' 
				END AS btc_trader
		, SUM(quantity) coin_amount
		, SUM(amount_usd) usd_amount
	FROM 
		analytics.trades_master t
		LEFT JOIN sep_btc_trader s
			ON t.ap_account_id = s.ap_account_id
	WHERE 
		s.ap_account_id IS NOT NULL
		AND created_at >= '2021-09-01 00:00:00'
		AND created_at < '2021-10-01 00:00:00'	
		AND t.ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','27443','37807','37955','38121','38260','38262','38263'
					,'40683','40706','44056','44057','44679','48948','49649','49658','49659','52018','52019','63152','161347','316078','317029','335645','496001','610371','710015','729499')
		AND t.signup_hostcountry IN ('TH','ID','AU','global')
	GROUP BY 1,2,3,4
)	, mix_trader AS (
	SELECT 
		*
		, COUNT(ap_account_id) OVER(PARTITION BY ap_account_id) id_count
	FROM oct_repeat
)
SELECT
	created_at 
	, ap_account_id 
	, signup_hostcountry 
	, CASE WHEN id_count = 1 THEN btc_trader
		--	(CASE WHEN btc_trader = 'BTC' THEN 'btc_only' ELSE 'alts_only' END)
			ELSE 'mix_trade' END AS remark
	, SUM(coin_amount) coin_amount
--	, SUM( CASE WHEN btc_trader = 'BTC' THEN coin_amount END) btc_coin_amount
--	, SUM( CASE WHEN btc_trader = 'other' THEN coin_amount END) alt_coin_amount
	, SUM(usd_amount) usd_amount
--	, SUM( CASE WHEN btc_trader = 'BTC' THEN usd_amount END) btc_usd_amount
--	, SUM( CASE WHEN btc_trader = 'other' THEN usd_amount END) alt_usd_amount
FROM mix_trader
GROUP BY 1,2,3,4
;


WITH verified_users AS (
	SELECT 
		DISTINCT 
		ap_account_id
		, signup_hostcountry 
		, DATE_TRUNC('month', onfido_completed_at) verified_month
	FROM analytics.users_master um 
	WHERE 
		is_verified = TRUE 
		AND DATE_TRUNC('month', onfido_completed_at) >= '2021-03-01 00:00:00'
		AND signup_hostcountry IN ('TH','ID','AU','global')
)--	, trade_report AS (
SELECT 
	verified_month
	, u.signup_hostcountry 
	, CASE 	
			WHEN product_1_symbol IN ('BTC') THEN 'BTC' 
			WHEN product_1_symbol IN ('ETH') THEN 'ETH' 
			WHEN product_1_symbol IN ('ZMT') THEN 'ZMT'
		--	WHEN product_1_symbol IN ('BTC', 'GOLD', 'LTC', 'ETH') THEN 'zipup_coin'
			WHEN product_1_symbol IN ('USDT', 'USDC') THEN 'usdc_usdt'
			WHEN product_1_symbol IN ('AXS') THEN 'AXS'
			WHEN product_1_symbol IN ('TOK',	'EOS',	'BTT',	'PAX',	'ONT',	'DGB',	'ICX',	'ANKR',	'WRX',	'KNC',	'XVS'
									,	'KAVA',	'OGN',	'CTSI',	'SRM',	'BAL',	'BTS',	'JST',	'COTI',	'ZEN',	'ANT'
									,	'RLC',	'STORJ',	'WTC',	'XVG',	'LSK',	'EGLD',	'FTM',	'RVN',	'UMA',	'LRC',	'FIL',	'AAVE'
									,	'TFUEL',	'RUNE',	'HBAR',	'CHZ',	'HOT',	'SUSHI',	'GRT',	'BNT',	'IOST',	'SLP')
				THEN 'batch_08_25'
			WHEN product_1_symbol IN ('MATIC',	'AAVE',	'HOT',	'SNX',	'BAT',	'FTT',	'UNI',	'1INCH',	'CHZ',	'CRV',	'ZRX',	'BNT',	'KNC')
				THEN 'batch_09_17'
			WHEN product_1_symbol IN ('ALPHA','BAND') THEN 'batch_band_alpha' 
			WHEN t.product_1_symbol IN ('GALA','SUSHI','GRT','SLP','ATOM','LUNA','RUNE','AVAX','TRX','ALGO','XTZ')
					THEN 'batch_10_28'
			WHEN t.product_1_symbol IN ('ADA','BNB','SOL','DOT') THEN 'ada_bnb_sol_dot'
			ELSE 'other' END AS symbol
	, COUNT(DISTINCT u.ap_account_id) verified_user_count
	, COUNT(DISTINCT CASE WHEN amount_usd > 0 THEN u.ap_account_id END) active_trader_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', t.created_at) = verified_month AND amount_usd > 0 THEN u.ap_account_id END) AS m0_trader_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', t.created_at) = verified_month + '1 month'::INTERVAL AND amount_usd > 0 THEN u.ap_account_id END) AS m1_trader_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', t.created_at) = verified_month + '2 month'::INTERVAL AND amount_usd > 0 THEN u.ap_account_id END) AS m2_trader_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', t.created_at) = verified_month + '3 month'::INTERVAL AND amount_usd > 0 THEN u.ap_account_id END) AS m3_trader_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', t.created_at) = verified_month + '4 month'::INTERVAL AND amount_usd > 0 THEN u.ap_account_id END) AS m4_trader_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', t.created_at) = verified_month + '5 month'::INTERVAL AND amount_usd > 0 THEN u.ap_account_id END) AS m5_trader_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', t.created_at) = verified_month + '6 month'::INTERVAL AND amount_usd > 0 THEN u.ap_account_id END) AS m6_trader_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', t.created_at) = verified_month + '7 month'::INTERVAL AND amount_usd > 0 THEN u.ap_account_id END) AS m7_trader_count
	, SUM( CASE WHEN DATE_TRUNC('month', t.created_at) = verified_month THEN COALESCE(amount_usd, 0) END) m0_amount_usd
	, SUM( CASE WHEN DATE_TRUNC('month', t.created_at) = verified_month + '1 month'::INTERVAL THEN COALESCE(amount_usd, 0) END) m1_amount_usd
	, SUM( CASE WHEN DATE_TRUNC('month', t.created_at) = verified_month + '2 month'::INTERVAL THEN COALESCE(amount_usd, 0) END) m2_amount_usd
	, SUM( CASE WHEN DATE_TRUNC('month', t.created_at) = verified_month + '3 month'::INTERVAL THEN COALESCE(amount_usd, 0) END) m3_amount_usd
	, SUM( CASE WHEN DATE_TRUNC('month', t.created_at) = verified_month + '4 month'::INTERVAL THEN COALESCE(amount_usd, 0) END) m4_amount_usd
	, SUM( CASE WHEN DATE_TRUNC('month', t.created_at) = verified_month + '5 month'::INTERVAL THEN COALESCE(amount_usd, 0) END) m5_amount_usd
	, SUM( CASE WHEN DATE_TRUNC('month', t.created_at) = verified_month + '6 month'::INTERVAL THEN COALESCE(amount_usd, 0) END) m6_amount_usd
	, SUM( CASE WHEN DATE_TRUNC('month', t.created_at) = verified_month + '7 month'::INTERVAL THEN COALESCE(amount_usd, 0) END) m7_amount_usd
	, SUM(quantity) coin_amount
	, SUM(amount_usd) usd_amount
FROM 
	verified_users u
	LEFT JOIN analytics.trades_master t
		ON t.ap_account_id = u.ap_account_id
		AND t.signup_hostcountry = u.signup_hostcountry
GROUP BY 1,2,3
;


