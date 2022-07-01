---- TH country performance -- excluded MM
WITH user_funnel AS (
-- register and verified users (tied to verifed date)
	SELECT 
		register_month created_at
		, signup_hostcountry 
		, registered_user_count 
		, reporting_verified_user_count 
		, total_registered_user
		, total_reporting_verified_user 
	FROM 
		reportings_data.dm_user_funnel_monthly dufm
	WHERE 
		register_month >= '2022-01-01'
)	, trade_zmt_base AS (
-- trade volume brokendown by ZMT and organic
	SELECT
		created_at 
		, signup_hostcountry 
		, CASE WHEN is_zmt_trade IS TRUE THEN sum_usd_trade_volume END AS trade_volume_usd_zmt
		, CASE WHEN is_zmt_trade IS FALSE THEN sum_usd_trade_volume END AS trade_volume_usd_non_zmt
		, CASE WHEN is_organic_trade IS TRUE THEN sum_usd_trade_volume END AS trade_volume_usd_organic
		, CASE WHEN is_organic_trade IS FALSE THEN sum_usd_trade_volume END AS trade_volume_usd_inorganic
		, sum_usd_trade_volume
	FROM 
		reportings_data.dm_trade_zmt_organic_monthly dtzom 
	WHERE 
-- excluding gaming in july 2021
		is_july_gaming IS FALSE 
		AND created_at >= '2022-01-01'
	ORDER BY 1 DESC
)	, trade_zmt AS (
	SELECT 
		created_at
		, signup_hostcountry 
		, SUM( COALESCE( trade_volume_usd_zmt, 0)) trade_volume_usd_zmt
		, SUM( COALESCE( trade_volume_usd_non_zmt, 0)) trade_volume_usd_non_zmt
		, SUM( COALESCE( sum_usd_trade_volume, 0)) total_usd_trade_volume
	FROM trade_zmt_base
	GROUP BY 1,2
	ORDER BY 1 DESC 
)	, trade_organic AS (
	SELECT 
		created_at
		, signup_hostcountry 
		, SUM( COALESCE( trade_volume_usd_organic, 0)) trade_volume_usd_organic
		, SUM( COALESCE( trade_volume_usd_inorganic, 0)) trade_volume_usd_inorganic
	FROM trade_zmt_base
	GROUP BY 1,2
	ORDER BY 1 DESC 
)	, trade_avg AS (
	SELECT 
		created_at
		, signup_hostcountry 
		, SUM( sum_usd_trade_volume) total_usd_trade_volume
		, SUM( count_trader) total_traders
		, SUM( sum_usd_trade_volume) / SUM(count_trader) avg_trade_incl_whale
		, SUM( CASE WHEN is_whales IS FALSE THEN sum_usd_trade_volume END) /
				 SUM( CASE WHEN is_whales IS FALSE THEN count_trader END) avg_trade_excl_whale
	FROM 
		reportings_data.dm_trade_whale_monthly dtwm 
	WHERE 
		created_at >= '2022-01-01'
	GROUP BY 1,2
	ORDER BY 1 DESC 
)	, aum_base AS (
	-- interest AUM
	SELECT 
		DATE_TRUNC('month', dad.created_at)::DATE created_at
		, dad.signup_hostcountry 
		, dad.total_aum_usd 
		, dad.zw_zipup_nonzmt_usd  
		, dad.zw_zipup_zmt_usd 
		, dad.ziplock_nonzmt_usd 
		, dad.ziplock_zmt_usd 
	FROM reportings_data.dm_aum_daily dad 
	WHERE 
		dad.created_at >= '2022-01-01'
		AND ((dad.created_at = DATE_TRUNC('month', dad.created_at) + '1 month' - '1 day'::INTERVAL) OR (dad.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
	ORDER BY 1
)
SELECT 
	uf.*
	, tz.trade_volume_usd_zmt
	, tz.trade_volume_usd_non_zmt
	, tor.trade_volume_usd_organic
	, tor.trade_volume_usd_inorganic
	, ta.total_usd_trade_volume
	, ta.total_traders
	, ta.avg_trade_incl_whale
	, ta.avg_trade_excl_whale
	, ab.zw_zipup_nonzmt_usd  
	, ab.zw_zipup_zmt_usd 
	, ab.ziplock_nonzmt_usd 
	, ab.ziplock_zmt_usd 
	, ab.total_aum_usd 
FROM 
	user_funnel uf
	LEFT JOIN
		trade_zmt tz 
		ON uf.created_at = tz.created_at
		AND uf.signup_hostcountry = tz.signup_hostcountry
	LEFT JOIN
		trade_organic tor 
		ON uf.created_at = tor.created_at
		AND uf.signup_hostcountry = tor.signup_hostcountry
	LEFT JOIN
		trade_avg ta 
		ON uf.created_at = ta.created_at
		AND uf.signup_hostcountry = ta.signup_hostcountry
	LEFT JOIN 
		aum_base ab 
		ON uf.created_at = ab.created_at
		AND uf.signup_hostcountry = ab.signup_hostcountry
WHERE uf.signup_hostcountry = 'TH'
;


SELECT 
	mtu_month::DATE 
	, signup_hostcountry
	, COUNT(DISTINCT CASE WHEN mtu IS TRUE THEN ap_account_id END) AS mtu_count
FROM analytics.dm_mtu_monthly dmm 
GROUP BY 1,2


