WITH user_info AS (
	SELECT 
		u.created_at::DATE register_date
		, u.onfido_completed_at::DATE verified_date
		, first_traded_at::DATE first_traded_at 
		, last_traded_at::DATE last_traded_at
		, u.ap_account_id
		, u.user_id 
		, u.email
		, u.signup_hostcountry
		, COALESCE (lower(u.gender),s.gender) as gender --gender onfido is priority selection
		, DATE_TRUNC('day', u.first_traded_at) first_traded_date
		, (DATE_TRUNC('day', NOW()) - DATE_TRUNC('day', u.first_traded_at)) trade_with_zipmex
		, COALESCE (CASE WHEN u.age < 30  THEN 'below30' 	
						WHEN u.age >= 30 AND u.age <= 40 THEN '30-40'	
						WHEN u.age >= 41 AND u.age <= 55 THEN '41-55'	
						WHEN u.age >= 56 THEN 'over55' 
					ELSE NULL
					END
			,s.age) AS age_grp
		,u.dob 
		,s.income
		,s.occupation
		,s.education
		,is_zipup_subscribed
		,u.sum_trade_volume_usd 
	FROM 
		analytics.users_master u 
	LEFT JOIN (				
		SELECT 
			DISTINCT
			s.user_id 			
			,cast (s.survey ->> 'gender' as text) as gender
			,cast (s.survey ->> 'age' as text) as age
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
		u.signup_hostcountry IN ('TH','ID','AU','global')
		AND u.ap_account_id IS NOT NULL
)	, user_trade AS (
	SELECT
		DATE_TRUNC('month', created_at)::DATE trade_month
		, ap_account_id 
		, COUNT(DISTINCT t.product_1_symbol) number_of_coin_traded
		, COUNT(DISTINCT order_id) count_orders
		, SUM(amount_usd) sum_trade_usd
	FROM analytics.trades_master t
	WHERE
		signup_hostcountry IN ('TH','ID','AU','global')
		AND created_at >= '2021-01-01 00:00:00'
		AND created_at < NOW()::DATE 
		AND ap_account_id IS NOT NULL
	GROUP BY 1,2
)	, user_aum AS (
	SELECT
		DATE_TRUNC('month', a.created_at)::DATE aum_month
		, a.ap_account_id 
		, COUNT(DISTINCT symbol) hold_asset_count
		, SUM( 	CASE WHEN rm.product_type = 1 THEN 
				(COALESCE (trade_wallet_amount, 0) * 1/rm.price 
				+ COALESCE (z_wallet_amount, 0) * 0 
				+ COALESCE (ziplock_amount, 0) * 0)
				ELSE
				(COALESCE (trade_wallet_amount, 0) * rm.price 
				+ COALESCE (z_wallet_amount, 0) * rm.price  
				+ COALESCE (ziplock_amount, 0) * rm.price)
				END) total_aum_usd_amount
	FROM analytics.wallets_balance_eod a
		LEFT JOIN analytics.users_master u
			ON a.ap_account_id = u.ap_account_id 
		LEFT JOIN analytics.rates_master rm 
			ON a.symbol = rm.product_1_symbol 
			AND a.created_at::DATE = rm.created_at::DATE 
	WHERE 
			a.created_at >= '2021-01-01 00:00:00' AND a.created_at < NOW()::DATE  
		AND signup_hostcountry IN ('TH','ID','AU','global')
		AND ((DATE_TRUNC('day', a.created_at) = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
		AND a.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		AND a.symbol NOT IN ('TST1','TST2','ZMT')
	GROUP BY 1,2
)	, user_zipworld AS (
	SELECT
		DATE_TRUNC('month', p.completed_at)::DATE purchase_month
		, um.ap_account_id 
		, SUM(p.purchase_price) zmt_spent
		, SUM(p.purchase_price * r.price) zmt_spent_usd
	FROM zipworld_public.purchases p 
		LEFT JOIN zipworld_public.users u 
			ON p.user_id = u.id 
		LEFT JOIN analytics.users_master um 
			ON u.zipmex_user_id = um.user_id 
		LEFT JOIN analytics.rates_master r
			ON DATE_TRUNC('day', completed_at) = r.created_at 
			AND r.product_1_symbol = 'ZMT'
	GROUP BY 1,2
)	, zlaunch_snapshot AS (
	SELECT
		DATE_TRUNC('day', event_timestamp) created_at
		, user_id 
		, UPPER(SPLIT_PART(lock_product_id,'.',1)) symbol
		, SUM(CASE WHEN event_type = 'lock' THEN amount END) lock_amount
		, SUM(CASE WHEN event_type = 'unlock' THEN amount END) released_amount
	FROM 
		z_launch_service_public.lock_unlock_histories luh 
	GROUP BY 1,2,3
)	, zmt_lock AS (
	SELECT 
		p.created_at 
		, z.user_id
		, u.ap_account_id 
		, u.email 
		, u.signup_hostcountry 
		, symbol
		, SUM(COALESCE (lock_amount, 0)) - SUM(COALESCE (released_amount, 0)) zmt_amount
	FROM analytics.period_master p
		LEFT JOIN zlaunch_snapshot z 
			ON p.created_at >= z.created_at
		LEFT JOIN analytics.users_master u
			ON z.user_id = u.user_id 
	WHERE 
		p."period" = 'day'
		AND p.created_at >= '2021-10-26 00:00:00'
		AND p.created_at < NOW()::DATE 
	GROUP BY 1,2,3,4,5,6
)	, user_zlaunch AS (
	SELECT 
		DATE_TRUNC('month', z.created_at)::DATE zlaunch_month
		, ap_account_id 
		, SUM(zmt_amount) zmt_amount
		, SUM(zmt_amount * r.price) zmt_launch_usd
	FROM zmt_lock z 
		LEFT JOIN analytics.rates_master r
		ON z.symbol = r.product_1_symbol 
		AND z.created_at = r.created_at 
	WHERE 
		((z.created_at = DATE_TRUNC('month', z.created_at) + '1 month' - '1 day'::INTERVAL) OR (z.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
		AND z.created_at < NOW()::DATE 
	GROUP BY 1,2
)
SELECT
	a.ap_account_id
	, signup_hostcountry 
	, income
	, dob
	, age_grp
--	, register_date
--	, verified_date
--	, first_traded_at
--	, last_traded_at 
	, AVG(hold_asset_count) avg_asset_hold
	, AVG(total_aum_usd_amount) avg_monthly_aum
	, SUM(count_orders) total_traded_orders
	, SUM(sum_trade_usd) total_trade_volume
	, AVG(sum_trade_usd) avg_monthly_trade
	, AVG(number_of_coin_traded) avg_alt_traded
	, AVG(zmt_spent_usd) avg_zipworld_spent_usd
	, AVG(zmt_launch_usd) avg_zlaunch_usd 
FROM user_aum a 
	LEFT JOIN user_trade t 
		ON a.ap_account_id = t.ap_account_id 
		AND trade_month = aum_month
	LEFT JOIN user_zipworld w 
		ON a.ap_account_id = w.ap_account_id 
		AND purchase_month = aum_month
	LEFT JOIN user_zlaunch z 
		ON a.ap_account_id = z.ap_account_id
		AND zlaunch_month = aum_month
	LEFT JOIN user_info i 
		ON a.ap_account_id = i.ap_account_id
GROUP BY 1,2,3,4,5
ORDER BY 1
;


WITH base AS (
SELECT 
	u.created_at::DATE register_date
	, u.ap_account_id 
	, t.created_at::DATE traded_date
	, SUM(t.amount_usd) sum_trade_volume 
FROM analytics.users_master u
	LEFT JOIN analytics.trades_master t
		ON u.ap_account_id = t.ap_account_id 
GROUP BY 1,2,3
)
SELECT 
	ap_account_id 
	, register_date
	, SUM( CASE WHEN traded_date < register_date + '8 day'::INTERVAL THEN sum_trade_volume END) first_7d_usd_vol
FROM base 
WHERE register_date >= '2021-11-01'
GROUP BY 1,2