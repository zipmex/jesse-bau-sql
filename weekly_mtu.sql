---- MTU 2021-11-17 -- OCT 2021 onward  -- active balance (Zipup/ Ziplock) -- active Trader -- active depositor/ withdrawer -- zipworld purchaser -- zlaunch staker
WITH base AS (
		SELECT 
			a.created_at 
			, CASE WHEN u.signup_hostcountry IN ('test', 'error','xbullion') THEN 'test' ELSE u.signup_hostcountry END AS signup_hostcountry 
			, a.ap_account_id 
			, CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping) 
					THEN TRUE ELSE FALSE END AS is_nominee
			, a.symbol 
			, u.zipup_subscribed_at 
			, u.is_zipup_subscribed 
			, SUM(trade_wallet_amount) trade_wallet_amount
			, SUM(z_wallet_amount) z_wallet_amount
			, SUM(ziplock_amount) ziplock_amount
			, SUM( CASE WHEN a.symbol = 'USD' THEN trade_wallet_amount * 1
						WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
						WHEN r.product_type = 2 THEN trade_wallet_amount * r.price END) trade_wallet_amount_usd
			, SUM( z_wallet_amount * r.price ) z_wallet_amount_usd
			, SUM( ziplock_amount * r.price ) ziplock_amount_usd
		FROM 
			analytics.wallets_balance_eod a 
			LEFT JOIN 
				analytics.users_master u 
				ON a.ap_account_id = u.ap_account_id 
			LEFT JOIN 
				analytics.rates_master r 
				ON a.symbol = r.product_1_symbol 
			    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
		WHERE 
			a.created_at >= '2021-10-01 00:00:00'
			AND a.created_at < DATE_TRUNC('year', NOW()) 
			AND a.symbol NOT IN ('TST1','TST2')
		--	AND ((a.created_at = DATE_TRUNC('week', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
			AND u.signup_hostcountry IN ('TH','ID','AU','global')
			AND a.symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
		GROUP BY 1,2,3,4,5,6,7
		ORDER BY 1 DESC 
)	, aum_snapshot AS (
		SELECT 
			DATE_TRUNC('day', a.created_at) created_at
			, a.signup_hostcountry 
			, a.ap_account_id 
			, CASE WHEN is_zipup_subscribed = TRUE AND a.created_at >= DATE_TRUNC('day', zipup_subscribed_at) THEN TRUE ELSE FALSE END AS is_zipup
			, CASE WHEN symbol = 'ZMT' THEN 'ZMT' 
					WHEN symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH') THEN 'zipup_coin'
					ELSE 'non_zipup' END AS asset_type
			, SUM( COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
			, SUM( COALESCE (trade_wallet_amount_usd,0)) trade_wallet_amount_usd
			, SUM( COALESCE (z_wallet_amount, 0)) z_wallet_amount
			, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
			, SUM( COALESCE (trade_wallet_amount, 0) + COALESCE (z_wallet_amount, 0)) total_wallet_amount 
			, SUM( COALESCE (trade_wallet_amount_usd,0) + COALESCE (z_wallet_amount_usd, 0)) total_wallet_usd
			, SUM( COALESCE (ziplock_amount, 0)) ziplock_amount
			, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
		FROM 
			base a 
		WHERE  
			signup_hostcountry IS NOT NULL AND signup_hostcountry <> 'test'
			AND is_nominee = FALSE
		GROUP BY 
			1,2,3,4,5
		ORDER BY 
			1 DESC
)	, active_zipup_balance AS (
		SELECT 
			DATE_TRUNC('week', created_at) created_at 
			, signup_hostcountry
			, ap_account_id 
			, COUNT( DISTINCT created_at) day_count
			, SUM( total_wallet_usd) wallet_usd_amount
			, SUM( z_wallet_amount_usd) z_wallet_amount_usd
			, SUM( CASE WHEN asset_type = 'ZMT' THEN total_wallet_usd END) zmt_usd_amount
			, SUM( CASE WHEN asset_type <> 'ZMT' THEN total_wallet_usd END) nonzmt_usd_amount
			, SUM( CASE WHEN asset_type = 'ZMT' THEN z_wallet_amount_usd END) zmt_zw_usd_amount
			, SUM( CASE WHEN asset_type <> 'ZMT' THEN z_wallet_amount_usd END) nonzmt_zw_usd_amount
		FROM 
			aum_snapshot a 
		WHERE 
			asset_type <> 'non_zipup'
			AND is_zipup = TRUE
		GROUP BY 1,2,3
)	, active_ziplock_balance AS (
		SELECT 
			DATE_TRUNC('week', created_at) created_at 
			, signup_hostcountry
			, ap_account_id 
			, COUNT( DISTINCT created_at) day_count
			, SUM( CASE WHEN asset_type = 'ZMT' THEN ziplock_amount_usd END) zmt_lock_usd_amount
			, SUM( CASE WHEN asset_type <> 'ZMT' THEN ziplock_amount_usd END) nonzmt_lock_usd_amount
			, SUM( ziplock_amount_usd) ziplock_amount_usd
		FROM 
			aum_snapshot a 
		WHERE 
			asset_type <> 'non_zipup'
		--	AND is_zipup = TRUE
		GROUP BY 1,2,3
)	, active_balance AS (
	SELECT 
		COALESCE (u.created_at, l.created_at) created_at 
		, COALESCE (u.signup_hostcountry, l.signup_hostcountry) signup_hostcountry
		, COALESCE (u.ap_account_id, l.ap_account_id) ap_account_id
		, CASE WHEN nonzmt_usd_amount / u.day_count >= 1 THEN u.ap_account_id END AS zipup_user
		, CASE WHEN nonzmt_zw_usd_amount / u.day_count >= 1 THEN u.ap_account_id END AS avg_zipup_nonzmt_zw_user
		, CASE WHEN (COALESCE (zmt_lock_usd_amount,0) / l.day_count + COALESCE (nonzmt_lock_usd_amount,0)) / l.day_count >= 1 THEN l.ap_account_id END AS total_ziplock_user
		, CASE WHEN COALESCE (zmt_lock_usd_amount,0) / l.day_count >= 1 AND COALESCE (nonzmt_lock_usd_amount,0) / l.day_count >= 1 THEN l.ap_account_id END AS ziplock_mix_user
		, CASE WHEN COALESCE (zmt_lock_usd_amount,0) / l.day_count < 1 AND COALESCE (nonzmt_lock_usd_amount,0) / l.day_count >= 1 THEN l.ap_account_id END AS ziplock_nozmt_user
		, CASE WHEN COALESCE (zmt_lock_usd_amount,0) / l.day_count >= 1 AND COALESCE (nonzmt_lock_usd_amount,0) / l.day_count < 1 THEN l.ap_account_id END AS ziplock_zmt_user
		, CASE WHEN u.created_at < '2021-09-01 00:00:00' THEN 
				(CASE WHEN (COALESCE (nonzmt_usd_amount,0) / u.day_count >= 1 OR COALESCE (zmt_lock_usd_amount,0) / l.day_count >= 1 OR COALESCE (nonzmt_lock_usd_amount,0) / l.day_count >= 1) THEN COALESCE (u.ap_account_id, l.ap_account_id) END)
				ELSE 
				(CASE WHEN (COALESCE (nonzmt_zw_usd_amount,0) / u.day_count >= 1 OR COALESCE (zmt_lock_usd_amount,0) / l.day_count >= 1 OR COALESCE (nonzmt_lock_usd_amount,0) / l.day_count >= 1) THEN COALESCE (u.ap_account_id, l.ap_account_id) END)
				END AS active_balance_user
		, CASE WHEN u.created_at < '2021-09-01 00:00:00' THEN 
				(CASE WHEN (COALESCE (wallet_usd_amount,0) / u.day_count >= 1 OR COALESCE (zmt_lock_usd_amount,0) / l.day_count >= 1 OR COALESCE (nonzmt_lock_usd_amount,0) / l.day_count >= 1) THEN COALESCE (u.ap_account_id, l.ap_account_id) END)
				ELSE 
				(CASE WHEN (COALESCE (z_wallet_amount_usd,0) / u.day_count >= 1 OR COALESCE (zmt_lock_usd_amount,0) / l.day_count >= 1 OR COALESCE (nonzmt_lock_usd_amount,0) / l.day_count >= 1) THEN COALESCE (u.ap_account_id, l.ap_account_id) END)
				END AS active_balance_user_n
		, CASE WHEN z_wallet_amount_usd / u.day_count >= 1 THEN u.ap_account_id END AS zipup_zw_user_n
		, CASE WHEN wallet_usd_amount / u.day_count >= 1 THEN u.ap_account_id END AS zipup_user_n
	FROM 
		active_zipup_balance u 
		FULL OUTER JOIN active_ziplock_balance l 
			ON u.created_at = l.created_at
			AND u.ap_account_id = l.ap_account_id
			AND u.signup_hostcountry = l.signup_hostcountry
	WHERE (z_wallet_amount_usd >= 1 OR ziplock_amount_usd >= 1 OR wallet_usd_amount >= 1)
)	, active_trader AS (
		SELECT 
			DISTINCT DATE_TRUNC('week', created_at) created_at 
			, ap_account_id
			, signup_hostcountry 
		FROM analytics.trades_master 
		WHERE 
			ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
			AND signup_hostcountry IN ('TH','ID','AU','global')
			AND created_at >= '2021-10-01 00:00:00' 
			AND created_at < DATE_TRUNC('year', NOW()) 
--- deposit + withdrawl 
)	, deposit_ AS ( 
		SELECT 
			date_trunc('day', d.updated_at) AS month_  
			, d.ap_account_id 
			, d.signup_hostcountry 
			, d.product_type 
			, d.product_symbol 
			,CASE WHEN d.ap_account_id in (1373,1432,13266,16211,16308,22576,34535,48900,53463,80871,84319) THEN TRUE ELSE FALSE END AS is_whale
			, COUNT(d.*) AS deposit_number 
			, SUM(d.amount) AS deposit_amount 
			, SUM(d.amount_usd) deposit_usd
		FROM 
			analytics.deposit_tickets_master d 
		WHERE 
			d.status = 'FullyProcessed' 
			AND d.signup_hostcountry IN ('TH','AU','ID','global')
			AND d.updated_at::date >= '2021-01-01' 
			AND d.updated_at::date < DATE_TRUNC('year', NOW())  
			AND d.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping) 
		GROUP  BY 
			1,2,3,4,5,6
)	, withdraw_ AS (
		SELECT 
			date_trunc('day', w.updated_at) AS month_  
			, w.ap_account_id 
			, w.signup_hostcountry 
			, w.product_type 
			, w.product_symbol 
			,CASE WHEN w.ap_account_id IN (1373,1432,13266,16211,16308,22576,34535,48900,53463,80871,84319) THEN TRUE ELSE FALSE END AS is_whale
			, COUNT(w.*) AS withdraw_number 
			, SUM(w.amount) AS withdraw_amount 
			, SUM(w.amount_usd) withdraw_usd
		FROM  
			analytics.withdraw_tickets_master w 
		WHERE 
			w.status = 'FullyProcessed'
			AND w.signup_hostcountry IN ('TH','AU','ID','global')
			AND w.updated_at::date >= '2021-01-01' 
			AND w.updated_at::date < DATE_TRUNC('year', NOW())  
			AND w.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		GROUP BY 
			1,2,3,4,5,6
)	, depositor_withdrawer AS (
		SELECT 
			DATE_TRUNC('week', COALESCE(d.month_, w.month_)) created_at  
			, COALESCE(d.signup_hostcountry, w.signup_hostcountry) signup_hostcountry
			, COALESCE (d.ap_account_id, w.ap_account_id) ap_account_id 
		--	, COALESCE (d.product_type, w.product_type) product_type 
		--	, COALESCE (d.product_symbol, w.product_symbol) symbol 
		--	, COALESCE(d.is_whale, w.is_whale) is_whale
			, SUM( COALESCE(d.deposit_number, 0)) depost_count 
			, SUM( deposit_amount) deposit_amount
			, SUM( COALESCE(d.deposit_usd, 0)) deposit_usd
			, SUM( COALESCE(w.withdraw_number, 0)) withdraw_count
			, SUM( withdraw_amount) withdraw_amount
			, SUM( COALESCE(w.withdraw_usd, 0)) withdraw_usd
		FROM 
			deposit_ d 
			FULL OUTER JOIN 
				withdraw_ w 
				ON d.ap_account_id = w.ap_account_id 
				AND d.signup_hostcountry = w.signup_hostcountry 
				AND d.product_type = w.product_type 
				AND d.month_ = w.month_ 
				AND d.product_symbol = w.product_symbol 
		WHERE 
			COALESCE(d.month_, w.month_) >= '2021-10-01 00:00:00' 
			AND COALESCE(d.month_, w.month_) < DATE_TRUNC('year', NOW())
		GROUP BY 
			1,2,3
		ORDER BY 
			1,2 
)	, active_trade_deposit_withdraw AS (
	SELECT 
		COALESCE (t.created_at, d.created_at) created_at
		, COALESCE (t.signup_hostcountry, d.signup_hostcountry) signup_hostcountry
		, COALESCE (t.ap_account_id, d.ap_account_id) ap_account_id
		, CASE WHEN t.ap_account_id IS NOT NULL THEN t.ap_account_id END AS active_trader
		, CASE WHEN d.ap_account_id IS NOT NULL THEN d.ap_account_id END AS depositor_withdrawer
	FROM 
		active_trader t
		FULL OUTER JOIN depositor_withdrawer d
			ON t.created_at = d.created_at
			AND t.signup_hostcountry = d.signup_hostcountry
			AND t.ap_account_id = d.ap_account_id
-- zipworld active users = having a completed purchase in zipworld
)	, zipworld AS (
	SELECT 
		DATE_TRUNC('week', p.completed_at) purchased_month
		, um.ap_account_id 
		, um.signup_hostcountry 
		, SUM(p.purchase_price) zmt_amount
	FROM 
		zipworld_public.purchases p 
		LEFT JOIN zipworld_public.users u 
			ON p.user_id = u.id 
		LEFT JOIN analytics.users_master um 
			ON u.zipmex_user_id = um.user_id 
	WHERE 
		p.completed_at IS NOT NULL
		AND um.ap_account_id IS NOT NULL 
		AND p.completed_at >= '2021-10-01 00:00:00'
		AND p.completed_at < DATE_TRUNC('year', NOW())
	GROUP BY 1,2,3
-- Z Launch active users = average ZMT lock amount > 0
)	, zlaunch_base AS (
	SELECT
		DATE_TRUNC('day', event_timestamp) created_at
		, user_id 
		, UPPER(SPLIT_PART(lock_product_id,'.',1)) symbol
		, SUM(CASE WHEN event_type = 'lock' THEN amount END) lock_amount
		, SUM(CASE WHEN event_type IN ('unlock','release') THEN amount END) released_amount
	FROM 
		z_launch_service_public.lock_unlock_histories luh 
	GROUP BY 1,2,3
)	, zlaunch_snapshot AS (
	SELECT 
		DATE_TRUNC('week', p.created_at) staked_month
		, u.ap_account_id 
		, u.signup_hostcountry 
		, symbol
		, SUM(COALESCE (lock_amount, 0)) - SUM(COALESCE (released_amount, 0)) zmt_amount
		, SUM(COALESCE (lock_amount, 0)) - SUM(COALESCE (released_amount, 0)) / COUNT(DISTINCT p.created_at) avg_zmt_staked
	FROM analytics.period_master p
		LEFT JOIN zlaunch_base z 
			ON p.created_at >= z.created_at
		LEFT JOIN analytics.users_master u
			ON z.user_id = u.user_id 
	WHERE 
		p."period" = 'day'
		AND p.created_at >= '2021-10-26 00:00:00'
		AND p.created_at < DATE_TRUNC('year', NOW())
	GROUP BY 1,2,3,4
-- join zipworld and z Launch active users as Zip Products active users
)	, z_products AS (
	SELECT 
		COALESCE (w.purchased_month , l.staked_month) created_at 
		, COALESCE (w.ap_account_id, l.ap_account_id) ap_account_id
		, COALESCE (w.signup_hostcountry, l.signup_hostcountry) signup_hostcountry
		, w.ap_account_id zipwold_user
		, l.ap_account_id zlaunch_staker
	FROM zipworld w 
		FULL OUTER JOIN zlaunch_snapshot l 
			ON w.ap_account_id = l.ap_account_id
			AND w.purchased_month = l.staked_month
-- join Zip Products active users with active traders/ depositors/ withdrawers
)	, active_trader_dw_and_z_products AS (
	SELECT 
		COALESCE (t.created_at, z.created_at) created_at
		, COALESCE (t.signup_hostcountry, z.signup_hostcountry) signup_hostcountry
		, COALESCE (t.ap_account_id, z.ap_account_id) active_tdw_zp
		, t.ap_account_id active_tdw
		, z.ap_account_id active_zp
		, active_trader	, depositor_withdrawer
		, zipwold_user, zlaunch_staker
	FROM active_trade_deposit_withdraw t 
		FULL OUTER JOIN z_products z 
			ON t.created_at = z.created_at
			AND t.signup_hostcountry = z.signup_hostcountry
			AND t.ap_account_id = z.ap_account_id
-- join active traders/ depositors/ withdrawers/ zip products users with Zipup/ Ziplock active balance
)	, final_table AS (
	SELECT
		COALESCE (a.created_at , t.created_at) created_at
		, COALESCE (a.signup_hostcountry , t.signup_hostcountry) signup_hostcountry
		, COALESCE (a.ap_account_id, t.active_tdw_zp) mtu_1
		, COALESCE (a.active_balance_user, t.active_tdw_zp) mtu_2
		, zipup_user , total_ziplock_user
		, ziplock_mix_user, ziplock_nozmt_user, ziplock_zmt_user 
		, active_balance_user
		, active_balance_user_n, zipup_zw_user_n
		, active_trader, depositor_withdrawer
		, avg_zipup_nonzmt_zw_user
		, COALESCE (a.active_balance_user_n, t.active_tdw_zp) mtu_3
		, zipwold_user, zlaunch_staker
	FROM 
		active_balance a 
		FULL OUTER JOIN active_trader_dw_and_z_products t 
			ON a.created_at = t.created_at
			AND a.signup_hostcountry = t.signup_hostcountry
			AND a.ap_account_id = t.active_tdw_zp
)
SELECT
	created_at::date 
	, signup_hostcountry
	, COUNT(DISTINCT active_trader) trader_count
	, COUNT(DISTINCT zipup_user) zipup_user_count
	, COUNT(DISTINCT total_ziplock_user) total_ziplock_user
	, COUNT(DISTINCT ziplock_mix_user) ziplock_mix_user
	, COUNT(DISTINCT ziplock_nozmt_user) ziplock_nozmt_user
	, COUNT(DISTINCT ziplock_zmt_user) ziplock_zmt_user
	, COUNT(DISTINCT CASE WHEN created_at <= '2021-09-30 00:00:00' THEN mtu_1 ELSE mtu_3 END) mtu_count
	, COUNT(DISTINCT active_balance_user_n) active_balance_count
--	, COUNT(DISTINCT avg_zipup_nonzmt_zw_user) non_zmt_zipup_zw_user_count
--	, COUNT(DISTINCT depositor_withdrawer) depositor_withdrawer
--	, COUNT(DISTINCT mtu_1) mtu_1_all_balance
--	, COUNT(DISTINCT mtu_2) mtu_2_1usd_nonzmt
--	, COUNT(DISTINCT mtu_3) mtu_3_1usd_all
--	, COUNT(DISTINCT zipwold_user) zipwold_user
--	, COUNT(DISTINCT zlaunch_staker) zlaunch_staker
FROM
	final_table
GROUP BY 1,2
;


---- MTU 2021-10-05 -- JAN_ SEP 2021 -- active balance (Zipup/ Ziplock) -- active Trader -- active depositor/ withdrawer
WITH base AS (
		SELECT 
			a.created_at
			, CASE WHEN u.signup_hostcountry IN ('test', 'error','xbullion') THEN 'test' ELSE u.signup_hostcountry END AS signup_hostcountry 
			, a.ap_account_id 
			, CASE WHEN a.ap_account_id IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 496001) 
					THEN TRUE ELSE FALSE END AS is_nominee
			, a.symbol 
			, u.zipup_subscribed_at 
			, u.is_zipup_subscribed 
			, SUM(trade_wallet_amount) trade_wallet_amount
			, SUM(z_wallet_amount) z_wallet_amount
			, SUM(ziplock_amount) ziplock_amount
			, SUM( CASE WHEN a.symbol = 'USD' THEN trade_wallet_amount * 1
						WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
						WHEN r.product_type = 2 THEN trade_wallet_amount * r.price END) trade_wallet_amount_usd
			, SUM( z_wallet_amount * r.price ) z_wallet_amount_usd
			, SUM( ziplock_amount * r.price ) ziplock_amount_usd
		FROM 
			analytics.wallets_balance_eod a 
			LEFT JOIN 
				analytics.users_master u 
				ON a.ap_account_id = u.ap_account_id 
			LEFT JOIN 
				analytics.rates_master r 
				ON a.symbol = r.product_1_symbol 
			    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
		WHERE 
			a.created_at >= '2021-07-01 00:00:00'
			AND a.created_at < '2021-10-01 00:00:00'
			AND a.symbol NOT IN ('TST1','TST2')
			AND ((a.created_at = DATE_TRUNC('week', a.created_at) + '1 week' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
			AND u.signup_hostcountry IN ('TH','ID','AU','global')
			AND a.symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
		GROUP BY 1,2,3,4,5,6,7
		ORDER BY 1 DESC 
)	, aum_snapshot AS (
		SELECT 
			DATE_TRUNC('week', a.created_at) created_at
			, a.signup_hostcountry 
			, a.ap_account_id 
			, CASE WHEN is_zipup_subscribed = TRUE AND a.created_at >= DATE_TRUNC('day', zipup_subscribed_at) THEN TRUE ELSE FALSE END AS is_zipup
			, CASE WHEN symbol = 'ZMT' THEN 'ZMT' 
					WHEN symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH') THEN 'zipup_coin'
					ELSE 'non_zipup' END AS asset_type
			, SUM( COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
			, SUM( COALESCE (trade_wallet_amount_usd,0)) trade_wallet_amount_usd
			, SUM( COALESCE (z_wallet_amount, 0)) z_wallet_amount
			, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
			, SUM( COALESCE (trade_wallet_amount, 0) + COALESCE (z_wallet_amount, 0)) total_wallet_amount 
			, SUM( COALESCE (trade_wallet_amount_usd,0) + COALESCE (z_wallet_amount_usd, 0)) total_wallet_usd
			, SUM( COALESCE (ziplock_amount, 0)) ziplock_amount
			, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
		FROM 
			base a 
		WHERE  
			signup_hostcountry IS NOT NULL AND signup_hostcountry <> 'test'
			AND is_nominee = FALSE
		GROUP BY 
			1,2,3,4,5
		ORDER BY 
			1 DESC
)	, active_zipup_balance AS (
		SELECT 
			created_at 
			, signup_hostcountry
			, ap_account_id 
			, SUM( total_wallet_usd) wallet_usd_amount
			, SUM( z_wallet_amount_usd) z_wallet_amount_usd
			, SUM( CASE WHEN asset_type = 'ZMT' THEN total_wallet_usd END) zmt_usd_amount
			, SUM( CASE WHEN asset_type <> 'ZMT' THEN total_wallet_usd END) nonzmt_usd_amount
			, SUM( CASE WHEN asset_type = 'ZMT' THEN z_wallet_amount_usd END) zmt_zw_usd_amount
			, SUM( CASE WHEN asset_type <> 'ZMT' THEN z_wallet_amount_usd END) nonzmt_zw_usd_amount
		FROM 
			aum_snapshot a 
		WHERE 
			asset_type <> 'non_zipup'
			AND is_zipup = TRUE
		GROUP BY 1,2,3
)	, active_ziplock_balance AS (
		SELECT 
			created_at 
			, signup_hostcountry
			, ap_account_id 
			, SUM( CASE WHEN asset_type = 'ZMT' THEN ziplock_amount_usd END) zmt_lock_usd_amount
			, SUM( CASE WHEN asset_type <> 'ZMT' THEN ziplock_amount_usd END) nonzmt_lock_usd_amount
		FROM 
			aum_snapshot a 
		WHERE 
			asset_type <> 'non_zipup'
		--	AND is_zipup = TRUE
		GROUP BY 1,2,3
)	, active_balance AS (
	SELECT 
		COALESCE (u.created_at, l.created_at) created_at 
		, COALESCE (u.signup_hostcountry, l.signup_hostcountry) signup_hostcountry
		, COALESCE (u.ap_account_id, l.ap_account_id) ap_account_id
		, CASE WHEN nonzmt_usd_amount >= 1 THEN u.ap_account_id END AS zipup_user
		, CASE WHEN nonzmt_zw_usd_amount >= 1 THEN u.ap_account_id END AS zipup_zw_user
		, CASE WHEN (COALESCE (zmt_lock_usd_amount,0) + COALESCE (nonzmt_lock_usd_amount,0)) >= 1 THEN l.ap_account_id END AS total_ziplock_user
		, CASE WHEN COALESCE (zmt_lock_usd_amount,0) >= 1 AND COALESCE (nonzmt_lock_usd_amount,0) >= 1 THEN l.ap_account_id END AS ziplock_mix_user
		, CASE WHEN COALESCE (zmt_lock_usd_amount,0) < 1 AND COALESCE (nonzmt_lock_usd_amount,0) >= 1 THEN l.ap_account_id END AS ziplock_nozmt_user
		, CASE WHEN COALESCE (zmt_lock_usd_amount,0) >= 1 AND COALESCE (nonzmt_lock_usd_amount,0) < 1 THEN l.ap_account_id END AS ziplock_zmt_user
		, CASE WHEN u.created_at < '2021-09-01 00:00:00' THEN 
				(CASE WHEN (COALESCE (nonzmt_usd_amount,0) >= 1 OR COALESCE (zmt_lock_usd_amount,0) >= 1 OR COALESCE (nonzmt_lock_usd_amount,0) >= 1) THEN COALESCE (u.ap_account_id, l.ap_account_id) END)
				ELSE 
				(CASE WHEN (COALESCE (nonzmt_zw_usd_amount,0) >= 1 OR COALESCE (zmt_lock_usd_amount,0) >= 1 OR COALESCE (nonzmt_lock_usd_amount,0) >= 1) THEN COALESCE (u.ap_account_id, l.ap_account_id) END)
				END AS active_balance_user
		, CASE WHEN u.created_at < '2021-09-01 00:00:00' THEN 
				(CASE WHEN (COALESCE (wallet_usd_amount,0) >= 1 OR COALESCE (zmt_lock_usd_amount,0) >= 1 OR COALESCE (nonzmt_lock_usd_amount,0) >= 1) THEN COALESCE (u.ap_account_id, l.ap_account_id) END)
				ELSE 
				(CASE WHEN (COALESCE (z_wallet_amount_usd,0) >= 1 OR COALESCE (zmt_lock_usd_amount,0) >= 1 OR COALESCE (nonzmt_lock_usd_amount,0) >= 1) THEN COALESCE (u.ap_account_id, l.ap_account_id) END)
				END AS active_balance_user_n
		, CASE WHEN z_wallet_amount_usd >= 1 THEN u.ap_account_id END AS zipup_zw_user_n
		, CASE WHEN wallet_usd_amount >= 1 THEN u.ap_account_id END AS zipup_user_n
	FROM 
		active_zipup_balance u 
		FULL OUTER JOIN active_ziplock_balance l 
			ON u.created_at = l.created_at
			AND u.ap_account_id = l.ap_account_id
			AND u.signup_hostcountry = l.signup_hostcountry
	WHERE
		(nonzmt_usd_amount >= 1 OR zmt_lock_usd_amount >= 1 OR nonzmt_lock_usd_amount >= 1 OR nonzmt_zw_usd_amount >= 1)
)	, active_trader AS (
		SELECT 
			DISTINCT DATE_TRUNC('week', created_at) created_at 
			, ap_account_id
			, signup_hostcountry 
		FROM analytics.trades_master 
		WHERE 
			ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','27443','37807','37955','38121','38260','38262','38263'
,'40683','40706','44056','44057','44679','48948','49649','49658','49659','52018','52019','63152','161347','316078','317029','335645','496001','610371','710015','729499',
			0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 496001)
			AND signup_hostcountry IN ('TH','ID','AU','global')
			AND created_at >= '2021-07-01 00:00:00' 
			AND created_at < '2021-10-01 00:00:00'
--- deposit + withdrawl 
)	, deposit_ AS ( 
		SELECT 
			date_trunc('day', d.updated_at) AS month_  
			, d.ap_account_id 
			, d.signup_hostcountry 
			, d.product_type 
			, d.product_symbol 
			,CASE WHEN d.ap_account_id in (1373,1432,13266,16211,16308,22576,34535,48900,53463,80871,84319) THEN TRUE ELSE FALSE END AS is_whale
			, COUNT(d.*) AS deposit_number 
			, SUM(d.amount) AS deposit_amount 
			, SUM(d.amount_usd) deposit_usd
		FROM 
			analytics.deposit_tickets_master d 
		WHERE 
			d.status = 'FullyProcessed' 
			AND d.signup_hostcountry IN ('TH','AU','ID','global')
			AND d.updated_at::date >= '2021-07-01' 
			AND d.updated_at::date < '2021-10-01'
			AND d.ap_account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347, 317029, 496001) 
		GROUP  BY 
			1,2,3,4,5,6
)	, withdraw_ AS (
		SELECT 
			date_trunc('day', w.updated_at) AS month_  
			, w.ap_account_id 
			, w.signup_hostcountry 
			, w.product_type 
			, w.product_symbol 
			,CASE WHEN w.ap_account_id IN (1373,1432,13266,16211,16308,22576,34535,48900,53463,80871,84319) THEN TRUE ELSE FALSE END AS is_whale
			, COUNT(w.*) AS withdraw_number 
			, SUM(w.amount) AS withdraw_amount 
			, SUM(w.amount_usd) withdraw_usd
		FROM  
			analytics.withdraw_tickets_master w 
		WHERE 
			w.status = 'FullyProcessed'
			AND w.signup_hostcountry IN ('TH','AU','ID','global')
			AND w.updated_at::date >= '2021-07-01' 
			AND w.updated_at::date < '2021-10-01' 
			AND w.ap_account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347, 317029, 496001)
		GROUP BY 
			1,2,3,4,5,6
)	, depositor_withdrawer AS (
		SELECT 
			DATE_TRUNC('week', COALESCE(d.month_, w.month_)) created_at  
			, COALESCE(d.signup_hostcountry, w.signup_hostcountry) signup_hostcountry
			, COALESCE (d.ap_account_id, w.ap_account_id) ap_account_id 
		--	, COALESCE (d.product_type, w.product_type) product_type 
		--	, COALESCE (d.product_symbol, w.product_symbol) symbol 
		--	, COALESCE(d.is_whale, w.is_whale) is_whale
			, SUM( COALESCE(d.deposit_number, 0)) depost_count 
			, SUM( deposit_amount) deposit_amount
			, SUM( COALESCE(d.deposit_usd, 0)) deposit_usd
			, SUM( COALESCE(w.withdraw_number, 0)) withdraw_count
			, SUM( withdraw_amount) withdraw_amount
			, SUM( COALESCE(w.withdraw_usd, 0)) withdraw_usd
		FROM 
			deposit_ d 
			FULL OUTER JOIN 
				withdraw_ w 
				ON d.ap_account_id = w.ap_account_id 
				AND d.signup_hostcountry = w.signup_hostcountry 
				AND d.product_type = w.product_type 
				AND d.month_ = w.month_ 
				AND d.product_symbol = w.product_symbol 
		WHERE 
			COALESCE(d.month_, w.month_) >= '2021-07-01 00:00:00' 
			AND COALESCE(d.month_, w.month_) < '2021-10-01 00:00:00'
		GROUP BY 
			1,2,3
		ORDER BY 
			1,2 
)	, active_trade_deposit_withdraw AS (
	SELECT 
		COALESCE (t.created_at, d.created_at) created_at
		, COALESCE (t.signup_hostcountry, d.signup_hostcountry) signup_hostcountry
		, COALESCE (t.ap_account_id, d.ap_account_id) ap_account_id
		, CASE WHEN t.ap_account_id IS NOT NULL THEN t.ap_account_id END AS active_trader
		, CASE WHEN d.ap_account_id IS NOT NULL THEN d.ap_account_id END AS depositor_withdrawer
	FROM 
		active_trader t
		FULL OUTER JOIN depositor_withdrawer d
			ON t.created_at = d.created_at
			AND t.signup_hostcountry = d.signup_hostcountry
			AND t.ap_account_id = d.ap_account_id
)	, final_table AS (
	SELECT
		COALESCE (a.created_at , t.created_at) created_at
		, COALESCE (a.signup_hostcountry , t.signup_hostcountry) signup_hostcountry
		, COALESCE (a.ap_account_id, t.ap_account_id) mtu_1
		, COALESCE (a.active_balance_user, t.ap_account_id) mtu_2
		, zipup_user , zipup_zw_user , total_ziplock_user, ziplock_mix_user, ziplock_nozmt_user, ziplock_zmt_user , active_balance_user, active_balance_user_n, zipup_zw_user_n
		, active_trader, depositor_withdrawer
		, COALESCE (a.active_balance_user_n, t.ap_account_id) mtu_3
	FROM 
		active_balance a 
		FULL OUTER JOIN active_trade_deposit_withdraw t 
			ON a.created_at = t.created_at
			AND a.signup_hostcountry = t.signup_hostcountry
			AND a.ap_account_id = t.ap_account_id
)
SELECT
	created_at::DATE
	, signup_hostcountry
--	, COUNT(DISTINCT active_trader) trader_count
--	, COUNT(DISTINCT zipup_user) zipup_user_count
--	, COUNT(DISTINCT total_ziplock_user) total_ziplock_user
--	, COUNT(DISTINCT ziplock_mix_user) ziplock_mix_user
--	, COUNT(DISTINCT ziplock_nozmt_user) ziplock_nozmt_user
--	, COUNT(DISTINCT ziplock_zmt_user) ziplock_zmt_user
	, COUNT(DISTINCT CASE WHEN created_at <= '2021-09-30 00:00:00' THEN mtu_1 ELSE mtu_3 END) mtu_count
--	, COUNT(DISTINCT active_balance_user_n) active_balance_count
--	, COUNT(DISTINCT zipup_zw_user) zipup_zw_user_count
--	, COUNT(DISTINCT depositor_withdrawer) depositor_withdrawer
--	, COUNT(DISTINCT mtu_1) mtu_1_all_balance
--	, COUNT(DISTINCT mtu_2) mtu_2_1usd_nonzmt
--	, COUNT(DISTINCT mtu_3) mtu_3_1usd_all
FROM
	final_table
GROUP BY 1,2
;