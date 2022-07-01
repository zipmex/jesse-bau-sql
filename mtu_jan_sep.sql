---- MTU 2021-10-05 -- active balance (Zipup/ Ziplock) -- active Trader -- active depositor/ withdrawer
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
			a.created_at >= '2021-01-01 00:00:00'
			AND a.created_at < '2021-10-01 00:00:00' -- DATE_TRUNC('day', NOW()) 
			AND a.symbol NOT IN ('TST1','TST2')
			AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
			AND u.signup_hostcountry IN ('TH','ID','AU','global')
			AND a.symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
		GROUP BY 1,2,3,4,5,6,7
		ORDER BY 1 DESC 
	-- AUM snapshot by end of the month or yesterday
)	, aum_snapshot AS (
		SELECT 
			DATE_TRUNC('month', a.created_at) created_at
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
	-- zipup users defined by zipup_subscribed status
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
	-- ziplock users regardless of zipup_subscribed status
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
	-- combine zipup and ziplock users
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
	-- active traders
)	, active_trader AS (
		SELECT 
			DISTINCT DATE_TRUNC('month', created_at) created_at 
			, ap_account_id
			, signup_hostcountry 
		FROM analytics.trades_master 
		WHERE 
			ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','27443','37807','37955','38121','38260','38262','38263'
					,'40683','40706','44056','44057','44679','48948','49649','49658','49659','52018','52019','63152','161347','316078','317029','335645','496001','610371','710015','729499',
			0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 496001)
			AND signup_hostcountry IN ('TH','ID','AU','global')
			AND created_at >= '2021-01-01 00:00:00' 
			AND created_at < '2021-10-01 00:00:00' -- DATE_TRUNC('day', NOW()) 
--- deposit + withdrawl 
)	, deposit_ AS ( 
		SELECT 
			date_trunc('day', d.updated_at) AS month_  
			, d.ap_account_id 
			, d.signup_hostcountry 
			, d.product_type 
			, d.product_symbol 
			, COUNT(d.*) AS deposit_number 
			, SUM(d.amount) AS deposit_amount 
			, SUM(d.amount_usd) deposit_usd
		FROM 
			analytics.deposit_tickets_master d 
		WHERE 
			d.status = 'FullyProcessed' 
			AND d.signup_hostcountry IN ('TH','AU','ID','global')
			AND d.updated_at::date >= '2021-01-01' 
			AND d.updated_at::date < DATE_TRUNC('day', NOW())  
			AND d.ap_account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347, 317029, 496001) 
		GROUP  BY 
			1,2,3,4,5
)	, withdraw_ AS (
		SELECT 
			date_trunc('day', w.updated_at) AS month_  
			, w.ap_account_id 
			, w.signup_hostcountry 
			, w.product_type 
			, w.product_symbol 
			, COUNT(w.*) AS withdraw_number 
			, SUM(w.amount) AS withdraw_amount 
			, SUM(w.amount_usd) withdraw_usd
		FROM  
			analytics.withdraw_tickets_master w 
		WHERE 
			w.status = 'FullyProcessed'
			AND w.signup_hostcountry IN ('TH','AU','ID','global')
			AND w.updated_at::date >= '2021-01-01' 
			AND w.updated_at::date < DATE_TRUNC('day', NOW())  
			AND w.ap_account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347, 317029, 496001)
		GROUP BY 
			1,2,3,4,5
	-- active depositor and withdrawer
)	, depositor_withdrawer AS (
		SELECT 
			DATE_TRUNC('month', COALESCE(d.month_, w.month_)) created_at  
			, COALESCE(d.signup_hostcountry, w.signup_hostcountry) signup_hostcountry
			, COALESCE (d.ap_account_id, w.ap_account_id) ap_account_id 
			, SUM( COALESCE(d.deposit_number, 0)) deposit_count 
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
			COALESCE(d.month_, w.month_) >= '2021-01-01 00:00:00' 
			AND COALESCE(d.month_, w.month_) < '2021-10-01 00:00:00' --  DATE_TRUNC('day', NOW())
		GROUP BY 
			1,2,3
		ORDER BY 
			1,2 
	-- combine trade - deposit - withdraw
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
	-- combine trade - deposit - withdraw - active balance (zipup-ziplock)
)	, final_table AS (
	SELECT
		COALESCE (a.created_at , t.created_at) created_at
		, COALESCE (a.signup_hostcountry , t.signup_hostcountry) signup_hostcountry
		, COALESCE (a.ap_account_id, t.ap_account_id) mtu_1
		, COALESCE (a.active_balance_user, t.ap_account_id) mtu_2
		, zipup_user , zipup_zw_user , zipup_zw_user_n
		, total_ziplock_user, ziplock_mix_user, ziplock_nozmt_user, ziplock_zmt_user 
		, active_balance_user, active_balance_user_n
		, active_trader, depositor_withdrawer
		, COALESCE (a.active_balance_user_n, t.ap_account_id) mtu_3
	FROM 
		active_balance a 
		FULL OUTER JOIN active_trade_deposit_withdraw t 
			ON a.created_at = t.created_at
			AND a.signup_hostcountry = t.signup_hostcountry
			AND a.ap_account_id = t.ap_account_id
-- unique count
)
SELECT
	created_at 
	, signup_hostcountry
-- active balance MTU, before Sep: taking from both trade + z wallets
-- after Sep: taking z wallet only
	, COUNT(DISTINCT CASE WHEN created_at <= '2021-09-30 00:00:00' THEN mtu_1 ELSE mtu_3 END) mtu_count 
	, COUNT(DISTINCT active_balance_user_n) active_balance_count
--zipup non-zmt active users
	, COUNT(DISTINCT zipup_zw_user) zipup_zw_user_count
	, COUNT(DISTINCT depositor_withdrawer) depositor_withdrawer
	, COUNT(DISTINCT mtu_1) mtu_1_all_balance
	, COUNT(DISTINCT mtu_2) mtu_2_1usd_nonzmt
	, COUNT(DISTINCT mtu_3) mtu_3_1usd_all
FROM
	final_table
GROUP BY 1,2
;