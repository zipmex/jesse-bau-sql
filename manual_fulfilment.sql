SELECT
	d.snapshot_utc::timestamp snapshot_utc, u.ap_account_id , u.signup_hostcountry , UPPER(SPLIT_PART(s.product_id,'.',1)) symbol , SUM(s.balance) zmt_stake_amount
FROM
	generate_series('2021-09-25'::date, NOW()::date, '1 day'::INTERVAL) d (snapshot_utc)
LEFT JOIN LATERAL 
	(
	SELECT 
		DISTINCT ON (user_id, product_id) user_id, product_id, balance, balance_datetime
	FROM 
		zip_lock_service_public.vault_accumulated_balances
	WHERE 
	balance_datetime::date <= d.snapshot_utc 
	ORDER BY user_id, product_id, balance_datetime desc
	) s ON TRUE
		LEFT JOIN 
		analytics.users_master u
		ON s.user_id = u.user_id
WHERE 
	s.product_id IN ('zmt.th','zmt.global')
GROUP BY 
	1,2,3,4
ORDER BY 
	1,2,3
;

WITH vip_tier AS (	-- calculate daily vip tier using zmt lock balance
		SELECT 
			created_at , ap_account_id , symbol 
			, CASE WHEN ziplock_amount >= 100 AND ziplock_amount < 1000 THEN 'vip1'
					WHEN ziplock_amount >= 1000 AND ziplock_amount < 5000 THEN 'vip2'
					WHEN ziplock_amount >= 5000 AND ziplock_amount < 20000 THEN 'vip3'
					WHEN ziplock_amount >= 20000 THEN 'vip4'
					ELSE 'no_tier' END AS vip_tier
			, CASE WHEN ziplock_amount >= 100 AND ziplock_amount < 20000 THEN 'ZipMember'
					WHEN ziplock_amount >= 20000 THEN 'ZipCrew'
					ELSE 'ZipStarter' END AS zip_tier
		FROM 
			analytics.wallets_balance_eod wbe 
		WHERE 
			symbol = 'ZMT'
	)	, daily_interest AS (
	SELECT 
		w.created_at 
		, w.ap_account_id 
		, signup_hostcountry
		, w.symbol 
		, w.z_wallet_amount , w.ziplock_amount 
		, vip_tier
		-- calculate daily Zip Up interest by vip tier
		-- formula for zipup/ ziplock interest is available in Confluence
		, is_zipup_subscribed
		, CASE WHEN u.is_zipup_subscribed = TRUE THEN 
				(CASE WHEN vip_tier = 'no_tier' THEN
						(CASE WHEN w.symbol IN ('ZMT') THEN z_wallet_amount * ((1.0 + 0.06)^(1.0/365)-1)
								WHEN w.symbol IN ('BTC','LTC','ETH','USDT') THEN z_wallet_amount * ((1.0 + 0.02)^(1.0/365)-1)
								WHEN w.symbol IN ('USDC','GOLD') THEN z_wallet_amount * ((1.0 + 0.03)^(1.0/365)-1)
								END)
						WHEN vip_tier = 'vip1' THEN
						(CASE WHEN w.symbol IN ('ZMT') THEN z_wallet_amount * ((1.0 + 0.0675)^(1.0/365)-1)
								WHEN w.symbol IN ('BTC','LTC','ETH','USDT') THEN z_wallet_amount * ((1.0 + 0.0275)^(1.0/365)-1)
								WHEN w.symbol IN ('USDC','GOLD') THEN z_wallet_amount * ((1.0 + 0.0375)^(1.0/365)-1)
								END)	
						WHEN vip_tier = 'vip2' THEN
						(CASE WHEN w.symbol IN ('ZMT') THEN z_wallet_amount * ((1.0 + 0.07)^(1.0/365)-1)
								WHEN w.symbol IN ('BTC','LTC','ETH','USDT') THEN z_wallet_amount * ((1.0 + 0.03)^(1.0/365)-1)
								WHEN w.symbol IN ('USDC','GOLD') THEN z_wallet_amount * ((1.0 + 0.04)^(1.0/365)-1)
								END)	
						WHEN vip_tier = 'vip3' THEN
						(CASE WHEN w.symbol IN ('ZMT') THEN z_wallet_amount * ((1.0 + 0.075)^(1.0/365)-1)
								WHEN w.symbol IN ('BTC','LTC','ETH','USDT') THEN z_wallet_amount * ((1.0 + 0.035)^(1.0/365)-1)
								WHEN w.symbol IN ('USDC','GOLD') THEN z_wallet_amount * ((1.0 + 0.045)^(1.0/365)-1)
								END)	
						WHEN vip_tier = 'vip4' THEN
						(CASE WHEN w.symbol IN ('ZMT') THEN z_wallet_amount * ((1.0 + 0.08)^(1.0/365)-1)
								WHEN w.symbol IN ('BTC','LTC','ETH','USDT') THEN z_wallet_amount * ((1.0 + 0.04)^(1.0/365)-1)
								WHEN w.symbol IN ('USDC','GOLD') THEN z_wallet_amount * ((1.0 + 0.05)^(1.0/365)-1)
								END)
					END)
				ELSE 0 END AS zipup_interest
		-- calculate daily Zip Lock interest by vip tier
		, CASE WHEN vip_tier = 'no_tier' THEN
				(CASE WHEN w.symbol IN ('ZMT') THEN ziplock_amount * ((1.0 + 0.12)^(1.0/365)-1)
						WHEN w.symbol IN ('BTC','ETH','USDT') THEN ziplock_amount * ((1.0 + 0.04)^(1.0/365)-1)
						WHEN w.symbol IN ('LTC') THEN ziplock_amount * ((1.0 + 0.03)^(1.0/365)-1)
						WHEN w.symbol IN ('USDC') THEN ziplock_amount * ((1.0 + 0.09)^(1.0/365)-1)
						END)
				WHEN vip_tier = 'vip1' THEN
				(CASE WHEN w.symbol IN ('ZMT') THEN ziplock_amount * ((1.0 + 0.1275)^(1.0/365)-1)
						WHEN w.symbol IN ('BTC','ETH','USDT') THEN ziplock_amount * ((1.0 + 0.0475)^(1.0/365)-1)
						WHEN w.symbol IN ('LTC') THEN ziplock_amount * ((1.0 + 0.0375)^(1.0/365)-1)
						WHEN w.symbol IN ('USDC') THEN ziplock_amount * ((1.0 + 0.0975)^(1.0/365)-1)
						END)	
				WHEN vip_tier = 'vip2' THEN
				(CASE WHEN w.symbol IN ('ZMT') THEN ziplock_amount * ((1.0 + 0.13)^(1.0/365)-1)
						WHEN w.symbol IN ('BTC','ETH','USDT') THEN ziplock_amount * ((1.0 + 0.05)^(1.0/365)-1)
						WHEN w.symbol IN ('LTC') THEN ziplock_amount * ((1.0 + 0.04)^(1.0/365)-1)
						WHEN w.symbol IN ('USDC') THEN ziplock_amount * ((1.0 + 0.1)^(1.0/365)-1)
						END)	
				WHEN vip_tier = 'vip3' THEN
				(CASE WHEN w.symbol IN ('ZMT') THEN ziplock_amount * ((1.0 + 0.135)^(1.0/365)-1)
						WHEN w.symbol IN ('BTC','ETH','USDT') THEN ziplock_amount * ((1.0 + 0.055)^(1.0/365)-1)
						WHEN w.symbol IN ('LTC') THEN ziplock_amount * ((1.0 + 0.045)^(1.0/365)-1)
						WHEN w.symbol IN ('USDC') THEN ziplock_amount * ((1.0 + 0.105)^(1.0/365)-1)
						END)	
				WHEN vip_tier = 'vip4' THEN
				(CASE WHEN w.symbol IN ('ZMT') THEN ziplock_amount * ((1.0 + 0.14)^(1.0/365)-1)
						WHEN w.symbol IN ('BTC','ETH','USDT') THEN ziplock_amount * ((1.0 + 0.06)^(1.0/365)-1)
						WHEN w.symbol IN ('LTC') THEN ziplock_amount * ((1.0 + 0.05)^(1.0/365)-1)
						WHEN w.symbol IN ('USDC') THEN ziplock_amount * ((1.0 + 0.11)^(1.0/365)-1)
						END)
			END AS ziplock_interest
	FROM 
		analytics.wallets_balance_eod w
		LEFT JOIN analytics.users_master u
			ON w.ap_account_id = u.ap_account_id 
		LEFT JOIN vip_tier z 
			ON w.ap_account_id = z.ap_account_id
			AND w.created_at = z.created_at
	WHERE 
		w.symbol IN ('ZMT') --'BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 
		AND w.created_at >= DATE_TRUNC('day', NOW()) - '1 month'::INTERVAL
		AND w.ap_account_id NOT IN 
									(
									6147, 9249, 27308 --airdrop marketing       
									,63312 ,63313 --dedicated locked 
									,161347 --fake acct zmt.trader@zipmex.com
									,37955 ,37807 ,38121 ,38260 ,38262 ,38263 ,40683 ,40706 --ZMT in TH Circulation
									,48870 ,48948 --zipinterest
									,496001 --new combined wallet & lock
									)
		AND (z_wallet_amount > 0 OR ziplock_amount > 0)
		AND vip_tier IS NOT NULL 
		AND w.ap_account_id = 99961 -- TEST account here 
	)
-- cumulative interest for end of month transfer
SELECT 
	*
	, SUM(zipup_interest) OVER(PARTITION BY DATE_TRUNC('month', created_at), ap_account_id, symbol ORDER BY created_at) cumulative_zipup_intestest
	, SUM(ziplock_interest) OVER(PARTITION BY ap_account_id, symbol ORDER BY created_at) cumulative_ziplock_intestest 
FROM daily_interest
ORDER BY 1 DESC 
;


SELECT 63.68421052 * ((1.0 + 0.14)^(1.0/365)-1)