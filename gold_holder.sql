---- user_location by mobile number 
WITH user_info AS (
SELECT 
	u.user_id 
	, u.ap_account_id 
	, u.email 
	, is_zipup_subscribed 
	, u.mobile_number
	, CASE WHEN LEFT(u.mobile_number, 3) = '+65' THEN 'SG' 
			WHEN LEFT(u.mobile_number, 3) = '+66' THEN 'TH'
			WHEN LEFT(u.mobile_number, 4) = '+628' THEN 'ID'
			WHEN LEFT(u.mobile_number, 3) = '+61' THEN 'AU'
			WHEN u.mobile_number IS NULL THEN 'UNKNOWN'
			ELSE 'global' END AS user_located
--	, p.info ->> 'permanent_address' permanent_address	, p.info ->> 'address_in_id_card' address_in_id_card
--	, p.info ->> 'present_address' present_address	, p.info ->> 'work_address' work_address
	, u.signup_hostcountry 
	, p.info ->> 'country' pi_country 
	, d.country od_country 
	, m.signup_hostname 
	, COALESCE (SUM( CASE WHEN l.service_id = 'zip_lock' AND UPPER(SPLIT_PART(l.product_id,'.',1)) = 'ZMT' THEN l.credit - l.debit END), 0) zmt_staked_amount 
	, COALESCE (SUM( CASE WHEN l.service_id = 'main_wallet' AND UPPER(SPLIT_PART(l.product_id,'.',1)) = 'GOLD' THEN l.credit - l.debit END), 0) z_wallet_gold_amount 
FROM analytics.users_master u
	LEFT JOIN user_app_public.personal_infos p 
		ON u.user_id = p.user_id 
		AND p.archived_at IS NULL
	LEFT JOIN 
		( 	SELECT * , ROW_NUMBER() OVER(PARTITION BY applicant_id ORDER BY inserted_at DESC) row_ 
			FROM user_app_public.onfido_documents
			WHERE archived_at IS NULL 
			) d 
		ON u.onfido_applicant_id = d.applicant_id 
		AND d.row_ = 1
	LEFT JOIN user_app_public.users m 
		ON u.user_id = m.id 
	LEFT JOIN 
		asset_manager_public.ledgers l 
		ON u.user_id = l.account_id 
		AND DATE_TRUNC('day', NOW()) >= DATE_TRUNC('day', l.updated_at)
--		AND l.service_id = 'zip_lock'
WHERE 
	u.signup_hostcountry IN ('AU','global') --,'TH','ID'
	AND is_verified = TRUE 
GROUP BY 1,2,3,4,5,6,7,8,9,10
), account_balance AS (
	SELECT 
		created_at
		, account_id 
		, symbol 
		, mid_price gold_price 
		, SUM(amount) quantity 
		, SUM(usd_amount) AS usd_amount
	FROM (
		SELECT date_trunc('day',a.created_at) AS created_at ,a.account_id , a.product_id, p.symbol 
			, amount , g.mid_price , z.price, 1/e.exchange_rate AS exchange_rate , c.average_high_low
			, SUM(CASE WHEN a.product_id = 6 THEN a.amount * 1
			ELSE a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) END) usd_amount
		FROM public.accounts_positions_daily a
		LEFT JOIN apex.products p
				ON a.product_id = p.product_id
			LEFT JOIN oms_data_public.cryptocurrency_prices c 
			    ON ((CONCAT(p.symbol, 'USD') = c.instrument_symbol) OR (c.instrument_symbol = 'MIOTAUSD' AND p.symbol ='IOTA'))
			    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.last_updated) + '1 day'::INTERVAL 
			LEFT join public.daily_closing_gold_prices g
				ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)
				AND a.product_id IN (15, 35)
			LEFT JOIN public.daily_ap_prices z
				ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
				AND z.instrument_symbol  = 'ZMTUSD'
				AND a.product_id IN (16, 50)
			LEFT JOIN oms_data_public.exchange_rates e
				ON date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
				AND e.product_2_symbol  = p.symbol
				AND e."source" = 'coinmarketcap'
		WHERE 
			a.created_at >= DATE_TRUNC('day',NOW()) - '1 day'::INTERVAL 
			AND a.created_at < DATE_TRUNC('day',NOW()) --<<<<<<<<CHANGE DATE HERE
			AND a.account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 496001) 
			AND a.product_id IN (15,35) --(14,30,33,34) -- GOLD
		GROUP BY 1,2,3,4,5,6,7,8,9
		) a
	GROUP BY 1,2,3,4
	ORDER BY 1 DESC 
)
SELECT 
	u.*
	, CASE WHEN user_located = 'SG' OR pi_country = 'SGP' OR od_country = 'SGP' THEN TRUE ELSE FALSE END AS is_sg_resident 
	, CASE WHEN zmt_staked_amount >= 100 AND zmt_staked_amount < 20000 THEN 'zip_member' 
			WHEN zmt_staked_amount >= 20000 THEN 'zip_crew'
--			WHEN zmt_staked_amount >= 0 AND zmt_staked_amount < 100 THEN 'zip_starter'
			ELSE 'zip_starter'
			END AS membership_level 
	, b.symbol 
	, b.quantity tradewallet_gold_balance 
	, b.usd_amount tradewallet_gold_balance_usd 
	, b.gold_price 
FROM 
	user_info u 
	LEFT JOIN account_balance b 
		ON u.ap_account_id = b.account_id
WHERE 
	b.quantity IS NOT NULL 
;



SELECT 
	signup_hostcountry 
	, signup_hostname 
	, COUNT(DISTINCT CASE WHEN LEFT(mobile_number,3) = '+66' THEN user_id END) AS th_register
	, COUNT(DISTINCT CASE WHEN LEFT(mobile_number,3) = '+66' AND is_verified = TRUE THEN user_id END) AS th_verified 
FROM base 
GROUP BY 1,2
;



SELECT 
	wbe.created_at::DATE
	, um.signup_hostcountry 
	, COUNT(DISTINCT wbe.ap_account_id) gold_holder
	, SUM( COALESCE (trade_wallet_amount, 0) + COALESCE (z_wallet_amount, 0) ) total_coin_amount
	, AVG( COALESCE (trade_wallet_amount, 0) + COALESCE (z_wallet_amount, 0) ) avg_coin_amount
	, SUM( (COALESCE (trade_wallet_amount, 0) + COALESCE (z_wallet_amount, 0)) * rm.price ) total_amount_usd
	, AVG( (COALESCE (trade_wallet_amount, 0) + COALESCE (z_wallet_amount, 0)) * rm.price ) avg_amount_usd
FROM analytics.wallets_balance_eod wbe 
	LEFT JOIN analytics.users_master um 
		ON wbe.ap_account_id = um.ap_account_id 
	LEFT JOIN analytics.rates_master rm 
		ON wbe.symbol = rm.product_1_symbol 
		AND wbe.created_at::DATE = rm.created_at::DATE 
WHERE 
	um.signup_hostcountry IN ('AU','global','TH','ID')
	AND wbe.created_at = NOW()::DATE - '1 day'::INTERVAL
	AND wbe.symbol = 'GOLD'
GROUP BY 1,2
;


