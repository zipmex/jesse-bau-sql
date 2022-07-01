 
-- legacy user identification
WITH base AS (
SELECT 
	date_trunc('day',a.created_at) datadate
	, a.account_id 
	, SUM(a.amount) quantity 
	, SUM(a.usd_amount) as usd_amount 
FROM (
	SELECT date_trunc('day',a.created_at) AS created_at ,a.account_id , a.product_id, p.symbol--, u.signup_hostcountry 
		,SUM(amount) amount 
		, SUM(CASE WHEN a.product_id = 6 THEN a.amount * 1
		ELSE a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) END) usd_amount
	FROM oms_data.public.accounts_positions_daily a
		LEFT JOIN oms_data.mysql_replica_apex.products p
			ON a.product_id = p.product_id
		LEFT JOIN oms_data.public.cryptocurrency_prices c 
		    ON CONCAT(p.symbol, 'USD') = c.instrument_symbol
		    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.last_updated) + '1 day'::INTERVAL 
		LEFT join oms_data.public.daily_closing_gold_prices g
			ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)
			AND a.product_id IN (15, 35)
		LEFT join oms_data.public.daily_ap_prices z
			ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
			AND z.instrument_symbol  = 'ZMTUSD'
			AND a.product_id IN (16, 50)
		LEFT JOIN public.exchange_rates e
			ON date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
			AND e.product_2_symbol  = p.symbol
			AND e."source" = 'coinmarketcap'
	WHERE 
		a.created_at >= DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL 
		AND a.created_at < DATE_TRUNC('day', NOW()) --<<<<<<<<CHANGE DATE HERE
		AND a.account_id NOT IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 496001) 
	GROUP BY 1,2,3,4
	) a
GROUP BY 1,2 
ORDER BY 1 DESC 
)--, final_ AS (
SELECT 
	u.id 
	, DATE_TRUNC('day', u.inserted_at) registered_date
	, COALESCE(um.first_name , od.first_name, p.first_name) first_name 
	, COALESCE(um.last_name , od.last_name, p.last_name) last_name  
	, COALESCE(um.dob::date, od.dob::date, p.dob::date) dob 
	, COALESCE(od.country, p.country) country  
	, od.document_type 
	, u.email
	, u.signup_hostname 
	, ad.frankie_entity_id 
	, oa.level_increase_status 
	, ak.risk_type 
	, ak.amlo 
	, ak.led 
	, ak.google_check 
	, um.has_traded 
	, DATE_TRUNC('day', um.last_traded_at) last_traded_at 
	, um.sum_trade_volume_usd 
	, um.count_withdraws 
	, um.sum_withdraw_amount_usd 
	, um.count_deposits 
	, um.sum_deposit_amount_usd 
	, COALESCE (b.quantity , 0) coin_balance 
	, COALESCE (b.usd_amount, 0) usd_balance 
	, CASE WHEN ad.frankie_entity_id IS NULL THEN 1 ELSE 0 END AS is_legacy 
FROM
	user_app_public.users u 
	LEFT JOIN analytics.users_master um -- FOR trade /deposit/ withdraw info
		ON u.id = um.user_id 
	LEFT JOIN user_app_public.onfido_applicants oa --  for level increase status
		ON oa.user_id = u.id
	LEFT JOIN -- for frankieOne entityn id 
		(	SELECT DISTINCT applicant_id , frankie_entity_id
			FROM user_app_public.applicant_data
		) ad 
		ON oa.id = ad.applicant_id 
	LEFT JOIN base b 
		ON um.ap_account_id = b.account_id -- FOR wallet balance AS OF yesterday
	LEFT JOIN -- FOR names, dob 
		(	SELECT 
				*
				, RANK() OVER(PARTITION BY applicant_id ORDER BY updated_at DESC) rank_ 
			FROM user_app_public.onfido_documents od 
			WHERE od.archived_at IS NULL 
		) od 
		ON um.onfido_applicant_id = od.applicant_id 
		AND od.rank_ = 1
	LEFT JOIN -- FOR names, dob, DOCUMENT type
		( 	SELECT 
			user_id
			, info ->> 'first_name' first_name 
			, info ->> 'last_name' last_name
			, info ->> 'dob' dob
			, info ->> 'country' country 
			FROM user_app_public.personal_infos p
			WHERE archived_at IS NULL 
			) p 
		ON u.id = p.user_id
	LEFT JOIN exchange_admin_public.additional_kyc_details ak -- FOR dopas info: AMLO, LED, GG CHECK, risk type
		ON u.id = ak.user_id 
WHERE
 	oa.level_increase_status = 'pass' 
 	AND um.signup_hostcountry NOT IN ('test', 'error','xbullion') 
 	AND um.ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227',27443
,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659','49658','52018','52019','44057','161347')
--	AND u.id IN ('01FC5T6SR63Z8R0A143F2ANH64')--,'01EC47YDREFKRK27BR7YY0JG7V')



), validation AS (
SELECT 
	id 
	, count(*) duplicate
FROM final_ 
GROUP BY 1
)
SELECT * 
FROM validation
WHERE duplicate > 1
