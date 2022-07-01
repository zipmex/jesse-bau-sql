ap_account_id = 53073; user_id = '01EPB97EP6PPTB070VPZ445111' -- pluang IN AlphaPoint
'01F14GTKR63YS7QSPGCQDNVJRR' -- liquidmex IN zipmex_otc_public

-- liquidmex balance
WITH hourly_accumulated_balances AS (
	SELECT *
	FROM (
		SELECT * , date_trunc('day', created_at) AS thour
		, ROW_NUMBER() OVER(PARTITION BY user_id, product_id , date_trunc('day', created_at) ORDER BY created_at DESC) AS r
		FROM zipmex_otc_prod_public.accumulated_balances
		) t
	WHERE t.r = 1
)	, pluang_aum_snapshot AS (
	SELECT
		thour, user_id
		, UPPER(h.product_id) symbol 
		, h.balance
		, h.created_at
		, h.id
		, rm.price 
		, CASE WHEN UPPER(h.product_id) = 'IDR' THEN h.balance * 1/rm.price 
				ELSE h.balance * rm.price END usd_amount
		, ROW_NUMBER() OVER(PARTITION BY user_id, UPPER(h.product_id), DATE_TRUNC('year', thour) ORDER BY thour DESC) rank_ 
	FROM 
		hourly_accumulated_balances h 
		LEFT JOIN 
			analytics.rates_master rm 
		    ON UPPER(h.product_id) = rm.product_1_symbol 
		    AND rm.created_at::DATE = NOW()::DATE - '1 day'::INTERVAL
	WHERE
		user_id = '01F14GTKR63YS7QSPGCQDNVJRR'
	--	AND extract(hour from thour) = 23
	ORDER BY thour DESC, user_id, product_id
)
SELECT 
	p.thour last_activity_on
	, p.symbol
	, p.balance
	, NOW()::DATE - '1 day'::INTERVAL reported_at
	, p.price token_price
	, p.usd_amount
	, rm2.price idr_usd_rate
	, CASE WHEN p.symbol = 'IDR' THEN p.balance
			ELSE p.usd_amount * rm2.price END idr_amount
FROM pluang_aum_snapshot p
	LEFT JOIN analytics.rates_master rm2 
		ON rm2.product_1_symbol = 'IDR'
		AND rm2.created_at::DATE = NOW()::DATE - '1 day'::INTERVAL
WHERE 
	rank_ = 1
;


-- pluang zipup/ziplock interest daily paidout
WITH ziplock_interest AS (
	SELECT 
		id.distributed_at::DATE "date"
		, UPPER(SPLIT_PART(id.product_id,'.',1)) product
		, id.amount 
		, rm.price "CryptoPrice"
		, 'ziplock' classification
		, id.amount * rm.price "USD_Amount"
	FROM 
	-- ziplock interest distribution
		zip_lock_service_public.interest_distributions id 
		LEFT JOIN
	-- daily coin prices
			analytics.rates_master rm 
			ON UPPER(SPLIT_PART(id.product_id,'.',1)) = rm.product_1_symbol 
			AND id.inserted_at::DATE = rm.created_at::DATE
	WHERE 
	-- pluang account 53073
		user_id = '01EPB97EP6PPTB070VPZ445111'
		-- interest paidout of last month
		AND distributed_at >= DATE_TRUNC('month', NOW()) - '1 month'::INTERVAL
		AND distributed_at < DATE_TRUNC('month', NOW())
	ORDER BY 1
)	, zipup_interest AS (
	SELECT 
		id.distributed_at::DATE "date"
		, UPPER(SPLIT_PART(id.product_id,'.',1)) product
		, id.amount 
		, rm.price "CryptoPrice"
		, 'zipup' classification
		, id.amount * rm.price "USD_Amount"
	FROM 
	-- zipup interest distribution
		zip_up_service_public.interest_distributions id 
		LEFT JOIN
	-- daily coin prices
			analytics.rates_master rm 
			ON UPPER(SPLIT_PART(id.product_id,'.',1)) = rm.product_1_symbol 
			AND id.inserted_at::DATE = rm.created_at::DATE
	WHERE 
	-- pluang account 53073
		user_id = '01EPB97EP6PPTB070VPZ445111'
		-- interest paidout of last month
		AND distributed_at >= DATE_TRUNC('month', NOW()) - '1 month'::INTERVAL
		AND distributed_at < DATE_TRUNC('month', NOW())
	ORDER BY 1
)
SELECT * FROM zipup_interest
UNION ALL
SELECT * FROM ziplock_interest
;



---- pluang zipup/ ziplock balance
WITH zipup AS (
	SELECT
		d.snapshot_utc::DATE created_at
		, ap_account_id
		, 'zipup' classification
		, UPPER(SPLIT_PART(s.product_id,'.',1)) symbol
		, sum(s.balance) amount
	FROM
		generate_series('2021-12-31'::DATE, '2022-01-01'::DATE, '1 day'::INTERVAL) d (snapshot_utc)
		LEFT JOIN LATERAL (
			SELECT 
				DISTINCT ON (user_id, product_id) user_id, product_id, balance, created_at + '7 hour'::INTERVAL
			FROM zip_up_service_public.balance_snapshots
			WHERE DATE_TRUNC('day', balance_snapshots.created_at + '7 hour'::INTERVAL) <= d.snapshot_utc
			ORDER BY user_id, product_id, created_at DESC 
				) s ON TRUE
		LEFT JOIN analytics.users_master u
			ON s.user_id = u.user_id 
	WHERE u.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
	AND u.ap_account_id = 53073
	GROUP BY 1,2,3,4
	ORDER BY 1,2
)	, ziplock AS (
SELECT
		d.snapshot_utc::DATE created_at
		, ap_account_id
		, 'ziplock' classification
		, UPPER(SPLIT_PART(s.product_id,'.',1)) symbol
		, sum(s.balance) amount
	FROM
		generate_series('2021-12-31'::DATE, '2022-01-01'::DATE, '1 day'::INTERVAL) d (snapshot_utc)
		LEFT JOIN LATERAL (
			SELECT 
				DISTINCT ON (user_id, product_id) user_id, product_id, balance, balance_datetime + '7 hour'::INTERVAL
			FROM zip_lock_service_public.vault_accumulated_balances
			WHERE DATE_TRUNC('day', balance_datetime + '7 hour'::INTERVAL) <= d.snapshot_utc
			ORDER BY user_id, product_id, balance_datetime DESC
				) s ON TRUE
		LEFT JOIN analytics.users_master u
			ON s.user_id = u.user_id 
	WHERE 
		u.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		AND u.ap_account_id = 53073
	GROUP BY 1,2,3,4
	ORDER BY 1,2
)	, z_wallet AS (
SELECT * FROM zipup 
UNION ALL
SELECT * FROM ziplock 
)
SELECT 
	z.*
	, amount * rm.price amount_usd
	, amount * rm.price * rm2.price amount_idr
FROM z_wallet z
	LEFT JOIN analytics.rates_master rm 
		ON z.symbol = rm.product_1_symbol 
		AND z.created_at::DATE = rm.created_at::DATE 
	LEFT JOIN analytics.rates_master rm2 
		ON rm2.product_1_symbol = 'IDR'
		AND z.created_at::DATE = rm2.created_at::DATE 
;