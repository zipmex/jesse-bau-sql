SELECT 
	wbe.created_at::date
--	, wbe.ap_account_id 
	, SUM(trade_wallet_amount) trade_wallet_amount
	, SUM(z_wallet_amount) z_wallet_amount
	, SUM(ziplock_amount) ziplock_amount
FROM analytics.wallets_balance_eod wbe
WHERE created_at >= '2022-05-01'
--AND wbe.ap_account_id = 3
GROUP BY 1
UNION ALL 
SELECT 
	created_at::date
--	, ap_account_id 
	, SUM(trade_wallet_amount) trade_wallet_amount
	, SUM(z_wallet_amount) z_wallet_amount
	, SUM(ziplock_amount) ziplock_amount
FROM bo_testing.wallets_product_id wpi 
WHERE created_at >= '2022-05-01'
--AND ap_account_id = 3
GROUP BY 1
;

WITH base AS (
SELECT 
	wbe.created_at::date
--	, wbe.ap_account_id 
--	, wbe.symbol 
	, SUM(COALESCE (wbe.trade_wallet_amount,0)) trade_wallet_1
	, SUM(COALESCE (wpi.trade_wallet_amount,0)) trade_wallet_2
	, SUM(COALESCE (wbe.z_wallet_amount,0)) z_wallet_1
	, SUM(COALESCE (wpi.z_wallet_amount,0)) z_wallet_2
	, SUM(COALESCE (wbe.ziplock_amount,0)) ziplock_1
	, SUM(COALESCE (wpi.ziplock_amount,0)) ziplock_2
FROM analytics.wallets_balance_eod wbe
	LEFT JOIN bo_testing.wallets_product_id wpi 
		ON wbe.created_at::DATE = wpi.created_at 
		AND wbe.ap_account_id = wpi.ap_account_id 
		AND wbe.symbol = wpi.symbol 
WHERE wbe.created_at IN ('2022-05-12','2022-05-13') --,'2022-05-10','2022-05-09')
AND wbe.ap_account_id = 3
GROUP BY 1
)	, discrepancy AS (
SELECT 
	*
	, CASE WHEN trade_wallet_1 = trade_wallet_2 THEN FALSE ELSE TRUE END AS discrepancy_t
	, CASE WHEN z_wallet_1 = z_wallet_2 THEN FALSE ELSE TRUE END AS discrepancy_z
	, CASE WHEN ziplock_1 = ziplock_2 THEN FALSE ELSE TRUE END AS discrepancy_l
FROM base 
)
SELECT 
*
FROM discrepancy
--WHERE (discrepancy_t IS TRUE OR discrepancy_z IS TRUE OR discrepancy_l IS TRUE)
;


SELECT 
	DISTINCT 
	created_at 
--	, account_id 
--	, product_id 
--	, SUM(apd.amount)
FROM oms_data_public.accounts_positions_daily apd 
WHERE 
--account_id = 143639
 apd.created_at::DATE IN ('2022-05-01','2022-05-02','2022-04-30','2022-04-29')
--GROUP BY 1,2,3
;



SELECT 
	a.created_at 
--	, u.signup_hostcountry 
	, COUNT(DISTINCT a.account_id) user_count
	, SUM(amount)
FROM public.accounts_positions_daily a
	LEFT JOIN apex.products p 
	ON a.product_id = p.product_id 
	LEFT JOIN analytics.users_master u
	ON a.account_id = u.ap_account_id 
WHERE DATE_TRUNC('day', a.created_at) >= '2021-10-01 00:00:00'
AND DATE_TRUNC('day', a.created_at) < '2021-11-01 00:00:00'
AND u.signup_hostcountry IN ('TH','ID','AU','global')
GROUP BY 1
;

SELECT 
	w.created_at 
--	, u.signup_hostcountry 
	, COUNT(DISTINCT w.ap_account_id) user_count
	, SUM(trade_wallet_amount)
FROM analytics.wallets_balance_eod w
	LEFT JOIN analytics.users_master u
	ON w.ap_account_id = u.ap_account_id 
WHERE w.created_at >= '2021-10-01 00:00:00'
AND w.created_at < '2021-11-01 00:00:00'
AND u.signup_hostcountry IN ('TH','ID','AU','global')
GROUP BY 1
;



-- duplicate id asset_manager.ledgers
with dup_ids as (
	select count(*), min(id) as id
	from asset_manager_public.ledgers
	where service_id = 'main_wallet'
	group by ref_caller, ref_action, ref_id, created_at
	having count(*) > 1
)	, base AS (
	SELECT
		DATE_TRUNC('day', l.created_at) created_at 
		, u.ap_account_id 
		, u.signup_hostcountry 
		, UPPER(SPLIT_PART(product_id,'.',1)) symbol
		, l.service_id 
		, l.ref_caller 
		, l.ref_action 
		, SUM(credit) - SUM(debit) balance
	from asset_manager_public.ledgers l 
		LEFT JOIN analytics.users_master u
			ON l.account_id = u.user_id 
	WHERE 
		l.id in (select id from dup_ids)
	GROUP BY 1,2,3,4,5,6,7
	ORDER BY 1
)	, total_impact AS (
	SELECT 
		t.*
		, t.balance* r.price balance_usd
	FROM base t 
		LEFT JOIN analytics.rates_master r
			ON t.created_at = r.created_at 
			AND t.symbol = r.product_1_symbol
	ORDER BY 2,1
)
SELECT 
	created_at 
	, symbol
	, SUM(balance_usd) balance_usd
	, SUM(balance_usd) / 2.0 dup_balance_usd
FROM total_impact
GROUP BY 1,2
;


SELECT max(created_at)
FROM zip_up_service_public.balance_snapshots bs 