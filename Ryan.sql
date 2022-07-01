---- AML rules 
WITH deposit AS (
SELECT date_trunc('day', updated_at) yesterday
	, signup_hostcountry 
	, ap_account_id 
	, product_type 
	, count(DISTINCT ticket_id) deposit_c
	, SUM(amount) amount_d 
	, SUM(amount_usd) usd_d
FROM analytics.deposit_tickets_master d
WHERE status = 'FullyProcessed' AND signup_hostcountry IN ('AU','global','ID','TH')
GROUP BY 1,2,3,4
), rule1 AS ( --<<<<<<<<<<< RULE 1 - yesterday Top 15 users for each country by daily FIAT deposit value, minimum value >10,000
SELECT *
	, ROW_NUMBER () OVER(PARTITION BY signup_hostcountry ORDER BY usd_d DESC) rank_1
FROM deposit WHERE usd_d > 10000 AND product_type = 'NationalCurrency'
AND signup_hostcountry IN ('AU','global','ID')
AND date_trunc('day', yesterday) = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
ORDER BY 1,3
), rule2 AS ( --<<<<<<<<<<< RULE 2 - yesterday Top 15 users for each country by daily CRYPTO deposit value, minimum value >10,000
SELECT *
	, ROW_NUMBER () OVER(PARTITION BY signup_hostcountry ORDER BY usd_d DESC) rank_2
FROM deposit WHERE usd_d > 10000 AND product_type = 'CryptoCurrency'
AND signup_hostcountry IN ('AU','global')
AND date_trunc('day', yesterday) = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
ORDER BY 2
), withdrawal AS (
SELECT date_trunc('day', updated_at) yesterday
	, signup_hostcountry 
	, ap_account_id 
	, product_type 
	, COUNT(DISTINCT ticket_id) withdraw_c
	, SUM(amount) amount_wd 
	, SUM(amount_usd) usd_wd
FROM analytics.withdraw_tickets_master w 
WHERE status = 'FullyProcessed' AND signup_hostcountry IN ('AU','global','ID','TH')
GROUP BY 1,2,3,4
), rule3 AS ( --<<<<<<<<<<< RULE 3 - yesterday Top 15 users for each country by daily FIAT withdrawal value, minimum value >10,000
SELECT *
	, ROW_NUMBER () OVER(PARTITION BY signup_hostcountry ORDER BY usd_wd DESC) rank_3
FROM withdrawal WHERE usd_wd > 10000 AND product_type = 'NationalCurrency'
AND signup_hostcountry IN ('AU','global','ID')
AND date_trunc('day', yesterday) = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
ORDER BY 2
), rule4 AS ( --<<<<<<<<<<< RULE 4 - yesterday Top 15 users for each country by daily FIAT withdrawal value, minimum value >10,000
SELECT *
	, ROW_NUMBER () OVER(PARTITION BY signup_hostcountry ORDER BY usd_wd DESC) rank_4
FROM withdrawal WHERE usd_wd > 10000 AND product_type = 'CryptoCurrency'
AND signup_hostcountry IN ('AU','global')
AND date_trunc('day', yesterday) = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
ORDER BY 2
), deposit_address AS (
SELECT date_trunc('day',d.updated_at) yesterday  
	, u.signup_hostcountry 
	, d.ap_account_id 
	, d.product_id 
	, d.cryptobase_pair 
	, TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM t.deposit_info),',',3),':',2)) from_add 
	, SUM(d.amount) coin_amount
	, SUM(d.amount_base_fiat) fiat_amount
	, SUM(d.amount_usd) usd_amount
FROM analytics.deposit_tickets_master d 
	LEFT JOIN mysql_replica_apex.deposit_tickets t ON d.ticket_id = t.deposit_ticket_id 
	LEFT JOIN analytics.users_master u ON d.ap_account_id = u.ap_account_id 
WHERE d.status = 'FullyProcessed'
AND d.product_type = 'CryptoCurrency'
AND d.ap_account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347) 
GROUP BY 1,2,3,4,5,6
ORDER BY 1
), rule5 AS ( --<<<<<<<<<<< RULE 5 - yesterday user_deposit_value > 20,000 & unique_deposit_address >= 3 ------ Crypto
SELECT yesterday
	, signup_hostcountry
	, ap_account_id
	, COUNT(DISTINCT from_add) unique_deposit_address
	, SUM(coin_amount) coin_amount 
	, SUM(fiat_amount) fiat_amount 
	, SUM(usd_amount) usd_amount 
FROM deposit_address 
WHERE date_trunc('day',yesterday) = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL 
AND signup_hostcountry IN ('AU','global')
GROUP BY 1,2,3 
), rule6 AS ( --<<<<<<<<<<< RULE 6 - yesterday deposit count >= 2 and each deposit_value >= 9,850 & =< 9,999
SELECT updated_at 
	, d.signup_hostcountry 
	, d.ap_account_id 
	, d.amount_usd 
FROM analytics.deposit_tickets_master d 
	LEFT JOIN deposit d1 ON d.ap_account_id = d1.ap_account_id 
	AND d1.yesterday = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
WHERE date_trunc('day',updated_at) = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
AND d.signup_hostcountry IN ('AU','global')
AND d.status = 'FullyProcessed' AND d.product_type = 'NationalCurrency'
AND d1.deposit_c >= 2 
AND d.amount_usd >= 9850 AND d.amount_usd <= 9999
), rule7 AS ( --<<<<<<<<<<< RULE 7 - yesterday withdrawal count >= 2 and each withdrawal_value >= 9,850 & =< 9,999
SELECT updated_at 
	, w.signup_hostcountry 
	, w.ap_account_id 
	, w.amount_usd 
FROM analytics.withdraw_tickets_master w  
	LEFT JOIN withdrawal w1 ON w.ap_account_id = w1.ap_account_id 
	AND w1.yesterday = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
WHERE date_trunc('day',updated_at) = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
AND w.signup_hostcountry IN ('AU','global')
AND w.status = 'FullyProcessed' AND w.product_type = 'NationalCurrency' 
AND w1.withdraw_c >= 2 
AND w.amount_usd >= 9850 AND w.amount_usd <= 9999 
), user_age AS (
SELECT ap_account_id 
	, signup_hostcountry 
	, NOW() - created_at age_ 
	, sum_deposit_amount_usd 
	, count_trades
FROM analytics.users_master u 
WHERE signup_hostcountry IN ('AU','global','ID','TH') 
), rule8 AS ( --<<<<<<<<<<< RULE 8 - user_age =< 2 (days) and total_deposit_amount > 15,000
SELECT ap_account_id, signup_hostcountry
FROM user_age 
WHERE sum_deposit_amount_usd > 15000 
AND signup_hostcountry IN ('AU','global')
AND age_ <= '2' 
), rule9_base AS ( --<<<<<<<<<<< RULE 9 - FIAT_deposit_amount > 15,000 & CRYPTO_ withdrawal_ amount > 95% & trade_count <= 2
SELECT u.ap_account_id 
	, u.signup_hostcountry
	, u.count_trades 
	, u.sum_deposit_amount_usd 
	, d.usd_d 
	, COALESCE(w.usd_wd,0) usd_wd
	, COALESCE(w.usd_wd,0) / d.usd_d wd_percent
FROM user_age u 
	LEFT JOIN (SELECT ap_account_id, SUM(usd_d) usd_d FROM deposit WHERE product_type = 'NationalCurrency' GROUP BY 1) d 
	ON u.ap_account_id = d.ap_account_id 
	LEFT JOIN (SELECT ap_account_id, SUM(usd_wd) usd_wd FROM withdrawal WHERE product_type = 'CryptoCurrency' GROUP BY 1) w 
	ON u.ap_account_id = w. ap_account_id 
WHERE d.usd_d > 15000 
AND age_ <= '1'
), rule9 AS (
SELECT ap_account_id 
	, signup_hostcountry
	, count_trades 
	, wd_percent 
FROM rule9_base  
WHERE count_trades <= 2 AND wd_percent > 0.95
AND signup_hostcountry IN ('AU','global')
), final_rule  AS (
	SELECT CASE WHEN r1.rank_1 < 16 THEN 'rule_1'  
				WHEN r2.rank_2 < 16 THEN 'rule_2' 
				WHEN r3.rank_3 < 16 THEN 'rule_3'  
				WHEN r4.rank_4 < 16 THEN 'rule_4' 
				WHEN r5.unique_deposit_address >= 3 AND r5.usd_amount > 20000 THEN 'rule_5' 
				WHEN r6.ap_account_id IS NOT NULL THEN 'rule_6'
				WHEN r7.ap_account_id IS NOT NULL THEN 'rule_7'
				WHEN r8.ap_account_id IS NOT NULL THEN 'rule_8'
				WHEN r9.ap_account_id IS NOT NULL THEN 'rule_9'
				ELSE NULL
				END AS rule_number
	, COALESCE(r1.ap_account_id, r2.ap_account_id, r3.ap_account_id, r4.ap_account_id, r5.ap_account_id, r6.ap_account_id, r7.ap_account_id, r8.ap_account_id, r9.ap_account_id) ap_account_id 
	FROM rule1 r1
	FULL OUTER JOIN rule2 r2 ON r1.ap_account_id = r2.ap_account_id 
	FULL OUTER JOIN rule3 r3 ON r1.ap_account_id = r3.ap_account_id 
	FULL OUTER JOIN rule4 r4 ON r1.ap_account_id = r4.ap_account_id 
	FULL OUTER JOIN rule5 r5 ON r1.ap_account_id = r5.ap_account_id 
	FULL OUTER JOIN rule6 r6 ON r1.ap_account_id = r6.ap_account_id 
	FULL OUTER JOIN rule7 r7 ON r1.ap_account_id = r7.ap_account_id 
	FULL OUTER JOIN rule8 r8 ON r1.ap_account_id = r8.ap_account_id 
	FULL OUTER JOIN rule9 r9 ON r1.ap_account_id = r9.ap_account_id 
)
SELECT r.*
	, u.signup_hostcountry 
	, u.email 
	, u.sum_deposit_amount_usd total_deposit_usd 
	, u.sum_withdraw_amount_usd total_withdraw_usd
	, u.sum_trade_volume_usd total_trade_usd 
FROM final_rule r 
LEFT JOIN analytics.users_master u ON r.ap_account_id = u.ap_account_id 
WHERE r.rule_number IS NOT NULL 
ORDER BY 1,3,2 
;



-- AML rules v2
WITH deposit AS (
	SELECT 
	    DATE_TRUNC('day', updated_at) updated_at
		, signup_hostcountry 
		, ap_account_id 
		, product_type 
		, count(DISTINCT ticket_id) deposit_c
		, SUM(amount) amount_d 
		, SUM(amount_usd) usd_d
	FROM 
	    analytics.deposit_tickets_master d
	WHERE 
	    status = 'FullyProcessed' AND signup_hostcountry IN ('AU','global','ID','TH')
	    AND ap_account_id NOT IN (SELECT DISTINCT ap_account_id::NUMERIC FROM mappings.users_mapping) 
	GROUP BY 1,2,3,4
), rule1 AS ( 
--<<<<<<<<<<< RULE 1 - updated_at Top 15 users for each country by daily FIAT deposit value, minimum value >10,000
	SELECT 
	    *
		, ROW_NUMBER () OVER(PARTITION BY signup_hostcountry, updated_at ORDER BY usd_d DESC) rank_1
		, COUNT(ap_account_id) OVER(PARTITION BY ap_account_id) counter_
	FROM 
	    deposit 
	WHERE 
		usd_d > 10000 AND product_type = 'NationalCurrency'
	    AND signup_hostcountry IN ('AU','global','ID')
	    AND DATE_TRUNC('day', updated_at) >= '2022-01-01'
	ORDER BY 1,3
), rule2 AS ( 
--<<<<<<<<<<< RULE 2 - updated_at Top 15 users for each country by daily CRYPTO deposit value, minimum value >10,000
	SELECT 
	    *
		, ROW_NUMBER () OVER(PARTITION BY signup_hostcountry, updated_at ORDER BY usd_d DESC) rank_2
		, COUNT(ap_account_id) OVER(PARTITION BY ap_account_id) counter_
	FROM 
	    deposit
	WHERE usd_d > 10000 AND product_type = 'CryptoCurrency'
	    AND signup_hostcountry IN ('AU','global')
	    AND date_trunc('day', updated_at) >= '2022-01-01'
	ORDER BY 1,3
), withdrawal AS (
	SELECT 
	    DATE_TRUNC('day', updated_at) updated_at
		, signup_hostcountry 
		, ap_account_id 
		, product_type 
		, COUNT(DISTINCT ticket_id) withdraw_c
		, SUM(amount) amount_wd 
		, SUM(amount_usd) usd_wd
	FROM 
	    analytics.withdraw_tickets_master w 
	WHERE 
	    status = 'FullyProcessed' AND signup_hostcountry IN ('AU','global','ID','TH')
	    AND ap_account_id NOT IN (SELECT DISTINCT ap_account_id::NUMERIC FROM mappings.users_mapping) 
	GROUP BY 1,2,3,4
), rule3 AS ( 
--<<<<<<<<<<< RULE 3 - updated_at Top 15 users for each country by daily FIAT withdrawal value, minimum value >10,000
	SELECT 
	    *
		, ROW_NUMBER () OVER(PARTITION BY signup_hostcountry, updated_at ORDER BY usd_wd DESC) rank_3
		, COUNT(ap_account_id) OVER(PARTITION BY ap_account_id) counter_
	FROM 
	    withdrawal 
	WHERE usd_wd > 10000 AND product_type = 'NationalCurrency'
	    AND signup_hostcountry IN ('AU','global','ID')
	    AND DATE_TRUNC('day', updated_at) >= '2022-01-01'
	ORDER BY 1,3
), rule4 AS ( 
--<<<<<<<<<<< RULE 4 - updated_at Top 15 users for each country by daily FIAT withdrawal value, minimum value >10,000
	SELECT 
	    *
		, ROW_NUMBER () OVER(PARTITION BY signup_hostcountry, updated_at ORDER BY usd_wd DESC) rank_4
		, COUNT(ap_account_id) OVER(PARTITION BY ap_account_id) counter_
	FROM 
	    withdrawal 
	WHERE usd_wd > 10000 AND product_type = 'CryptoCurrency'
	    AND signup_hostcountry IN ('AU','global')
	    AND DATE_TRUNC('day', updated_at) >= '2022-01-01'
	ORDER BY 1,3
), deposit_address AS (
	SELECT 
	    date_trunc('day',d.updated_at) updated_at  
		, u.signup_hostcountry 
		, d.ap_account_id 
		, d.product_id 
		, d.cryptobase_pair 
		, TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM t.deposit_info),',',3),':',2)) from_add 
		, SUM(d.amount) coin_amount
		, SUM(d.amount_base_fiat) fiat_amount
		, SUM(d.amount_usd) usd_amount
	FROM 
	    analytics.deposit_tickets_master d 
		LEFT JOIN apex.deposit_tickets t 
		    ON d.ticket_id = t.deposit_ticket_id 
		LEFT JOIN analytics.users_master u 
		    ON d.ap_account_id = u.ap_account_id 
	WHERE 
	    d.status = 'FullyProcessed'
	    AND d.product_type = 'CryptoCurrency'
	    AND d.ap_account_id NOT IN (SELECT DISTINCT ap_account_id::NUMERIC FROM mappings.users_mapping) 
	GROUP BY 1,2,3,4,5,6
	ORDER BY 1
), rule5 AS ( 
--<<<<<<<<<<< RULE 5 - updated_at user_deposit_value > 20,000 & unique_deposit_address >= 3 ------ Crypto
	SELECT 
	    updated_at
		, signup_hostcountry
		, ap_account_id
		, COUNT(DISTINCT from_add) unique_deposit_address
		, SUM(coin_amount) coin_amount 
		, SUM(fiat_amount) fiat_amount 
		, SUM(usd_amount) usd_amount 
		, COUNT(ap_account_id) OVER(PARTITION BY ap_account_id) counter_
	FROM 
	    deposit_address 
	WHERE  
	    DATE_TRUNC('day',updated_at) >= '2022-01-01'
	    AND signup_hostcountry IN ('AU','global')
	GROUP BY 1,2,3
	ORDER BY 1
)	, base_r6 AS (
--<<<<<<<<<<< RULE 6 - updated_at deposit count >= 2 and each deposit_value >= 9,850 & =< 9,999
	SELECT 
		DATE_TRUNC('day', created_at) updated_at 
		, signup_hostcountry 
		, ap_account_id 
		, COUNT(DISTINCT ticket_id) ticket_count
	FROM 
		analytics.deposit_tickets_master d
	WHERE 
		amount_usd >= 9850 AND amount_usd <= 9999
	    AND ap_account_id NOT IN (SELECT DISTINCT ap_account_id::NUMERIC FROM mappings.users_mapping) 
	GROUP BY 1,2,3
)	, rule6 AS (
	SELECT 
		*
		, COUNT(ap_account_id) OVER(PARTITION BY ap_account_id) counter_
	FROM base_r6
	WHERE
	    DATE_TRUNC('day',updated_at) >= '2022-01-01'
)	, base_r7 AS (
--<<<<<<<<<<< RULE 7 - updated_at withdrawal count >= 2 and each withdrawal_value >= 9,850 & =< 9,999
	SELECT 
		DATE_TRUNC('day', created_at) updated_at 
		, signup_hostcountry 
		, ap_account_id 
		, COUNT(DISTINCT ticket_id) ticket_count
	FROM 
		analytics.withdraw_tickets_master w
	WHERE 
		amount_usd >= 9850 AND amount_usd <= 9999
	    AND ap_account_id NOT IN (SELECT DISTINCT ap_account_id::NUMERIC FROM mappings.users_mapping) 
	GROUP BY 1,2,3
)	, rule7 AS (
	SELECT 
		*
		, COUNT(ap_account_id) OVER(PARTITION BY ap_account_id) counter_
	FROM base_r7
	WHERE
	    DATE_TRUNC('day',updated_at) >= '2022-01-01'
), user_age AS (
	SELECT 
	    ap_account_id 
		, signup_hostcountry 
		, NOW() - created_at age_ 
		, sum_deposit_amount_usd 
		, count_trades
	FROM 
	    analytics.users_master u 
	WHERE 
	    signup_hostcountry IN ('AU','global','ID','TH') 
	    AND ap_account_id NOT IN (SELECT DISTINCT ap_account_id::NUMERIC FROM mappings.users_mapping) 
), rule8 AS ( 
--<<<<<<<<<<< RULE 8 - user_age =< 2 (days) and total_deposit_amount > 15,000
	SELECT 
		ap_account_id, signup_hostcountry
	FROM 
	    user_age 
	WHERE 
	    sum_deposit_amount_usd > 15000 
	    AND signup_hostcountry IN ('AU','global')
	    AND age_ <= '2' 
), rule9_base AS ( 
--<<<<<<<<<<< RULE 9 - FIAT_deposit_amount > 15,000 & CRYPTO_ withdrawal_ amount > 95% & trade_count <= 2
	SELECT 
	    u.ap_account_id 
		, u.signup_hostcountry
		, u.count_trades 
		, u.sum_deposit_amount_usd 
		, d.usd_d 
		, COALESCE(w.usd_wd,0) usd_wd
		, COALESCE(w.usd_wd,0) / d.usd_d wd_percent
	FROM 
	    user_age u 
		LEFT JOIN (SELECT ap_account_id, SUM(usd_d) usd_d FROM deposit WHERE product_type = 'NationalCurrency' GROUP BY 1) d 
		    ON u.ap_account_id = d.ap_account_id 
		LEFT JOIN (SELECT ap_account_id, SUM(usd_wd) usd_wd FROM withdrawal WHERE product_type = 'CryptoCurrency' GROUP BY 1) w 
		    ON u.ap_account_id = w. ap_account_id 
	WHERE 
	    d.usd_d > 15000 
	    AND age_ <= '1'
), rule9 AS (
	SELECT 
	    ap_account_id 
		, signup_hostcountry
		, count_trades 
		, wd_percent 
	FROM 
	    rule9_base  
	WHERE 
	    count_trades <= 2 AND wd_percent > 0.95
	    AND signup_hostcountry IN ('AU','global')
), final_rule  AS (
	SELECT updated_at, CASE WHEN rank_1 < 16 THEN 'rule_1' ELSE NULL END rule_number , ap_account_id , counter_  FROM rule1 
	UNION ALL
	SELECT updated_at, CASE WHEN rank_2 < 16 THEN 'rule_2' ELSE NULL END rule_number , ap_account_id , counter_  FROM rule2 
	UNION ALL
	SELECT updated_at, CASE WHEN rank_3 < 16 THEN 'rule_3' ELSE NULL END rule_number , ap_account_id , counter_  FROM rule3 
	UNION ALL
	SELECT updated_at, CASE WHEN rank_4 < 16 THEN 'rule_4' ELSE NULL END rule_number , ap_account_id , counter_  FROM rule4 
	UNION ALL
	SELECT updated_at, CASE WHEN unique_deposit_address >= 3 AND usd_amount > 20000 THEN 'rule_5' ELSE NULL END rule_number , ap_account_id , counter_  FROM rule5 
	UNION ALL
	SELECT updated_at, CASE WHEN ticket_count >= 2 THEN 'rule_6' ELSE NULL END rule_number , ap_account_id , counter_  FROM rule6 
	UNION ALL
	SELECT updated_at, CASE WHEN ticket_count >= 2 THEN 'rule_7' ELSE NULL END rule_number , ap_account_id , counter_  FROM rule7 
	UNION ALL
	SELECT DATE_TRUNC('day', NOW()) updated_at, CASE WHEN ap_account_id IS NOT NULL THEN 'rule_8' ELSE NULL END rule_number , ap_account_id , '1' counter_  FROM rule8 
	UNION ALL
	SELECT DATE_TRUNC('day', NOW()) updated_at, CASE WHEN ap_account_id IS NOT NULL THEN 'rule_9' ELSE NULL END rule_number , ap_account_id , '1' counter_  FROM rule9 
	ORDER BY 1
)
SELECT 
	r.updated_at::DATE , r.rule_number , r.ap_account_id , r.counter_ monthly_count 
	, u.signup_hostcountry 
	, up.email 
	, u.sum_deposit_amount_usd total_deposit_usd 
	, u.sum_withdraw_amount_usd total_withdraw_usd
	, u.sum_trade_volume_usd total_trade_usd 
FROM 
	final_rule r 
	LEFT JOIN analytics.users_master u 
		ON r.ap_account_id = u.ap_account_id 
	LEFT JOIN analytics_pii.users_pii up 
		ON u.user_id = up.user_id 
WHERE 
    r.rule_number IS NOT NULL 
	AND r.updated_at >= '2022-01-01' -- DATE_TRUNC('month', NOW()::DATE - '1 day'::INTERVAL)
	AND r.updated_at < '2022-03-01'
ORDER BY 1 DESC ,3,2 
;





----- rule 7
WITH base_r7 AS (
--<<<<<<<<<<< RULE 7 - updated_at withdrawal count >= 2 and each withdrawal_value >= 9,850 & =< 9,999
	SELECT 
		DATE_TRUNC('day', created_at) updated_at 
		, signup_hostcountry 
		, ap_account_id 
		, COUNT(DISTINCT ticket_id) ticket_count
	FROM 
		analytics.withdraw_tickets_master w
	WHERE 
		amount_usd >= 9850 AND amount_usd <= 9999
	    AND ap_account_id NOT IN (SELECT DISTINCT ap_account_id::NUMERIC FROM mappings.users_mapping) 
	GROUP BY 1,2,3
)--	, rule7 AS (
	SELECT 
		*
		, COUNT(ap_account_id) OVER(PARTITION BY ap_account_id) counter_
	FROM base_r7
	WHERE
	    DATE_TRUNC('day',updated_at) >= '2022-01-01'
	    AND DATE_TRUNC('day',updated_at) < '2022-03-01'
		AND ticket_count >= 2
-- DATE_TRUNC('month', DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL)
;



------- rule 5
WITH deposit_address AS (
SELECT date_trunc('day',d.updated_at) yesterday  
	, u.signup_hostcountry 
	, d.ap_account_id 
	, d.product_id 
	, d.cryptobase_pair 
	, TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM t.deposit_info),',',3),':',2)) from_add 
--	, TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM t.deposit_info),',',4),':',2)) to_add 
	, SUM(d.amount) coin_amount
	, SUM(d.amount_base_fiat) fiat_amount
	, SUM(d.amount_usd) usd_amount 
FROM analytics.deposit_tickets_master d 
	LEFT JOIN apex.deposit_tickets t ON d.ticket_id = t.deposit_ticket_id 
	LEFT JOIN analytics.users_master u ON d.ap_account_id = u.ap_account_id 
WHERE d.status = 'FullyProcessed'
AND d.product_type = 'CryptoCurrency'
AND d.ap_account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347) 
GROUP BY 1,2,3,4,5,6
ORDER BY 1
)
SELECT yesterday
	, signup_hostcountry
	, ap_account_id
	, COUNT(DISTINCT from_add) unique_deposit_address
	, SUM(coin_amount) coin_amount 
	, SUM(fiat_amount) fiat_amount 
	, SUM(usd_amount) usd_amount 
FROM deposit_address 
WHERE date_trunc('day',yesterday) = '2021-06-01 00:00:00' 
AND signup_hostcountry IN ('AU','global')
GROUP BY 1,2,3 
;


SELECT 
	tick_to_timestamp(w.created_on_ticks) withdraw_time
	, withdraw_ticket_id
	, withdraw_transaction_details
	, template_form
	, TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.withdraw_transaction_details),',',1),':',2)) tx_id  
	, CASE WHEN TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',1),':',1)) = 'ExternalAddress'
			THEN CONCAT(TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',1),':',3))
			,',', TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',1),':',2)))
			END AS ext_address 
	, request_user_id , request_username
FROM apex.withdraw_tickets w 
WHERE withdraw_ticket_id IN (511463,511547,511559,511347,511685)
;




---- address screening - crypto
WITH base AS (
-- get raw withdraw/ deposit data from source apex
	SELECT 
		 DATE_TRUNC('day', tick_to_timestamp(w.created_on_ticks)) withdraw_date 
	-- split record for txID, send/ received address
--		, TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.withdraw_transaction_details),',',1),':',2)) tx_id  
		, w.withdraw_transaction_details::json ->> 'TxId' tx_id 
		, w.template_form::json ->> 'ExternalAddress' ext_address
--		, CASE WHEN TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',1),':',1)) = 'ExternalAddress'
--				THEN CONCAT(TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',1),':',3))
--				,',', TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',1),':',2)))
--				WHEN TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',2),':',1)) = 'ExternalAddress'
--				THEN CONCAT(TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',2),':',3))
--				,',', TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',2),':',2)) )
--				WHEN TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',3),':',1)) = 'ExternalAddress'
--				THEN CONCAT(TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',3),':',3)) 
--				,',', TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',3),':',2)) )			
--				WHEN TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',4),':',1)) = 'ExternalAddress'
--				THEN CONCAT(TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',4),':',3)) 
--				,',', TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',4),':',2)) )
--				WHEN TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',5),':',1)) = 'ExternalAddress'
--				THEN CONCAT(TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',5),':',3)) 
--				,',', TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',5),':',2)) )
--				END AS ext_address 
		, request_user_id , request_username , withdraw_ticket_id
		, u.user_id customer_id , up.email , u.signup_hostcountry 
		, w.asset_id 
		, p.symbol 
	FROM 
		apex.withdraw_tickets w 
	-- get ticket status to filter completed transactions
		LEFT JOIN oms_data_public.withdraw_tickets_status wts 
			ON w.status = wts."number" 
	-- get product symbol
		LEFT JOIN apex.products p 
			ON w.asset_id = p.product_id 
	-- get user id to map pii
		LEFT JOIN analytics.users_master u
			ON w.account_id = u.ap_account_id 
	-- get email pii data
		LEFT JOIN analytics_pii.users_pii up 
			ON u.user_id = up.user_id 
	WHERE 
	-- only completed transactions
		wts."name" = 'FullyProcessed'
	-- crypto = type 2
		AND p."type" = 2 
	-- country filter
		AND u.signup_hostcountry IN ('AU','ID','global','TH')
	-- exclude bot/ nominee/ MM
		AND w.account_id NOT IN (SELECT DISTINCT ap_account_id::NUMERIC FROM mappings.users_mapping)		
)	, withdraw_ AS (
	SELECT 
		tx_id "TransactionID"
--		, ext_address
		, CASE 	WHEN asset_id IN (13,29) THEN SPLIT_PART(ext_address,':',2) 
				ELSE ext_address
				END AS "OutputAddress"
		, CASE WHEN symbol IS NOT NULL THEN 'withdrawal' END AS "Direction"
		, DATE_TRUNC('month', withdraw_date)::DATE month_
		, signup_hostcountry
		, email "CustomerID"
		, CASE WHEN symbol IS NOT NULL THEN NULL END AS "LogIndex"
		, symbol "Asset" 
	FROM 
		base 
	WHERE 
	-- time frame as of yesterday
		withdraw_date >= '2022-03-01'
	--	AND withdraw_date < '2022-03-01'
	-- excluding unsupported products
		AND symbol NOT IN ('ZMT','AXS')
) ---- deposit address for each asset from user
, base_d AS (
	SELECT 
		DATE_TRUNC('day', tick_to_timestamp(d.created_on_ticks)) deposit_date 
		, d.deposit_info::json ->> 'TXId' tx_id
		, d.deposit_info::json ->> 'FromAddress' from_add 
		, d.deposit_info::json ->> 'ToAddress' to_add
--		, TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM d.deposit_info),':',4),',',1)) tx_id  
--		, TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM d.deposit_info),':',5),',',1)) from_add 
--		, SPLIT_PART(TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM d.deposit_info),':',6),',',1)),'?',1) to_add 
		, request_user_id  , d.account_id , d.deposit_ticket_id 
		, u.user_id customer_id , up.email , u.signup_hostcountry --, d.ap_account_id
		, d.asset_id 
		, p.symbol asset 
	FROM 
		apex.deposit_tickets d 
		LEFT JOIN oms_data_public.deposit_tickets_status dts 
			ON d.status = dts."number" 
		LEFT JOIN apex.products p 
			ON d.asset_id = p.product_id 
		LEFT JOIN analytics.users_master u	
			ON d.account_id = u.ap_account_id  
		LEFT JOIN analytics_pii.users_pii up 
			ON u.user_id = up.user_id 
	WHERE 
		dts."name" = 'FullyProcessed' 
		AND p."type" = 2 ---- crypto ONLY 
		AND u.signup_hostcountry IN ('AU','ID','global','TH')
		AND d.account_id NOT IN (SELECT DISTINCT ap_account_id::NUMERIC FROM mappings.users_mapping)
--		AND DATE_TRUNC('day', tick_to_timestamp(d.created_on_ticks)) >= '2021-01-01'
	ORDER BY 1 DESC 
), deposit_ AS (
	SELECT 
		tx_id "TransactionID"
		, CASE WHEN asset_id IN (1,25,2,26,13,29,57,58,65,66) THEN to_add ELSE NULL END AS "OutputAddress" 
	--	, to_add AS "OutputAddress" 
	--	, deposit_info
		, CASE WHEN asset IS NOT NULL THEN 'deposit' END AS "Direction"
	--	, customer_id 
	--	, deposit_ticket_id 
		, DATE_TRUNC('month', deposit_date)::DATE month_
		, signup_hostcountry
		, email "CustomerID"
		, CASE WHEN asset IS NOT NULL THEN NULL END AS "LogIndex"
		, asset "Asset" 
	FROM 
		base_d
	WHERE 
		deposit_date >= '2022-03-01'
	--	AND deposit_date < '2022-03-01'
)
SELECT * FROM withdraw_
UNION ALL 
SELECT * FROM deposit_
;



---- address screening - crypto and fiat
WITH base AS (
	SELECT 
		 t.created_at 
		, TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.withdraw_transaction_details),',',1),':',2)) tx_id  
		, CASE WHEN TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',1),':',1)) = 'ExternalAddress'
				THEN CONCAT(TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',1),':',3))
				,',', TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',1),':',2)))
				WHEN TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',2),':',1)) = 'ExternalAddress'
				THEN CONCAT(TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',2),':',3))
				,',', TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',2),':',2)) )
				WHEN TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',3),':',1)) = 'ExternalAddress'
				THEN CONCAT(TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',3),':',3)) 
				,',', TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',3),':',2)) )			
				WHEN TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',4),':',1)) = 'ExternalAddress'
				THEN CONCAT(TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',4),':',3)) 
				,',', TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',4),':',2)) )
				WHEN TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',5),':',1)) = 'ExternalAddress'
				THEN CONCAT(TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',5),':',3)) 
				,',', TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.template_form),',',5),':',2)) )
				END AS ext_address 
		, request_user_id , request_username 
		, u.user_id customer_id , u.email 
		, w.asset_id 
		, p.symbol 
		, CASE WHEN p."type" = 1 THEN 'fiat' WHEN p."type" = 2 THEN 'crypto' END AS category 
		, template_form_type 
		, w.amount
		, w.notional_value 
	FROM 
		apex.withdraw_tickets w 
		LEFT JOIN analytics.withdraw_tickets_master t 
			ON w.withdraw_ticket_id = t.ticket_id 
		LEFT JOIN mysql_replica_apex.products p 
			ON w.asset_id = p.product_id 
		LEFT JOIN analytics.users_master u
			ON w.request_user_id = u.ap_user_id 
	WHERE 
		w.status = 5 ---- successful TRANSACTION 
		AND t.status = 'FullyProcessed'
		--AND p."type" = 2 ---- crypto ONLY 
		AND u.signup_hostcountry IN ('AU','ID','global')
		AND w.account_id NOT IN (SELECT DISTINCT ap_account_id::NUMERIC FROM mappings.users_mapping)
) 
	, withdraw_ AS (
	SELECT
		b.created_at "Timestamp"
		, customer_id 
		, COALESCE (tx_id, '') "Tx_Hash"
		, symbol "Asset" 
		, category 
		, CASE WHEN symbol IS NOT NULL THEN 'SEND' END AS "Direction"
		, amount --, notional_value
		, CASE WHEN notional_value = 0 THEN 
			( CASE WHEN symbol IN ('USD','USDT','USDC') THEN amount * 1 
					WHEN symbol NOT IN ('USD','USDT','USDC') THEN amount * COALESCE(c."close",0)  
					ELSE 0 END) 
			ELSE notional_value END AS usd_amount 
		, CASE WHEN asset_id NOT IN (13,29) THEN TRIM(BOTH ',' FROM ext_address) 
				WHEN asset_id IN (13,29) THEN TRIM(BOTH 'bitcoincash,'FROM TRIM(BOTH ',' FROM ext_address))
				END AS "Output_Address"
		, '' "Counterparty_Root_Address"
	FROM 
		base b 
		LEFT JOIN 
			oms_data_public.cryptocurrency_prices_hourly c 	
			ON b.symbol = c.product_1_symbol 
			AND c.product_2_symbol = 'USD'
			AND date_trunc('hour', b.created_at) = date_trunc('hour', c.last_updated)
			AND c."source" = 'coinmarketcap'
	WHERE 
		b.created_at >= '2021-03-01 00:00:00' 
) 
---- deposit address for each asset from user
	, base_d AS (
	SELECT 
		 t.created_at  
		, TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM d.deposit_info),':',3),',',1)) tx_id  
		, TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM d.deposit_info),':',4),',',1)) from_add 
		, SPLIT_PART(TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM d.deposit_info),':',5),',',1)),'?',1) to_add 
		, request_user_id  , d.account_id , d.deposit_ticket_id 
		, u.user_id customer_id , u.email --, d.ap_account_id
		, d.asset_id 
		, p.symbol  
		, CASE WHEN p."type" = 1 THEN 'fiat' WHEN p."type" = 2 THEN 'crypto' END AS category 
		, d.amount
		, d.notional_value
	FROM 
		apex.deposit_tickets d 
		LEFT JOIN analytics.deposit_tickets_master t 
			ON d.deposit_ticket_id = t.ticket_id 
		LEFT JOIN apex.products p 
			ON d.asset_id = p.product_id 
		LEFT JOIN analytics.users_master u	
			ON d.request_user_id = u.ap_user_id 
	WHERE 
		d.status = 5 ---- successful TRANSACTION 
		AND t.status = 'FullyProcessed'
		--AND p."type" = 2 ---- crypto ONLY 
		AND u.signup_hostcountry IN ('AU','ID','global')
		AND d.account_id NOT IN (SELECT DISTINCT ap_account_id::NUMERIC FROM mappings.users_mapping)
)
	, deposit_ AS (
	SELECT 
		b.created_at "Timestamp"
		, b.customer_id "Customer_ID"
		, COALESCE (tx_id, '') "Tx_Hash"
		, b.symbol "Asset" 
		, b.category
		, CASE WHEN b.symbol IS NOT NULL THEN 'RECEIVED' END AS "Direction"
		, b.amount
		, CASE WHEN notional_value = 0 THEN 
			( CASE WHEN b.symbol IN ('USD','USDT','USDC') THEN amount * 1 
					WHEN b.symbol NOT IN ('USD','USDT','USDC') THEN amount * COALESCE(c."close",0) 
					ELSE 0 END) 
			ELSE notional_value END AS usd_amount 
		, from_add AS "Output_Address" 
		, to_add AS "Counterparty_Root_Address" 
	FROM 
		base_d b 
		LEFT JOIN oms_data_public.cryptocurrency_prices_hourly c 	
			ON b.symbol = c.product_1_symbol 
			AND c.product_2_symbol = 'USD'
			AND date_trunc('hour', b.created_at) = date_trunc('hour', c.last_updated)
			AND c."source" = 'coinmarketcap'
	WHERE 
		b.created_at >= '2021-03-01 00:00:00'
)
SELECT * FROM withdraw_
UNION ALL 
SELECT * FROM deposit_
;


-- sec deposit >= 60 or >= 120 transactions a month
WITH base_d AS (
	SELECT 
		DATE_TRUNC('day', tick_to_timestamp(d.created_on_ticks)) deposit_date 
		, d.deposit_ticket_id 
		, d.deposit_info::json ->> 'TXId' tx_id
		, d.deposit_info::json ->> 'FromAddress' from_add 
		, d.deposit_info::json ->> 'ToAddress' to_add
		, d.account_id 
		, u.signup_hostcountry
		, d.asset_id 
		, p.symbol asset 
		, d.amount::NUMERIC 
	FROM 
		apex.deposit_tickets d 
		LEFT JOIN oms_data_public.deposit_tickets_status dts 
			ON d.status = dts."number" 
		LEFT JOIN apex.products p 
			ON d.asset_id = p.product_id 
		LEFT JOIN analytics.users_master u	
			ON d.request_user_id = u.ap_user_id 
		LEFT JOIN analytics_pii.users_pii up 
			ON u.user_id = up.user_id 
	WHERE 
		dts."name" = 'FullyProcessed' 
		AND p."type" = 2 ---- crypto ONLY 
		AND u.signup_hostcountry IN ('TH')
		AND d.account_id NOT IN (SELECT DISTINCT ap_account_id::NUMERIC FROM mappings.users_mapping)
		AND DATE_TRUNC('day', tick_to_timestamp(d.created_on_ticks)) >= DATE_TRUNC('month', NOW()::DATE - '1 day'::INTERVAL)
	ORDER BY 1 DESC 
)	, ticket_count AS (
	SELECT 
		DATE_TRUNC('month', deposit_date)::DATE report_month
		, account_id 
		, signup_hostcountry
		, asset 
		, from_add 
		, to_add 
		, SUM(amount) deposit_amount
		, COUNT(DISTINCT deposit_ticket_id) deposit_ticket_count
	FROM base_d 
	GROUP BY 1,2,3,4,5,6
)
SELECT 
	*
	, SUM(deposit_ticket_count) OVER(PARTITION BY account_id) total_transaction
	, CASE WHEN SUM(deposit_ticket_count) OVER(PARTITION BY account_id) >= 60 THEN TRUE ELSE FALSE END AS above_60
	, CASE WHEN SUM(deposit_ticket_count) OVER(PARTITION BY account_id) >= 120 THEN TRUE ELSE FALSE END AS above_120
FROM ticket_count
;


