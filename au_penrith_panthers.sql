WITH base AS (
	SELECT 
		*
		, lower(REPLACE(regexp_replace(split_part(ca."Email",'@',1), '\d+', '','g'),'.','')) email_cleaned
		, COUNT(lower(REPLACE(regexp_replace(split_part(ca."Email",'@',1), '\d+', '','g'),'.',''))) 
		OVER(PARTITION BY lower(REPLACE(regexp_replace(split_part(ca."Email",'@',1), '\d+', '','g'),'.',''))) duplicate_check
		, SPLIT_PART(ca."Email",'@',2) domain_check
	FROM mappings.commercial_au_lashcreative ca
	ORDER BY 11 DESC
)
SELECT 
	*
FROM base 
--GROUP BY 1,2
--ORDER BY 3 DESC 
;




WITH email_check AS (
    SELECT 
        *
        , lower(REPLACE(regexp_replace(split_part(ca."Email",'@',1), '\d+', '','g'),'.','')) email_cleaned
        , COUNT(lower(REPLACE(regexp_replace(split_part(ca."Email",'@',1), '\d+', '','g'),'.',''))) 
        OVER(PARTITION BY lower(REPLACE(regexp_replace(split_part(ca."Email",'@',1), '\d+', '','g'),'.',''))) duplicate_check
		, SPLIT_PART(ca."Email",'@',2) domain_check
	FROM mappings.commercial_au_lashcreative ca
	ORDER BY 4
)   , base AS (
    SELECT
        um.user_id 
        , um.ap_account_id 
        , um.signup_hostcountry
        , up.mobile_number 
        , lower(cal."Email") email 
        , email_cleaned
        , duplicate_check
        , domain_check
        , CASE WHEN cal."Email" LIKE '%+%' THEN 'excluded'
                WHEN cal."Email" LIKE '%@sofrge.com%' THEN 'excluded'
                when cal."Email" in ('ahew1991@gmail.com','tivona@ryteto.me','xenak38058@sofrge.com','ferrisamanda608@gmail.com','fahout@gmail.com','srrbthomas@gmail.com','yanoflies@gmail.com','bazumogo@musiccode.me','376r@yottanom.com','nat_495@mightycus.com')
                then 'excluded'
                WHEN duplicate_check >= 5 THEN 'excluded' END AS gaming_check
        , cal."Prize" 
        , (um.created_at + '11 hour'::INTERVAL)::DATE register_date_gmt11
        , (um.verification_approved_at + '11 hour'::INTERVAL)::DATE verify_date_gmt11
        , um.level_increase_status 
        , CASE WHEN SPLIT_PART(cal."Prize",' ',2) = 'Bitcoin' THEN 'BTC'
                WHEN SPLIT_PART(cal."Prize",' ',2) = 'Ethereum' THEN 'ETH' 
                ELSE 'NFT' END AS symbol
    FROM email_check cal 
        LEFT JOIN analytics_pii.users_pii up 
            ON lower(cal."Email") = lower(up.email) 
        LEFT JOIN analytics.users_master um 
            ON up.user_id = um.user_id 
    WHERE um.signup_hostcountry = 'AU'
    ORDER BY 3
)   , airdrop_amount AS (
    SELECT 
        b.ap_account_id
        , b.signup_hostcountry
        , b.email
        , b.mobile_number
        , CASE WHEN register_date_gmt11 >= '2022-03-09' THEN TRUE ELSE FALSE END AS is_new_user
        , b.register_date_gmt11
        , b.verify_date_gmt11
        , b.level_increase_status
        , b."Prize"
        , b.symbol
        , p.product_id 
        , CASE WHEN gaming_check = 'excluded' THEN NULL ELSE 
                --(CASE WHEN verify_date_gmt11 IS NOT NULL AND b.symbol <> 'NFT' THEN
                (CASE WHEN register_date_gmt11 >= '2022-03-08' AND verify_date_gmt11 IS NOT NULL AND b.symbol <> 'NFT' THEN
                        ROUND( REPLACE (SPLIT_PART("Prize",' ',1),'$','')::NUMERIC / pcp.average_high_low::NUMERIC, 8)
                ELSE NULL END)
                END AS airdrop_amount_unit
        , NULL notes
        , 719754 from_account_id
        , b.email to_email
        , b.gaming_check
        , b.duplicate_check
        , b.domain_check
        , CASE WHEN b.symbol <> 'NFT' THEN REPLACE (SPLIT_PART("Prize",' ',1),'$','')::INT 
                ELSE NULL END AS airdrop_amount_aud
    FROM base b
        LEFT JOIN mappings.public_cryptocurrency_prices pcp 
            ON b.symbol = pcp.product_1_symbol 
            AND NOW()::DATE - '1 day'::INTERVAL = pcp.last_updated::DATE
            AND pcp.product_2_symbol = 'AUD'
        LEFT JOIN (SELECT *, ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY p.product_id ) row_ FROM apex.products p) p 
            ON b.symbol = p.symbol 
            AND p.row_ = 1
)   , airdrop_notes AS (
    SELECT 
        tm2.receiver_ap_account_id 
        , tm2.notes 
        , tm2.product_symbol 
        , tm2.amount received_amount
    FROM analytics.transfers_master tm2 
    WHERE 
        tm2.receiver_signup_hostcountry IN ('AU')
        AND (tm2.notes LIKE '%AU_%GROWTH_ACQUI_SIGNUP%' OR tm2.notes LIKE '%AU_GROWTH_ACQ_PANTHERSPARTNERSHIP%')
        AND tm2.product_symbol IN ('BTC','ETH')
)
SELECT 
    aa.*
    , CASE WHEN aa.symbol = an.product_symbol THEN an.notes ELSE NULL END AS is_airdropped
    , CASE WHEN aa.symbol = an.product_symbol THEN received_amount ELSE NULL END AS received_amount
FROM airdrop_amount aa
    LEFT JOIN airdrop_notes an 
        ON aa.ap_account_id = an.receiver_ap_account_id 
ORDER BY verify_date_gmt11 
;