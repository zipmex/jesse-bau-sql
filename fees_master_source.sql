-- DROP TABLE IF EXISTS oms_data.analytics.fees_master;

CREATE TABLE IF NOT EXISTS oms_data.analytics.fees_master
(
	transaction_id					INTEGER
	,created_at						TIMESTAMPTZ
	,ap_user_id						INTEGER
	,ap_account_id					INTEGER
	,signup_hostcountry 			VARCHAR(255)
	,document_country 				VARCHAR(255)
	,base_fiat			 			VARCHAR(255)
	,fee_type 						VARCHAR(255)
	,fee_reference_id				INTEGER
	,fee_product_id					INTEGER
	,fee_product 					VARCHAR(255)
	,fee_product_type 				VARCHAR(255)
	,fee_amount 					NUMERIC
	,fee_conversion_type 			INTEGER
	,crypto_usd_price				NUMERIC
	,base_fiat_usd_rate				NUMERIC
	,fiat_product_usd_rate			NUMERIC
	,fee_base_fiat_amount			NUMERIC
	,fee_usd_amount					NUMERIC
	,th_vat							NUMERIC
);

-- CREATE INDEX transactions_keys ON oms_data.public.transactions
-- (transaction_id, reference_id, account_id, counterparty, product_id);

-- CREATE INDEX trade_keys ON oms_data.public.trades
-- (execution_id, trade_id, order_id, account_id, instrument_id,counter_party);

-- CREATE INDEX fees_master_keys ON oms_data.analytics.fees_master
-- (transaction_id, ap_user_id, ap_account_id, fee_reference_id, fee_product_id);

-- DROP TABLE IF EXISTS tmp_fees_master;

TRUNCATE TABLE oms_data.analytics.fees_master;

CREATE TEMP TABLE tmp_fees_master AS
(
	WITH "fee_list" AS
		(
			SELECT
				DISTINCT
				tx.transaction_id
				,tx.converted_time_stamp "created_at"
				-- user details
				,u.ap_user_id
				,tx.counterparty "account_id"
				,u.signup_hostcountry
				,u.document_country
				,u.base_fiat
				-- fee details
				,tx.reference_type "fee_type"
				,tx.reference_id "fee_reference_id"
				,tx.product_id "fee_product_id"
				,p.symbol "fee_product"
				,pt.type_name "fee_product_type"
				,CASE WHEN tx.cr <> 0 THEN tx.cr ELSE -tx.dr END "fee_amount"
			FROM
				oms_data.public.transactions tx
			LEFT JOIN
				oms_data.mysql_replica_apex.products p
				ON tx.product_id = p.product_id
			LEFT JOIN
				oms_data.public.products_types pt
				ON p.type = pt.id
			LEFT JOIN
				oms_data.analytics.users_master u
				ON tx.counterparty = u.ap_account_id
			WHERE
				tx.account_id = 1
				AND tx.transaction_type = 'Fee'
				AND tx.reference_type IN ('Trade', 'Deposit', 'Withdraw')
				-- not sure why reference_id = 0 happens
				AND tx.reference_id <> 0
				AND tx.counterparty NOT IN (3, 10, 21, 25041, 27443, 63152, 316078)
				/* 
				filter
				- market making accounts with no link to user
				- accounts shared by multiple users

				ap_user_id, ap_account_id, email
				2	3	remarketer
				10	10	rencelin@gmail.com
				8	10	harry.miller@alphapoint.com
				19	21	111@123.com
				21	21	123213@gamild.com
				44062	27443	francois+liquidmex@zipmex.com
				27448	27443	james+xbfloat@zipmex.com
				63200	63152	francois+liquidmexzmt@zipmex.com
				316226	316078	francois+liquidmexstocks@zipmex.com
				 */
		)
		, "fee_conversion_type" AS
		(
			SELECT
				f.*
				,CASE	WHEN f.base_fiat = 'USD' 	AND f.fee_product_type = 'CryptoCurrency' 											THEN 1
						WHEN f.base_fiat = 'USD' 	AND f.fee_product_type = 'NationalCurrency' 	AND f.fee_product = 'USD'		 	THEN 2
						WHEN f.base_fiat = 'USD' 	AND f.fee_product_type = 'NationalCurrency' 	AND f.fee_product <> f.base_fiat 	THEN 3
						WHEN f.base_fiat <> 'USD' 	AND f.fee_product_type = 'CryptoCurrency' 											THEN 4
						WHEN f.base_fiat <> 'USD' 	AND f.fee_product_type = 'NationalCurrency' 	AND f.fee_product = f.base_fiat 	THEN 5
						WHEN f.base_fiat <> 'USD' 	AND f.fee_product_type = 'NationalCurrency' 	AND f.fee_product = 'USD'			THEN 6
						WHEN f.base_fiat <> 'USD' 	AND f.fee_product_type = 'NationalCurrency' 	AND f.fee_product <> f.base_fiat	THEN 7
						ELSE 0
				END "fee_conversion_type"
			FROM
				fee_list f
		)
		, "fee_conversion_rates" AS
		(
			SELECT
				f.*
				,COALESCE(c.average_high_low, g.mid_price, ap.price) "crypto_usd_price"
				,COALESCE(e.exchange_rate, b.exchange_rate) "base_fiat_usd_rate"
				,COALESCE(e2.exchange_rate, b2.exchange_rate) "fiat_product_usd_rate"
			FROM
				fee_conversion_type f
			LEFT JOIN
				oms_data.public.cryptocurrency_prices c
				ON f.fee_product = c.product_1_symbol
				AND c.product_2_symbol ='USD'
				AND DATE_TRUNC('day', f.created_at) = DATE_TRUNC('day', c.last_updated)
				AND f.fee_conversion_type IN (1, 4)
				AND f.fee_product NOT IN ('GOLD', 'C8P', 'ZMT')
			LEFT JOIN
				oms_data.public.daily_closing_gold_prices g
				ON DATE_TRUNC('day', f.created_at) = DATE_TRUNC('day', g.last_updated)
				AND f.fee_conversion_type IN (1, 4)
				AND f.fee_product = 'GOLD'
			LEFT JOIN
				oms_data.public.daily_ap_prices ap
				ON f.fee_product = ap.product_1_symbol
				AND DATE_TRUNC('day', f.created_at) = DATE_TRUNC('day', ap.created_at)
				AND f.fee_conversion_type IN (1, 4)
				AND f.fee_product IN ('C8P', 'ZMT')
				AND ap.instrument_symbol IN ('C8PUSDT', 'ZMTUSD')
			LEFT JOIN
				oms_data.public.exchange_rates e
				ON f.base_fiat = e.product_2_symbol
				AND DATE_TRUNC('day', f.created_at) = DATE_TRUNC('day', e.last_updated)
				AND f.fee_conversion_type IN (4, 5, 6, 7)
				AND f.base_fiat <> 'THB'
			LEFT JOIN
				oms_data.public.bank_of_thailand_usdthb_filled_holes b
				ON DATE_TRUNC('day', f.created_at) = DATE_TRUNC('day', b.created_at)
				AND f.fee_conversion_type IN (4, 5, 6, 7)
				AND f.base_fiat = 'THB'
			LEFT JOIN
				oms_data.public.exchange_rates e2
				ON f.fee_product = e2.product_2_symbol
				AND DATE_TRUNC('day', f.created_at) = DATE_TRUNC('day', e2.last_updated)
				AND f.fee_conversion_type IN (3, 7)
				AND f.fee_product <> 'THB'
			LEFT JOIN
				oms_data.public.bank_of_thailand_usdthb_filled_holes b2
				ON DATE_TRUNC('day', f.created_at) = DATE_TRUNC('day', b2.created_at)
				AND f.fee_conversion_type IN (3, 7)
				AND f.fee_product = 'THB'
		)
		,"fee_amount_converted" AS
		(
			SELECT
				f.*
				,CASE	WHEN f.fee_conversion_type = 1 THEN f.fee_amount * f.crypto_usd_price
						WHEN f.fee_conversion_type = 2 THEN f.fee_amount
						WHEN f.fee_conversion_type = 3 THEN f.fee_amount / f.fiat_product_usd_rate
						WHEN f.fee_conversion_type = 4 THEN f.fee_amount * f.crypto_usd_price * f.base_fiat_usd_rate
						WHEN f.fee_conversion_type = 5 THEN f.fee_amount
						WHEN f.fee_conversion_type = 6 THEN f.fee_amount * f.base_fiat_usd_rate
						WHEN f.fee_conversion_type = 7 THEN f.fee_amount / f.fiat_product_usd_rate * f.base_fiat_usd_rate
						ELSE 0
				END "fee_base_fiat_amount"
				,CASE	WHEN f.fee_conversion_type = 1 THEN f.fee_amount * f.crypto_usd_price
						WHEN f.fee_conversion_type = 2 THEN f.fee_amount
						WHEN f.fee_conversion_type = 3 THEN f.fee_amount / f.fiat_product_usd_rate
						WHEN f.fee_conversion_type = 4 THEN f.fee_amount * f.crypto_usd_price
						WHEN f.fee_conversion_type = 5 THEN f.fee_amount / f.base_fiat_usd_rate
						WHEN f.fee_conversion_type = 6 THEN f.fee_amount
						WHEN f.fee_conversion_type = 7 THEN f.fee_amount / f.fiat_product_usd_rate
						ELSE 0
				END "fee_usd_amount"
			FROM 
				fee_conversion_rates f
		)
		, "th_vat" AS
		(
			SELECT
				f.*
				,CASE WHEN f.base_fiat = 'THB' THEN f.fee_base_fiat_amount /107 * 7 ELSE NULL END "th_vat"
			FROM
				fee_amount_converted f
		)
	SELECT
		*
	FROM
		fee_amount_converted f
	ORDER BY
		1 DESC
);

INSERT INTO oms_data.analytics.fees_master
(
	SELECT * FROM tmp_fees_master
);

DROP TABLE IF EXISTS tmp_fees_master;
