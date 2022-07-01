/*
 * Sources: aws - warehouse
 * 1. oms_data_public.cryptocurrency_prices with source = 'coinmarketcap'
 * 2. oms_data_public.ap_prices: ZMT, C8P, TOK with source = 'alphapoint'
 * 3. oms_data_public.gold_prices: GOLD
 * 4. oms_data_public.exchange_rates: USD to AUD, IDR, SGD with source = 'coinmarketcap'
 * 5. oms_data_public.exchange_rates: THB with source = 'bank-of-thailand'
*/

--DROP TABLE IF EXISTS warehouse.analytics.rates_master;

CREATE TABLE IF NOT EXISTS warehouse.analytics.rates_master
(
	id								SERIAL PRIMARY KEY 
	, created_at	 				TIMESTAMPTZ
	, instrument_symbol	 			VARCHAR(255) 
	, product_1_symbol 				VARCHAR(255)
	, product_2_symbol				VARCHAR(255)
	, price							NUMERIC
	, product_type					INTEGER
	, "source"						VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS rates_master_keys ON warehouse.analytics.rates_master
(created_at, instrument_symbol, product_1_symbol, product_2_symbol);

TRUNCATE TABLE warehouse.analytics.rates_master;

DROP TABLE IF EXISTS tmp_daily_closing_gold_prices;
DROP TABLE IF EXISTS tmp_daily_ap_prices;
DROP TABLE IF EXISTS tmp_usd_rate;
DROP TABLE IF EXISTS tmp_thb_exchange_rates;
DROP TABLE IF EXISTS tmp_non_thb_exchange_rates;
DROP TABLE IF EXISTS tmp_daily_cryptocurrency_prices;

---- daily closing gold prices -- missing July 31st, Aug 1st, Sep 13-16th -- pending Adrien to backfill
CREATE TEMP TABLE IF NOT EXISTS tmp_daily_closing_gold_prices AS
(
	WITH gold_price_row AS 
	(
		SELECT 
			DATE_TRUNC('day'::text, g_1.updated)::timestamp AS created_at
			, (g_1.ask + g_1.bid) / 2::numeric::double precision AS mid_price
			, g_1.updated AS last_updated
			, ROW_NUMBER () OVER (PARTITION BY (DATE_TRUNC('day'::TEXT, g_1.updated)) ORDER BY g_1.updated DESC) AS row_number
		FROM 
			oms_data_public.gold_prices g_1
	)
	SELECT 
	--	ROW_NUMBER() OVER () AS id
		g.created_at
		, 'GOLDUSD' instrument_symbol
		, 'GOLD' product_1_symbol
		, 'USD' product_2_symbol
		, g.mid_price
		, 2 "product_type"
	--	, g.last_updated
	FROM 
		gold_price_row g
	WHERE 
		g.row_number = 1
);


---- daily alpha point prices: ZMT, C8P, TOK
CREATE TEMP TABLE IF NOT EXISTS tmp_daily_ap_prices AS
(
	WITH ap_prices_row AS 
	(
		SELECT 
			DATE_TRUNC('day'::TEXT, a_1.inserted_at)::timestamp AS created_at
			, a_1.instrument_symbol
			, a_1.product_1_symbol
			, a_1.product_2_symbol
			, a_1.price
			, a_1.inserted_at
			, ROW_NUMBER() OVER (PARTITION BY (DATE_TRUNC('day'::TEXT, a_1.inserted_at)), a_1.instrument_symbol ORDER BY a_1.inserted_at DESC) AS row_number
		FROM
			oms_data_public.ap_prices a_1
	)
	SELECT 
	--	ROW_NUMBER() OVER () AS id
		a.created_at
		, a.instrument_symbol
		, a.product_1_symbol
		, CASE WHEN a.instrument_symbol = 'C8PUSDT' THEN 'USD' ELSE a.product_2_symbol END AS product_2_symbol
		, a.price
		, 2 "product_type"
		, 'alphapoint' "source" 
	FROM 
		ap_prices_row a
	WHERE 
		a.row_number = 1
		AND a.product_2_symbol IN ('USD','USDT')
		AND a.instrument_symbol <> 'ZMTUSDT'
);
	

---- daily THB USD exchange rate by 'bank-of-thailand'
CREATE TEMP TABLE IF NOT EXISTS tmp_thb_exchange_rates AS
(
	WITH exchange_rates_bot AS 
	(
		SELECT 
			exchange_rates.id
			, exchange_rates.created_at
			, exchange_rates.exchange_rate
			, exchange_rates.last_updated 
		FROM 
			oms_data_public.exchange_rates 
		WHERE 
			exchange_rates.source::TEXT = 'bank-of-thailand'::TEXT 
	)
		, created_at_filled_holes AS 
	( 
		SELECT 
			generate_series(min(exchange_rates_bot.created_at)::timestamp with time ZONE
			, GREATEST(max(exchange_rates_bot.last_updated), NOW() - '1 day'::INTERVAL), '1 day'::INTERVAL)::date AS date
		FROM 
			exchange_rates_bot 
	)
		, exchange_rates_filled_holes AS 
	( 
		SELECT 
			e.id
			, c.date AS created_at
			, e.exchange_rate
			, e.last_updated 
		FROM 
			exchange_rates_bot e 
			RIGHT JOIN 
				created_at_filled_holes c 
				ON c.date = e.created_at
	)
	SELECT 
	--	first_value(t.id) OVER (PARTITION BY t.grp_exchange_rate) AS id
		t.created_at::timestamp 
		, 'USDTHB' instrument_symbol 
		, 'THB' product_2_symbol 
		, 'USD' product_1_symbol 
		, first_value(t.exchange_rate) OVER (PARTITION BY t.grp_exchange_rate) AS exchange_rate
	--	, first_value(t.last_updated) OVER (PARTITION BY t.grp_exchange_rate) AS last_updated
		, 1 "product_type"
		, 'bank-of-thailand' "source" 
	FROM 
		( 
		SELECT 
		exchange_rates_filled_holes.id
		, exchange_rates_filled_holes.created_at
		, exchange_rates_filled_holes.exchange_rate
		, exchange_rates_filled_holes.last_updated
		, SUM(  CASE WHEN exchange_rates_filled_holes.exchange_rate IS NOT NULL THEN 1
					ELSE NULL::integer 
					END) 
			OVER (ORDER BY exchange_rates_filled_holes.created_at) 
			AS grp_exchange_rate
		FROM exchange_rates_filled_holes
		) t
	ORDER BY 1 DESC 
);


---- daily exchange rates for other currency: AUD, IDR, SGD, VND 
-- to QA (select created_at , product_2_symbol , COUNT(*) from oms_data_public.exchange_rates er group by 1,2)
CREATE TEMP TABLE IF NOT EXISTS tmp_non_thb_exchange_rates AS
(
	SELECT 
	---- created_at shows incorrectly if any prices were backfilled (check created_at = 2020-07-16, 2021-05-12, 2021-07-10) --> using last updated + 1 day instead.
		DATE_TRUNC('day', last_updated) last_updated 
		, instrument_symbol 
		, product_2_symbol 
		, product_1_symbol 
		, exchange_rate 
		, 1 "product_type"
		, 'coinmarketcap' "source" 
	FROM 
		oms_data_public.exchange_rates er 
	WHERE 
		product_2_symbol <> 'THB'
		AND "source" = 'coinmarketcap'
	ORDER BY 1 DESC
);


---- rest of crypto prices - using average_high_low to have consistency with master tables (trades, deposit, withdraw, fees)
-- to QA (SELECT product_1_symbol , product_1_cmc_id , COUNT(DISTINCT product_2_symbol) FROM oms_data_public.cryptocurrency_prices cp WHERE product_2_symbol = 'USD' GROUP BY 1,2 ORDER BY 3 DESC )
CREATE TEMP TABLE IF NOT EXISTS tmp_daily_cryptocurrency_prices AS
(
	SELECT
	---- created_at shows incorrectly if any prices were backfilled (check created_at = 2020-07-16, 2021-05-12, 2021-07-10) --> using last updated + 1 day instead.
		DATE_TRUNC('day', last_updated) created_time 
		, instrument_symbol
		, CASE WHEN instrument_symbol = 'MIOTAUSD' THEN 'IOTA' 
				WHEN instrument_symbol = 'USDPUSD' THEN 'PAX' 
				ELSE product_1_symbol 
				END AS product_1_symbol
				/*
				 * Exception: 
				 * 1. token symbol MIOTA, fullname: IOTA, listed on exchange as IOTA
				 * 2. token symbol USDP, fullname: PAX DOLLAR, listed on exchange as PAX
				 */ 
		, product_2_symbol 
		, average_high_low 
		, 2 "product_type"
		, 'coinmarketcap' "source" 
	FROM 
		oms_data_public.cryptocurrency_prices cp 
	WHERE 
		product_2_symbol = 'USD'
	ORDER BY 1 DESC   
);

-- USD rate = 1
CREATE TEMP TABLE IF NOT EXISTS tmp_usd_rate AS
(
	SELECT
		DISTINCT 
		pm.created_at 
		, 'USDUSD' instrument_symbol
		, 'USD' product_1_symbol
		, 'USD' product_2_symbol
		, 1 price
		, 1 "product_type"
	FROM 
		analytics.period_master pm 
	WHERE 
		"period" = 'day'
		AND pm.created_at <= DATE_TRUNC('day', NOW())
	ORDER BY 1
);

INSERT INTO warehouse.analytics.rates_master (created_at, instrument_symbol, product_1_symbol, product_2_symbol, price, product_type) 
(
	SELECT * FROM tmp_daily_closing_gold_prices
);

INSERT INTO warehouse.analytics.rates_master (created_at, instrument_symbol, product_1_symbol, product_2_symbol, price, product_type , "source") 
(
	SELECT * FROM tmp_daily_ap_prices
);

INSERT INTO warehouse.analytics.rates_master (created_at, instrument_symbol, product_1_symbol, product_2_symbol, price, product_type , "source") 
(
	SELECT * FROM tmp_thb_exchange_rates
);

INSERT INTO warehouse.analytics.rates_master (created_at, instrument_symbol, product_1_symbol, product_2_symbol, price, product_type , "source") 
(
	SELECT * FROM tmp_non_thb_exchange_rates
);

INSERT INTO warehouse.analytics.rates_master (created_at, instrument_symbol, product_1_symbol, product_2_symbol, price, product_type , "source") 
(
	SELECT * FROM tmp_daily_cryptocurrency_prices
);

INSERT INTO warehouse.analytics.rates_master (created_at, instrument_symbol, product_1_symbol, product_2_symbol, price, product_type) 
(
	SELECT * FROM tmp_usd_rate
);


DROP TABLE IF EXISTS tmp_daily_closing_gold_prices;
DROP TABLE IF EXISTS tmp_daily_ap_prices;
DROP TABLE IF EXISTS tmp_usd_rate;
DROP TABLE IF EXISTS tmp_thb_exchange_rates;
DROP TABLE IF EXISTS tmp_non_thb_exchange_rates;
DROP TABLE IF EXISTS tmp_daily_cryptocurrency_prices;


