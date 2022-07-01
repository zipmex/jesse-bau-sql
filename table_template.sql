DROP TABLE IF EXISTS oms_data.analytics.period_master;

CREATE TABLE IF NOT EXISTS oms_data.analytics.period_master
(
	"date" 		 						TIMESTAMPTZ
	,start_date 						TIMESTAMPTZ
	,end_date 							TIMESTAMPTZ
	,"period"							VARCHAR(255)
	,signup_hostcountry					VARCHAR(255)
);

DROP TABLE IF EXISTS tmp_period_master_1;
DROP TABLE IF EXISTS tmp_period_master_2;
DROP TABLE IF EXISTS tmp_period_master_3;
DROP TABLE IF EXISTS tmp_period_master_4;
DROP TABLE IF EXISTS tmp_period_master_5;
DROP TABLE IF EXISTS tmp_period_master_6;

CREATE TEMP TABLE tmp_period_master_1 AS
(
	WITH zipmex_start AS (
	SELECT DISTINCT DATE_TRUNC('day', MIN(created_at)) zipmex_start
	FROM analytics.users_master u 
	)
	SELECT
		DISTINCT "date"
		, "date" start_date 
		, "date" + '1 day'::INTERVAL end_date 
		, 'day' "period" 
		, 'total' signup_hostcountry 
	FROM zipmex_start, GENERATE_SERIES(zipmex_start::timestamp, zipmex_start + '10 year'::INTERVAL, '1 day'::INTERVAL) "date"
	ORDER BY 2,1
);

CREATE TEMP TABLE tmp_period_master_2 AS
(
	WITH zipmex_start AS (
	SELECT DISTINCT DATE_TRUNC('day', MIN(created_at)) zipmex_start
	FROM analytics.users_master u 
	)
	SELECT
		DISTINCT "date"
		, "date" start_date 
		, "date" + '6 day'::INTERVAL end_date 
		, 'week' "period"
		, 'total' signup_hostcountry 
	FROM zipmex_start, GENERATE_SERIES(DATE_TRUNC('week', zipmex_start), zipmex_start + '10 year'::INTERVAL, '1 week'::INTERVAL) "date"
	ORDER BY 2,1 DESC 
);

CREATE TEMP TABLE tmp_period_master_3 AS
(
	WITH zipmex_start AS (
	SELECT DISTINCT DATE_TRUNC('day', MIN(created_at)) zipmex_start
	FROM analytics.users_master u 
	)
	SELECT
		DISTINCT "date"
		, "date" start_date 
		, "date" + '1 month - 1 day'::INTERVAL end_date 
		, 'month' "period"
		, 'total' signup_hostcountry 
	FROM zipmex_start, GENERATE_SERIES(date_trunc('month',zipmex_start), zipmex_start + '10 year'::INTERVAL, '1 month'::INTERVAL) "date"
	ORDER BY 2,1 DESC 
); 

CREATE TEMP TABLE tmp_period_master_4 AS
(
	WITH zipmex_start AS (
	SELECT DISTINCT DATE_TRUNC('day', MIN(created_at)) zipmex_start
	FROM analytics.users_master u 
	)
	SELECT
		DISTINCT "date"
		, "date" start_date 
		, "date" + '1 day'::INTERVAL end_date 
		, 'day' "period"
		, signup_hostcountry 
	FROM zipmex_start, GENERATE_SERIES(zipmex_start::timestamp, zipmex_start + '10 year'::INTERVAL, '1 day'::INTERVAL) "date"
	CROSS JOIN (SELECT DISTINCT signup_hostcountry FROM analytics.users_master WHERE signup_hostcountry IN ('AU','ID','global','TH')) c
	ORDER BY 2,1
);

CREATE TEMP TABLE tmp_period_master_5 AS
(
	WITH zipmex_start AS (
	SELECT DISTINCT DATE_TRUNC('day', MIN(created_at)) zipmex_start
	FROM analytics.users_master u 
	)
	SELECT
		DISTINCT "date"
		, "date" start_date 
		, "date" + '6 day'::INTERVAL end_date 
		, 'week' "period"
		, signup_hostcountry 
	FROM zipmex_start, GENERATE_SERIES(DATE_TRUNC('week', zipmex_start), zipmex_start + '10 year'::INTERVAL, '1 week'::INTERVAL) "date"
	CROSS JOIN (SELECT DISTINCT signup_hostcountry FROM analytics.users_master WHERE signup_hostcountry IN ('AU','ID','global','TH')) c
	ORDER BY 2,1 DESC 
);

CREATE TEMP TABLE tmp_period_master_6 AS
(
	WITH zipmex_start AS (
	SELECT DISTINCT DATE_TRUNC('day', MIN(created_at)) zipmex_start
	FROM analytics.users_master u 
	)
	SELECT
		DISTINCT "date"
		, "date" start_date 
		, "date" + '1 month - 1 day'::INTERVAL end_date 
		, 'month' "period"
		, signup_hostcountry 
	FROM zipmex_start, GENERATE_SERIES(date_trunc('month',zipmex_start), zipmex_start + '10 year'::INTERVAL, '1 month'::INTERVAL) "date"
	CROSS JOIN (SELECT DISTINCT signup_hostcountry FROM analytics.users_master WHERE signup_hostcountry IN ('AU','ID','global','TH')) c
	ORDER BY 2,1 DESC 
); 


INSERT INTO oms_data.analytics.period_master 
( SELECT * FROM tmp_period_master_1);

INSERT INTO oms_data.analytics.period_master 
( SELECT * FROM tmp_period_master_2);

INSERT INTO oms_data.analytics.period_master 
( SELECT * FROM tmp_period_master_3);

INSERT INTO oms_data.analytics.period_master 
( SELECT * FROM tmp_period_master_4);

INSERT INTO oms_data.analytics.period_master 
( SELECT * FROM tmp_period_master_5);

INSERT INTO oms_data.analytics.period_master 
( SELECT * FROM tmp_period_master_6);

DROP TABLE IF EXISTS tmp_period_master_1;
DROP TABLE IF EXISTS tmp_period_master_2;
DROP TABLE IF EXISTS tmp_period_master_3;
DROP TABLE IF EXISTS tmp_period_master_4;
DROP TABLE IF EXISTS tmp_period_master_5;
DROP TABLE IF EXISTS tmp_period_master_6;

