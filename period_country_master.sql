/*
	period_masters are used as base tables for management reports
	- ensures consistent use of dates
	- prevents gaps in reports if there are 0 records
	
	there are 2 period_masters
	1. period_master > day, week, month periods
	2. period_country_master > day, week, month periods with countries (signup_hostcountry)
	
	first date of period_master
	'2018-12-01' > first user registered on 2018-12-03
	
	last date of period_mater
	'2028-12-01' > 10 yrs after
 */

/*
	CREATE period_master script
	1. period_master > day, week, month periods
 */

DROP TABLE IF EXISTS oms_data.analytics.jes_period_master;

CREATE TABLE IF NOT EXISTS oms_data.analytics.jes_period_master
(
	created_at				TIMESTAMPTZ
	,start_date 			TIMESTAMPTZ
	,end_date 				TIMESTAMPTZ
	,period					VARCHAR(255)
);

DROP TABLE IF EXISTS tmp_period_master_day;
DROP TABLE IF EXISTS tmp_period_master_week;
DROP TABLE IF EXISTS tmp_period_master_month;

CREATE TEMP TABLE tmp_period_master_day AS
(
	SELECT
		DISTINCT
		date_list "created_at"
		,date_list "start_date"
		,date_list + '1 day'::INTERVAL "end_date"
		,'day' "period"  
	FROM 
		GENERATE_SERIES('2018-12-01'::TIMESTAMP, '2018-12-01'::TIMESTAMP + '10 year'::INTERVAL, '1 day'::INTERVAL) date_list
	ORDER BY
		1 ASC
);

CREATE TEMP TABLE tmp_period_master_week AS
(
	SELECT
		DISTINCT
		date_list "created_at"
		,date_list "start_date"
		,date_list + '6 day'::INTERVAL "end_date"
		,'week' "period"  
	FROM 
		GENERATE_SERIES('2018-12-01'::TIMESTAMP, '2018-12-01'::TIMESTAMP + '10 year'::INTERVAL, '1 week'::INTERVAL) date_list
	ORDER BY
		1 ASC
);

CREATE TEMP TABLE tmp_period_master_month AS
(
	SELECT
		DISTINCT
		date_list "created_at"
		,date_list "start_date"
		,date_list + '1 month - 1 day'::INTERVAL "end_date" 
		,'month' "period"  
	FROM 
		GENERATE_SERIES('2018-12-01'::TIMESTAMP, '2018-12-01'::TIMESTAMP + '10 year'::INTERVAL, '1 month'::INTERVAL) date_list
	ORDER BY
		1 ASC
);

INSERT INTO oms_data.analytics.jes_period_master 
(SELECT * FROM tmp_period_master_day);

INSERT INTO oms_data.analytics.jes_period_master 
(SELECT * FROM tmp_period_master_week);

INSERT INTO oms_data.analytics.jes_period_master 
(SELECT * FROM tmp_period_master_month);

DROP TABLE IF EXISTS tmp_period_master_day;
DROP TABLE IF EXISTS tmp_period_master_week;
DROP TABLE IF EXISTS tmp_period_master_month;

/*
	CREATE period_country_master script
	2. period_country_master > day, week, month periods with countries (signup_hostcountry)
 */

DROP TABLE IF EXISTS oms_data.analytics.jes_period_country_master;

CREATE TABLE IF NOT EXISTS oms_data.analytics.jes_period_country_master
(
	created_at				TIMESTAMPTZ
	,start_date 			TIMESTAMPTZ
	,end_date 				TIMESTAMPTZ
	,period					VARCHAR(255)
	,signup_hostcountry		VARCHAR(255)
);

DROP TABLE IF EXISTS tmp_period_country_master;

CREATE TEMP TABLE tmp_period_country_master AS
(
	WITH "country_list" AS
	(
		SELECT DISTINCT u.signup_hostcountry FROM oms_data.analytics.users_master u
	)
	SELECT
		p.*
		,c.signup_hostcountry
	FROM
		oms_data.analytics.jes_period_master p
	CROSS JOIN
		country_list c
	ORDER BY
		2 ASC, 3 ASC, 5 ASC
);

INSERT INTO oms_data.analytics.jes_period_country_master 
(SELECT * FROM tmp_period_country_master);

DROP TABLE IF EXISTS tmp_period_country_master;