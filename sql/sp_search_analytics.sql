
CREATE OR REPLACE PROCEDURE search_analytics.sp_search_analytics_mart(data_type int4)
	LANGUAGE plpgsql
AS $$
	
	 
	
declare
st_dt date; en_dt date; st_yr int; en_yr int; st_mnth int; en_mnth int; 
curr_table_start_time timestamp; curr_table_end_time timestamp;
datatype varchar(15);
BEGIN
curr_table_start_time := current_timestamp at time zone 'Asia/Kolkata';
	
	if data_type = 1 then
	
		st_dt := (select (current_date - ((extract(dow from current_date)+7) || ' day')::interval)::date);
		en_dt := (select (current_date - ((extract(dow from current_date)+7) || ' day')::interval+6)::date);
		st_yr := (select EXTRACT(YEAR FROM st_dt));
		en_yr := (select EXTRACT(YEAR FROM en_dt));
		st_mnth := (select EXTRACT(MONTH FROM st_dt));
		en_mnth := (select EXTRACT(MONTH FROM en_dt));
		datatype:='w';
--	
	execute 'DELETE FROM search_analytics.search_ctr
		WHERE start_date <= date(date(' || '''' || st_dt || ''') - INTERVAL ''52 weeks'') and time_period_flag = ''w'' ';
	
	elsif data_type = 2 then
	
		st_dt := (select DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month'));
		en_dt :=((select DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day'));
		st_yr := (select EXTRACT(YEAR FROM st_dt));
		en_yr := (select EXTRACT(YEAR FROM en_dt));
		st_mnth := (select EXTRACT(MONTH FROM st_dt));
		en_mnth := (select EXTRACT(MONTH FROM en_dt));
		datatype:='m';
	
	execute 'DELETE FROM search_analytics.search_ctr
		WHERE start_date <= date(date(' || '''' || st_dt || ''')- INTERVAL ''12 months'') and time_period_flag = ''m'' ';
	ELSIF data_type = 3 THEN
    -- Daily
	    st_dt := CURRENT_DATE - INTERVAL '1 day';
	    en_dt := CURRENT_DATE - INTERVAL '1 day';
	
	    st_yr   := EXTRACT(YEAR FROM st_dt);
	    en_yr   := EXTRACT(YEAR FROM en_dt);
	    st_mnth := EXTRACT(MONTH FROM st_dt);
	    en_mnth := EXTRACT(MONTH FROM en_dt);
	
	    datatype := 'd';

		EXECUTE 'DELETE FROM search_analytics.search_ctr
		    WHERE start_date <= (DATE ''' || st_dt || ''' - INTERVAL ''42 days'')
		    AND time_period_flag = ''d'' ';

	end if;




if data_type = 3
then
---------------------------------------------------------
---------------------------------------------------------
Delete from staging.stg_dir_query;
insert into staging.stg_dir_query 
SELECT
	date(date_r) as date, query_modid as modid, sender_country_iso as country, enquiry_id,
	left(query_reference_url,4000) query_reference_url, dir_query_categoryid, query_reference_text AS cd_additional_data,
	dir_query_login_mode, user_id, distance_city, 1 as enq_type,
	left(replace(replace(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(replace(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(query_reference_url, '%21', '!'), '%23', '#'), '%24', '$'), '%26', '&'), '%28', '('), '%29', ')'), '%2A', '*'), '%2B', '+'), '%2C', ','), '%20', ' '), '%3D', '='), '%3F', '?'), '%25', '%'), '%2F', '/'), '%5C', '\\'), '%3A', ':'), '%3B', ';'), '%7C', '|'), '%3D', '='), '%20', ' '), '%22', '"'), '%20', ' '), '%2B', '+'),4000) AS ref_url
FROM dwh.fact_dir_query
WHERE date(date_r) BETWEEN st_dt and en_dt
UNION ALL
SELECT
	date(date_r) as date, query_modid as modid, sender_country_iso as country, enquiry_id,
	left(query_reference_url,4000) query_reference_url, dir_query_categoryid, query_reference_text AS cd_additional_data,
	dir_query_login_mode, FK_user_id as user_id, null::numeric(10,5) as distance_city, 2 as enq_type,
	left(replace(replace(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(replace(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(query_reference_url, '%21', '!'), '%23', '#'), '%24', '$'), '%26', '&'), '%28', '('), '%29', ')'), '%2A', '*'), '%2B', '+'), '%2C', ','), '%20', ' '), '%3D', '='), '%3F', '?'), '%25', '%'), '%2F', '/'), '%5C', '\\'), '%3A', ':'), '%3B', ';'), '%7C', '|'), '%3D', '='), '%20', ' '), '%22', '"'), '%20', ' '), '%2B', '+'),4000) AS ref_url
FROM dwh.fact_dir_query_bounced
WHERE date(date_r) BETWEEN st_dt and en_dt
UNION ALL
SELECT
	date(date_r) as date, query_modid as modid, null::varchar(5) as country, enquiry_id,
	left(query_reference_url,4000) query_reference_url, dir_query_categoryid, query_reference_text AS cd_additional_data,
	dir_query_login_mode, FK_user_id as user_id, null::numeric(10,5) as distance_city, 3 as enq_type,
	left(replace(replace(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(replace(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(query_reference_url, '%21', '!'), '%23', '#'), '%24', '$'), '%26', '&'), '%28', '('), '%29', ')'), '%2A', '*'), '%2B', '+'), '%2C', ','), '%20', ' '), '%3D', '='), '%3F', '?'), '%25', '%'), '%2F', '/'), '%5C', '\\'), '%3A', ':'), '%3B', ';'), '%7C', '|'), '%3D', '='), '%20', ' '), '%22', '"'), '%20', ' '), '%2B', '+'),4000) AS ref_url
FROM dwh.fact_dir_query_waiting
WHERE date(date_r) BETWEEN st_dt and en_dt;
---------------------------------------------------------
---------------------------------------------------------
Delete from staging.stg_c2c_records;
insert into staging.stg_c2c_records 
SELECT
	2 as login_mode, c2c_call_time, c2c_caller_country_iso, left(c2c_page_type,4000) AS cd_additional_data,
	C2C_MODID AS modid, c2c_caller_glusr_id, c2c_record_id, c2c_caller_city_id, c2c_receiver_city_id,
	left(replace(replace(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(replace(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(c2c_referer_url,'%21', '!'),'%23', '#'),'%24', '$'),'%26', '"&"'),'%28','('),'%29', ')'),'%2A', '*'),'%2B', '+'),'%2C',','),'%20',' '),'%3D','='),'%3F','?'),'%25','%'),'%2F','/'),'%5C','"\"'),'%3A',':'),'%3B',';'),'%7C','|'),'%3D','='),'%20',' '),'%22','"""'),'%20',' '),'%2B','+'),4000) AS ref_url,
	c2c_record_type,
	fk_c2c_record_unidentified_id
	FROM dwh.fact_c2c_records
WHERE trunc(date(C2C_CALL_TIME)) BETWEEN st_dt and en_dt
and fk_c2c_record_unidentified_id is null
UNION ALL
SELECT
	1 as login_mode, c2c_call_time, c2c_caller_country_iso, left(c2c_page_type,4000) AS cd_additional_data,
	C2C_MODID AS modid, c2c_caller_glusr_id, c2c_record_id, c2c_caller_city_id, c2c_receiver_city_id,
	left(replace(replace(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(replace(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(c2c_referer_url,'%21', '!'),'%23', '#'),'%24', '$'),'%26', '"&"'),'%28','('),'%29', ')'),'%2A', '*'),'%2B', '+'),'%2C',','),'%20',' '),'%3D','='),'%3F','?'),'%25','%'),'%2F','/'),'%5C','"\"'),'%3A',':'),'%3B',';'),'%7C','|'),'%3D','='),'%20',' '),'%22','"""'),'%20',' '),'%2B','+'),4000) AS ref_url,
	c2c_record_type,
	fk_c2c_record_unidentified_id
FROM dwh.fact_c2c_records_unidentified
WHERE trunc(date(C2C_CALL_TIME)) BETWEEN st_dt and en_dt;

---------------------------------------------------------
---------------------------------------------------------
DELETE FROM staging.search_web_data;
INSERT INTO staging.search_web_data
SELECT
    st_dt                                                   AS DATE,
    status, modid,
    LEFT(trim(lower(keyword)), 990)                         AS keyword_new,
    kwd_word_count, country, category_id,
    LEFT(search_url_city, 490)                              AS search_url_city,
    QU_CX, QU_TR, biz_type_filter_new,
    LEFT(url_resultcount_new, 490)                          AS url_resultcount_new,
    LEFT(voice_search_language_new, 490)                    AS voice_search_language_new,
    LEFT(kwd_type_new, 490)                                 AS kwd_type_new,
    attr_srch_new, attr_brand_new, query_type_new,
    LEFT(src_path_new, 490)                                 AS src_path_new,
    NULL                                                    AS enquiry_click_type,
    0 AS city_only, 0 AS ecom_filter, 0 AS rcmnd_srch,
    cd_user_mode,
    CAST(
        CASE WHEN positions ~ '^[0-9]+(\.[0-9]+)?$' THEN positions ELSE NULL END
    AS DECIMAL(10,5))                                       AS positions,
    0 pageviews, 0 pdp_clicks, 0 event_catalogue_clicks, 0 enquiry_cta_clicks,
    COUNT(DISTINCT CASE WHEN status = 3 THEN enquiry_id END)  AS enquiries,
    COUNT(DISTINCT CASE WHEN status = 4 THEN enquiry_id END)  AS calls,
    0 AS page_type,
    NULL                                                    AS special_srchs,
    LEFT(city_tier, 490)                                    AS city_tier,
    price_filter_enabled, price_intent, custom_price,
    LEFT(min_price, 490)                                    AS min_price,
    LEFT(max_price, 490)                                    AS max_price,
    NULL                                                    AS user_pseudo_id,
    LEFT(result_Count, 490)                                 AS result_Count,
    user_id,
    prdsrc, no_sugg, qu_to, qu_comp, list_vw, category_to, qu_attr_to,
    compass_confidence, category_type,
    is_spec_filter_available,
    LEFT(firstValue, 190)                                   AS firstValue,
    LEFT(second_value, 190)                                 AS second_value,
    is_group_filter_available,
    CASE WHEN flavl_No_of_Filters_displayed ~ '^[0-9]+$' THEN flavl_No_of_Filters_displayed::INT ELSE 0 END AS flavl_No_of_Filters_displayed,
    CASE WHEN spcfl_No_of_times_clicked     ~ '^[0-9]+$' THEN spcfl_No_of_times_clicked::INT     ELSE 0 END AS spcfl_No_of_times_clicked,
    CASE WHEN sprs_No_of_results_shown      ~ '^[0-9]+$' THEN sprs_No_of_results_shown::INT      ELSE 0 END AS sprs_No_of_results_shown,
    CASE WHEN Position_of_Filter_Clicked    ~ '^[0-9]+$' THEN Position_of_Filter_Clicked::INT    ELSE 0 END AS Position_of_Filter_Clicked,
    LEFT(spec_filtername, 190)                              AS spec_filtername,
    LEFT(spec_filtervalue, 190)                             AS spec_filtervalue,
    popular_filter_clicked,
    LEFT(Redirection, 190)                                  AS Redirection,
    Price_filter_position, Search_redirection,
    LEFT(Price_bucket, 190)                                 AS Price_bucket,
    Locality_Filter_Present, Locality_Filter_Clicked,
    Annual_gst_turnover, gst_registration_date,
    distance_city,
    LEFT(Exact_match_present, 190)                          AS Exact_match_present,
    enq_type

FROM (

    -- ── status = 3 : enquiries ────────────────────────────────────────────────
    SELECT
        3                                                   AS status,
        CASE
            WHEN modid = 'IMOB'                THEN 'Mobile'
            WHEN modid = 'DIR'                 THEN 'Desktop'
            WHEN modid IN ('ANDROID','ANDWEB') THEN 'Android'
            WHEN modid IN ('IOS','IOSWEB')     THEN 'IOS'
        END                                                 AS modid,
        CASE WHEN country = 'IN' THEN 1 ELSE 2 END          AS country,
        CASE
            WHEN modid = 'DIR'  AND cd_additional_data LIKE '%|Position=%' THEN REGEXP_SUBSTR(cd_additional_data, '\\|Position=([^|&]+)', 1, 1, 'e')
            WHEN modid = 'IMOB' AND cd_additional_data LIKE '%|pos=%'      THEN REGEXP_SUBSTR(cd_additional_data, '\\|pos=([^|&]+)',      1, 1, 'e')
            WHEN modid = 'DIR'  AND cd_additional_data LIKE '%|Position-%' THEN REGEXP_SUBSTR(cd_additional_data, '\\|Position-([^|&]+)', 1, 1, 'e')
            WHEN modid = 'IMOB' AND cd_additional_data LIKE '%|pos-%'      THEN REGEXP_SUBSTR(cd_additional_data, '\\|pos-([^|&]+)',      1, 1, 'e')
            WHEN modid = 'DIR'  AND cd_additional_data LIKE '%|Position:%' THEN REGEXP_SUBSTR(cd_additional_data, '\\|Position:([^|&]+)', 1, 1, 'e')
            WHEN modid = 'IMOB' AND cd_additional_data LIKE '%|pos:%'      THEN REGEXP_SUBSTR(cd_additional_data, '\\|pos:([^|&]+)',      1, 1, 'e')
            ELSE NULL
        END                                                 AS Positions,
        CASE
            WHEN ref_url ~ 'stype:slrtxt-graph' THEN 1
            WHEN ref_url ~ 'stype:graph-cse'    THEN 2
            WHEN ref_url ~ 'stype:graph'        THEN 3
            ELSE 0
        END                                                 AS page_type,
        CAST(CASE WHEN ref_url LIKE '%attr=1%' THEN 1 WHEN ref_url LIKE '%attr:1%' THEN 1 ELSE 0 END AS INTEGER) AS attr_srch_new,
        CASE
            WHEN query_reference_url LIKE '%&qry_typ=C%'     THEN 2  WHEN query_reference_url LIKE '%qry_typ:C%'     THEN 2  WHEN query_reference_url LIKE '%qry_typ"":""C%'  THEN 2
            WHEN ref_url            LIKE '%qry_typ"":""C%'   THEN 2  WHEN ref_url            LIKE '%qry_typ:C%'      THEN 2  WHEN ref_url            LIKE '%&qry_typ=C%'      THEN 2
            WHEN query_reference_url LIKE '%&qry_typ=P%'     THEN 1  WHEN query_reference_url LIKE '%qry_typ:P%'     THEN 1  WHEN query_reference_url LIKE '%qry_typ"":""P%'  THEN 1
            WHEN ref_url            LIKE '%qry_typ:P%'       THEN 1  WHEN ref_url            LIKE '%&qry_typ=P%'     THEN 1  WHEN ref_url            LIKE '%qry_typ"":""P%'   THEN 1
            WHEN query_reference_url LIKE '%&qry_typ=S%'     THEN 3  WHEN query_reference_url LIKE '%qry_typ:S%'     THEN 3  WHEN query_reference_url LIKE '%qry_typ"":""S%'  THEN 3
            WHEN ref_url            LIKE '%qry_typ:S%'       THEN 3  WHEN ref_url            LIKE '%&qry_typ=S%'     THEN 3  WHEN ref_url            LIKE '%qry_typ"":""S%'   THEN 3
        END                                                 AS Query_type_new,
        CAST(CASE WHEN ref_url LIKE '%qu-cx=1%' THEN 1 WHEN ref_url LIKE '%qcr:1%'    THEN 1 WHEN ref_url LIKE '%qcr"":""1%' THEN 1 ELSE NULL END AS INT) AS qu_cx,
        CAST(CASE WHEN ref_url LIKE '%qu-tr=1%' THEN 1 WHEN ref_url LIKE '%qtr:1%'    THEN 1 WHEN ref_url LIKE '%qtr"":""1%' THEN 1 ELSE NULL END AS INT) AS qu_tr,
        CAST(
            CASE
                WHEN ref_url LIKE '%mc:%'
                 AND SPLIT_PART(SPLIT_PART(ref_url, 'mc:', 2), '|', 1) ~ '^[0-9]+$'
                    THEN SPLIT_PART(SPLIT_PART(ref_url, 'mc:', 2), '|', 1)
                ELSE NULL
            END AS INTEGER
        )                                                   AS category_id,

        -- search_url_city: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN (ref_url LIKE '%cq=%'  AND ref_url NOT LIKE '%cq=all&%')                            THEN REPLACE(REGEXP_SUBSTR(ref_url,'&cq=([^&|#?]+)', 1, 1, 'e'),'+', ' ')
                WHEN ((ref_url LIKE '%&cq=&%' OR ref_url LIKE '%cq=all&%')  AND ref_url NOT LIKE '%cq=%') THEN 'All India'
                WHEN (ref_url LIKE '%cq:%'  AND ref_url NOT LIKE '%cq:all%')                              THEN REPLACE(REGEXP_SUBSTR(ref_url,'cq:([^&|#?]+)',  1, 1, 'e'),'+', ' ')
                WHEN ((ref_url LIKE '%=cq:|%' OR ref_url LIKE '%cq:all%')    AND ref_url NOT LIKE '%cq:%') THEN 'All India'
                ELSE 'All India'
            END
        AS VARCHAR(490))                                    AS search_url_city,

        -- keyword: CAST to VARCHAR(990)
        CAST(
            CASE
                WHEN ref_url LIKE '%?s=%'  THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?s=([^&]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%?ss=%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?ss=([^&]+)', 1, 1, 'e')
                WHEN ref_url LIKE '%ss =%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), 'ss =([^&|]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%ss%'   THEN REGEXP_SUBSTR(ref_url, 'ss[:=]([^-]+)', 1, 1, 'e')
                WHEN cd_additional_data = 'Search' THEN
                    CASE WHEN POSITION('|' IN ref_url) > 0 THEN LEFT(ref_url, POSITION('|' IN ref_url) - 1) ELSE ref_url END
            END
        AS VARCHAR(990))                                    AS keyword,

        -- kwd_word_count derived from already-cast keyword expression
        LENGTH(TRIM(CAST(
            CASE
                WHEN ref_url LIKE '%?s=%'  THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?s=([^&]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%?ss=%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?ss=([^&]+)', 1, 1, 'e')
                WHEN ref_url LIKE '%ss =%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), 'ss =([^&|]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%ss%'   THEN REGEXP_SUBSTR(ref_url, 'ss[:=]([^-]+)', 1, 1, 'e')
                WHEN cd_additional_data = 'Search' THEN
                    CASE WHEN POSITION('|' IN ref_url) > 0 THEN LEFT(ref_url, POSITION('|' IN ref_url) - 1) ELSE ref_url END
            END
        AS VARCHAR(990)))) -
        LENGTH(REPLACE(TRIM(CAST(
            CASE
                WHEN ref_url LIKE '%?s=%'  THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?s=([^&]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%?ss=%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?ss=([^&]+)', 1, 1, 'e')
                WHEN ref_url LIKE '%ss =%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), 'ss =([^&|]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%ss%'   THEN REGEXP_SUBSTR(ref_url, 'ss[:=]([^-]+)', 1, 1, 'e')
                WHEN cd_additional_data = 'Search' THEN
                    CASE WHEN POSITION('|' IN ref_url) > 0 THEN LEFT(ref_url, POSITION('|' IN ref_url) - 1) ELSE ref_url END
            END
        AS VARCHAR(990))), ' ', '')) + 1                    AS kwd_word_count,

        -- url_resultcount_new: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN query_reference_url LIKE '%res=RC%'     THEN REGEXP_SUBSTR(query_reference_url, '&res=RC(\\d+)',      1, 1, 'e')
                WHEN query_reference_url LIKE '%res:RC%'     THEN REGEXP_SUBSTR(query_reference_url, 'res:RC(\\d+)',       1, 1, 'e')
                WHEN query_reference_url LIKE '%res"":""RC%' THEN REGEXP_SUBSTR(query_reference_url, 'res"":""RC(\\d+)',   1, 1, 'e')
                WHEN ref_url            LIKE '%res=RC%'      THEN REGEXP_SUBSTR(ref_url,             '&res=RC(\\d+)',      1, 1, 'e')
                WHEN ref_url            LIKE '%res:RC%'      THEN REGEXP_SUBSTR(ref_url,             'res:RC(\\d+)',       1, 1, 'e')
                WHEN ref_url            LIKE '%res"":""RC%'  THEN REGEXP_SUBSTR(ref_url,             'res"":""RC(\\d+)',   1, 1, 'e')
                WHEN ref_url            LIKE '%res=RC%'      THEN REGEXP_SUBSTR(ref_url,             'res=RC([^-|]*)',     1, 1, 'e')
                WHEN ref_url            LIKE '%res:RC%'      THEN REGEXP_SUBSTR(ref_url,             'res:RC([^-|]*)',     1, 1, 'e')
            END
        AS VARCHAR(490))                                    AS url_resultcount_new,

        -- src_path_new: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN LOWER(ref_url) LIKE '%&src=%' THEN REGEXP_SUBSTR(ref_url, '&src=([^|&:%""]+)', 1, 1, 'e')
            END
        AS VARCHAR(490))                                    AS src_path_new,

        enquiry_id,
        CASE
            WHEN dir_query_login_mode = 3 THEN 1
            WHEN dir_query_login_mode = 1 THEN 2
            WHEN dir_query_login_mode = 2 THEN 3
            ELSE 0
        END                                                 AS cd_user_mode,

        -- city_tier: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN ref_url LIKE '%tyr"":""%' THEN SPLIT_PART(SPLIT_PART(ref_url, 'tyr"":""', 2), '|', 1)
                WHEN ref_url LIKE '%tyr:%'     THEN SPLIT_PART(SPLIT_PART(ref_url, 'tyr:', 2),    '|', 1)
                ELSE NULL
            END
        AS VARCHAR(490))                                    AS City_tier,

		CASE
		    WHEN ref_url LIKE '%biz:10%' THEN 1
		    WHEN ref_url LIKE '%biz:20%' THEN 2
		    WHEN ref_url LIKE '%biz:30%' THEN 3
		    WHEN ref_url LIKE '%biz:40%' THEN 4
		    WHEN ref_url LIKE '%biz=10%' THEN 1
		    WHEN ref_url LIKE '%biz=20%' THEN 2
		    WHEN ref_url LIKE '%biz=30%' THEN 3
		    WHEN ref_url LIKE '%biz=40%' THEN 4
		    ELSE 0
		END AS biz_type_filter_new,

        -- voice_search_language_new: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN (ref_url LIKE '%&src=vs%' AND ref_url LIKE '%&lang=%') THEN REGEXP_SUBSTR(ref_url, '&lang=([a-z]{2})', 1, 1, 'e')
                WHEN (ref_url LIKE '%&src_vs%' AND ref_url LIKE '%&lang:%') THEN REGEXP_SUBSTR(ref_url, '&lang:([a-z]{2})', 1, 1, 'e')
            END
        AS VARCHAR(490))                                    AS voice_search_language_new,

        -- kwd_type_new: CAST to VARCHAR(490)
        CAST(
            CASE WHEN LOWER(ref_url) LIKE '%ktp:%' THEN REGEXP_SUBSTR(ref_url, 'ktp:([^|/?&"]*)', 1, 1, 'e') END
        AS VARCHAR(490))                                    AS kwd_type_new,

        CAST(CASE WHEN ref_url LIKE '%stype:attr=1-br%' THEN 1 ELSE 0 END AS INT) AS attr_brand_new,
        CASE WHEN (ref_url LIKE '%pfen:1%' OR cd_additional_data LIKE '%pfen:1%') THEN 1 ELSE 0 END AS price_filter_enabled,
        CASE WHEN (ref_url LIKE '%pin:1%'  OR cd_additional_data LIKE '%pin:1%')  THEN 1 ELSE 0 END AS price_intent,
        CASE WHEN (ref_url LIKE '%csp:1%'  OR cd_additional_data LIKE '%csp:1%')  THEN 1 ELSE 0 END AS custom_price,

        -- min_price, max_price, result_count: CAST to VARCHAR(490)
        CAST(COALESCE(REGEXP_SUBSTR(ref_url,'minp:([^|]+)',1,1,'e'), REGEXP_SUBSTR(cd_additional_data,'minp:([^|]+)',1,1,'e')) AS VARCHAR(490)) AS min_price,
        CAST(COALESCE(REGEXP_SUBSTR(ref_url,'maxp:([^|]+)',1,1,'e'), REGEXP_SUBSTR(cd_additional_data,'maxp:([^|]+)',1,1,'e')) AS VARCHAR(490)) AS max_price,
        CAST(COALESCE(REGEXP_SUBSTR(ref_url,'res:RC1-R([^|]+)',1,1,'e'), REGEXP_SUBSTR(cd_additional_data,'res:RC1-R([^|]+)',1,1,'e')) AS VARCHAR(490)) AS result_count,

        user_id,
        CASE WHEN ref_url LIKE '%&prdsrc=1%'    THEN 1 ELSE 0 END                                  AS prdsrc,
        CASE WHEN ref_url LIKE '%no_sugg=1%' OR ref_url LIKE '%no_sugg=true%' THEN 1 ELSE 0 END    AS no_sugg,
        CASE WHEN ref_url LIKE '%qu:to%'        THEN 1 ELSE 0 END                                  AS qu_to,
        CASE WHEN ref_url LIKE '%qu_comp=to%' OR ref_url LIKE '%qu-comp:to%'   THEN 1 ELSE 0 END   AS qu_comp,
        CASE WHEN ref_url LIKE '%list_vw=1%'    THEN 1 ELSE 0 END                                  AS list_vw,
        CASE WHEN ref_url LIKE '%category:to%'      THEN 1 ELSE 0 END                                  AS category_to,
        CASE WHEN ref_url LIKE '%qu-attr:to%'   THEN 1 ELSE 0 END                                  AS qu_attr_to,
        CASE
            WHEN LOWER(ref_url) LIKE '%com-cf:hh%' THEN 1
            WHEN LOWER(ref_url) LIKE '%com-cf:lw%' THEN 2
            WHEN LOWER(ref_url) LIKE '%com-cf:nl%' THEN 3
            ELSE 0
        END                                                 AS compass_confidence,
		CASE
		    WHEN LOWER(ref_url) LIKE '%mtp:brn%' THEN 1
		    WHEN LOWER(ref_url) LIKE '%mtp:sp%'  THEN 2 
		    WHEN LOWER(ref_url) LIKE '%mtp:g%'   THEN 3  
		    WHEN LOWER(ref_url) LIKE '%mtp:s%'   THEN 4
		    WHEN LOWER(ref_url) LIKE '%mtp=brn%' THEN 1
		    WHEN LOWER(ref_url) LIKE '%mtp=sp%'  THEN 2 
		    WHEN LOWER(ref_url) LIKE '%mtp=g%'   THEN 3  
		    WHEN LOWER(ref_url) LIKE '%mtp=s%'   THEN 4
		    ELSE 0
		END AS category_type,
        distance_city,
        CASE WHEN ref_url LIKE '%spec-filter%' THEN 1 ELSE 0 END                                   AS is_spec_filter_available,
        -- firstValue, second_value: CAST to VARCHAR(190)
        CAST(SPLIT_PART(REGEXP_SUBSTR(ref_url, 'spec-filter_[1-8]_[1-8]'), '_', 2) AS VARCHAR(190)) AS firstValue,
        CAST(SPLIT_PART(REGEXP_SUBSTR(ref_url, 'spec-filter_[1-8]_[1-8]'), '_', 3) AS VARCHAR(190)) AS second_value,
        CASE WHEN ref_url LIKE '%grpfl%' THEN 1 ELSE 0 END                                         AS is_group_filter_available,
        CAST(CASE WHEN LOWER(ref_url) LIKE '%flavl:%' THEN REGEXP_SUBSTR(ref_url, 'flavl:([0-9]+)', 1, 1, 'e') END AS TEXT) AS flavl_No_of_Filters_displayed,
        CAST(CASE WHEN LOWER(ref_url) LIKE '%spcfl:%' THEN REGEXP_SUBSTR(ref_url, 'spcfl:([0-9]+)', 1, 1, 'e') END AS TEXT) AS spcfl_No_of_times_clicked,
        CAST(CASE WHEN LOWER(ref_url) LIKE '%sprs:%'  THEN REGEXP_SUBSTR(ref_url, 'sprs:([0-9]+)',  1, 1, 'e') END AS TEXT) AS sprs_No_of_results_shown,
        CAST(CASE WHEN LOWER(ref_url) LIKE '%src=spec-filter%' THEN REGEXP_SUBSTR(ref_url, 'src=spec-filter\_([0-9]+)', 1, 1, 'e') END AS TEXT) AS Position_of_Filter_Clicked,
        -- spec_filtername, spec_filtervalue: CAST to VARCHAR(190)
        CAST(CASE WHEN LOWER(ref_url) LIKE '%spc-%' THEN REPLACE(REGEXP_SUBSTR(ref_url, 'spc-([^=:&]+)',           1, 1, 'e'), '+', ' ') END AS VARCHAR(190)) AS spec_filtername,
        CAST(CASE WHEN LOWER(ref_url) LIKE '%spc-%' THEN REPLACE(REGEXP_SUBSTR(ref_url, 'spc-[^:=|&]+[:=]([^|&]+)',1, 1, 'e'), '+', ' ') END AS VARCHAR(190)) AS spec_filtervalue,
        CAST(CASE WHEN LOWER(ref_url) LIKE '%spc-%' THEN 1 ELSE 0 END AS int)                     AS popular_filter_clicked,
        -- Redirection: CAST to VARCHAR(190)
        CAST(CASE WHEN ref_url LIKE '%crs=%' THEN REGEXP_SUBSTR(ref_url, 'crs=([^|&]+)', 1, 1, 'e') END AS VARCHAR(190)) AS Redirection,
        CASE WHEN ref_url LIKE '%pf=tb%' THEN 1 WHEN ref_url LIKE '%pf=sb%' THEN 2 WHEN ref_url LIKE '%pf=lb%' THEN 3 ELSE 0 END AS Price_filter_position,
        CASE WHEN ref_url LIKE '%src=category%' OR ref_url LIKE '%Frm_M%' THEN 1 ELSE 2 END            AS Search_redirection,
        -- Price_bucket: CAST to VARCHAR(190)
        CAST(CASE WHEN ref_url LIKE '%pbs:%' OR cd_additional_data LIKE '%pbs:%' THEN REGEXP_SUBSTR(ref_url, 'pbs:([^|]+)', 1, 1, 'e') END AS VARCHAR(190)) AS Price_bucket,
        -- Exact_match_present: CAST to VARCHAR(190)
        CAST(
            CASE
                WHEN cd_additional_data LIKE '%emt:%' THEN REGEXP_SUBSTR(cd_additional_data, 'emt:([^|]+)', 1, 1, 'e')
                WHEN cd_additional_data LIKE '%em:1%' THEN 'Exact Match'
                WHEN cd_additional_data LIKE '%em:0%' THEN 'Not Exact Match'
            END
        AS VARCHAR(190))                                    AS Exact_match_present,
        CASE WHEN cd_additional_data LIKE '%lf:%' OR ref_url LIKE '%lf:%' THEN 1 ELSE 0 END        AS Locality_Filter_Present,
        CASE WHEN (cd_additional_data LIKE '%lc:%' AND cd_additional_data LIKE '%lp:%') OR (ref_url LIKE '%lc:%' AND ref_url LIKE '%lp:%') THEN 1 ELSE 0 END AS Locality_Filter_Clicked,
        CASE WHEN ref_url LIKE '%|at:1%' THEN 1 WHEN ref_url LIKE '%|at:2%' THEN 2 WHEN ref_url LIKE '%|at:3%' THEN 3 ELSE 0 END AS Annual_gst_turnover,
        CASE WHEN ref_url LIKE '%|d:1%'  THEN 1 WHEN ref_url LIKE '%|d:2%'  THEN 2 WHEN ref_url LIKE '%|d:3%'  THEN 3 ELSE 0 END AS gst_registration_date,
        enq_type

    FROM (
        SELECT
            date, modid, country, enquiry_id, query_reference_url,
            dir_query_categoryid, cd_additional_data, dir_query_login_mode,
            user_id, distance_city, enq_type, ref_url
        FROM staging.stg_dir_query
        WHERE
            (   (modid = 'DIR'  AND UPPER(cd_additional_data) LIKE '%PT=SEARCH%')
             OR (modid = 'IMOB' AND UPPER(cd_additional_data) LIKE '%IMOB_SEARCH%')
             OR ((modid IN ('ANDROID','ANDWEB') AND UPPER(cd_additional_data) LIKE '%ANDROID-SEARCH%') OR UPPER(cd_additional_data) LIKE '%IMOB_SEARCH%')
             OR (modid IN ('IOS','IOSWEB') AND (cd_additional_data = 'Search Products' OR UPPER(cd_additional_data) LIKE '%IOS-SEARCH-PRODUCTS%' OR UPPER(cd_additional_data) LIKE '%IMOB_SEARCH%'))
            )
    )

    UNION ALL

    -- ── status = 4 : calls ────────────────────────────────────────────────────
    SELECT
        4                                                   AS status,
        CASE
            WHEN modid IN ('IMOB')             THEN 'Mobile'
            WHEN modid IN ('DIR')              THEN 'Desktop'
            WHEN modid IN ('ANDROID','ANDWEB') THEN 'Android'
            WHEN modid IN ('IOS','IOSWEB')     THEN 'IOS'
        END                                                 AS modid,
        CASE WHEN c2c_caller_country_iso = 'IN' THEN 1 ELSE 2 END AS country,
        CAST(NULLIF(REGEXP_SUBSTR(cd_additional_data, 'pos=(\\d+)', 1, 1, 'e'), '') AS TEXT) AS Positions,
        CASE
            WHEN ref_url ~ 'stype:slrtxt-graph' THEN 1
            WHEN ref_url ~ 'stype:graph-cse'    THEN 2
            WHEN ref_url ~ 'stype:graph'        THEN 3
            ELSE 0
        END                                                 AS page_type,
        CAST(CASE WHEN ref_url LIKE '%attr=1%' THEN 1 WHEN ref_url LIKE '%attr:1%' THEN 1 ELSE 0 END AS INTEGER) AS attr_srch_new,
        CASE
            WHEN ref_url LIKE '%&qry_typ=C%'     THEN 2  WHEN ref_url LIKE '%qry_typ:C%'     THEN 2  WHEN ref_url LIKE '%qry_typ"":""C%' THEN 2
            WHEN ref_url LIKE '%&qry_typ=P%'     THEN 1  WHEN ref_url LIKE '%qry_typ:P%'     THEN 1  WHEN ref_url LIKE '%qry_typ"":""P%' THEN 1
            WHEN ref_url LIKE '%&qry_typ=S%'     THEN 3  WHEN ref_url LIKE '%qry_typ:S%'     THEN 3  WHEN ref_url LIKE '%qry_typ"":""S%' THEN 3
        END                                                 AS Query_type_new,
        CAST(CASE WHEN ref_url LIKE '%qu-cx=1%' THEN 1 WHEN ref_url LIKE '%qcr:1%'    THEN 1 WHEN ref_url LIKE '%qcr"":""1%' THEN 1 ELSE NULL END AS INT) AS qu_cx,
        CAST(CASE WHEN ref_url LIKE '%qu-tr=1%' THEN 1 WHEN ref_url LIKE '%qtr:1%'    THEN 1 WHEN ref_url LIKE '%qtr"":""1%' THEN 1 ELSE NULL END AS INT) AS qu_tr,
        CAST(
            CASE
                WHEN ref_url LIKE '%mc:%'
                 AND SPLIT_PART(SPLIT_PART(ref_url, 'mc:', 2), '|', 1) ~ '^[0-9]+$'
                    THEN SPLIT_PART(SPLIT_PART(ref_url, 'mc:', 2), '|', 1)
                ELSE NULL
            END AS INTEGER
        )                                                   AS category_id,

        -- search_url_city: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN (ref_url LIKE '%cq=%'  AND ref_url NOT LIKE '%cq=all&%')                            THEN REPLACE(REGEXP_SUBSTR(ref_url,'&cq=([^&|#?]+)', 1, 1, 'e'),'+', ' ')
                WHEN ((ref_url LIKE '%&cq=&%' OR ref_url LIKE '%cq=all&%')   AND ref_url NOT LIKE '%cq=%') THEN 'All India'
                WHEN (ref_url LIKE '%cq:%'  AND ref_url NOT LIKE '%cq:all%')                              THEN REPLACE(REGEXP_SUBSTR(ref_url,'cq:([^&|#?]+)',  1, 1, 'e'),'+', ' ')
                WHEN ((ref_url LIKE '%=cq:|%' OR ref_url LIKE '%cq:all%')    AND ref_url NOT LIKE '%cq:%') THEN 'All India'
                ELSE 'All India'
            END
        AS VARCHAR(490))                                    AS search_url_city,

        -- keyword_new: CAST to VARCHAR(990)
        CAST(
            CASE
                WHEN ref_url LIKE '%?s=%'  THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?s=([^&]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%?ss=%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?ss=([^&]+)', 1, 1, 'e')
                WHEN ref_url LIKE '%ss =%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), 'ss =([^&|]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%ss%'   THEN REGEXP_SUBSTR(ref_url, 'ss[:=]([^-]+)', 1, 1, 'e')
                WHEN cd_additional_data = 'Search' THEN
                    CASE WHEN POSITION('|' IN ref_url) > 0 THEN LEFT(ref_url, POSITION('|' IN ref_url) - 1) ELSE ref_url END
            END
        AS VARCHAR(990))                                    AS keyword_new,

        -- kwd_word_count derived inline from already-cast keyword expression
        LENGTH(TRIM(CAST(
            CASE
                WHEN ref_url LIKE '%?s=%'  THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?s=([^&]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%?ss=%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?ss=([^&]+)', 1, 1, 'e')
                WHEN ref_url LIKE '%ss =%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), 'ss =([^&|]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%ss%'   THEN REGEXP_SUBSTR(ref_url, 'ss[:=]([^-]+)', 1, 1, 'e')
                WHEN cd_additional_data = 'Search' THEN
                    CASE WHEN POSITION('|' IN ref_url) > 0 THEN LEFT(ref_url, POSITION('|' IN ref_url) - 1) ELSE ref_url END
            END
        AS VARCHAR(990)))) -
        LENGTH(REPLACE(TRIM(CAST(
            CASE
                WHEN ref_url LIKE '%?s=%'  THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?s=([^&]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%?ss=%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?ss=([^&]+)', 1, 1, 'e')
                WHEN ref_url LIKE '%ss =%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), 'ss =([^&|]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%ss%'   THEN REGEXP_SUBSTR(ref_url, 'ss[:=]([^-]+)', 1, 1, 'e')
                WHEN cd_additional_data = 'Search' THEN
                    CASE WHEN POSITION('|' IN ref_url) > 0 THEN LEFT(ref_url, POSITION('|' IN ref_url) - 1) ELSE ref_url END
            END
        AS VARCHAR(990))), ' ', '')) + 1                    AS kwd_word_count,

        -- url_resultcount_new: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN ref_url LIKE '%res=RC%'     THEN REGEXP_SUBSTR(ref_url, '&res=RC(\\d+)',    1, 1, 'e')
                WHEN ref_url LIKE '%res:RC%'     THEN REGEXP_SUBSTR(ref_url, 'res:RC(\\d+)',     1, 1, 'e')
                WHEN ref_url LIKE '%res"":""RC%' THEN REGEXP_SUBSTR(ref_url, 'res"":""RC(\\d+)', 1, 1, 'e')
                WHEN ref_url LIKE '%res=RC%'     THEN REGEXP_SUBSTR(ref_url, 'res=RC([^-|]*)',   1, 1, 'e')
                WHEN ref_url LIKE '%res:RC%'     THEN REGEXP_SUBSTR(ref_url, 'res:RC([^-|]*)',   1, 1, 'e')
            END
        AS VARCHAR(490))                                    AS url_resultcount_new,

        -- src_path_new: CAST to VARCHAR(490) — enum-style so values are short, but guard added
        CAST(
            CASE
                WHEN ref_url LIKE '%src=as-popular%'       THEN 'as-popular'
                WHEN ref_url LIKE '%src=as-rcnt%'          THEN 'as-rcnt'
                WHEN ref_url LIKE '%src=as-default%'       THEN 'as-default'
                WHEN ref_url LIKE '%src=as-comp%'          THEN 'as-comp'
                WHEN ref_url LIKE '%src=as-incity%'        THEN 'as-incity'
                WHEN ref_url LIKE '%src=as-kwd%'           THEN 'as-kwd'
                WHEN ref_url LIKE '%&src=vs%'              THEN 'vs'
                WHEN ref_url LIKE '%src=as-blrcnt%'        THEN 'as-blrcnt'
                WHEN ref_url LIKE '%src=advanced-filter%'  THEN 'advanced-filter'
                WHEN ref_url LIKE '%src=as-context%'       THEN 'as-context'
                WHEN ref_url LIKE '%src=category-rlt-srch%'    THEN 'category-rlt-srch'
                WHEN ref_url LIKE '%src=rcv%'              THEN 'rcv'
                WHEN ref_url LIKE '%src=adv-srch%'         THEN 'adv-srch'
                WHEN ref_url LIKE '%as-selcnxt%'           THEN 'as-selcnxt'
                WHEN ref_url LIKE '%as-rcmnd%'             THEN 'as-rcmnd'
            END
        AS VARCHAR(490))                                    AS src_path_new,

        c2c_record_id,
        login_mode,

        -- city_tier: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN ref_url LIKE '%tyr"":""%' THEN SPLIT_PART(SPLIT_PART(ref_url, 'tyr"":""', 2), '|', 1)
                WHEN ref_url LIKE '%tyr:%'     THEN SPLIT_PART(SPLIT_PART(ref_url, 'tyr:', 2),    '|', 1)
                ELSE NULL
            END
        AS VARCHAR(490))                                    AS City_tier,

		CASE
		    WHEN ref_url LIKE '%biz:10%' THEN 1
		    WHEN ref_url LIKE '%biz:20%' THEN 2
		    WHEN ref_url LIKE '%biz:30%' THEN 3
		    WHEN ref_url LIKE '%biz:40%' THEN 4
		    WHEN ref_url LIKE '%biz=10%' THEN 1
		    WHEN ref_url LIKE '%biz=20%' THEN 2
		    WHEN ref_url LIKE '%biz=30%' THEN 3
		    WHEN ref_url LIKE '%biz=40%' THEN 4
		    ELSE 0
		END AS biz_type_filter_new,

        -- voice_search_language_new: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN (ref_url LIKE '%&src=vs%' AND ref_url LIKE '%&lang=%') THEN REGEXP_SUBSTR(ref_url, '&lang=([a-z]{2})', 1, 1, 'e')
                WHEN (ref_url LIKE '%&src_vs%' AND ref_url LIKE '%&lang:%') THEN REGEXP_SUBSTR(ref_url, '&lang:([a-z]{2})', 1, 1, 'e')
            END
        AS VARCHAR(490))                                    AS voice_search_language_new,

        -- kwd_type_new: CAST to VARCHAR(490)
        CAST(
            CASE WHEN LOWER(ref_url) LIKE '%ktp:%' THEN REGEXP_SUBSTR(ref_url, 'ktp:([^|/?&"]*)', 1, 1, 'e') END
        AS VARCHAR(490))                                    AS kwd_type_new,

        CAST(CASE WHEN ref_url LIKE '%stype:attr=1-br%' THEN 1 ELSE 0 END AS INT) AS attr_brand_new,
        CASE WHEN (ref_url LIKE '%pfen:1%' OR cd_additional_data LIKE '%pfen:1%') THEN 1 ELSE 0 END AS price_filter_enabled,
        CASE WHEN (ref_url LIKE '%pin:1%'  OR cd_additional_data LIKE '%pin:1%')  THEN 1 ELSE 0 END AS price_intent,
        CASE WHEN (ref_url LIKE '%csp:1%'  OR cd_additional_data LIKE '%csp:1%')  THEN 1 ELSE 0 END AS custom_price,

        -- min_price, max_price, result_count: CAST to VARCHAR(490)
        CAST(COALESCE(REGEXP_SUBSTR(ref_url,'minp:([^|]+)',1,1,'e'), REGEXP_SUBSTR(cd_additional_data,'minp:([^|]+)',1,1,'e')) AS VARCHAR(490)) AS min_price,
        CAST(COALESCE(REGEXP_SUBSTR(ref_url,'maxp:([^|]+)',1,1,'e'), REGEXP_SUBSTR(cd_additional_data,'maxp:([^|]+)',1,1,'e')) AS VARCHAR(490)) AS max_price,
        CAST(COALESCE(REGEXP_SUBSTR(ref_url,'res:RC1-R([^|]+)',1,1,'e'), REGEXP_SUBSTR(cd_additional_data,'res:RC1-R([^|]+)',1,1,'e')) AS VARCHAR(490)) AS result_count,

        c2c_caller_glusr_id                                 AS user_id,
        CASE WHEN ref_url LIKE '%&prdsrc=1%'    THEN 1 ELSE 0 END                                  AS prdsrc,
        CASE WHEN ref_url LIKE '%no_sugg=1%' OR ref_url LIKE '%no_sugg=true%' THEN 1 ELSE 0 END    AS no_sugg,
        CASE WHEN ref_url LIKE '%qu:to%'        THEN 1 ELSE 0 END                                  AS qu_to,
        CASE WHEN ref_url LIKE '%qu_comp=to%' OR ref_url LIKE '%qu-comp:to%'   THEN 1 ELSE 0 END   AS qu_comp,
        CASE WHEN ref_url LIKE '%list_vw=1%'    THEN 1 ELSE 0 END                                  AS list_vw,
        CASE WHEN ref_url LIKE '%category:to%'      THEN 1 ELSE 0 END                                  AS category_to,
        CASE WHEN ref_url LIKE '%qu-attr:to%'   THEN 1 ELSE 0 END                                  AS qu_attr_to,
        CASE
            WHEN LOWER(ref_url) LIKE '%com-cf:hh%' THEN 1
            WHEN LOWER(ref_url) LIKE '%com-cf:lw%' THEN 2
            WHEN LOWER(ref_url) LIKE '%com-cf:nl%' THEN 3
            ELSE 0
        END                                                 AS compass_confidence,
		CASE
		    WHEN LOWER(ref_url) LIKE '%mtp:brn%' THEN 1
		    WHEN LOWER(ref_url) LIKE '%mtp:sp%'  THEN 2 
		    WHEN LOWER(ref_url) LIKE '%mtp:g%'   THEN 3  
		    WHEN LOWER(ref_url) LIKE '%mtp:s%'   THEN 4
		    WHEN LOWER(ref_url) LIKE '%mtp=brn%' THEN 1
		    WHEN LOWER(ref_url) LIKE '%mtp=sp%'  THEN 2 
		    WHEN LOWER(ref_url) LIKE '%mtp=g%'   THEN 3  
		    WHEN LOWER(ref_url) LIKE '%mtp=s%'   THEN 4
		    ELSE 0
		END AS category_type,
        distance_city,
        CASE WHEN ref_url LIKE '%spec-filter%' THEN 1 ELSE 0 END                                   AS is_spec_filter_available,
        CAST(SPLIT_PART(REGEXP_SUBSTR(ref_url, 'spec-filter_[1-8]_[1-8]'), '_', 2) AS VARCHAR(190)) AS firstValue,
        CAST(SPLIT_PART(REGEXP_SUBSTR(ref_url, 'spec-filter_[1-8]_[1-8]'), '_', 3) AS VARCHAR(190)) AS second_value,
        CASE WHEN ref_url LIKE '%grpfl%' THEN 1 ELSE 0 END                                         AS is_group_filter_available,
        CAST(CASE WHEN LOWER(ref_url) LIKE '%flavl:%' THEN REGEXP_SUBSTR(ref_url, 'flavl:([0-9]+)', 1, 1, 'e') END AS TEXT) AS flavl_No_of_Filters_displayed,
        CAST(CASE WHEN LOWER(ref_url) LIKE '%spcfl:%' THEN REGEXP_SUBSTR(ref_url, 'spcfl:([0-9]+)', 1, 1, 'e') END AS TEXT) AS spcfl_No_of_times_clicked,
        CAST(CASE WHEN LOWER(ref_url) LIKE '%sprs:%'  THEN REGEXP_SUBSTR(ref_url, 'sprs:([0-9]+)',  1, 1, 'e') END AS TEXT) AS sprs_No_of_results_shown,
        CAST(CASE WHEN LOWER(ref_url) LIKE '%src=spec-filter%' THEN REGEXP_SUBSTR(ref_url, 'src=spec-filter\_([0-9]+)', 1, 1, 'e') END AS TEXT) AS Position_of_Filter_Clicked,
        CAST(CASE WHEN LOWER(ref_url) LIKE '%spc-%' THEN REPLACE(REGEXP_SUBSTR(ref_url, 'spc-([^=:&]+)',            1, 1, 'e'), '+', ' ') END AS VARCHAR(190)) AS spec_filtername,
        CAST(CASE WHEN LOWER(ref_url) LIKE '%spc-%' THEN REPLACE(REGEXP_SUBSTR(ref_url, 'spc-[^:=|&]+[:=]([^|&]+)',1, 1, 'e'), '+', ' ') END AS VARCHAR(190)) AS spec_filtervalue,
        CAST(CASE WHEN LOWER(ref_url) LIKE '%spc-%' THEN 1 ELSE 0 END AS int)                     AS popular_filter_clicked,
        CAST(CASE WHEN ref_url LIKE '%crs=%' THEN REGEXP_SUBSTR(ref_url, 'crs=([^|&]+)', 1, 1, 'e') END AS VARCHAR(190)) AS Redirection,
        CASE WHEN ref_url LIKE '%pf=tb%' THEN 1 WHEN ref_url LIKE '%pf=sb%' THEN 2 WHEN ref_url LIKE '%pf=lb%' THEN 3 ELSE 0 END AS Price_filter_position,
        CASE WHEN ref_url LIKE '%src=category%' OR ref_url LIKE '%Frm_M%' THEN 1 ELSE 2 END            AS Search_redirection,
        CAST(CASE WHEN ref_url LIKE '%pbs:%' OR cd_additional_data LIKE '%pbs:%' THEN REGEXP_SUBSTR(ref_url, 'pbs:([^|]+)', 1, 1, 'e') END AS VARCHAR(190)) AS Price_bucket,
        CAST(
            CASE
                WHEN cd_additional_data LIKE '%emt:%' THEN REGEXP_SUBSTR(cd_additional_data, 'emt:([^|]+)', 1, 1, 'e')
                WHEN cd_additional_data LIKE '%em:1%' THEN 'Exact Match'
                WHEN cd_additional_data LIKE '%em:0%' THEN 'Not Exact Match'
            END
        AS VARCHAR(190))                                    AS Exact_match_present,
        CASE WHEN cd_additional_data LIKE '%lf:%' OR ref_url LIKE '%lf:%' THEN 1 ELSE 0 END        AS Locality_Filter_Present,
        CASE WHEN (cd_additional_data LIKE '%lc:%' AND cd_additional_data LIKE '%lp:%') OR (ref_url LIKE '%lc:%' AND ref_url LIKE '%lp:%') THEN 1 ELSE 0 END AS Locality_Filter_Clicked,
        CASE WHEN ref_url LIKE '%|at:1%' THEN 1 WHEN ref_url LIKE '%|at:2%' THEN 2 WHEN ref_url LIKE '%|at:3%' THEN 3 ELSE 0 END AS Annual_gst_turnover,
        CASE WHEN ref_url LIKE '%|d:1%'  THEN 1 WHEN ref_url LIKE '%|d:2%'  THEN 2 WHEN ref_url LIKE '%|d:3%'  THEN 3 ELSE 0 END AS gst_registration_date,
        NULL                                                AS enq_type

    FROM (
        SELECT
            login_mode, c2c_call_time, c2c_caller_country_iso, cd_additional_data,
            modid, c2c_caller_glusr_id, c2c_record_id,
            c2c_caller_city_id, c2c_receiver_city_id, ref_url
        FROM staging.stg_c2c_records
        WHERE
            (modid = 'IMOB' AND fk_c2c_record_unidentified_id IS NULL AND UPPER(cd_additional_data) LIKE '%IMOB_SEARCH%' AND ref_url NOT LIKE '%/messages/%')
         OR (modid IN ('ANDROID','ANDWEB') AND cd_additional_data LIKE 'Search%' OR UPPER(cd_additional_data) LIKE '%IMOB_SEARCH%')
         OR (modid IN ('IOS','IOSWEB') AND c2c_record_type = 1 AND cd_additional_data LIKE 'Search' AND UPPER(cd_additional_data) NOT LIKE '%BL-SEARCH%' OR UPPER(cd_additional_data) LIKE '%IMOB_SEARCH%')
    ) a
    LEFT JOIN (
        SELECT from_city_id, to_city_id, distance_city
        FROM dwh.fact_gl_city_distance
    ) b ON a.c2c_caller_city_id = b.from_city_id
       AND a.c2c_receiver_city_id = b.to_city_id
)

GROUP BY
    status, modid, keyword_new, kwd_word_count, country, category_id,
    search_url_city, qu_cx, qu_tr, biz_type_filter_new,
    url_resultcount_new, voice_search_language_new, kwd_type_new,
    attr_srch_new, attr_brand_new, query_type_new, src_path_new, enquiry_click_type,
    city_only, ecom_filter, rcmnd_srch, cd_user_mode, positions, page_type,
    special_srchs, city_tier, price_filter_enabled, price_intent, custom_price,
    min_price, max_price, result_Count, user_id,
    prdsrc, no_sugg, qu_to, qu_comp, list_vw, category_to, qu_attr_to,
    compass_confidence, category_type, distance_city,
    is_spec_filter_available, firstValue, second_value, is_group_filter_available,
    flavl_No_of_Filters_displayed, spcfl_No_of_times_clicked, sprs_No_of_results_shown,
    Position_of_Filter_Clicked, spec_filtername, spec_filtervalue, popular_filter_clicked,
    Redirection, Price_filter_position, Search_redirection, Price_bucket,
    Exact_match_present, Locality_Filter_Present, Locality_Filter_Clicked,
    Annual_gst_turnover, gst_registration_date, enq_type
;
else

---------------------------------------------------------
---------------------------------------------------------
Delete from staging.stg_dir_query;
insert into staging.stg_dir_query 
SELECT
	date(date_r) as date, query_modid as modid, sender_country_iso as country, enquiry_id,
	left(query_reference_url,4000) query_reference_url, dir_query_categoryid, query_reference_text AS cd_additional_data,
	dir_query_login_mode, user_id, distance_city, 1 as enq_type,
	left(replace(replace(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(replace(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(query_reference_url, '%21', '!'), '%23', '#'), '%24', '$'), '%26', '&'), '%28', '('), '%29', ')'), '%2A', '*'), '%2B', '+'), '%2C', ','), '%20', ' '), '%3D', '='), '%3F', '?'), '%25', '%'), '%2F', '/'), '%5C', '\\'), '%3A', ':'), '%3B', ';'), '%7C', '|'), '%3D', '='), '%20', ' '), '%22', '"'), '%20', ' '), '%2B', '+'),4000) AS ref_url
FROM dwh.fact_dir_query
WHERE date(date_r) BETWEEN st_dt and en_dt;
---------------------------------------------------------
---------------------------------------------------------
Delete from staging.stg_c2c_records;
insert into staging.stg_c2c_records 
SELECT
	2 as login_mode, c2c_call_time, c2c_caller_country_iso, left(c2c_page_type,4000) AS cd_additional_data,
	C2C_MODID AS modid, c2c_caller_glusr_id, c2c_record_id, c2c_caller_city_id, c2c_receiver_city_id,
	left(replace(replace(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(replace(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(c2c_referer_url,'%21', '!'),'%23', '#'),'%24', '$'),'%26', '"&"'),'%28','('),'%29', ')'),'%2A', '*'),'%2B', '+'),'%2C',','),'%20',' '),'%3D','='),'%3F','?'),'%25','%'),'%2F','/'),'%5C','"\"'),'%3A',':'),'%3B',';'),'%7C','|'),'%3D','='),'%20',' '),'%22','"""'),'%20',' '),'%2B','+'),4000) AS ref_url,
	c2c_record_type,
	fk_c2c_record_unidentified_id
	FROM dwh.fact_c2c_records
WHERE trunc(date(C2C_CALL_TIME)) BETWEEN st_dt and en_dt
and fk_c2c_record_unidentified_id is null
UNION ALL
SELECT
	1 as login_mode, c2c_call_time, c2c_caller_country_iso, left(c2c_page_type,4000) AS cd_additional_data,
	C2C_MODID AS modid, c2c_caller_glusr_id, c2c_record_id, c2c_caller_city_id, c2c_receiver_city_id,
	left(replace(replace(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(replace(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(c2c_referer_url,'%21', '!'),'%23', '#'),'%24', '$'),'%26', '"&"'),'%28','('),'%29', ')'),'%2A', '*'),'%2B', '+'),'%2C',','),'%20',' '),'%3D','='),'%3F','?'),'%25','%'),'%2F','/'),'%5C','"\"'),'%3A',':'),'%3B',';'),'%7C','|'),'%3D','='),'%20',' '),'%22','"""'),'%20',' '),'%2B','+'),4000) AS ref_url,
	c2c_record_type,
	fk_c2c_record_unidentified_id
FROM dwh.fact_c2c_records_unidentified
WHERE trunc(date(C2C_CALL_TIME)) BETWEEN st_dt and en_dt;


-------------------------------------staging.search_web_data -------------------

DELETE FROM staging.search_web_data;
INSERT INTO staging.search_web_data
SELECT
    DATE,
    1 AS status, modid,
    LEFT(trim(lower(keyword_new)), 990)                     AS keyword_new,
    kwd_word_count, country, unified_categoryid                  AS category_id,
    LEFT(search_url_city, 490)                              AS search_url_city,
    qu_cx, qu_tr, biz_type_filter_new,
    LEFT(url_resultcount_new, 490)                          AS url_resultcount_new,
    LEFT(voice_search_language_new, 490)                    AS voice_search_language_new,
    LEFT(kwd_type_new, 490)                                 AS kwd_type_new,
    attr_srch_new, attr_brand_new, query_type_new,
    LEFT(src_path_new, 490)                                 AS src_path_new,
    LEFT(enquiry_click_type, 490)                           AS enquiry_click_type,
    city_only, ecom_filter, rcmnd_srch, cd_user_mode,
    CAST(
        CASE WHEN positions ~ '^[0-9]+(\.[0-9]+)?$' THEN positions ELSE NULL END
    AS DECIMAL(10,5))                                       AS positions,
    SUM(pageviews)                                          AS pageviews,
    SUM(pdp_clicks)                                         AS pdp_clicks,
    SUM(event_catalogue_clicks)                             AS event_catalogue_clicks,
    SUM(enquiry_cta_clicks)                                 AS enquiry_cta_clicks,
    0                                                       AS enquiries,
    0                                                       AS calls,
    page_type,
    LEFT(special_srchs, 245)                                AS special_srchs,
    LEFT(city_tier, 490)                                    AS city_tier,
    price_filter_enabled, price_intent, custom_price,
    LEFT(min_price, 490)                                    AS min_price,
    LEFT(max_price, 490)                                    AS max_price,
    LEFT(user_pseudo_id, 490)                               AS user_pseudo_id,
    LEFT(result_count, 490)                                 AS result_count,
    0                                                       AS user_id

FROM (
    SELECT
        DATE(search_web_date)                               AS DATE,
        CAST(modid AS VARCHAR(50))                          AS modid,
        CASE
            WHEN page_location ~ 'stype:slrtxt-graph'       THEN 1
            WHEN page_location ~ 'stype:graph-cse'          THEN 2
            WHEN page_location ~ 'stype:graph'              THEN 3
            ELSE 0
        END                                                 AS page_type,
        CAST(
            CASE
                WHEN (pageviews IS NULL OR pageviews = 0)
                 AND (   (pdp_clicks            IS NOT NULL AND pdp_clicks            > 0)
                      OR (event_catalogue_clicks IS NOT NULL AND event_catalogue_clicks > 0)
                      OR (enquiry_cta_clicks     IS NOT NULL AND enquiry_cta_clicks    > 0))
                 AND cd_add LIKE '%Pos=%'
                    THEN REGEXP_SUBSTR(cd_add, 'Pos=([0-9]+)', 1, 1, 'e')
                ELSE CAST(position AS TEXT)
            END AS TEXT
        )                                                   AS positions,
        CAST(user_pseudo_id AS VARCHAR(490))                AS user_pseudo_id,
        CASE
            WHEN cd_user_mode_f = 'unidentified' THEN 1
            WHEN cd_user_mode_f = 'identified'   THEN 2
            WHEN cd_user_mode_f = 'full login'   THEN 3
            ELSE 0
        END                                                 AS cd_user_mode,

        -- keyword_new: CAST to VARCHAR(990) to enforce limit before GROUP BY
        CAST(
            CASE
                WHEN page_location LIKE '%isearch.php?s=%'
                    THEN REPLACE(REPLACE(REGEXP_SUBSTR(
                             REPLACE(REPLACE(REPLACE(page_location,'\+', ' '),'%20',' '),'\\?s=([^&]+)','e'),
                             '\\?s=([^&]+)'),'+', ' '),'?s=','')
                WHEN page_location LIKE '%search.mp?ss=%'
                    THEN REPLACE(REPLACE(REGEXP_SUBSTR(
                             REPLACE(REPLACE(REPLACE(page_location,'\+', ' '),'%20',' '),'\\?ss=([^&]+)','e'),
                             '\\?ss=([^&]+)'),'+', ' '),'?ss=','')
                WHEN page_location LIKE '%/proddetail/%'
                    THEN REPLACE(REPLACE(REPLACE(REGEXP_SUBSTR(page_location, 'kwd=([^&]+)'),'%20', ' '),'+', ' '),'kwd=','')
                ELSE LOWER(keyword)
            END
        AS VARCHAR(990))                                    AS keyword_new,

        LENGTH(TRIM(
            CAST(
                CASE
                    WHEN page_location LIKE '%isearch.php?s=%'
                        THEN REPLACE(REPLACE(REGEXP_SUBSTR(
                                 REPLACE(REPLACE(REPLACE(page_location,'\+', ' '),'%20',' '),'\\?s=([^&]+)','e'),
                                 '\\?s=([^&]+)'),'+', ' '),'?s=','')
                    WHEN page_location LIKE '%search.mp?ss=%'
                        THEN REPLACE(REPLACE(REGEXP_SUBSTR(
                                 REPLACE(REPLACE(REPLACE(page_location,'\+', ' '),'%20',' '),'\\?ss=([^&]+)','e'),
                                 '\\?ss=([^&]+)'),'+', ' '),'?ss=','')
                    WHEN page_location LIKE '%/proddetail/%'
                        THEN REPLACE(REPLACE(REPLACE(REGEXP_SUBSTR(page_location, 'kwd=([^&]+)'),'%20', ' '),'+', ' '),'kwd=','')
                    ELSE LOWER(keyword)
                END
            AS VARCHAR(990))
        )) - LENGTH(REPLACE(TRIM(
            CAST(
                CASE
                    WHEN page_location LIKE '%isearch.php?s=%'
                        THEN REPLACE(REPLACE(REGEXP_SUBSTR(
                                 REPLACE(REPLACE(REPLACE(page_location,'\+', ' '),'%20',' '),'\\?s=([^&]+)','e'),
                                 '\\?s=([^&]+)'),'+', ' '),'?s=','')
                    WHEN page_location LIKE '%search.mp?ss=%'
                        THEN REPLACE(REPLACE(REGEXP_SUBSTR(
                                 REPLACE(REPLACE(REPLACE(page_location,'\+', ' '),'%20',' '),'\\?ss=([^&]+)','e'),
                                 '\\?ss=([^&]+)'),'+', ' '),'?ss=','')
                    WHEN page_location LIKE '%/proddetail/%'
                        THEN REPLACE(REPLACE(REPLACE(REGEXP_SUBSTR(page_location, 'kwd=([^&]+)'),'%20', ' '),'+', ' '),'kwd=','')
                    ELSE LOWER(keyword)
                END
            AS VARCHAR(990))
        ), ' ', '')) + 1                                    AS kwd_word_count,

        CASE WHEN country = 'India' THEN 1 ELSE 2 END       AS country,
        CAST(CASE
            WHEN page_location LIKE '%mc:%'
             AND SPLIT_PART(SPLIT_PART(page_location, 'mc:', 2), '|', 1) ~ '^[0-9]+$'
                THEN CAST(SPLIT_PART(SPLIT_PART(page_location, 'mc:', 2), '|', 1) AS INTEGER)
            ELSE NULL
        END AS INTEGER)                                     AS Unified_categoryid,

        -- search_url_city: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN page_location LIKE '%cq:%' OR page_location LIKE '%cq"":""%'
                    THEN LEFT(REPLACE(REGEXP_SUBSTR(page_location,'cq:([^&|#?]+)', 1, 1, 'e'),'+', ' '), 490)
                WHEN page_location NOT LIKE '%cq:%' OR page_location NOT LIKE '%cq"":""%'
                    THEN 'All India'
            END
        AS VARCHAR(490))                                    AS search_url_city,

        CAST(CASE WHEN page_location LIKE '%qu-cx=1%'   THEN 1
                  WHEN page_location LIKE '%qcr:1%'     THEN 1
                  WHEN page_location LIKE '%qcr"":""1%' THEN 1 ELSE NULL END AS INT) AS qu_cx,
        CAST(CASE WHEN page_location LIKE '%qu-tr=1%'   THEN 1
                  WHEN page_location LIKE '%qtr:1%'     THEN 1
                  WHEN page_location LIKE '%qtr"":""1%' THEN 1 ELSE NULL END AS INT) AS qu_tr,
		CASE
		    WHEN page_location LIKE '%biz:10%' THEN 1
		    WHEN page_location LIKE '%biz:20%' THEN 2
		    WHEN page_location LIKE '%biz:30%' THEN 3
		    WHEN page_location LIKE '%biz:40%' THEN 4
		    WHEN page_location LIKE '%biz=10%' THEN 1
		    WHEN page_location LIKE '%biz=20%' THEN 2
		    WHEN page_location LIKE '%biz=30%' THEN 3
		    WHEN page_location LIKE '%biz=40%' THEN 4
		    ELSE 0
		END AS biz_type_filter_new,

        -- url_resultcount_new: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN page_location LIKE '%res=RC%'     THEN REGEXP_SUBSTR(page_location, 'res=RC([^-|]*)',    1, 1, 'e')
                WHEN page_location LIKE '%res:RC%'     THEN REGEXP_SUBSTR(page_location, 'res:RC([^-|]*)',    1, 1, 'e')
                WHEN page_location LIKE '%res"":""RC%' THEN REGEXP_SUBSTR(page_location, 'res"":""RC([^-|]*)',1, 1, 'e')
            END
        AS VARCHAR(490))                                    AS url_resultcount_new,

        -- voice_search_language_new: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN page_location LIKE '%src=vs%'  AND page_location LIKE '%lang=%'  THEN REGEXP_SUBSTR(page_location, 'lang=([a-z]{2})',  1,1,'e')
                WHEN page_location LIKE '%src_vs%'  AND page_location LIKE '%lang:%'  THEN REGEXP_SUBSTR(page_location, 'lang:([a-z]{2})',  1,1,'e')
                WHEN PDP_CLICKS > 0 AND page_referrer LIKE '%src=vs%'  AND page_location LIKE '%lang:%'  THEN REGEXP_SUBSTR(page_location, 'lang:([a-z]{2})',  1,1,'e')
                WHEN PDP_CLICKS > 0 AND page_referrer LIKE '%src_vs%'  AND page_location LIKE '%lang:%'  THEN REGEXP_SUBSTR(page_location, 'lang:([a-z]{2})',  1,1,'e')
                WHEN page_location LIKE '%&src=vs%' AND page_location LIKE '%&lang=%' THEN REGEXP_SUBSTR(page_location, '&lang=([a-z]{2})', 1,1,'e')
                WHEN page_referrer LIKE '%&src=vs%' AND page_location LIKE '%lang:%'  THEN REGEXP_SUBSTR(page_location, 'lang:([a-z]{2})',  1,1,'e')
                WHEN page_location LIKE '%&src_vs%' AND page_location LIKE '%&lang=%' THEN REGEXP_SUBSTR(page_location, '&lang=([a-z]{2})', 1,1,'e')
                WHEN page_referrer LIKE '%&src_vs%' AND page_location LIKE '%lang:%'  THEN REGEXP_SUBSTR(page_location, 'lang:([a-z]{2})',  1,1,'e')
            END
        AS VARCHAR(490))                                    AS voice_search_language_new,

        -- kwd_type_new: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN LOWER(page_location) LIKE '%|ktp:%'
                    THEN REGEXP_SUBSTR(page_location, '\\\\|ktp:([^|/?&"]*)', 1, 1, 'e')
                ELSE NULL
            END
        AS VARCHAR(490))                                    AS kwd_type_new,

        CASE
            WHEN keyword_new ~ '^[0-9]{2}[A-Za-z]{5}[0-9]{4}[A-Za-z]{1}[1-9A-Za-z]{1}[Zz]{1}[0-9A-Za-z]{1}$'
              OR keyword_new ~* '[0-9]{2}[A-Za-z]{5}[0-9]{4}[A-Za-z]{1}[1-9A-Za-z]{1}[Zz]{1}[0-9A-Za-z]{1}'
              OR UPPER(kwd_type_new) = 'OWA1' THEN 1
            ELSE 0
        END                                                 AS is_gst,

        CAST(CASE
            WHEN (pageviews > 0 OR EVENT_CATALOGUE_CLICKS > 0 OR Enquiry_cta_clicks > 0) AND page_location LIKE '%attr=1%' THEN 1
            WHEN (PDP_CLICKS > 0) AND page_location LIKE '%attr=1%' THEN 1
            ELSE attr_srch::INT
        END AS INTEGER)                                     AS attr_srch_new,

        CASE
            WHEN (pageviews > 0 OR EVENT_CATALOGUE_CLICKS > 0 OR Enquiry_cta_clicks > 0) AND page_location LIKE '%attr=1|br%' THEN 1
            WHEN (PDP_CLICKS > 0) AND page_location LIKE '%attr=1-br%' THEN 1
            ELSE attr_brand::INT
        END                                                 AS attr_brand_new,

        CASE
            WHEN page_location LIKE '%qry_typ:P%'     THEN 1  WHEN page_location LIKE '%qry_typ:C%'     THEN 2  WHEN page_location LIKE '%qry_typ:S%'     THEN 3
            WHEN page_location LIKE '%qry_typ"":""P%' THEN 1  WHEN page_location LIKE '%qry_typ"":""C%' THEN 2  WHEN page_location LIKE '%qry_typ"":""S%' THEN 3
            WHEN page_location LIKE '%qry_typ=P%'     THEN 1  WHEN page_location LIKE '%qry_typ=C%'     THEN 2  WHEN page_location LIKE '%qry_typ=S%'     THEN 3
            WHEN query_type = 'Product' THEN 1  WHEN query_type = 'company' THEN 2  WHEN query_type = 'service' THEN 3
        END                                                 AS query_type_new,

        -- src_path_new: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN (pageviews > 0 OR EVENT_CATALOGUE_CLICKS > 0 OR Enquiry_cta_clicks > 0)
                  AND LOWER(page_location) LIKE '%&src=%'
                    THEN REGEXP_SUBSTR(page_location, '&src=([^|&:%""]+)', 1, 1, 'e')
                WHEN (PDP_CLICKS > 0) AND LOWER(page_referrer) LIKE '%&src=%'
                    THEN REGEXP_SUBSTR(page_referrer,  '&src=([^|&%"":]+)', 1, 1, 'e')
            END
        AS VARCHAR(490))                                    AS src_path_new,

        CASE WHEN LOWER(page_location) LIKE '%city_only=true%' THEN 1 ELSE 0 END AS city_only,
        CASE WHEN LOWER(page_location) LIKE '%ecom_only=true%' THEN 1 ELSE 0 END AS ecom_filter,
        CASE WHEN LOWER(page_location) LIKE '%rdp=rs%'         THEN 1 ELSE 0 END AS rcmnd_srch,

        -- special_srchs: CAST to VARCHAR(245)
        CAST(
            CASE
                WHEN LOWER(keyword_new) ~ '\\b(what|where|when|why|how|who|whom)\\b' THEN 'wh_how'
                WHEN keyword_new ~ '^[\\d\\s]+$'                                      THEN 'dig_spaces'
                WHEN keyword_new ~ '[\\d]+'                                           THEN 'withnum'
                WHEN keyword_new ~ '^[0-9]{2}[A-Za-z]{5}[0-9]{4}[A-Za-z]{1}[1-9A-Za-z]{1}[Zz]{1}[0-9A-Za-z]{1}$'
                  OR keyword_new ~* '[0-9]{2}[A-Za-z]{5}[0-9]{4}[A-Za-z]{1}[1-9A-Za-z]{1}[Zz]{1}[0-9A-Za-z]{1}'
                  OR UPPER(kwd_type_new) = 'OWA1'                                     THEN 'gst_srchs'
                ELSE NULL
            END
        AS VARCHAR(245))                                    AS special_srchs,

        pageviews,
        pdp_clicks,
        event_catalogue_clicks,
        CAST(enquiry_click_type AS VARCHAR(490))            AS enquiry_click_type,
        enquiry_cta_clicks,

        -- city_tier: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN page_location LIKE '%tyr"":""%' THEN SPLIT_PART(SPLIT_PART(page_location, 'tyr"":""', 2), '|', 1)
                WHEN page_location LIKE '%tyr:%'     THEN SPLIT_PART(SPLIT_PART(page_location, 'tyr:', 2),    '|', 1)
                ELSE City_tier
            END
        AS VARCHAR(490))                                    AS City_tier,

        CASE WHEN page_location LIKE '%pfen:1%' THEN 1 ELSE 0 END AS price_filter_enabled,
        CASE WHEN page_location LIKE '%pin:1%'  THEN 1 ELSE 0 END AS price_intent,
        CASE WHEN page_location LIKE '%csp:1%'  THEN 1 ELSE 0 END AS custom_price,

        -- min_price, max_price, result_count: CAST to VARCHAR(490)
        CAST(REGEXP_SUBSTR(page_location, 'minp:([^|]+)',    1, 1, 'e') AS VARCHAR(490)) AS min_price,
        CAST(REGEXP_SUBSTR(page_location, 'maxp:([^|]+)',    1, 1, 'e') AS VARCHAR(490)) AS max_price,
        CAST(REGEXP_SUBSTR(page_location, 'res:RC1-R([^|]+)',1, 1, 'e') AS VARCHAR(490)) AS result_count

    FROM im_datamart_bigquery.fact_bigquery_search_web_data
    WHERE
        DATE(search_web_date) BETWEEN st_dt and en_dt
        AND (pageviews < 10000 AND pdp_clicks < 10000 AND event_catalogue_clicks < 10000)
        AND NOT (
            COALESCE(LOWER(page_location), '') LIKE '%andweb%'
            OR COALESCE(LOWER(eventlabel),   '') LIKE '%andweb%'
        )
        AND modid IN ('Mobile', 'Desktop')
)
GROUP BY
    DATE, status, modid, keyword_new, kwd_word_count, country, category_id,
    search_url_city, qu_cx, qu_tr, biz_type_filter_new,
    url_resultcount_new, voice_search_language_new, kwd_type_new,
    attr_srch_new, attr_brand_new, query_type_new, src_path_new, enquiry_click_type,
    city_only, ecom_filter, rcmnd_srch, cd_user_mode, positions, page_type,
    special_srchs, city_tier, price_filter_enabled, price_intent, custom_price,
    min_price, max_price, user_pseudo_id, result_Count

UNION ALL

SELECT
    DATE,
    2 AS status, modid,
    LEFT(trim(lower(keyword_new)), 990)                     AS keyword_new,
    kwd_word_count, country, unified_categoryid                  AS category_id,
    LEFT(search_url_city, 490)                              AS search_url_city,
    qu_cx, qu_tr, biz_type_filter_new,
    LEFT(url_resultcount_new, 490)                          AS url_resultcount_new,
    NULL                                                    AS voice_search_language_new,
    LEFT(kwd_type_new, 490)                                 AS kwd_type_new,
    attr_srch_new, attr_brand_new, query_type_new,
    NULL                                                    AS src_path_new,
    NULL                                                    AS enquiry_click_type,
    0 city_only, 0 ecom_filter, 0 rcmnd_srch, cd_user_mode,
    CAST(
        CASE WHEN positions ~ '^[0-9]+(\.[0-9]+)?$' THEN positions ELSE NULL END
    AS DECIMAL(10,5))                                       AS positions,
    SUM(pageviews)                                          AS pageviews,
    SUM(pdp_clicks)                                         AS pdp_clicks,
    SUM(event_catalogue_clicks)                             AS event_catalogue_clicks,
    0                                                       AS enquiry_cta_clicks,
    0                                                       AS enquiries,
    0                                                       AS calls,
    0                                                       AS page_type,
    NULL                                                    AS special_srchs,
    LEFT(city_tier, 490)                                    AS city_tier,
    price_filter_enabled, price_intent, custom_price,
    LEFT(min_price, 490)                                    AS min_price,
    LEFT(max_price, 490)                                    AS max_price,
    LEFT(user_pseudo_id, 490)                               AS user_pseudo_id,
    LEFT(result_count, 490)                                 AS result_count,
    0                                                       AS user_id

FROM (
    SELECT
        search_date                                         AS date,

        -- keyword_new: CAST to VARCHAR(990)
        CAST(
            CASE
                WHEN cd_page_info LIKE '%&kwd=%'
                    THEN REPLACE(REPLACE(REPLACE(REGEXP_SUBSTR(cd_page_info, 'kwd=([^&]+)'),'%20', ' '),'+', ' '),'kwd=','')
                ELSE LOWER(cd_search_query)
            END
        AS VARCHAR(990))                                    AS keyword_new,

        LENGTH(TRIM(
            CAST(
                CASE
                    WHEN cd_page_info LIKE '%&kwd=%'
                        THEN REPLACE(REPLACE(REPLACE(REGEXP_SUBSTR(cd_page_info, 'kwd=([^&]+)'),'%20', ' '),'+', ' '),'kwd=','')
                    ELSE LOWER(cd_search_query)
                END
            AS VARCHAR(990))
        )) - LENGTH(REPLACE(TRIM(
            CAST(
                CASE
                    WHEN cd_page_info LIKE '%&kwd=%'
                        THEN REPLACE(REPLACE(REPLACE(REGEXP_SUBSTR(cd_page_info, 'kwd=([^&]+)'),'%20', ' '),'+', ' '),'kwd=','')
                    ELSE LOWER(cd_search_query)
                END
            AS VARCHAR(990))
        ), ' ', '')) + 1                                    AS kwd_word_count,

		CASE
		    WHEN cd_page_info LIKE '%biz:10%' THEN 1
		    WHEN cd_page_info LIKE '%biz:20%' THEN 2
		    WHEN cd_page_info LIKE '%biz:30%' THEN 3
		    WHEN cd_page_info LIKE '%biz:40%' THEN 4
		    WHEN cd_page_info LIKE '%biz=10%' THEN 1
		    WHEN cd_page_info LIKE '%biz=20%' THEN 2
		    WHEN cd_page_info LIKE '%biz=30%' THEN 3
		    WHEN cd_page_info LIKE '%biz=40%' THEN 4
		    ELSE 0
		END AS biz_type_filter_new,

        -- url_resultcount_new: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN LOWER(cd_page_info) LIKE '%res=rc%'      THEN REGEXP_SUBSTR(LOWER(cd_page_info), 'res=rc([^-|]*)',    1, 1, 'e')
                WHEN LOWER(cd_page_info) LIKE '%res:rc%'      THEN REGEXP_SUBSTR(LOWER(cd_page_info), 'res:rc([^-|]*)',    1, 1, 'e')
                WHEN LOWER(cd_page_info) LIKE '%res"":""rc%'  THEN REGEXP_SUBSTR(LOWER(cd_page_info), 'res"":""rc([^-|]*)',1, 1, 'e')
                ELSE CAST(url_resultcount AS TEXT)
            END
        AS VARCHAR(490))                                    AS url_resultcount_new,

        -- kwd_type_new: CAST to VARCHAR(490)
        CAST(
            UPPER(CASE
                WHEN LOWER(cd_page_info) LIKE '%|ktp:%'     THEN REGEXP_SUBSTR(LOWER(cd_page_info), '\\|ktp:([^|/?&"]*)',    1, 1, 'e')
                WHEN LOWER(cd_page_info) LIKE '%|ktp"":""%' THEN REGEXP_SUBSTR(LOWER(cd_page_info), '\\|ktp"":""([^|/?&"]*)',1, 1, 'e')
                ELSE UPPER(kwd_type)
            END)
        AS VARCHAR(490))                                    AS kwd_type_new,

        CAST(attr_brand AS INT)                             AS attr_brand_new,
        modid,
        -- search_url_city already sourced from column; guard with CAST
        CAST(search_url_city AS VARCHAR(490))               AS search_url_city,
        CASE WHEN country = 'India' THEN 1 ELSE 2 END       AS country,
        CASE
            WHEN (pageviews IS NULL OR pageviews = 0)
             AND (   (pdp_clicks            IS NOT NULL AND pdp_clicks            > 0)
                  OR (event_catalogue_clicks IS NOT NULL AND event_catalogue_clicks > 0))
             AND position ~ '^[0-9]+(\.[0-9]+)?$'
                THEN position
            ELSE NULL
        END                                                 AS positions,
        CASE WHEN attr_srch = 1 THEN 1 ELSE 0 END           AS attr_srch_new,
        CASE
            WHEN LOWER(cd_page_info) LIKE '%qry_typ:p%'     THEN 1  WHEN LOWER(cd_page_info) LIKE '%qry_typ:c%'     THEN 2  WHEN LOWER(cd_page_info) LIKE '%qry_typ:s%'     THEN 3
            WHEN LOWER(cd_page_info) LIKE '%qry_typ"":""p%' THEN 1  WHEN LOWER(cd_page_info) LIKE '%qry_typ"":""c%' THEN 2  WHEN LOWER(cd_page_info) LIKE '%qry_typ"":""s%' THEN 3
        END                                                 AS query_type_new,
        CASE
            WHEN cd_user_mode_f = 'unidentified'                   THEN 1
            WHEN cd_user_mode_f IN ('identified_P','identified_N') THEN 2
            WHEN cd_user_mode_f = 'identified_F'                   THEN 3
            ELSE 0
        END                                                 AS cd_user_mode,
        CAST(CASE WHEN cd_page_info LIKE '%qu-cx=1%'   THEN 1 WHEN cd_page_info LIKE '%qcr:1%'     THEN 1 WHEN cd_page_info LIKE '%qcr"":""1%' THEN 1 ELSE NULL END AS INT) AS qu_cx,
        CAST(CASE WHEN cd_page_info LIKE '%qu-tr=1%'   THEN 1 WHEN cd_page_info LIKE '%qtr:1%'     THEN 1 WHEN cd_page_info LIKE '%qtr"":""1%' THEN 1 ELSE NULL END AS INT) AS qu_tr,
        CAST(
            CASE
                WHEN cd_page_info LIKE '%mc:%'
                 AND SPLIT_PART(SPLIT_PART(cd_page_info, 'mc:', 2), '|', 1) ~ '^[0-9]+$'
                    THEN SPLIT_PART(SPLIT_PART(cd_page_info, 'mc:', 2), '|', 1)
                ELSE NULL
            END AS INTEGER
        )                                                   AS Unified_categoryid,
        pageviews, pdp_clicks, event_catalogue_clicks,
        CAST(user_pseudo_id AS VARCHAR(490))                AS user_pseudo_id,

        -- city_tier: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN cd_page_info LIKE '%tyr"":""%' THEN SPLIT_PART(SPLIT_PART(cd_page_info, 'tyr"":""', 2), '|', 1)
                WHEN cd_page_info LIKE '%tyr:%'     THEN SPLIT_PART(SPLIT_PART(cd_page_info, 'tyr:', 2),    '|', 1)
                ELSE NULL
            END
        AS VARCHAR(490))                                    AS City_tier,

        CASE WHEN cd_page_info LIKE '%pfen:1%' THEN 1 ELSE 0 END AS price_filter_enabled,
        CASE WHEN cd_page_info LIKE '%pin:1%'  THEN 1 ELSE 0 END AS price_intent,
        CASE WHEN cd_page_info LIKE '%csp:1%'  THEN 1 ELSE 0 END AS custom_price,

        CAST(REGEXP_SUBSTR(cd_page_info, 'minp:([^|]+)',    1, 1, 'e') AS VARCHAR(490)) AS min_price,
        CAST(REGEXP_SUBSTR(cd_page_info, 'maxp:([^|]+)',    1, 1, 'e') AS VARCHAR(490)) AS max_price,
        CAST(REGEXP_SUBSTR(cd_page_info, 'res:RC1-R([^|]+)',1, 1, 'e') AS VARCHAR(490)) AS result_count

    FROM im_datamart_bigquery.fact_bigquery_android_ios_search_data
    WHERE
        DATE(search_date) BETWEEN st_dt and en_dt
        AND modid IN ('Android', 'IOS')
        AND (pageviews < 10000 AND pdp_clicks < 10000 AND event_catalogue_clicks < 10000)
)
GROUP BY
    DATE, status, modid, keyword_new, kwd_word_count, country, category_id,
    search_url_city, qu_cx, qu_tr, biz_type_filter_new,
    url_resultcount_new, voice_search_language_new, kwd_type_new,
    attr_srch_new, attr_brand_new, query_type_new, src_path_new, enquiry_click_type,
    city_only, ecom_filter, rcmnd_srch, cd_user_mode, positions, page_type,
    special_srchs, city_tier, price_filter_enabled, price_intent, custom_price,
    min_price, max_price, user_pseudo_id, result_Count

UNION ALL

SELECT
    DATE,
    status, modid,
    LEFT(trim(lower(keyword)), 990)                         AS keyword_new,
    kwd_word_count, country, category_id,
    LEFT(search_url_city, 490)                              AS search_url_city,
    qu_cx, qu_tr, biz_type_filter_new,
    LEFT(url_resultcount_new, 490)                          AS url_resultcount_new,
    LEFT(voice_search_language_new, 490)                    AS voice_search_language_new,
    LEFT(kwd_type_new, 490)                                 AS kwd_type_new,
    attr_srch_new, attr_brand_new, query_type_new,
    LEFT(src_path_new, 490)                                 AS src_path_new,
    NULL                                                    AS enquiry_click_type,
    0 AS city_only, 0 AS ecom_filter, 0 AS rcmnd_srch, cd_user_mode,
    CAST(
        CASE WHEN positions ~ '^[0-9]+(\.[0-9]+)?$' THEN positions ELSE NULL END
    AS DECIMAL(10,5))                                       AS positions,
    0 pageviews, 0 pdp_clicks, 0 event_catalogue_clicks, 0 enquiry_cta_clicks,
    COUNT(DISTINCT CASE WHEN status = 3 THEN enquiry_id END)  AS enquiries,
    COUNT(DISTINCT CASE WHEN status = 4 THEN enquiry_id END)  AS calls,
    0 AS page_type,
    NULL                                                    AS special_srchs,
    LEFT(city_tier, 490)                                    AS city_tier,
    price_filter_enabled, price_intent, custom_price,
    LEFT(min_price, 490)                                    AS min_price,
    LEFT(max_price, 490)                                    AS max_price,
    NULL                                                    AS user_pseudo_id,
    LEFT(result_count, 490)                                 AS result_count,
    user_id

FROM (
    -- ── status = 3: enquiries ────────────────────────────────────────────────
    SELECT
        date,
        3                                                   AS status,
        CASE
            WHEN modid = 'IMOB'                    THEN 'Mobile'
            WHEN modid = 'DIR'                     THEN 'Desktop'
            WHEN modid IN ('ANDROID','ANDWEB')     THEN 'Android'
            WHEN modid IN ('IOS','IOSWEB')         THEN 'IOS'
        END                                                 AS modid,
        CASE WHEN country = 'IN' THEN 1 ELSE 2 END          AS country,
        CASE
            WHEN modid = 'DIR'  AND cd_additional_data LIKE '%|Position=%' THEN REGEXP_SUBSTR(cd_additional_data, '\\|Position=([^|&]+)', 1, 1, 'e')
            WHEN modid = 'IMOB' AND cd_additional_data LIKE '%|pos=%'      THEN REGEXP_SUBSTR(cd_additional_data, '\\|pos=([^|&]+)',      1, 1, 'e')
            WHEN modid = 'DIR'  AND cd_additional_data LIKE '%|Position-%' THEN REGEXP_SUBSTR(cd_additional_data, '\\|Position-([^|&]+)', 1, 1, 'e')
            WHEN modid = 'IMOB' AND cd_additional_data LIKE '%|pos-%'      THEN REGEXP_SUBSTR(cd_additional_data, '\\|pos-([^|&]+)',      1, 1, 'e')
            WHEN modid = 'DIR'  AND cd_additional_data LIKE '%|Position:%' THEN REGEXP_SUBSTR(cd_additional_data, '\\|Position:([^|&]+)', 1, 1, 'e')
            WHEN modid = 'IMOB' AND cd_additional_data LIKE '%|pos:%'      THEN REGEXP_SUBSTR(cd_additional_data, '\\|pos:([^|&]+)',      1, 1, 'e')
            ELSE NULL
        END                                                 AS Positions,
        CASE
            WHEN ref_url ~ 'stype:slrtxt-graph' THEN 1
            WHEN ref_url ~ 'stype:graph-cse'    THEN 2
            WHEN ref_url ~ 'stype:graph'        THEN 3
            ELSE 0
        END                                                 AS page_type,
        CAST(CASE WHEN ref_url LIKE '%attr=1%' THEN 1 WHEN ref_url LIKE '%attr:1%' THEN 1 ELSE 0 END AS INTEGER) AS attr_srch_new,
        CASE
            WHEN query_reference_url LIKE '%&qry_typ=C%'     THEN 2  WHEN query_reference_url LIKE '%qry_typ:C%'      THEN 2  WHEN query_reference_url LIKE '%qry_typ"":""C%'  THEN 2
            WHEN ref_url            LIKE '%qry_typ"":""C%'   THEN 2  WHEN ref_url            LIKE '%qry_typ:C%'       THEN 2  WHEN ref_url            LIKE '%&qry_typ=C%'      THEN 2
            WHEN query_reference_url LIKE '%&qry_typ=P%'     THEN 1  WHEN query_reference_url LIKE '%qry_typ:P%'      THEN 1  WHEN query_reference_url LIKE '%qry_typ"":""P%'  THEN 1
            WHEN ref_url            LIKE '%qry_typ:P%'       THEN 1  WHEN ref_url            LIKE '%&qry_typ=P%'      THEN 1  WHEN ref_url            LIKE '%qry_typ"":""P%'   THEN 1
            WHEN query_reference_url LIKE '%&qry_typ=S%'     THEN 3  WHEN query_reference_url LIKE '%qry_typ:S%'      THEN 3  WHEN query_reference_url LIKE '%qry_typ"":""S%'  THEN 3
            WHEN ref_url            LIKE '%qry_typ:S%'       THEN 3  WHEN ref_url            LIKE '%&qry_typ=S%'      THEN 3  WHEN ref_url            LIKE '%qry_typ"":""S%'   THEN 3
        END                                                 AS Query_type_new,
        CAST(CASE WHEN ref_url LIKE '%qu-cx=1%' THEN 1 WHEN ref_url LIKE '%qcr:1%' THEN 1 WHEN ref_url LIKE '%qcr"":""1%' THEN 1 ELSE NULL END AS INT) AS qu_cx,
        CAST(CASE WHEN ref_url LIKE '%qu-tr=1%' THEN 1 WHEN ref_url LIKE '%qtr:1%' THEN 1 WHEN ref_url LIKE '%qtr"":""1%' THEN 1 ELSE NULL END AS INT) AS qu_tr,
        CAST(
            CASE
                WHEN ref_url LIKE '%mc:%'
                 AND SPLIT_PART(SPLIT_PART(ref_url, 'mc:', 2), '|', 1) ~ '^[0-9]+$'
                    THEN SPLIT_PART(SPLIT_PART(ref_url, 'mc:', 2), '|', 1)
                ELSE NULL
            END AS INTEGER
        )                                                   AS category_id,

        -- search_url_city: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN (ref_url LIKE '%cq=%'  AND ref_url NOT LIKE '%cq=all&%')                           THEN REPLACE(REGEXP_SUBSTR(ref_url,'&cq=([^&|#?]+)', 1, 1, 'e'),'+', ' ')
                WHEN ((ref_url LIKE '%&cq=&%' OR ref_url LIKE '%cq=all&%') AND ref_url NOT LIKE '%cq=%') THEN 'All India'
                WHEN (ref_url LIKE '%cq:%'  AND ref_url NOT LIKE '%cq:all%')                             THEN REPLACE(REGEXP_SUBSTR(ref_url,'cq:([^&|#?]+)',  1, 1, 'e'),'+', ' ')
                WHEN ((ref_url LIKE '%=cq:|%' OR ref_url LIKE '%cq:all%')   AND ref_url NOT LIKE '%cq:%') THEN 'All India'
                ELSE 'All India'
            END
        AS VARCHAR(490))                                    AS search_url_city,

        -- keyword: CAST to VARCHAR(990)
        CAST(
            CASE
                WHEN ref_url LIKE '%?s=%'  THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?s=([^&]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%?ss=%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?ss=([^&]+)', 1, 1, 'e')
                WHEN ref_url LIKE '%ss =%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), 'ss =([^&|]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%ss%'   THEN REGEXP_SUBSTR(ref_url, 'ss[:=]([^-]+)', 1, 1, 'e')
                WHEN cd_additional_data = 'Search' THEN
                    CASE WHEN POSITION('|' IN ref_url) > 0 THEN LEFT(ref_url, POSITION('|' IN ref_url) - 1) ELSE ref_url END
            END
        AS VARCHAR(990))                                    AS keyword,

        LENGTH(TRIM(CAST(
            CASE
                WHEN ref_url LIKE '%?s=%'  THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?s=([^&]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%?ss=%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?ss=([^&]+)', 1, 1, 'e')
                WHEN ref_url LIKE '%ss =%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), 'ss =([^&|]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%ss%'   THEN REGEXP_SUBSTR(ref_url, 'ss[:=]([^-]+)', 1, 1, 'e')
                WHEN cd_additional_data = 'Search' THEN
                    CASE WHEN POSITION('|' IN ref_url) > 0 THEN LEFT(ref_url, POSITION('|' IN ref_url) - 1) ELSE ref_url END
            END
        AS VARCHAR(990)))) - LENGTH(REPLACE(TRIM(CAST(
            CASE
                WHEN ref_url LIKE '%?s=%'  THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?s=([^&]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%?ss=%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?ss=([^&]+)', 1, 1, 'e')
                WHEN ref_url LIKE '%ss =%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), 'ss =([^&|]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%ss%'   THEN REGEXP_SUBSTR(ref_url, 'ss[:=]([^-]+)', 1, 1, 'e')
                WHEN cd_additional_data = 'Search' THEN
                    CASE WHEN POSITION('|' IN ref_url) > 0 THEN LEFT(ref_url, POSITION('|' IN ref_url) - 1) ELSE ref_url END
            END
        AS VARCHAR(990))), ' ', '')) + 1                    AS kwd_word_count,

        -- url_resultcount_new: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN query_reference_url LIKE '%res=RC%'     THEN REGEXP_SUBSTR(query_reference_url, '&res=RC(\\d+)',      1, 1, 'e')
                WHEN query_reference_url LIKE '%res:RC%'     THEN REGEXP_SUBSTR(query_reference_url, 'res:RC(\\d+)',       1, 1, 'e')
                WHEN query_reference_url LIKE '%res"":""RC%' THEN REGEXP_SUBSTR(query_reference_url, 'res"":""RC(\\d+)',   1, 1, 'e')
                WHEN ref_url            LIKE '%res=RC%'      THEN REGEXP_SUBSTR(ref_url,             '&res=RC(\\d+)',      1, 1, 'e')
                WHEN ref_url            LIKE '%res:RC%'      THEN REGEXP_SUBSTR(ref_url,             'res:RC(\\d+)',       1, 1, 'e')
                WHEN ref_url            LIKE '%res"":""RC%'  THEN REGEXP_SUBSTR(ref_url,             'res"":""RC(\\d+)',   1, 1, 'e')
                WHEN ref_url            LIKE '%res=RC%'      THEN REGEXP_SUBSTR(ref_url,             'res=RC([^-|]*)',     1, 1, 'e')
                WHEN ref_url            LIKE '%res:RC%'      THEN REGEXP_SUBSTR(ref_url,             'res:RC([^-|]*)',     1, 1, 'e')
            END
        AS VARCHAR(490))                                    AS url_resultcount_new,

        -- src_path_new: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN LOWER(ref_url) LIKE '%&src=%' THEN REGEXP_SUBSTR(ref_url, '&src=([^|&:%""]+)', 1, 1, 'e')
            END
        AS VARCHAR(490))                                    AS src_path_new,

        enquiry_id,
        CASE
            WHEN dir_query_login_mode = 3 THEN 1
            WHEN dir_query_login_mode = 1 THEN 2
            WHEN dir_query_login_mode = 2 THEN 3
            ELSE 0
        END                                                 AS cd_user_mode,

        -- city_tier: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN ref_url LIKE '%tyr"":""%' THEN SPLIT_PART(SPLIT_PART(ref_url, 'tyr"":""', 2), '|', 1)
                WHEN ref_url LIKE '%tyr:%'     THEN SPLIT_PART(SPLIT_PART(ref_url, 'tyr:', 2),    '|', 1)
                ELSE NULL
            END
        AS VARCHAR(490))                                    AS City_tier,

		CASE
		    WHEN ref_url LIKE '%biz:10%' THEN 1
		    WHEN ref_url LIKE '%biz:20%' THEN 2
		    WHEN ref_url LIKE '%biz:30%' THEN 3
		    WHEN ref_url LIKE '%biz:40%' THEN 4
		    WHEN ref_url LIKE '%biz=10%' THEN 1
		    WHEN ref_url LIKE '%biz=20%' THEN 2
		    WHEN ref_url LIKE '%biz=30%' THEN 3
		    WHEN ref_url LIKE '%biz=40%' THEN 4
		    ELSE 0
		END AS biz_type_filter_new,

        -- voice_search_language_new: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN (ref_url LIKE '%&src=vs%' AND ref_url LIKE '%&lang=%') THEN REGEXP_SUBSTR(ref_url, '&lang=([a-z]{2})', 1, 1, 'e')
                WHEN (ref_url LIKE '%&src_vs%' AND ref_url LIKE '%&lang:%') THEN REGEXP_SUBSTR(ref_url, '&lang:([a-z]{2})', 1, 1, 'e')
            END
        AS VARCHAR(490))                                    AS voice_search_language_new,

        -- kwd_type_new: CAST to VARCHAR(490)
        CAST(
            CASE WHEN LOWER(ref_url) LIKE '%ktp:%' THEN REGEXP_SUBSTR(ref_url, 'ktp:([^|/?&"]*)', 1, 1, 'e') END
        AS VARCHAR(490))                                    AS kwd_type_new,

        CAST(CASE WHEN ref_url LIKE '%stype:attr=1-br%' THEN 1 ELSE 0 END AS INT)  AS attr_brand_new,
        CASE WHEN (ref_url LIKE '%pfen:1%' OR cd_additional_data LIKE '%pfen:1%') THEN 1 ELSE 0 END AS price_filter_enabled,
        CASE WHEN (ref_url LIKE '%pin:1%'  OR cd_additional_data LIKE '%pin:1%')  THEN 1 ELSE 0 END AS price_intent,
        CASE WHEN (ref_url LIKE '%csp:1%'  OR cd_additional_data LIKE '%csp:1%')  THEN 1 ELSE 0 END AS custom_price,

        CAST(COALESCE(REGEXP_SUBSTR(ref_url,'minp:([^|]+)',1,1,'e'), REGEXP_SUBSTR(cd_additional_data,'minp:([^|]+)',1,1,'e')) AS VARCHAR(490)) AS min_price,
        CAST(COALESCE(REGEXP_SUBSTR(ref_url,'maxp:([^|]+)',1,1,'e'), REGEXP_SUBSTR(cd_additional_data,'maxp:([^|]+)',1,1,'e')) AS VARCHAR(490)) AS max_price,
        CAST(COALESCE(REGEXP_SUBSTR(ref_url,'res:RC1-R([^|]+)',1,1,'e'), REGEXP_SUBSTR(cd_additional_data,'res:RC1-R([^|]+)',1,1,'e')) AS VARCHAR(490)) AS result_count,
        user_id

    FROM (
        SELECT
            date, modid, country, enquiry_id, query_reference_url,
            dir_query_categoryid, cd_additional_data, dir_query_login_mode,
            user_id, distance_city, enq_type, ref_url
        FROM staging.stg_dir_query
        WHERE
            (   (modid = 'DIR'  AND UPPER(cd_additional_data) LIKE '%PT=SEARCH%')
             OR (modid = 'IMOB' AND UPPER(cd_additional_data) LIKE '%IMOB_SEARCH%')
             OR ((modid IN ('ANDROID','ANDWEB') AND UPPER(cd_additional_data) LIKE '%ANDROID-SEARCH%') OR UPPER(cd_additional_data) LIKE '%IMOB_SEARCH%')
             OR (modid IN ('IOS','IOSWEB') AND (cd_additional_data = 'Search Products' OR UPPER(cd_additional_data) LIKE '%IOS-SEARCH-PRODUCTS%' OR UPPER(cd_additional_data) LIKE '%IMOB_SEARCH%'))
            )
            AND enq_type = 1
            AND date BETWEEN st_dt and en_dt
    )

    UNION ALL

    -- ── status = 4: calls ────────────────────────────────────────────────────
    SELECT
        DATE(c2c_call_time)                                 AS date,
        4                                                   AS status,
        CASE
            WHEN modid IN ('IMOB')             THEN 'Mobile'
            WHEN modid IN ('DIR')              THEN 'Desktop'
            WHEN modid IN ('ANDROID','ANDWEB') THEN 'Android'
            WHEN modid IN ('IOS','IOSWEB')     THEN 'IOS'
        END                                                 AS modid,
        CASE WHEN c2c_caller_country_iso = 'IN' THEN 1 ELSE 2 END AS country,
        CAST(NULLIF(REGEXP_SUBSTR(cd_additional_data, 'pos=(\\d+)', 1, 1, 'e'), '') AS TEXT) AS Positions,
        CASE
            WHEN ref_url ~ 'stype:slrtxt-graph' THEN 1
            WHEN ref_url ~ 'stype:graph-cse'    THEN 2
            WHEN ref_url ~ 'stype:graph'        THEN 3
            ELSE 0
        END                                                 AS page_type,
        CAST(CASE WHEN ref_url LIKE '%attr=1%' THEN 1 WHEN ref_url LIKE '%attr:1%' THEN 1 ELSE 0 END AS INTEGER) AS attr_srch_new,
        CASE
            WHEN ref_url LIKE '%&qry_typ=C%'     THEN 2  WHEN ref_url LIKE '%qry_typ:C%'     THEN 2  WHEN ref_url LIKE '%qry_typ"":""C%' THEN 2
            WHEN ref_url LIKE '%&qry_typ=P%'     THEN 1  WHEN ref_url LIKE '%qry_typ:P%'     THEN 1  WHEN ref_url LIKE '%qry_typ"":""P%' THEN 1
            WHEN ref_url LIKE '%&qry_typ=S%'     THEN 3  WHEN ref_url LIKE '%qry_typ:S%'     THEN 3  WHEN ref_url LIKE '%qry_typ"":""S%' THEN 3
        END                                                 AS Query_type_new,
        CAST(CASE WHEN ref_url LIKE '%qu-cx=1%' THEN 1 WHEN ref_url LIKE '%qcr:1%' THEN 1 WHEN ref_url LIKE '%qcr"":""1%' THEN 1 ELSE NULL END AS INT) AS qu_cx,
        CAST(CASE WHEN ref_url LIKE '%qu-tr=1%' THEN 1 WHEN ref_url LIKE '%qtr:1%' THEN 1 WHEN ref_url LIKE '%qtr"":""1%' THEN 1 ELSE NULL END AS INT) AS qu_tr,
        CAST(
            CASE
                WHEN ref_url LIKE '%mc:%'
                 AND SPLIT_PART(SPLIT_PART(ref_url, 'mc:', 2), '|', 1) ~ '^[0-9]+$'
                    THEN SPLIT_PART(SPLIT_PART(ref_url, 'mc:', 2), '|', 1)
                ELSE NULL
            END AS INTEGER
        )                                                   AS category_id,

        -- search_url_city: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN (ref_url LIKE '%cq=%'  AND ref_url NOT LIKE '%cq=all&%')                            THEN REPLACE(REGEXP_SUBSTR(ref_url,'&cq=([^&|#?]+)', 1, 1, 'e'),'+', ' ')
                WHEN ((ref_url LIKE '%&cq=&%' OR ref_url LIKE '%cq=all&%') AND ref_url NOT LIKE '%cq=%')  THEN 'All India'
                WHEN (ref_url LIKE '%cq:%'  AND ref_url NOT LIKE '%cq:all%')                              THEN REPLACE(REGEXP_SUBSTR(ref_url,'cq:([^&|#?]+)',  1, 1, 'e'),'+', ' ')
                WHEN ((ref_url LIKE '%=cq:|%' OR ref_url LIKE '%cq:all%')   AND ref_url NOT LIKE '%cq:%') THEN 'All India'
                ELSE 'All India'
            END
        AS VARCHAR(490))                                    AS search_url_city,

        -- keyword_new: CAST to VARCHAR(990)
        CAST(
            CASE
                WHEN ref_url LIKE '%?s=%'  THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?s=([^&]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%?ss=%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?ss=([^&]+)', 1, 1, 'e')
                WHEN ref_url LIKE '%ss =%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), 'ss =([^&|]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%ss%'   THEN REGEXP_SUBSTR(ref_url, 'ss[:=]([^-]+)', 1, 1, 'e')
                WHEN cd_additional_data = 'Search' THEN
                    CASE WHEN POSITION('|' IN ref_url) > 0 THEN LEFT(ref_url, POSITION('|' IN ref_url) - 1) ELSE ref_url END
            END
        AS VARCHAR(990))                                    AS keyword_new,

        LENGTH(TRIM(CAST(
            CASE
                WHEN ref_url LIKE '%?s=%'  THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?s=([^&]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%?ss=%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?ss=([^&]+)', 1, 1, 'e')
                WHEN ref_url LIKE '%ss =%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), 'ss =([^&|]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%ss%'   THEN REGEXP_SUBSTR(ref_url, 'ss[:=]([^-]+)', 1, 1, 'e')
                WHEN cd_additional_data = 'Search' THEN
                    CASE WHEN POSITION('|' IN ref_url) > 0 THEN LEFT(ref_url, POSITION('|' IN ref_url) - 1) ELSE ref_url END
            END
        AS VARCHAR(990)))) - LENGTH(REPLACE(TRIM(CAST(
            CASE
                WHEN ref_url LIKE '%?s=%'  THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?s=([^&]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%?ss=%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), '\\?ss=([^&]+)', 1, 1, 'e')
                WHEN ref_url LIKE '%ss =%' THEN REGEXP_SUBSTR(REPLACE(REPLACE(ref_url,'+', ' '),'%20', ' '), 'ss =([^&|]+)',  1, 1, 'e')
                WHEN ref_url LIKE '%ss%'   THEN REGEXP_SUBSTR(ref_url, 'ss[:=]([^-]+)', 1, 1, 'e')
                WHEN cd_additional_data = 'Search' THEN
                    CASE WHEN POSITION('|' IN ref_url) > 0 THEN LEFT(ref_url, POSITION('|' IN ref_url) - 1) ELSE ref_url END
            END
        AS VARCHAR(990))), ' ', '')) + 1                    AS kwd_word_count,

        -- url_resultcount_new: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN ref_url LIKE '%res=RC%'     THEN REGEXP_SUBSTR(ref_url, '&res=RC(\\d+)',    1, 1, 'e')
                WHEN ref_url LIKE '%res:RC%'     THEN REGEXP_SUBSTR(ref_url, 'res:RC(\\d+)',     1, 1, 'e')
                WHEN ref_url LIKE '%res"":""RC%' THEN REGEXP_SUBSTR(ref_url, 'res"":""RC(\\d+)', 1, 1, 'e')
                WHEN ref_url LIKE '%res=RC%'     THEN REGEXP_SUBSTR(ref_url, 'res=RC([^-|]*)',   1, 1, 'e')
                WHEN ref_url LIKE '%res:RC%'     THEN REGEXP_SUBSTR(ref_url, 'res:RC([^-|]*)',   1, 1, 'e')
            END
        AS VARCHAR(490))                                    AS url_resultcount_new,

        -- src_path_new: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN ref_url LIKE '%src=as-popular%'      THEN 'as-popular'
                WHEN ref_url LIKE '%src=as-rcnt%'         THEN 'as-rcnt'
                WHEN ref_url LIKE '%src=as-default%'      THEN 'as-default'
                WHEN ref_url LIKE '%src=as-comp%'         THEN 'as-comp'
                WHEN ref_url LIKE '%src=as-incity%'       THEN 'as-incity'
                WHEN ref_url LIKE '%src=as-kwd%'          THEN 'as-kwd'
                WHEN ref_url LIKE '%&src=vs%'             THEN 'vs'
                WHEN ref_url LIKE '%src=as-blrcnt%'       THEN 'as-blrcnt'
                WHEN ref_url LIKE '%src=advanced-filter%' THEN 'advanced-filter'
                WHEN ref_url LIKE '%src=as-context%'      THEN 'as-context'
                WHEN ref_url LIKE '%src=category-rlt-srch%'   THEN 'category-rlt-srch'
                WHEN ref_url LIKE '%src=rcv%'             THEN 'rcv'
                WHEN ref_url LIKE '%src=adv-srch%'        THEN 'adv-srch'
                WHEN ref_url LIKE '%as-selcnxt%'          THEN 'as-selcnxt'
                WHEN ref_url LIKE '%as-rcmnd%'            THEN 'as-rcmnd'
            END
        AS VARCHAR(490))                                    AS src_path_new,

        c2c_record_id,
        login_mode,

        -- city_tier: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN ref_url LIKE '%tyr"":""%' THEN SPLIT_PART(SPLIT_PART(ref_url, 'tyr"":""', 2), '|', 1)
                WHEN ref_url LIKE '%tyr:%'     THEN SPLIT_PART(SPLIT_PART(ref_url, 'tyr:', 2),    '|', 1)
                ELSE NULL
            END
        AS VARCHAR(490))                                    AS City_tier,

		CASE
		    WHEN ref_url LIKE '%biz:10%' THEN 1
		    WHEN ref_url LIKE '%biz:20%' THEN 2
		    WHEN ref_url LIKE '%biz:30%' THEN 3
		    WHEN ref_url LIKE '%biz:40%' THEN 4
		    WHEN ref_url LIKE '%biz=10%' THEN 1
		    WHEN ref_url LIKE '%biz=20%' THEN 2
		    WHEN ref_url LIKE '%biz=30%' THEN 3
		    WHEN ref_url LIKE '%biz=40%' THEN 4
		    ELSE 0
		END AS biz_type_filter_new,

        -- voice_search_language_new: CAST to VARCHAR(490)
        CAST(
            CASE
                WHEN (ref_url LIKE '%&src=vs%' AND ref_url LIKE '%&lang=%') THEN REGEXP_SUBSTR(ref_url, '&lang=([a-z]{2})', 1, 1, 'e')
                WHEN (ref_url LIKE '%&src_vs%' AND ref_url LIKE '%&lang:%') THEN REGEXP_SUBSTR(ref_url, '&lang:([a-z]{2})', 1, 1, 'e')
            END
        AS VARCHAR(490))                                    AS voice_search_language_new,

        -- kwd_type_new: CAST to VARCHAR(490)
        CAST(
            CASE WHEN LOWER(ref_url) LIKE '%ktp:%' THEN REGEXP_SUBSTR(ref_url, 'ktp:([^|/?&"]*)', 1, 1, 'e') END
        AS VARCHAR(490))                                    AS kwd_type_new,

        CAST(CASE WHEN ref_url LIKE '%stype:attr=1-br%' THEN 1 ELSE 0 END AS INT) AS attr_brand_new,
        CASE WHEN (ref_url LIKE '%pfen:1%' OR cd_additional_data LIKE '%pfen:1%') THEN 1 ELSE 0 END AS price_filter_enabled,
        CASE WHEN (ref_url LIKE '%pin:1%'  OR cd_additional_data LIKE '%pin:1%')  THEN 1 ELSE 0 END AS price_intent,
        CASE WHEN (ref_url LIKE '%csp:1%'  OR cd_additional_data LIKE '%csp:1%')  THEN 1 ELSE 0 END AS custom_price,

        CAST(COALESCE(REGEXP_SUBSTR(ref_url,'minp:([^|]+)',1,1,'e'), REGEXP_SUBSTR(cd_additional_data,'minp:([^|]+)',1,1,'e')) AS VARCHAR(490)) AS min_price,
        CAST(COALESCE(REGEXP_SUBSTR(ref_url,'maxp:([^|]+)',1,1,'e'), REGEXP_SUBSTR(cd_additional_data,'maxp:([^|]+)',1,1,'e')) AS VARCHAR(490)) AS max_price,
        CAST(COALESCE(REGEXP_SUBSTR(ref_url,'res:RC1-R([^|]+)',1,1,'e'), REGEXP_SUBSTR(cd_additional_data,'res:RC1-R([^|]+)',1,1,'e')) AS VARCHAR(490)) AS result_count,
        c2c_caller_glusr_id                                 AS user_id

    FROM (
        SELECT
            login_mode, c2c_call_time, c2c_caller_country_iso, cd_additional_data,
            modid, c2c_caller_glusr_id, c2c_record_id,
            c2c_caller_city_id, c2c_receiver_city_id, ref_url
        FROM staging.stg_c2c_records
        WHERE
            (modid = 'IMOB' AND fk_c2c_record_unidentified_id IS NULL AND UPPER(cd_additional_data) LIKE '%IMOB_SEARCH%' AND ref_url NOT LIKE '%/messages/%')
         OR (modid IN ('ANDROID','ANDWEB') AND cd_additional_data LIKE 'Search%' OR UPPER(cd_additional_data) LIKE '%IMOB_SEARCH%')
         OR (modid IN ('IOS','IOSWEB') AND c2c_record_type = 1 AND cd_additional_data LIKE 'Search' AND UPPER(cd_additional_data) NOT LIKE '%BL-SEARCH%' OR UPPER(cd_additional_data) LIKE '%IMOB_SEARCH%')
        AND DATE(c2c_call_time) BETWEEN st_dt and en_dt
    ) a
    LEFT JOIN (
        SELECT from_city_id, to_city_id, distance_city
        FROM dwh.fact_gl_city_distance
    ) b ON a.c2c_caller_city_id = b.from_city_id
       AND a.c2c_receiver_city_id = b.to_city_id
)
GROUP BY
    date, status, modid, keyword_new, kwd_word_count, country, category_id,
    search_url_city, qu_cx, qu_tr, biz_type_filter_new,
    url_resultcount_new, voice_search_language_new, kwd_type_new,
    attr_srch_new, attr_brand_new, query_type_new, src_path_new, enquiry_click_type,
    city_only, ecom_filter, rcmnd_srch, cd_user_mode, positions, page_type,
    special_srchs, city_tier, price_filter_enabled, price_intent, custom_price,
    min_price, max_price, result_Count, user_id
;
------------------------------------------staging.category_search_cachedb------------------------------------------------------------------------
delete from staging.category_search_cachedb;
insert into staging.category_search_cachedb
select 
	ST_DT start_date ,
	en_dt end_date ,
	datatype time_period_flag ,
	count(distinct kwd_logic_type) kwd_cache_db,
	count(distinct case when ttl_key = 'default_cache' then kwd_logic_type end) kwd_default_cache, 
	count(distinct case when ttl_key = 'all_models_high' then kwd_logic_type end) kwd_all_model_high, 
	count(distinct case when ttl_key = 'exact_keyword_category' then kwd_logic_type end) exact_kwd_category, 
	count(distinct case when ttl_key = 'long_cache' or ttl_key = 'unlimited_ttl' then kwd_logic_type end) kwd_custom_cache, 
	count(distinct category_id) unique_categorys,
	count(distinct case when ttl_key = 'long_cache' or ttl_key = 'unlimited_ttl' then category_id end) unique_category_custom_cache
	from (
	select * from (		
	select 
		concat(category_search_keyword,logic_type) kwd_logic_type, insertiondate, category_search_keyword_id, ttl_key,
		dense_rank() over(partition by concat(category_search_keyword,logic_type) order by insertiondate desc) rnk
	from im_datamart_category.category_search_keywords
	where date(insertiondate) between ST_DT and EN_DT
		and year in (st_yr, en_yr)
        and month in ( st_mnth,  en_mnth)
		and day in (select extract(day from full_date) days from dwh.dim_date where full_date between date(st_dt) and date(en_dt))
	) a 
	join (
	select category_search_mapping_id, fk_category_search_keyword_id, category_id, modelname
	from im_datamart_category.category_search_mapping
	where modelname like 'Ensemble_%'
		and date(insertiondate) between ST_DT and EN_DT
		and year in (st_yr, en_yr)
        and month in ( st_mnth,  en_mnth)
		and day in (select extract(day from full_date) days from dwh.dim_date where full_date between date(st_dt) and date(en_dt))
	) b
	on a.category_search_keyword_id = b.fk_category_search_keyword_id
	where rnk = 1
	)
;




end if;
-------------------------------------------------------------------
insert into search_analytics.search_ctr
select 
	st_dt as start_date,
	en_dt as en_date,
	datatype as time_period_flag,
	modid, country, city,
	Position_Group, attribute_search,
	query_type, cd_user_mode,
	query_correction, query_translation,
	category_id,
	pageviews, pdp_clicks,event_catalogue_clicks,
	enquiry_cta_clicks,total_users,unique_users,enquiries,calls,
	url_resultcount, Word_Count,
	business_filter , voice_search_language,
	keyword_type, Attribute_brand,
	Search_path, enquiry_clicks_type,
	city_only, ecom_filter,
	rcmnd_srch, special_srchs,
	page_type,
	0 as kwd_cache_db,
	0 as kwd_default_cache, 
	0 as kwd_all_model_high, 
	0 as exact_kwd_category, 
	0 as kwd_custom_cache, 
	0 as unique_categorys,
	0 as unique_category_custom_cache,
	price_filter_enabled,
	price_intent,
	custom_price,
	min_price,
	max_price,
	result_count,
	Unique_Senders,
	prdsrc,
	no_sugg,
	qu_to,
	qu_comp,
	list_vw,
	category_to,
	qu_attr_to,
	compass_confidence,
	category_type,
	is_spec_filter_available,
	firstValue,
	second_value,
	is_group_filter_available,
	flavl_no_of_filters_displayed,
	spcfl_no_of_times_clicked,
	sprs_no_of_results_shown,
	position_of_filter_clicked,
	spec_filtername,
	spec_filtervalue,
	popular_filter_clicked,
	redirection,
	price_filter_position,
	search_redirection,
	price_bucket,
	locality_filter_present,
	locality_filter_clicked,
	annual_gst_turnover,
	gst_registration_date,
	distance_city,
	exact_match_present,
	user_id,
	enq_type,
	keyword_new,
	search_url_city
from (
select 
	modid, country,
	CASE
	    WHEN city_tier LIKE '1%' THEN 1
	    WHEN city_tier LIKE '2%' THEN 2
	    WHEN city_tier LIKE '3%' THEN 3
		when SEARCH_URL_CITY not ilike '%ALL INDIA%' then 4
		when SEARCH_URL_CITY ilike '%ALL INDIA%' then 5
	END AS city,
	CASE 
	    WHEN data_type = 3 
	        THEN FLOOR(positions)::INT
	    WHEN data_type != 3 THEN
	        CASE 
	            WHEN positions >= 1 AND positions <= 5 THEN 1
	            WHEN positions > 5 AND positions <= 10 THEN 2
	            WHEN positions > 10 AND positions <= 20 THEN 3
	            WHEN positions > 20 AND positions <= 30 THEN 4
	            WHEN positions > 30 AND positions <= 50 THEN 5
	            WHEN positions > 50 THEN 6
	            ELSE NULL
	        END
	    ELSE NULL
	END AS Position_Group,
	attr_srch_new as attribute_search,
	query_type_new as query_type,
	cd_user_mode,
	QU_CX as query_correction, 
	QU_TR as query_translation,
	category_id,
	CASE 
	    WHEN data_type = 3 THEN 
	        CASE 
	            WHEN url_resultcount_new ~ '^[0-9]+$' 
	                THEN url_resultcount_new::INT
	            ELSE NULL
	        END
	
	    WHEN data_type != 3 THEN 
	        CASE 
	            WHEN url_resultcount_new IN (0, 1) 
	                THEN url_resultcount_new::INT
	            WHEN url_resultcount_new > 1 
	                THEN 2
	            ELSE NULL
	        END
	
	    ELSE NULL
	END AS url_resultcount,
	CASE 
	    WHEN kwd_word_count BETWEEN 1 AND 5 THEN kwd_word_count
	    WHEN kwd_word_count > 5 THEN 5
	    ELSE NULL
	END AS Word_Count,
	biz_type_filter_new as business_filter ,
	trim(voice_search_language_new) as voice_search_language,
	CASE 
	    WHEN TRIM(kwd_type_new) IN ('N0', 'NO', 'n0') or trim(kwd_type_new) like 'N0-%' THEN 'N0'
	    ELSE TRIM(kwd_type_new)
	END AS keyword_type,
	attr_brand_new as Attribute_brand,
	src_path_new as Search_path ,
	trim(enquiry_click_type)  as enquiry_clicks_type,
	city_only,
	ecom_filter,
	rcmnd_srch ,
	special_srchs,
	page_type,
	price_filter_enabled,
	price_intent,
	custom_price,
--	min_price,
--	max_price, 
    CASE 
        WHEN min_price ~ '^[0-9]+$' THEN min_price::bigint
        ELSE null
    END AS min_price,
    CASE 
        WHEN max_price ~ '^[0-9]+$' THEN max_price::bigint
        ELSE null
    END AS max_price,
--	CASE
--	    WHEN result_count BETWEEN 1 AND 9 THEN result_count::int
--	    ELSE 0
--	END AS result_count,
	CASE
	    WHEN result_count ~ '^[0-9]+$' THEN result_count::int
	    ELSE 0
	END AS 
	result_count,
	sum(pageviews) pageviews, 
	sum(pdp_clicks) pdp_clicks,
	sum(event_catalogue_clicks) event_catalogue_clicks,
	sum(enquiry_cta_clicks) enquiry_cta_clicks,
	count(case when user_pseudo_id is not null and status in (1,2) then user_pseudo_id end) as total_users,
	count(distinct case when user_pseudo_id is not null and status in (1,2) then user_pseudo_id end) unique_users,
	sum(enquiries) as enquiries,
	sum(calls) as calls,
	count(distinct case when user_id is not null and status in (3,4) then user_id end) as Unique_Senders,
	prdsrc,
	no_sugg,
	qu_to,
	qu_comp,
	list_vw,
	category_to,
	qu_attr_to,
	compass_confidence,
	category_type,
	is_spec_filter_available,
	firstValue,
	second_value,
	is_group_filter_available,
	flavl_no_of_filters_displayed,
	spcfl_no_of_times_clicked,
	sprs_no_of_results_shown,
	position_of_filter_clicked,
	spec_filtername,
	spec_filtervalue,
	popular_filter_clicked,
	redirection,
	price_filter_position,
	search_redirection,
	price_bucket,
	locality_filter_present,
	locality_filter_clicked,
	annual_gst_turnover,
	gst_registration_date,
	distance_city,
	exact_match_present,
	keyword_new,
	search_url_city,
	user_id,
	enq_type

from
	staging.search_web_data
	where date between st_dt and en_dt
group by 	
	modid, country, city,
	Position_Group, attribute_search,
	query_type, cd_user_mode,
	query_correction, query_translation,
	category_id, url_resultcount, Word_Count,
	business_filter , voice_search_language,
	keyword_type, Attribute_brand,
	Search_path, enquiry_clicks_type,
	city_only, ecom_filter,
	rcmnd_srch, special_srchs,
	page_type,
	price_filter_enabled,
	price_intent,
	custom_price,
	min_price,
	max_price,
	result_count,
	prdsrc,
	no_sugg,
	qu_to,
	qu_comp,
	list_vw,
	category_to,
	qu_attr_to,
	compass_confidence,
	category_type,
	is_spec_filter_available,
	firstValue,
	second_value,
	is_group_filter_available,
	flavl_no_of_filters_displayed,
	spcfl_no_of_times_clicked,
	sprs_no_of_results_shown,
	position_of_filter_clicked,
	spec_filtername,
	spec_filtervalue,
	popular_filter_clicked,
	redirection,
	price_filter_position,
	search_redirection,
	price_bucket,
	locality_filter_present,
	locality_filter_clicked,
	annual_gst_turnover,
	gst_registration_date,
	distance_city,
	exact_match_present,
	keyword_new,
	search_url_city,
	user_id,
	enq_type

	) a 
		where modid in ('Mobile','Desktop','Android','IOS')
union all
select 
	st_dt start_date ,
	en_dt end_date ,
	datatype time_period_flag ,
	null, 0, 0,
	0, 0,
	0, 0,
	0, 0,
	0,
	0, 0,0,
	0,0,0,0,0,
	0, 0,
	0 , null,
	null, 0,
	null, null,
	0, 0,
	0, null,
	0,
	kwd_cache_db, kwd_default_cache, kwd_all_model_high, 
	exact_kwd_category, kwd_custom_cache, 
	unique_categorys, unique_category_custom_cache,
	0 as price_filter_enabled,
	0 as price_intent,
	0 as custom_price,
	0 as min_price,
	0 as max_price,
	0 as result_count,
	0 as Unique_senders,
	NULL::int            AS prdsrc,
	NULL::int            AS no_sugg,
	NULL::int            AS qu_to,
	NULL::int            AS qu_comp,
	NULL::int            AS list_vw,
	NULL::int            AS category_to,
	NULL::int            AS qu_attr_to,
	NULL::int   AS compass_confidence,
	NULL::int  AS category_type,
	NULL::int            AS is_spec_filter_available,
	NULL::varchar(200)  AS firstValue,
	NULL::varchar(200)  AS second_value,
	NULL::int            AS is_group_filter_available,
	NULL::int            AS flavl_no_of_filters_displayed,
	NULL::int            AS spcfl_no_of_times_clicked,
	NULL::int            AS sprs_no_of_results_shown,
	NULL::int            AS position_of_filter_clicked,
	NULL::varchar(200)   AS spec_filtername,
	NULL::varchar(200)   AS spec_filtervalue,
	NULL::int   AS popular_filter_clicked,
	NULL::varchar(200)   AS redirection,
	NULL::int   AS price_filter_position,
	NULL::int   AS search_redirection,
	NULL::varchar(200)   AS price_bucket,
	NULL::int            AS locality_filter_present,
	NULL::int            AS locality_filter_clicked,
	NULL::int   AS annual_gst_turnover,
	NULL::int   AS gst_registration_date,
	NULL::numeric(10,5)  AS distance_city,
	NULL::varchar(200)   AS exact_match_present,
	null::bigint as user_id,
	null::int as enq_type,
	null::varchar(1000) as keyword_new,
	null::varchar(500) as search_url_city

from 
	staging.category_search_cachedb

;



--
	commit;
	curr_table_end_time := current_timestamp at time zone 'Asia/Kolkata';
    insert into admin.dwh_refresh_status
	values('search_analytics.search_ctr','', curr_table_start_time, curr_table_end_time, true);
	commit;

	delete from staging.search_web_data;
	delete from staging.category_search_cachedb;
	delete from staging.stg_dir_query;
	delete from staging.stg_c2c_records;


end;







$$
;



------------------------ddl-------------------------------

DROP TABLE staging.search_web_data;
CREATE TABLE IF NOT EXISTS staging.search_web_data
(
	date DATE   ENCODE az64
	,status INTEGER   ENCODE az64
	,modid VARCHAR(10)   ENCODE lzo
	,keyword_new VARCHAR(1000)   ENCODE lzo
	,kwd_word_count INTEGER   ENCODE az64
	,country INTEGER   ENCODE az64
	,category_id INTEGER   ENCODE az64
	,search_url_city VARCHAR(500)   ENCODE lzo
	,qu_cx INTEGER   ENCODE lzo
	,qu_tr INTEGER   ENCODE lzo
	,biz_type_filter_new INTEGER   ENCODE az64
	,url_resultcount_new VARCHAR(500)   ENCODE lzo
	,voice_search_language_new VARCHAR(500)   ENCODE lzo
	,kwd_type_new VARCHAR(500)   ENCODE lzo
	,attr_srch_new INTEGER   ENCODE az64
	,attr_brand_new INTEGER   ENCODE az64
	,query_type_new INTEGER   ENCODE az64
	,src_path_new VARCHAR(500)   ENCODE lzo
	,enquiry_click_type VARCHAR(500)   ENCODE lzo
	,city_only INTEGER   ENCODE az64
	,ecom_filter INTEGER   ENCODE az64
	,rcmnd_srch INTEGER   ENCODE az64
	,cd_user_mode INTEGER   ENCODE az64
	,positions NUMERIC(10,5)   ENCODE az64
	,pageviews INTEGER   ENCODE az64
	,pdp_clicks INTEGER   ENCODE az64
	,event_catalogue_clicks INTEGER   ENCODE az64
	,enquiry_cta_clicks INTEGER   ENCODE az64
	,enquiries BIGINT   ENCODE az64
	,calls BIGINT   ENCODE az64
	,page_type INTEGER   ENCODE az64
	,special_srchs VARCHAR(255)   ENCODE lzo
	,city_tier VARCHAR(500)   ENCODE lzo
	,price_filter_enabled INTEGER   ENCODE az64
	,price_intent INTEGER   ENCODE az64
	,custom_price INTEGER   ENCODE az64
	,min_price VARCHAR(500)   ENCODE lzo
	,max_price VARCHAR(500)   ENCODE lzo
	,user_pseudo_id VARCHAR(500)   ENCODE lzo
	,result_count VARCHAR(500)   ENCODE lzo
	,user_id BIGINT   ENCODE az64
	,prdsrc INTEGER   ENCODE az64
	,no_sugg INTEGER   ENCODE az64
	,qu_to INTEGER   ENCODE az64
	,qu_comp INTEGER   ENCODE az64
	,list_vw INTEGER   ENCODE az64
	,category_to INTEGER   ENCODE az64
	,qu_attr_to INTEGER   ENCODE az64
	,compass_confidence INTEGER   ENCODE az64
	,category_type INTEGER   ENCODE az64
	,is_spec_filter_available INTEGER   ENCODE az64
	,firstvalue VARCHAR(200)   ENCODE lzo
	,second_value VARCHAR(200)   ENCODE lzo
	,is_group_filter_available INTEGER   ENCODE az64
	,flavl_no_of_filters_displayed INTEGER   ENCODE az64
	,spcfl_no_of_times_clicked INTEGER   ENCODE az64
	,sprs_no_of_results_shown INTEGER   ENCODE az64
	,position_of_filter_clicked INTEGER   ENCODE az64
	,spec_filtername VARCHAR(200)   ENCODE lzo
	,spec_filtervalue VARCHAR(200)   ENCODE lzo
	,popular_filter_clicked INTEGER   ENCODE az64
	,redirection VARCHAR(200)   ENCODE lzo
	,price_filter_position INTEGER   ENCODE az64
	,search_redirection INTEGER   ENCODE az64
	,price_bucket VARCHAR(200)   ENCODE lzo
	,locality_filter_present INTEGER   ENCODE az64
	,locality_filter_clicked INTEGER   ENCODE az64
	,annual_gst_turnover INTEGER   ENCODE az64
	,gst_registration_date INTEGER   ENCODE az64
	,distance_city NUMERIC(10,5)   ENCODE az64
	,exact_match_present VARCHAR(200)   ENCODE lzo
	,enq_type INTEGER   ENCODE az64
)

;




--DROP TABLE staging.stg_dir_query;
CREATE TABLE IF NOT EXISTS staging.stg_dir_query
(
	date DATE   ENCODE RAW
	,modid VARCHAR(20)   ENCODE bytedict
	,country VARCHAR(5)   ENCODE lzo
	,enquiry_id BIGINT   ENCODE az64
	,query_reference_url VARCHAR(4000)   ENCODE lzo
	,dir_query_categoryid INTEGER   ENCODE az64
	,cd_additional_data VARCHAR(4000)   ENCODE lzo
	,dir_query_login_mode SMALLINT   ENCODE az64
	,user_id BIGINT   ENCODE az64
	,distance_city NUMERIC(10,5)   ENCODE az64
	,enq_type SMALLINT   ENCODE az64
	,ref_url VARCHAR(4000)   ENCODE lzo
)

;
--DROP TABLE staging.stg_c2c_records;
CREATE TABLE IF NOT EXISTS staging.stg_c2c_records
(
	login_mode SMALLINT   ENCODE az64
	,c2c_call_time TIMESTAMP WITHOUT TIME ZONE   ENCODE RAW
	,c2c_caller_country_iso VARCHAR(5)   ENCODE lzo
	,cd_additional_data VARCHAR(4000)   ENCODE lzo
	,modid VARCHAR(20)   ENCODE bytedict
	,c2c_caller_glusr_id BIGINT   ENCODE az64
	,c2c_record_id BIGINT   ENCODE az64
	,c2c_caller_city_id INTEGER   ENCODE az64
	,c2c_receiver_city_id INTEGER   ENCODE az64
	,ref_url VARCHAR(4000)   ENCODE lzo
	,c2c_record_type INTEGER   ENCODE az64
	,fk_c2c_record_unidentified_id NUMERIC(18,0)   ENCODE az64
)
;


