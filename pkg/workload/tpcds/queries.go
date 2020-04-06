// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tpcds

// NumQueries specifies the number of queries in TPC-DS benchmark.
const NumQueries = 99

// QueriesByNumber is a mapping from the number of a TPC-DS query to the actual
// query. Only queries that can be parsed by CockroachDB are present.
var QueriesByNumber = map[int]string{
	1:  query1,
	2:  query2,
	3:  query3,
	4:  query4,
	5:  query5,
	6:  query6,
	7:  query7,
	8:  query8,
	9:  query9,
	10: query10,
	11: query11,
	12: query12,
	13: query13,
	14: query14,
	15: query15,
	16: query16,
	17: query17,
	18: query18,
	19: query19,
	20: query20,
	21: query21,
	22: query22,
	23: query23,
	24: query24,
	25: query25,
	26: query26,
	// TODO(yuzefovich): adjust the query.
	//27: query27,
	28: query28,
	29: query29,
	30: query30,
	31: query31,
	32: query32,
	33: query33,
	34: query34,
	35: query35,
	// TODO(yuzefovich): adjust the query.
	//36: query36,
	37: query37,
	38: query38,
	39: query39,
	40: query40,
	41: query41,
	42: query42,
	43: query43,
	44: query44,
	45: query45,
	46: query46,
	47: query47,
	48: query48,
	49: query49,
	50: query50,
	51: query51,
	52: query52,
	53: query53,
	54: query54,
	55: query55,
	56: query56,
	57: query57,
	58: query58,
	59: query59,
	60: query60,
	61: query61,
	62: query62,
	63: query63,
	64: query64,
	65: query65,
	66: query66,
	67: query67,
	68: query68,
	69: query69,
	// TODO(yuzefovich): adjust the query.
	//70: query70,
	71: query71,
	72: query72,
	73: query73,
	74: query74,
	75: query75,
	76: query76,
	77: query77,
	78: query78,
	79: query79,
	80: query80,
	81: query81,
	82: query82,
	83: query83,
	84: query84,
	85: query85,
	// TODO(yuzefovich): adjust the query.
	//86: query86,
	87: query87,
	88: query88,
	89: query89,
	90: query90,
	91: query91,
	92: query92,
	93: query93,
	94: query94,
	95: query95,
	96: query96,
	97: query97,
	98: query98,
	99: query99,
}

// TODO(yuzefovich): remove once these queries are "enabled."
var (
	_ = query27
	_ = query36
	_ = query70
	_ = query86
)

const (
	query1 = `
WITH
    customer_total_return
        AS (
            SELECT
                sr_customer_sk AS ctr_customer_sk,
                sr_store_sk AS ctr_store_sk,
                sum(sr_fee) AS ctr_total_return
            FROM
                store_returns, date_dim
            WHERE
                sr_returned_date_sk = d_date_sk
                AND d_year = 2000
            GROUP BY
                sr_customer_sk, sr_store_sk
        )
SELECT
    c_customer_id
FROM
    customer_total_return AS ctr1, store, customer
WHERE
    ctr1.ctr_total_return
    > (
            SELECT
                avg(ctr_total_return) * 1.2
            FROM
                customer_total_return AS ctr2
            WHERE
                ctr1.ctr_store_sk = ctr2.ctr_store_sk
        )
    AND s_store_sk = ctr1.ctr_store_sk
    AND s_state = 'TN'
    AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY
    c_customer_id
LIMIT
    100;
`

	query2 = `
WITH
	wscs
		AS (
			SELECT
				sold_date_sk, sales_price
			FROM
				(
					SELECT
						ws_sold_date_sk AS sold_date_sk,
						ws_ext_sales_price AS sales_price
					FROM
						web_sales
					UNION ALL
						SELECT
							cs_sold_date_sk AS sold_date_sk,
							cs_ext_sales_price
								AS sales_price
						FROM
							catalog_sales
				)
		),
	wswscs
		AS (
			SELECT
				d_week_seq,
				sum(
					CASE
					WHEN (d_day_name = 'Sunday')
					THEN sales_price
					ELSE NULL
					END
				)
					AS sun_sales,
				sum(
					CASE
					WHEN (d_day_name = 'Monday')
					THEN sales_price
					ELSE NULL
					END
				)
					AS mon_sales,
				sum(
					CASE
					WHEN (d_day_name = 'Tuesday')
					THEN sales_price
					ELSE NULL
					END
				)
					AS tue_sales,
				sum(
					CASE
					WHEN (d_day_name = 'Wednesday')
					THEN sales_price
					ELSE NULL
					END
				)
					AS wed_sales,
				sum(
					CASE
					WHEN (d_day_name = 'Thursday')
					THEN sales_price
					ELSE NULL
					END
				)
					AS thu_sales,
				sum(
					CASE
					WHEN (d_day_name = 'Friday')
					THEN sales_price
					ELSE NULL
					END
				)
					AS fri_sales,
				sum(
					CASE
					WHEN (d_day_name = 'Saturday')
					THEN sales_price
					ELSE NULL
					END
				)
					AS sat_sales
			FROM
				wscs, date_dim
			WHERE
				d_date_sk = sold_date_sk
			GROUP BY
				d_week_seq
		)
SELECT
	d_week_seq1,
	round(sun_sales1 / sun_sales2, 2),
	round(mon_sales1 / mon_sales2, 2),
	round(tue_sales1 / tue_sales2, 2),
	round(wed_sales1 / wed_sales2, 2),
	round(thu_sales1 / thu_sales2, 2),
	round(fri_sales1 / fri_sales2, 2),
	round(sat_sales1 / sat_sales2, 2)
FROM
	(
		SELECT
			wswscs.d_week_seq AS d_week_seq1,
			sun_sales AS sun_sales1,
			mon_sales AS mon_sales1,
			tue_sales AS tue_sales1,
			wed_sales AS wed_sales1,
			thu_sales AS thu_sales1,
			fri_sales AS fri_sales1,
			sat_sales AS sat_sales1
		FROM
			wswscs, date_dim
		WHERE
			date_dim.d_week_seq = wswscs.d_week_seq
			AND d_year = 1998
	)
		AS y,
	(
		SELECT
			wswscs.d_week_seq AS d_week_seq2,
			sun_sales AS sun_sales2,
			mon_sales AS mon_sales2,
			tue_sales AS tue_sales2,
			wed_sales AS wed_sales2,
			thu_sales AS thu_sales2,
			fri_sales AS fri_sales2,
			sat_sales AS sat_sales2
		FROM
			wswscs, date_dim
		WHERE
			date_dim.d_week_seq = wswscs.d_week_seq
			AND d_year = 1998 + 1
	)
		AS z
WHERE
	d_week_seq1 = d_week_seq2 - 53
ORDER BY
	d_week_seq1;
`

	query3 = `
SELECT
	dt.d_year,
	item.i_brand_id AS brand_id,
	item.i_brand AS brand,
	sum(ss_sales_price) AS sum_agg
FROM
	date_dim AS dt, store_sales, item
WHERE
	dt.d_date_sk = store_sales.ss_sold_date_sk
	AND store_sales.ss_item_sk = item.i_item_sk
	AND item.i_manufact_id = 816
	AND dt.d_moy = 11
GROUP BY
	dt.d_year, item.i_brand, item.i_brand_id
ORDER BY
	dt.d_year, sum_agg DESC, brand_id
LIMIT
	100;
`

	query4 = `
WITH
	year_total
		AS (
			SELECT
				c_customer_id AS customer_id,
				c_first_name AS customer_first_name,
				c_last_name AS customer_last_name,
				c_preferred_cust_flag
					AS customer_preferred_cust_flag,
				c_birth_country AS customer_birth_country,
				c_login AS customer_login,
				c_email_address AS customer_email_address,
				d_year AS dyear,
				sum(
					(
						ss_ext_list_price
						- ss_ext_wholesale_cost
						- ss_ext_discount_amt
						+ ss_ext_sales_price
					)
					/ 2
				)
					AS year_total,
				's' AS sale_type
			FROM
				customer, store_sales, date_dim
			WHERE
				c_customer_sk = ss_customer_sk
				AND ss_sold_date_sk = d_date_sk
			GROUP BY
				c_customer_id,
				c_first_name,
				c_last_name,
				c_preferred_cust_flag,
				c_birth_country,
				c_login,
				c_email_address,
				d_year
			UNION ALL
				SELECT
					c_customer_id AS customer_id,
					c_first_name AS customer_first_name,
					c_last_name AS customer_last_name,
					c_preferred_cust_flag
						AS customer_preferred_cust_flag,
					c_birth_country
						AS customer_birth_country,
					c_login AS customer_login,
					c_email_address
						AS customer_email_address,
					d_year AS dyear,
					sum(
						(
							cs_ext_list_price
							- cs_ext_wholesale_cost
							- cs_ext_discount_amt
							+ cs_ext_sales_price
						)
						/ 2
					)
						AS year_total,
					'c' AS sale_type
				FROM
					customer, catalog_sales, date_dim
				WHERE
					c_customer_sk = cs_bill_customer_sk
					AND cs_sold_date_sk = d_date_sk
				GROUP BY
					c_customer_id,
					c_first_name,
					c_last_name,
					c_preferred_cust_flag,
					c_birth_country,
					c_login,
					c_email_address,
					d_year
			UNION ALL
				SELECT
					c_customer_id AS customer_id,
					c_first_name AS customer_first_name,
					c_last_name AS customer_last_name,
					c_preferred_cust_flag
						AS customer_preferred_cust_flag,
					c_birth_country
						AS customer_birth_country,
					c_login AS customer_login,
					c_email_address
						AS customer_email_address,
					d_year AS dyear,
					sum(
						(
							ws_ext_list_price
							- ws_ext_wholesale_cost
							- ws_ext_discount_amt
							+ ws_ext_sales_price
						)
						/ 2
					)
						AS year_total,
					'w' AS sale_type
				FROM
					customer, web_sales, date_dim
				WHERE
					c_customer_sk = ws_bill_customer_sk
					AND ws_sold_date_sk = d_date_sk
				GROUP BY
					c_customer_id,
					c_first_name,
					c_last_name,
					c_preferred_cust_flag,
					c_birth_country,
					c_login,
					c_email_address,
					d_year
		)
SELECT
	t_s_secyear.customer_id,
	t_s_secyear.customer_first_name,
	t_s_secyear.customer_last_name,
	t_s_secyear.customer_birth_country
FROM
	year_total AS t_s_firstyear,
	year_total AS t_s_secyear,
	year_total AS t_c_firstyear,
	year_total AS t_c_secyear,
	year_total AS t_w_firstyear,
	year_total AS t_w_secyear
WHERE
	t_s_secyear.customer_id = t_s_firstyear.customer_id
	AND t_s_firstyear.customer_id = t_c_secyear.customer_id
	AND t_s_firstyear.customer_id
		= t_c_firstyear.customer_id
	AND t_s_firstyear.customer_id
		= t_w_firstyear.customer_id
	AND t_s_firstyear.customer_id = t_w_secyear.customer_id
	AND t_s_firstyear.sale_type = 's'
	AND t_c_firstyear.sale_type = 'c'
	AND t_w_firstyear.sale_type = 'w'
	AND t_s_secyear.sale_type = 's'
	AND t_c_secyear.sale_type = 'c'
	AND t_w_secyear.sale_type = 'w'
	AND t_s_firstyear.dyear = 1999
	AND t_s_secyear.dyear = 1999 + 1
	AND t_c_firstyear.dyear = 1999
	AND t_c_secyear.dyear = 1999 + 1
	AND t_w_firstyear.dyear = 1999
	AND t_w_secyear.dyear = 1999 + 1
	AND t_s_firstyear.year_total > 0
	AND t_c_firstyear.year_total > 0
	AND t_w_firstyear.year_total > 0
	AND CASE
		WHEN t_c_firstyear.year_total > 0
		THEN t_c_secyear.year_total
		/ t_c_firstyear.year_total
		ELSE NULL
		END
		> CASE
			WHEN t_s_firstyear.year_total > 0
			THEN t_s_secyear.year_total
			/ t_s_firstyear.year_total
			ELSE NULL
			END
	AND CASE
		WHEN t_c_firstyear.year_total > 0
		THEN t_c_secyear.year_total
		/ t_c_firstyear.year_total
		ELSE NULL
		END
		> CASE
			WHEN t_w_firstyear.year_total > 0
			THEN t_w_secyear.year_total
			/ t_w_firstyear.year_total
			ELSE NULL
			END
ORDER BY
	t_s_secyear.customer_id,
	t_s_secyear.customer_first_name,
	t_s_secyear.customer_last_name,
	t_s_secyear.customer_birth_country
LIMIT
	100;
`
	// NOTE: I added conversion of 14 days to an interval.
	query5 = `
WITH
	ssr
		AS (
			SELECT
				s_store_id,
				sum(sales_price) AS sales,
				sum(profit) AS profit,
				sum(return_amt) AS returns,
				sum(net_loss) AS profit_loss
			FROM
				(
					SELECT
						ss_store_sk AS store_sk,
						ss_sold_date_sk AS date_sk,
						ss_ext_sales_price AS sales_price,
						ss_net_profit AS profit,
						CAST(0 AS DECIMAL(7,2))
							AS return_amt,
						CAST(0 AS DECIMAL(7,2)) AS net_loss
					FROM
						store_sales
					UNION ALL
						SELECT
							sr_store_sk AS store_sk,
							sr_returned_date_sk AS date_sk,
							CAST(0 AS DECIMAL(7,2))
								AS sales_price,
							CAST(0 AS DECIMAL(7,2))
								AS profit,
							sr_return_amt AS return_amt,
							sr_net_loss AS net_loss
						FROM
							store_returns
				)
					AS salesreturns,
				date_dim,
				store
			WHERE
				date_sk = d_date_sk
				AND d_date BETWEEN CAST('2000-08-19' AS DATE) AND (CAST('2000-08-19' AS DATE) + '14 days'::INTERVAL)
				AND store_sk = s_store_sk
			GROUP BY
				s_store_id
		),
	csr
		AS (
			SELECT
				cp_catalog_page_id,
				sum(sales_price) AS sales,
				sum(profit) AS profit,
				sum(return_amt) AS returns,
				sum(net_loss) AS profit_loss
			FROM
				(
					SELECT
						cs_catalog_page_sk AS page_sk,
						cs_sold_date_sk AS date_sk,
						cs_ext_sales_price AS sales_price,
						cs_net_profit AS profit,
						CAST(0 AS DECIMAL(7,2))
							AS return_amt,
						CAST(0 AS DECIMAL(7,2)) AS net_loss
					FROM
						catalog_sales
					UNION ALL
						SELECT
							cr_catalog_page_sk AS page_sk,
							cr_returned_date_sk AS date_sk,
							CAST(0 AS DECIMAL(7,2))
								AS sales_price,
							CAST(0 AS DECIMAL(7,2))
								AS profit,
							cr_return_amount AS return_amt,
							cr_net_loss AS net_loss
						FROM
							catalog_returns
				)
					AS salesreturns,
				date_dim,
				catalog_page
			WHERE
				date_sk = d_date_sk
				AND d_date BETWEEN CAST('2000-08-19' AS DATE) AND (CAST('2000-08-19' AS DATE) + '14 days'::INTERVAL)
				AND page_sk = cp_catalog_page_sk
			GROUP BY
				cp_catalog_page_id
		),
	wsr
		AS (
			SELECT
				web_site_id,
				sum(sales_price) AS sales,
				sum(profit) AS profit,
				sum(return_amt) AS returns,
				sum(net_loss) AS profit_loss
			FROM
				(
					SELECT
						ws_web_site_sk AS wsr_web_site_sk,
						ws_sold_date_sk AS date_sk,
						ws_ext_sales_price AS sales_price,
						ws_net_profit AS profit,
						CAST(0 AS DECIMAL(7,2))
							AS return_amt,
						CAST(0 AS DECIMAL(7,2)) AS net_loss
					FROM
						web_sales
					UNION ALL
						SELECT
							ws_web_site_sk
								AS wsr_web_site_sk,
							wr_returned_date_sk AS date_sk,
							CAST(0 AS DECIMAL(7,2))
								AS sales_price,
							CAST(0 AS DECIMAL(7,2))
								AS profit,
							wr_return_amt AS return_amt,
							wr_net_loss AS net_loss
						FROM
							web_returns
							LEFT JOIN web_sales ON
									wr_item_sk = ws_item_sk
									AND wr_order_number
										= ws_order_number
				)
					AS salesreturns,
				date_dim,
				web_site
			WHERE
				date_sk = d_date_sk
				AND d_date BETWEEN CAST('2000-08-19' AS DATE) AND (CAST('2000-08-19' AS DATE) + '14 days'::INTERVAL)
				AND wsr_web_site_sk = web_site_sk
			GROUP BY
				web_site_id
		)
SELECT
	channel,
	id,
	sum(sales) AS sales,
	sum(returns) AS returns,
	sum(profit) AS profit
FROM
	(
		SELECT
			'store channel' AS channel,
			'store' || s_store_id AS id,
			sales,
			returns,
			profit - profit_loss AS profit
		FROM
			ssr
		UNION ALL
			SELECT
				'catalog channel' AS channel,
				'catalog_page' || cp_catalog_page_id AS id,
				sales,
				returns,
				profit - profit_loss AS profit
			FROM
				csr
		UNION ALL
			SELECT
				'web channel' AS channel,
				'web_site' || web_site_id AS id,
				sales,
				returns,
				profit - profit_loss AS profit
			FROM
				wsr
	)
		AS x
GROUP BY
	rollup(channel, id)
ORDER BY
	channel, id
LIMIT
	100;
`

	query6 = `
SELECT
	a.ca_state AS state, count(*) AS cnt
FROM
	customer_address AS a,
	customer AS c,
	store_sales AS s,
	date_dim AS d,
	item AS i
WHERE
	a.ca_address_sk = c.c_current_addr_sk
	AND c.c_customer_sk = s.ss_customer_sk
	AND s.ss_sold_date_sk = d.d_date_sk
	AND s.ss_item_sk = i.i_item_sk
	AND d.d_month_seq
		= (
				SELECT
					DISTINCT d_month_seq
				FROM
					date_dim
				WHERE
					d_year = 2002 AND d_moy = 3
			)
	AND i.i_current_price
		> 1.2
			* (
					SELECT
						avg(j.i_current_price)
					FROM
						item AS j
					WHERE
						j.i_category = i.i_category
				)
GROUP BY
	a.ca_state
HAVING
	count(*) >= 10
ORDER BY
	cnt, a.ca_state
LIMIT
	100;
`

	query7 = `
SELECT
	i_item_id,
	avg(ss_quantity) AS agg1,
	avg(ss_list_price) AS agg2,
	avg(ss_coupon_amt) AS agg3,
	avg(ss_sales_price) AS agg4
FROM
	store_sales,
	customer_demographics,
	date_dim,
	item,
	promotion
WHERE
	ss_sold_date_sk = d_date_sk
	AND ss_item_sk = i_item_sk
	AND ss_cdemo_sk = cd_demo_sk
	AND ss_promo_sk = p_promo_sk
	AND cd_gender = 'F'
	AND cd_marital_status = 'W'
	AND cd_education_status = 'College'
	AND (p_channel_email = 'N' OR p_channel_event = 'N')
	AND d_year = 2001
GROUP BY
	i_item_id
ORDER BY
	i_item_id
LIMIT
	100;
`

	query8 = `
SELECT
	s_store_name, sum(ss_net_profit)
FROM
	store_sales,
	date_dim,
	store,
	(
		SELECT
			ca_zip
		FROM
			(
				SELECT
					substr(ca_zip, 1, 5) AS ca_zip
				FROM
					customer_address
				WHERE
					substr(ca_zip, 1, 5)
					IN (
							'47602',
							'16704',
							'35863',
							'28577',
							'83910',
							'36201',
							'58412',
							'48162',
							'28055',
							'41419',
							'80332',
							'38607',
							'77817',
							'24891',
							'16226',
							'18410',
							'21231',
							'59345',
							'13918',
							'51089',
							'20317',
							'17167',
							'54585',
							'67881',
							'78366',
							'47770',
							'18360',
							'51717',
							'73108',
							'14440',
							'21800',
							'89338',
							'45859',
							'65501',
							'34948',
							'25973',
							'73219',
							'25333',
							'17291',
							'10374',
							'18829',
							'60736',
							'82620',
							'41351',
							'52094',
							'19326',
							'25214',
							'54207',
							'40936',
							'21814',
							'79077',
							'25178',
							'75742',
							'77454',
							'30621',
							'89193',
							'27369',
							'41232',
							'48567',
							'83041',
							'71948',
							'37119',
							'68341',
							'14073',
							'16891',
							'62878',
							'49130',
							'19833',
							'24286',
							'27700',
							'40979',
							'50412',
							'81504',
							'94835',
							'84844',
							'71954',
							'39503',
							'57649',
							'18434',
							'24987',
							'12350',
							'86379',
							'27413',
							'44529',
							'98569',
							'16515',
							'27287',
							'24255',
							'21094',
							'16005',
							'56436',
							'91110',
							'68293',
							'56455',
							'54558',
							'10298',
							'83647',
							'32754',
							'27052',
							'51766',
							'19444',
							'13869',
							'45645',
							'94791',
							'57631',
							'20712',
							'37788',
							'41807',
							'46507',
							'21727',
							'71836',
							'81070',
							'50632',
							'88086',
							'63991',
							'20244',
							'31655',
							'51782',
							'29818',
							'63792',
							'68605',
							'94898',
							'36430',
							'57025',
							'20601',
							'82080',
							'33869',
							'22728',
							'35834',
							'29086',
							'92645',
							'98584',
							'98072',
							'11652',
							'78093',
							'57553',
							'43830',
							'71144',
							'53565',
							'18700',
							'90209',
							'71256',
							'38353',
							'54364',
							'28571',
							'96560',
							'57839',
							'56355',
							'50679',
							'45266',
							'84680',
							'34306',
							'34972',
							'48530',
							'30106',
							'15371',
							'92380',
							'84247',
							'92292',
							'68852',
							'13338',
							'34594',
							'82602',
							'70073',
							'98069',
							'85066',
							'47289',
							'11686',
							'98862',
							'26217',
							'47529',
							'63294',
							'51793',
							'35926',
							'24227',
							'14196',
							'24594',
							'32489',
							'99060',
							'49472',
							'43432',
							'49211',
							'14312',
							'88137',
							'47369',
							'56877',
							'20534',
							'81755',
							'15794',
							'12318',
							'21060',
							'73134',
							'41255',
							'63073',
							'81003',
							'73873',
							'66057',
							'51184',
							'51195',
							'45676',
							'92696',
							'70450',
							'90669',
							'98338',
							'25264',
							'38919',
							'59226',
							'58581',
							'60298',
							'17895',
							'19489',
							'52301',
							'80846',
							'95464',
							'68770',
							'51634',
							'19988',
							'18367',
							'18421',
							'11618',
							'67975',
							'25494',
							'41352',
							'95430',
							'15734',
							'62585',
							'97173',
							'33773',
							'10425',
							'75675',
							'53535',
							'17879',
							'41967',
							'12197',
							'67998',
							'79658',
							'59130',
							'72592',
							'14851',
							'43933',
							'68101',
							'50636',
							'25717',
							'71286',
							'24660',
							'58058',
							'72991',
							'95042',
							'15543',
							'33122',
							'69280',
							'11912',
							'59386',
							'27642',
							'65177',
							'17672',
							'33467',
							'64592',
							'36335',
							'54010',
							'18767',
							'63193',
							'42361',
							'49254',
							'33113',
							'33159',
							'36479',
							'59080',
							'11855',
							'81963',
							'31016',
							'49140',
							'29392',
							'41836',
							'32958',
							'53163',
							'13844',
							'73146',
							'23952',
							'65148',
							'93498',
							'14530',
							'46131',
							'58454',
							'13376',
							'13378',
							'83986',
							'12320',
							'17193',
							'59852',
							'46081',
							'98533',
							'52389',
							'13086',
							'68843',
							'31013',
							'13261',
							'60560',
							'13443',
							'45533',
							'83583',
							'11489',
							'58218',
							'19753',
							'22911',
							'25115',
							'86709',
							'27156',
							'32669',
							'13123',
							'51933',
							'39214',
							'41331',
							'66943',
							'14155',
							'69998',
							'49101',
							'70070',
							'35076',
							'14242',
							'73021',
							'59494',
							'15782',
							'29752',
							'37914',
							'74686',
							'83086',
							'34473',
							'15751',
							'81084',
							'49230',
							'91894',
							'60624',
							'17819',
							'28810',
							'63180',
							'56224',
							'39459',
							'55233',
							'75752',
							'43639',
							'55349',
							'86057',
							'62361',
							'50788',
							'31830',
							'58062',
							'18218',
							'85761',
							'60083',
							'45484',
							'21204',
							'90229',
							'70041',
							'41162',
							'35390',
							'16364',
							'39500',
							'68908',
							'26689',
							'52868',
							'81335',
							'40146',
							'11340',
							'61527',
							'61794',
							'71997',
							'30415',
							'59004',
							'29450',
							'58117',
							'69952',
							'33562',
							'83833',
							'27385',
							'61860',
							'96435',
							'48333',
							'23065',
							'32961',
							'84919',
							'61997',
							'99132',
							'22815',
							'56600',
							'68730',
							'48017',
							'95694',
							'32919',
							'88217',
							'27116',
							'28239',
							'58032',
							'18884',
							'16791',
							'21343',
							'97462',
							'18569',
							'75660',
							'15475'
						)
				INTERSECT
					SELECT
						ca_zip
					FROM
						(
							SELECT
								substr(ca_zip, 1, 5)
									AS ca_zip,
								count(*) AS cnt
							FROM
								customer_address, customer
							WHERE
								ca_address_sk
								= c_current_addr_sk
								AND c_preferred_cust_flag
									= 'Y'
							GROUP BY
								ca_zip
							HAVING
								count(*) > 10
						)
							AS a1
			)
				AS a2
	)
		AS v1
WHERE
	ss_store_sk = s_store_sk
	AND ss_sold_date_sk = d_date_sk
	AND d_qoy = 2
	AND d_year = 1998
	AND substr(s_zip, 1, 2) = substr(v1.ca_zip, 1, 2)
GROUP BY
	s_store_name
ORDER BY
	s_store_name
LIMIT
	100;
`

	query9 = `
SELECT
	CASE
	WHEN (
		SELECT
			count(*)
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 1 AND 20
	)
	> 1071
	THEN (
		SELECT
			avg(ss_ext_tax)
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 1 AND 20
	)
	ELSE (
		SELECT
			avg(ss_net_paid_inc_tax)
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 1 AND 20
	)
	END
		AS bucket1,
	CASE
	WHEN (
		SELECT
			count(*)
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 21 AND 40
	)
	> 39161
	THEN (
		SELECT
			avg(ss_ext_tax)
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 21 AND 40
	)
	ELSE (
		SELECT
			avg(ss_net_paid_inc_tax)
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 21 AND 40
	)
	END
		AS bucket2,
	CASE
	WHEN (
		SELECT
			count(*)
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 41 AND 60
	)
	> 29434
	THEN (
		SELECT
			avg(ss_ext_tax)
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 41 AND 60
	)
	ELSE (
		SELECT
			avg(ss_net_paid_inc_tax)
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 41 AND 60
	)
	END
		AS bucket3,
	CASE
	WHEN (
		SELECT
			count(*)
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 61 AND 80
	)
	> 6568
	THEN (
		SELECT
			avg(ss_ext_tax)
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 61 AND 80
	)
	ELSE (
		SELECT
			avg(ss_net_paid_inc_tax)
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 61 AND 80
	)
	END
		AS bucket4,
	CASE
	WHEN (
		SELECT
			count(*)
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 81 AND 100
	)
	> 21216
	THEN (
		SELECT
			avg(ss_ext_tax)
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 81 AND 100
	)
	ELSE (
		SELECT
			avg(ss_net_paid_inc_tax)
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 81 AND 100
	)
	END
		AS bucket5
FROM
	reason
WHERE
	r_reason_sk = 1;
`

	query10 = `
SELECT
	cd_gender,
	cd_marital_status,
	cd_education_status,
	count(*) AS cnt1,
	cd_purchase_estimate,
	count(*) AS cnt2,
	cd_credit_rating,
	count(*) AS cnt3,
	cd_dep_count,
	count(*) AS cnt4,
	cd_dep_employed_count,
	count(*) AS cnt5,
	cd_dep_college_count,
	count(*) AS cnt6
FROM
	customer AS c,
	customer_address AS ca,
	customer_demographics
WHERE
	c.c_current_addr_sk = ca.ca_address_sk
	AND ca_county
		IN (
				'Fairfield County',
				'Campbell County',
				'Washtenaw County',
				'Escambia County',
				'Cleburne County'
			)
	AND cd_demo_sk = c.c_current_cdemo_sk
	AND EXISTS(
			SELECT
				*
			FROM
				store_sales, date_dim
			WHERE
				c.c_customer_sk = ss_customer_sk
				AND ss_sold_date_sk = d_date_sk
				AND d_year = 2001
				AND d_moy BETWEEN 3 AND (3 + 3)
		)
	AND (
			EXISTS(
				SELECT
					*
				FROM
					web_sales, date_dim
				WHERE
					c.c_customer_sk = ws_bill_customer_sk
					AND ws_sold_date_sk = d_date_sk
					AND d_year = 2001
					AND d_moy BETWEEN 3 AND (3 + 3)
			)
			OR EXISTS(
					SELECT
						*
					FROM
						catalog_sales, date_dim
					WHERE
						c.c_customer_sk
						= cs_ship_customer_sk
						AND cs_sold_date_sk = d_date_sk
						AND d_year = 2001
						AND d_moy BETWEEN 3 AND (3 + 3)
				)
		)
GROUP BY
	cd_gender,
	cd_marital_status,
	cd_education_status,
	cd_purchase_estimate,
	cd_credit_rating,
	cd_dep_count,
	cd_dep_employed_count,
	cd_dep_college_count
ORDER BY
	cd_gender,
	cd_marital_status,
	cd_education_status,
	cd_purchase_estimate,
	cd_credit_rating,
	cd_dep_count,
	cd_dep_employed_count,
	cd_dep_college_count
LIMIT
	100;
`

	query11 = `
WITH
	year_total
		AS (
			SELECT
				c_customer_id AS customer_id,
				c_first_name AS customer_first_name,
				c_last_name AS customer_last_name,
				c_preferred_cust_flag
					AS customer_preferred_cust_flag,
				c_birth_country AS customer_birth_country,
				c_login AS customer_login,
				c_email_address AS customer_email_address,
				d_year AS dyear,
				sum(ss_ext_list_price - ss_ext_discount_amt)
					AS year_total,
				's' AS sale_type
			FROM
				customer, store_sales, date_dim
			WHERE
				c_customer_sk = ss_customer_sk
				AND ss_sold_date_sk = d_date_sk
			GROUP BY
				c_customer_id,
				c_first_name,
				c_last_name,
				c_preferred_cust_flag,
				c_birth_country,
				c_login,
				c_email_address,
				d_year
			UNION ALL
				SELECT
					c_customer_id AS customer_id,
					c_first_name AS customer_first_name,
					c_last_name AS customer_last_name,
					c_preferred_cust_flag
						AS customer_preferred_cust_flag,
					c_birth_country
						AS customer_birth_country,
					c_login AS customer_login,
					c_email_address
						AS customer_email_address,
					d_year AS dyear,
					sum(
						ws_ext_list_price
						- ws_ext_discount_amt
					)
						AS year_total,
					'w' AS sale_type
				FROM
					customer, web_sales, date_dim
				WHERE
					c_customer_sk = ws_bill_customer_sk
					AND ws_sold_date_sk = d_date_sk
				GROUP BY
					c_customer_id,
					c_first_name,
					c_last_name,
					c_preferred_cust_flag,
					c_birth_country,
					c_login,
					c_email_address,
					d_year
		)
SELECT
	t_s_secyear.customer_id,
	t_s_secyear.customer_first_name,
	t_s_secyear.customer_last_name,
	t_s_secyear.customer_email_address
FROM
	year_total AS t_s_firstyear,
	year_total AS t_s_secyear,
	year_total AS t_w_firstyear,
	year_total AS t_w_secyear
WHERE
	t_s_secyear.customer_id = t_s_firstyear.customer_id
	AND t_s_firstyear.customer_id = t_w_secyear.customer_id
	AND t_s_firstyear.customer_id
		= t_w_firstyear.customer_id
	AND t_s_firstyear.sale_type = 's'
	AND t_w_firstyear.sale_type = 'w'
	AND t_s_secyear.sale_type = 's'
	AND t_w_secyear.sale_type = 'w'
	AND t_s_firstyear.dyear = 1998
	AND t_s_secyear.dyear = 1998 + 1
	AND t_w_firstyear.dyear = 1998
	AND t_w_secyear.dyear = 1998 + 1
	AND t_s_firstyear.year_total > 0
	AND t_w_firstyear.year_total > 0
	AND CASE
		WHEN t_w_firstyear.year_total > 0
		THEN t_w_secyear.year_total
		/ t_w_firstyear.year_total
		ELSE 0.0
		END
		> CASE
			WHEN t_s_firstyear.year_total > 0
			THEN t_s_secyear.year_total
			/ t_s_firstyear.year_total
			ELSE 0.0
			END
ORDER BY
	t_s_secyear.customer_id,
	t_s_secyear.customer_first_name,
	t_s_secyear.customer_last_name,
	t_s_secyear.customer_email_address
LIMIT
	100;
`

	// NOTE: I added conversion of 30 days to an interval.
	query12 = `
SELECT
	i_item_id,
	i_item_desc,
	i_category,
	i_class,
	i_current_price,
	sum(ws_ext_sales_price) AS itemrevenue,
	sum(ws_ext_sales_price) * 100
	/ sum(sum(ws_ext_sales_price)) OVER (
			PARTITION BY i_class
		)
		AS revenueratio
FROM
	web_sales, item, date_dim
WHERE
	ws_item_sk = i_item_sk
	AND i_category IN ('Men', 'Books', 'Electronics')
	AND ws_sold_date_sk = d_date_sk
	AND d_date BETWEEN CAST('2001-06-15' AS DATE) AND (CAST('2001-06-15' AS DATE) + '30 days'::INTERVAL)
GROUP BY
	i_item_id,
	i_item_desc,
	i_category,
	i_class,
	i_current_price
ORDER BY
	i_category,
	i_class,
	i_item_id,
	i_item_desc,
	revenueratio
LIMIT
	100;
`

	query13 = `
SELECT
	avg(ss_quantity),
	avg(ss_ext_sales_price),
	avg(ss_ext_wholesale_cost),
	sum(ss_ext_wholesale_cost)
FROM
	store_sales,
	store,
	customer_demographics,
	household_demographics,
	customer_address,
	date_dim
WHERE
	s_store_sk = ss_store_sk
	AND ss_sold_date_sk = d_date_sk
	AND d_year = 2001
	AND (
			(
				ss_hdemo_sk = hd_demo_sk
				AND cd_demo_sk = ss_cdemo_sk
				AND cd_marital_status = 'M'
				AND cd_education_status = 'College'
				AND ss_sales_price BETWEEN 100.00 AND 150.00
				AND hd_dep_count = 3
			)
			OR (
					ss_hdemo_sk = hd_demo_sk
					AND cd_demo_sk = ss_cdemo_sk
					AND cd_marital_status = 'D'
					AND cd_education_status = 'Primary'
					AND ss_sales_price BETWEEN 50.00 AND 100.00
					AND hd_dep_count = 1
				)
			OR (
					ss_hdemo_sk = hd_demo_sk
					AND cd_demo_sk = ss_cdemo_sk
					AND cd_marital_status = 'W'
					AND cd_education_status = '2 yr Degree'
					AND ss_sales_price BETWEEN 150.00 AND 200.00
					AND hd_dep_count = 1
				)
		)
	AND (
			(
				ss_addr_sk = ca_address_sk
				AND ca_country = 'United States'
				AND ca_state IN ('IL', 'TN', 'TX')
				AND ss_net_profit BETWEEN 100 AND 200
			)
			OR (
					ss_addr_sk = ca_address_sk
					AND ca_country = 'United States'
					AND ca_state IN ('WY', 'OH', 'ID')
					AND ss_net_profit BETWEEN 150 AND 300
				)
			OR (
					ss_addr_sk = ca_address_sk
					AND ca_country = 'United States'
					AND ca_state IN ('MS', 'SC', 'IA')
					AND ss_net_profit BETWEEN 50 AND 250
				)
		);
`

	query14 = `
WITH
	cross_items
		AS (
			SELECT
				i_item_sk AS ss_item_sk
			FROM
				item,
				(
					SELECT
						iss.i_brand_id AS brand_id,
						iss.i_class_id AS class_id,
						iss.i_category_id AS category_id
					FROM
						store_sales,
						item AS iss,
						date_dim AS d1
					WHERE
						ss_item_sk = iss.i_item_sk
						AND ss_sold_date_sk = d1.d_date_sk
						AND d1.d_year BETWEEN 1999 AND (1999 + 2)
					INTERSECT
						SELECT
							ics.i_brand_id,
							ics.i_class_id,
							ics.i_category_id
						FROM
							catalog_sales,
							item AS ics,
							date_dim AS d2
						WHERE
							cs_item_sk = ics.i_item_sk
							AND cs_sold_date_sk
								= d2.d_date_sk
							AND d2.d_year BETWEEN 1999 AND (1999 + 2)
					INTERSECT
						SELECT
							iws.i_brand_id,
							iws.i_class_id,
							iws.i_category_id
						FROM
							web_sales,
							item AS iws,
							date_dim AS d3
						WHERE
							ws_item_sk = iws.i_item_sk
							AND ws_sold_date_sk
								= d3.d_date_sk
							AND d3.d_year BETWEEN 1999 AND (1999 + 2)
				)
			WHERE
				i_brand_id = brand_id
				AND i_class_id = class_id
				AND i_category_id = category_id
		),
	avg_sales
		AS (
			SELECT
				avg(quantity * list_price) AS average_sales
			FROM
				(
					SELECT
						ss_quantity AS quantity,
						ss_list_price AS list_price
					FROM
						store_sales, date_dim
					WHERE
						ss_sold_date_sk = d_date_sk
						AND d_year BETWEEN 1999 AND (1999 + 2)
					UNION ALL
						SELECT
							cs_quantity AS quantity,
							cs_list_price AS list_price
						FROM
							catalog_sales, date_dim
						WHERE
							cs_sold_date_sk = d_date_sk
							AND d_year BETWEEN 1999 AND (1999 + 2)
					UNION ALL
						SELECT
							ws_quantity AS quantity,
							ws_list_price AS list_price
						FROM
							web_sales, date_dim
						WHERE
							ws_sold_date_sk = d_date_sk
							AND d_year BETWEEN 1999 AND (1999 + 2)
				)
					AS x
		)
SELECT
	channel,
	i_brand_id,
	i_class_id,
	i_category_id,
	sum(sales),
	sum(number_sales)
FROM
	(
		SELECT
			'store' AS channel,
			i_brand_id,
			i_class_id,
			i_category_id,
			sum(ss_quantity * ss_list_price) AS sales,
			count(*) AS number_sales
		FROM
			store_sales, item, date_dim
		WHERE
			ss_item_sk
			IN (SELECT ss_item_sk FROM cross_items)
			AND ss_item_sk = i_item_sk
			AND ss_sold_date_sk = d_date_sk
			AND d_year = 1999 + 2
			AND d_moy = 11
		GROUP BY
			i_brand_id, i_class_id, i_category_id
		HAVING
			sum(ss_quantity * ss_list_price)
			> (SELECT average_sales FROM avg_sales)
		UNION ALL
			SELECT
				'catalog' AS channel,
				i_brand_id,
				i_class_id,
				i_category_id,
				sum(cs_quantity * cs_list_price) AS sales,
				count(*) AS number_sales
			FROM
				catalog_sales, item, date_dim
			WHERE
				cs_item_sk
				IN (SELECT ss_item_sk FROM cross_items)
				AND cs_item_sk = i_item_sk
				AND cs_sold_date_sk = d_date_sk
				AND d_year = 1999 + 2
				AND d_moy = 11
			GROUP BY
				i_brand_id, i_class_id, i_category_id
			HAVING
				sum(cs_quantity * cs_list_price)
				> (SELECT average_sales FROM avg_sales)
		UNION ALL
			SELECT
				'web' AS channel,
				i_brand_id,
				i_class_id,
				i_category_id,
				sum(ws_quantity * ws_list_price) AS sales,
				count(*) AS number_sales
			FROM
				web_sales, item, date_dim
			WHERE
				ws_item_sk
				IN (SELECT ss_item_sk FROM cross_items)
				AND ws_item_sk = i_item_sk
				AND ws_sold_date_sk = d_date_sk
				AND d_year = 1999 + 2
				AND d_moy = 11
			GROUP BY
				i_brand_id, i_class_id, i_category_id
			HAVING
				sum(ws_quantity * ws_list_price)
				> (SELECT average_sales FROM avg_sales)
	)
		AS y
GROUP BY
	rollup(channel, i_brand_id, i_class_id, i_category_id)
ORDER BY
	channel, i_brand_id, i_class_id, i_category_id
LIMIT
	100;
WITH
	cross_items
		AS (
			SELECT
				i_item_sk AS ss_item_sk
			FROM
				item,
				(
					SELECT
						iss.i_brand_id AS brand_id,
						iss.i_class_id AS class_id,
						iss.i_category_id AS category_id
					FROM
						store_sales,
						item AS iss,
						date_dim AS d1
					WHERE
						ss_item_sk = iss.i_item_sk
						AND ss_sold_date_sk = d1.d_date_sk
						AND d1.d_year BETWEEN 1999 AND (1999 + 2)
					INTERSECT
						SELECT
							ics.i_brand_id,
							ics.i_class_id,
							ics.i_category_id
						FROM
							catalog_sales,
							item AS ics,
							date_dim AS d2
						WHERE
							cs_item_sk = ics.i_item_sk
							AND cs_sold_date_sk
								= d2.d_date_sk
							AND d2.d_year BETWEEN 1999 AND (1999 + 2)
					INTERSECT
						SELECT
							iws.i_brand_id,
							iws.i_class_id,
							iws.i_category_id
						FROM
							web_sales,
							item AS iws,
							date_dim AS d3
						WHERE
							ws_item_sk = iws.i_item_sk
							AND ws_sold_date_sk
								= d3.d_date_sk
							AND d3.d_year BETWEEN 1999 AND (1999 + 2)
				)
					AS x
			WHERE
				i_brand_id = brand_id
				AND i_class_id = class_id
				AND i_category_id = category_id
		),
	avg_sales
		AS (
			SELECT
				avg(quantity * list_price) AS average_sales
			FROM
				(
					SELECT
						ss_quantity AS quantity,
						ss_list_price AS list_price
					FROM
						store_sales, date_dim
					WHERE
						ss_sold_date_sk = d_date_sk
						AND d_year BETWEEN 1999 AND (1999 + 2)
					UNION ALL
						SELECT
							cs_quantity AS quantity,
							cs_list_price AS list_price
						FROM
							catalog_sales, date_dim
						WHERE
							cs_sold_date_sk = d_date_sk
							AND d_year BETWEEN 1999 AND (1999 + 2)
					UNION ALL
						SELECT
							ws_quantity AS quantity,
							ws_list_price AS list_price
						FROM
							web_sales, date_dim
						WHERE
							ws_sold_date_sk = d_date_sk
							AND d_year BETWEEN 1999 AND (1999 + 2)
				)
					AS x
		)
SELECT
	this_year.channel AS ty_channel,
	this_year.i_brand_id AS ty_brand,
	this_year.i_class_id AS ty_class,
	this_year.i_category_id AS ty_category,
	this_year.sales AS ty_sales,
	this_year.number_sales AS ty_number_sales,
	last_year.channel AS ly_channel,
	last_year.i_brand_id AS ly_brand,
	last_year.i_class_id AS ly_class,
	last_year.i_category_id AS ly_category,
	last_year.sales AS ly_sales,
	last_year.number_sales AS ly_number_sales
FROM
	(
		SELECT
			'store' AS channel,
			i_brand_id,
			i_class_id,
			i_category_id,
			sum(ss_quantity * ss_list_price) AS sales,
			count(*) AS number_sales
		FROM
			store_sales, item, date_dim
		WHERE
			ss_item_sk
			IN (SELECT ss_item_sk FROM cross_items)
			AND ss_item_sk = i_item_sk
			AND ss_sold_date_sk = d_date_sk
			AND d_week_seq
				= (
						SELECT
							d_week_seq
						FROM
							date_dim
						WHERE
							d_year = 1999 + 1
							AND d_moy = 12
							AND d_dom = 3
					)
		GROUP BY
			i_brand_id, i_class_id, i_category_id
		HAVING
			sum(ss_quantity * ss_list_price)
			> (SELECT average_sales FROM avg_sales)
	)
		AS this_year,
	(
		SELECT
			'store' AS channel,
			i_brand_id,
			i_class_id,
			i_category_id,
			sum(ss_quantity * ss_list_price) AS sales,
			count(*) AS number_sales
		FROM
			store_sales, item, date_dim
		WHERE
			ss_item_sk
			IN (SELECT ss_item_sk FROM cross_items)
			AND ss_item_sk = i_item_sk
			AND ss_sold_date_sk = d_date_sk
			AND d_week_seq
				= (
						SELECT
							d_week_seq
						FROM
							date_dim
						WHERE
							d_year = 1999
							AND d_moy = 12
							AND d_dom = 3
					)
		GROUP BY
			i_brand_id, i_class_id, i_category_id
		HAVING
			sum(ss_quantity * ss_list_price)
			> (SELECT average_sales FROM avg_sales)
	)
		AS last_year
WHERE
	this_year.i_brand_id = last_year.i_brand_id
	AND this_year.i_class_id = last_year.i_class_id
	AND this_year.i_category_id = last_year.i_category_id
ORDER BY
	this_year.channel,
	this_year.i_brand_id,
	this_year.i_class_id,
	this_year.i_category_id
LIMIT
	100;
`

	query15 = `
SELECT
	ca_zip, sum(cs_sales_price)
FROM
	catalog_sales, customer, customer_address, date_dim
WHERE
	cs_bill_customer_sk = c_customer_sk
	AND c_current_addr_sk = ca_address_sk
	AND (
			substr(ca_zip, 1, 5)
			IN (
					'85669',
					'86197',
					'88274',
					'83405',
					'86475',
					'85392',
					'85460',
					'80348',
					'81792'
				)
			OR ca_state IN ('CA', 'WA', 'GA')
			OR cs_sales_price > 500
		)
	AND cs_sold_date_sk = d_date_sk
	AND d_qoy = 2
	AND d_year = 2001
GROUP BY
	ca_zip
ORDER BY
	ca_zip
LIMIT
	100;
`

	// NOTE: I added conversion of 60 days to an interval.
	query16 = `
SELECT
	count(DISTINCT cs_order_number) AS "order count",
	sum(cs_ext_ship_cost) AS "total shipping cost",
	sum(cs_net_profit) AS "total net profit"
FROM
	catalog_sales AS cs1,
	date_dim,
	customer_address,
	call_center
WHERE
	d_date BETWEEN '2002-4-01' AND (CAST('2002-4-01' AS DATE) + '60 days'::INTERVAL)
	AND cs1.cs_ship_date_sk = d_date_sk
	AND cs1.cs_ship_addr_sk = ca_address_sk
	AND ca_state = 'PA'
	AND cs1.cs_call_center_sk = cc_call_center_sk
	AND cc_county
		IN (
				'Williamson County',
				'Williamson County',
				'Williamson County',
				'Williamson County',
				'Williamson County'
			)
	AND EXISTS(
			SELECT
				*
			FROM
				catalog_sales AS cs2
			WHERE
				cs1.cs_order_number = cs2.cs_order_number
				AND cs1.cs_warehouse_sk
					!= cs2.cs_warehouse_sk
		)
	AND NOT
			EXISTS(
				SELECT
					*
				FROM
					catalog_returns AS cr1
				WHERE
					cs1.cs_order_number
					= cr1.cr_order_number
			)
ORDER BY
	count(DISTINCT cs_order_number)
LIMIT
	100;
`

	query17 = `
SELECT
	i_item_id,
	i_item_desc,
	s_state,
	count(ss_quantity) AS store_sales_quantitycount,
	avg(ss_quantity) AS store_sales_quantityave,
	stddev_samp(ss_quantity) AS store_sales_quantitystdev,
	stddev_samp(ss_quantity) / avg(ss_quantity)
		AS store_sales_quantitycov,
	count(sr_return_quantity)
		AS store_returns_quantitycount,
	avg(sr_return_quantity) AS store_returns_quantityave,
	stddev_samp(sr_return_quantity)
		AS store_returns_quantitystdev,
	stddev_samp(sr_return_quantity)
	/ avg(sr_return_quantity)
		AS store_returns_quantitycov,
	count(cs_quantity) AS catalog_sales_quantitycount,
	avg(cs_quantity) AS catalog_sales_quantityave,
	stddev_samp(cs_quantity) AS catalog_sales_quantitystdev,
	stddev_samp(cs_quantity) / avg(cs_quantity)
		AS catalog_sales_quantitycov
FROM
	store_sales,
	store_returns,
	catalog_sales,
	date_dim AS d1,
	date_dim AS d2,
	date_dim AS d3,
	store,
	item
WHERE
	d1.d_quarter_name = '2001Q1'
	AND d1.d_date_sk = ss_sold_date_sk
	AND i_item_sk = ss_item_sk
	AND s_store_sk = ss_store_sk
	AND ss_customer_sk = sr_customer_sk
	AND ss_item_sk = sr_item_sk
	AND ss_ticket_number = sr_ticket_number
	AND sr_returned_date_sk = d2.d_date_sk
	AND d2.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3')
	AND sr_customer_sk = cs_bill_customer_sk
	AND sr_item_sk = cs_item_sk
	AND cs_sold_date_sk = d3.d_date_sk
	AND d3.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3')
GROUP BY
	i_item_id, i_item_desc, s_state
ORDER BY
	i_item_id, i_item_desc, s_state
LIMIT
	100;
`

	query18 = `
SELECT
	i_item_id,
	ca_country,
	ca_state,
	ca_county,
	avg(CAST(cs_quantity AS DECIMAL(12,2))) AS agg1,
	avg(CAST(cs_list_price AS DECIMAL(12,2))) AS agg2,
	avg(CAST(cs_coupon_amt AS DECIMAL(12,2))) AS agg3,
	avg(CAST(cs_sales_price AS DECIMAL(12,2))) AS agg4,
	avg(CAST(cs_net_profit AS DECIMAL(12,2))) AS agg5,
	avg(CAST(c_birth_year AS DECIMAL(12,2))) AS agg6,
	avg(CAST(cd1.cd_dep_count AS DECIMAL(12,2))) AS agg7
FROM
	catalog_sales,
	customer_demographics AS cd1,
	customer_demographics AS cd2,
	customer,
	customer_address,
	date_dim,
	item
WHERE
	cs_sold_date_sk = d_date_sk
	AND cs_item_sk = i_item_sk
	AND cs_bill_cdemo_sk = cd1.cd_demo_sk
	AND cs_bill_customer_sk = c_customer_sk
	AND cd1.cd_gender = 'F'
	AND cd1.cd_education_status = 'Primary'
	AND c_current_cdemo_sk = cd2.cd_demo_sk
	AND c_current_addr_sk = ca_address_sk
	AND c_birth_month IN (1, 3, 7, 11, 10, 4)
	AND d_year = 2001
	AND ca_state
		IN ('AL', 'MO', 'TN', 'GA', 'MT', 'IN', 'CA')
GROUP BY
	rollup(i_item_id, ca_country, ca_state, ca_county)
ORDER BY
	ca_country, ca_state, ca_county, i_item_id
LIMIT
	100;
`

	query19 = `
SELECT
	i_brand_id AS brand_id,
	i_brand AS brand,
	i_manufact_id,
	i_manufact,
	sum(ss_ext_sales_price) AS ext_price
FROM
	date_dim,
	store_sales,
	item,
	customer,
	customer_address,
	store
WHERE
	d_date_sk = ss_sold_date_sk
	AND ss_item_sk = i_item_sk
	AND i_manager_id = 14
	AND d_moy = 11
	AND d_year = 2002
	AND ss_customer_sk = c_customer_sk
	AND c_current_addr_sk = ca_address_sk
	AND substr(ca_zip, 1, 5) != substr(s_zip, 1, 5)
	AND ss_store_sk = s_store_sk
GROUP BY
	i_brand, i_brand_id, i_manufact_id, i_manufact
ORDER BY
	ext_price DESC,
	i_brand,
	i_brand_id,
	i_manufact_id,
	i_manufact
LIMIT
	100;
`

	// NOTE: I added conversion of 30 days to an interval.
	query20 = `
SELECT
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price,
    sum(cs_ext_sales_price) AS itemrevenue,
    sum(cs_ext_sales_price) * 100
    / sum(sum(cs_ext_sales_price)) OVER (
            PARTITION BY i_class
        )
        AS revenueratio
FROM
    catalog_sales, item, date_dim
WHERE
    cs_item_sk = i_item_sk
    AND i_category IN ('Books', 'Music', 'Sports')
    AND cs_sold_date_sk = d_date_sk
    AND d_date BETWEEN CAST('2002-06-18' AS DATE) AND (CAST('2002-06-18' AS DATE) + '30 days'::INTERVAL)
GROUP BY
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price
ORDER BY
    i_category,
    i_class,
    i_item_id,
    i_item_desc,
    revenueratio
LIMIT
    100;
`

	// NOTE: I added conversion of 30 days to an interval.
	query21 = `
SELECT
	*
FROM
	(
		SELECT
			w_warehouse_name,
			i_item_id,
			sum(
				CASE
				WHEN (
					CAST(d_date AS DATE)
					< CAST('1999-06-22' AS DATE)
				)
				THEN inv_quantity_on_hand
				ELSE 0
				END
			)
				AS inv_before,
			sum(
				CASE
				WHEN (
					CAST(d_date AS DATE)
					>= CAST('1999-06-22' AS DATE)
				)
				THEN inv_quantity_on_hand
				ELSE 0
				END
			)
				AS inv_after
		FROM
			inventory, warehouse, item, date_dim
		WHERE
			i_current_price BETWEEN 0.99 AND 1.49
			AND i_item_sk = inv_item_sk
			AND inv_warehouse_sk = w_warehouse_sk
			AND inv_date_sk = d_date_sk
			AND d_date BETWEEN (CAST('1999-06-22' AS DATE) - '30 days'::INTERVAL) AND (CAST('1999-06-22' AS DATE) + '30 days'::INTERVAL)
		GROUP BY
			w_warehouse_name, i_item_id
	)
		AS x
WHERE
	(CASE WHEN inv_before > 0 THEN inv_after / inv_before ELSE NULL END) BETWEEN (2.0 / 3.0) AND (3.0 / 2.0)
ORDER BY
	w_warehouse_name, i_item_id
LIMIT
	100;
`

	query22 = `
SELECT
	i_product_name,
	i_brand,
	i_class,
	i_category,
	avg(inv_quantity_on_hand) AS qoh
FROM
	inventory, date_dim, item
WHERE
	inv_date_sk = d_date_sk
	AND inv_item_sk = i_item_sk
	AND d_month_seq BETWEEN 1200 AND (1200 + 11)
GROUP BY
	rollup(i_product_name, i_brand, i_class, i_category)
ORDER BY
	qoh, i_product_name, i_brand, i_class, i_category
LIMIT
	100;
`

	query23 = `
WITH
	frequent_ss_items
		AS (
			SELECT
				substr(i_item_desc, 1, 30) AS itemdesc,
				i_item_sk AS item_sk,
				d_date AS solddate,
				count(*) AS cnt
			FROM
				store_sales, date_dim, item
			WHERE
				ss_sold_date_sk = d_date_sk
				AND ss_item_sk = i_item_sk
				AND d_year
					IN (2000, 2000 + 1, 2000 + 2, 2000 + 3)
			GROUP BY
				substr(i_item_desc, 1, 30),
				i_item_sk,
				d_date
			HAVING
				count(*) > 4
		),
	max_store_sales
		AS (
			SELECT
				max(csales) AS tpcds_cmax
			FROM
				(
					SELECT
						c_customer_sk,
						sum(ss_quantity * ss_sales_price)
							AS csales
					FROM
						store_sales, customer, date_dim
					WHERE
						ss_customer_sk = c_customer_sk
						AND ss_sold_date_sk = d_date_sk
						AND d_year
							IN (
									2000,
									2000 + 1,
									2000 + 2,
									2000 + 3
								)
					GROUP BY
						c_customer_sk
				)
		),
	best_ss_customer
		AS (
			SELECT
				c_customer_sk,
				sum(ss_quantity * ss_sales_price) AS ssales
			FROM
				store_sales, customer
			WHERE
				ss_customer_sk = c_customer_sk
			GROUP BY
				c_customer_sk
			HAVING
				sum(ss_quantity * ss_sales_price)
				> 95 / 100.0
					* (SELECT * FROM max_store_sales)
		)
SELECT
	sum(sales)
FROM
	(
		SELECT
			cs_quantity * cs_list_price AS sales
		FROM
			catalog_sales, date_dim
		WHERE
			d_year = 2000
			AND d_moy = 7
			AND cs_sold_date_sk = d_date_sk
			AND cs_item_sk
				IN (SELECT item_sk FROM frequent_ss_items)
			AND cs_bill_customer_sk
				IN (
						SELECT
							c_customer_sk
						FROM
							best_ss_customer
					)
		UNION ALL
			SELECT
				ws_quantity * ws_list_price AS sales
			FROM
				web_sales, date_dim
			WHERE
				d_year = 2000
				AND d_moy = 7
				AND ws_sold_date_sk = d_date_sk
				AND ws_item_sk
					IN (
							SELECT
								item_sk
							FROM
								frequent_ss_items
						)
				AND ws_bill_customer_sk
					IN (
							SELECT
								c_customer_sk
							FROM
								best_ss_customer
						)
	)
LIMIT
	100;
WITH
	frequent_ss_items
		AS (
			SELECT
				substr(i_item_desc, 1, 30) AS itemdesc,
				i_item_sk AS item_sk,
				d_date AS solddate,
				count(*) AS cnt
			FROM
				store_sales, date_dim, item
			WHERE
				ss_sold_date_sk = d_date_sk
				AND ss_item_sk = i_item_sk
				AND d_year
					IN (2000, 2000 + 1, 2000 + 2, 2000 + 3)
			GROUP BY
				substr(i_item_desc, 1, 30),
				i_item_sk,
				d_date
			HAVING
				count(*) > 4
		),
	max_store_sales
		AS (
			SELECT
				max(csales) AS tpcds_cmax
			FROM
				(
					SELECT
						c_customer_sk,
						sum(ss_quantity * ss_sales_price)
							AS csales
					FROM
						store_sales, customer, date_dim
					WHERE
						ss_customer_sk = c_customer_sk
						AND ss_sold_date_sk = d_date_sk
						AND d_year
							IN (
									2000,
									2000 + 1,
									2000 + 2,
									2000 + 3
								)
					GROUP BY
						c_customer_sk
				)
		),
	best_ss_customer
		AS (
			SELECT
				c_customer_sk,
				sum(ss_quantity * ss_sales_price) AS ssales
			FROM
				store_sales, customer
			WHERE
				ss_customer_sk = c_customer_sk
			GROUP BY
				c_customer_sk
			HAVING
				sum(ss_quantity * ss_sales_price)
				> 95 / 100.0
					* (SELECT * FROM max_store_sales)
		)
SELECT
	c_last_name, c_first_name, sales
FROM
	(
		SELECT
			c_last_name,
			c_first_name,
			sum(cs_quantity * cs_list_price) AS sales
		FROM
			catalog_sales, customer, date_dim
		WHERE
			d_year = 2000
			AND d_moy = 7
			AND cs_sold_date_sk = d_date_sk
			AND cs_item_sk
				IN (SELECT item_sk FROM frequent_ss_items)
			AND cs_bill_customer_sk
				IN (
						SELECT
							c_customer_sk
						FROM
							best_ss_customer
					)
			AND cs_bill_customer_sk = c_customer_sk
		GROUP BY
			c_last_name, c_first_name
		UNION ALL
			SELECT
				c_last_name,
				c_first_name,
				sum(ws_quantity * ws_list_price) AS sales
			FROM
				web_sales, customer, date_dim
			WHERE
				d_year = 2000
				AND d_moy = 7
				AND ws_sold_date_sk = d_date_sk
				AND ws_item_sk
					IN (
							SELECT
								item_sk
							FROM
								frequent_ss_items
						)
				AND ws_bill_customer_sk
					IN (
							SELECT
								c_customer_sk
							FROM
								best_ss_customer
						)
				AND ws_bill_customer_sk = c_customer_sk
			GROUP BY
				c_last_name, c_first_name
	)
ORDER BY
	c_last_name, c_first_name, sales
LIMIT
	100;
`

	query24 = `
WITH
	ssales
		AS (
			SELECT
				c_last_name,
				c_first_name,
				s_store_name,
				ca_state,
				s_state,
				i_color,
				i_current_price,
				i_manager_id,
				i_units,
				i_size,
				sum(ss_net_paid) AS netpaid
			FROM
				store_sales,
				store_returns,
				store,
				item,
				customer,
				customer_address
			WHERE
				ss_ticket_number = sr_ticket_number
				AND ss_item_sk = sr_item_sk
				AND ss_customer_sk = c_customer_sk
				AND ss_item_sk = i_item_sk
				AND ss_store_sk = s_store_sk
				AND c_current_addr_sk = ca_address_sk
				AND c_birth_country != upper(ca_country)
				AND s_zip = ca_zip
				AND s_market_id = 5
			GROUP BY
				c_last_name,
				c_first_name,
				s_store_name,
				ca_state,
				s_state,
				i_color,
				i_current_price,
				i_manager_id,
				i_units,
				i_size
		)
SELECT
	c_last_name,
	c_first_name,
	s_store_name,
	sum(netpaid) AS paid
FROM
	ssales
WHERE
	i_color = 'aquamarine'
GROUP BY
	c_last_name, c_first_name, s_store_name
HAVING
	sum(netpaid) > (SELECT 0.05 * avg(netpaid) FROM ssales)
ORDER BY
	c_last_name, c_first_name, s_store_name;
WITH
	ssales
		AS (
			SELECT
				c_last_name,
				c_first_name,
				s_store_name,
				ca_state,
				s_state,
				i_color,
				i_current_price,
				i_manager_id,
				i_units,
				i_size,
				sum(ss_net_paid) AS netpaid
			FROM
				store_sales,
				store_returns,
				store,
				item,
				customer,
				customer_address
			WHERE
				ss_ticket_number = sr_ticket_number
				AND ss_item_sk = sr_item_sk
				AND ss_customer_sk = c_customer_sk
				AND ss_item_sk = i_item_sk
				AND ss_store_sk = s_store_sk
				AND c_current_addr_sk = ca_address_sk
				AND c_birth_country != upper(ca_country)
				AND s_zip = ca_zip
				AND s_market_id = 5
			GROUP BY
				c_last_name,
				c_first_name,
				s_store_name,
				ca_state,
				s_state,
				i_color,
				i_current_price,
				i_manager_id,
				i_units,
				i_size
		)
SELECT
	c_last_name,
	c_first_name,
	s_store_name,
	sum(netpaid) AS paid
FROM
	ssales
WHERE
	i_color = 'seashell'
GROUP BY
	c_last_name, c_first_name, s_store_name
HAVING
	sum(netpaid) > (SELECT 0.05 * avg(netpaid) FROM ssales)
ORDER BY
	c_last_name, c_first_name, s_store_name;
`

	query25 = `
SELECT
	i_item_id,
	i_item_desc,
	s_store_id,
	s_store_name,
	max(ss_net_profit) AS store_sales_profit,
	max(sr_net_loss) AS store_returns_loss,
	max(cs_net_profit) AS catalog_sales_profit
FROM
	store_sales,
	store_returns,
	catalog_sales,
	date_dim AS d1,
	date_dim AS d2,
	date_dim AS d3,
	store,
	item
WHERE
	d1.d_moy = 4
	AND d1.d_year = 1999
	AND d1.d_date_sk = ss_sold_date_sk
	AND i_item_sk = ss_item_sk
	AND s_store_sk = ss_store_sk
	AND ss_customer_sk = sr_customer_sk
	AND ss_item_sk = sr_item_sk
	AND ss_ticket_number = sr_ticket_number
	AND sr_returned_date_sk = d2.d_date_sk
	AND d2.d_moy BETWEEN 4 AND 10
	AND d2.d_year = 1999
	AND sr_customer_sk = cs_bill_customer_sk
	AND sr_item_sk = cs_item_sk
	AND cs_sold_date_sk = d3.d_date_sk
	AND d3.d_moy BETWEEN 4 AND 10
	AND d3.d_year = 1999
GROUP BY
	i_item_id, i_item_desc, s_store_id, s_store_name
ORDER BY
	i_item_id, i_item_desc, s_store_id, s_store_name
LIMIT
	100;
`

	query26 = `
SELECT
	i_item_id,
	avg(cs_quantity) AS agg1,
	avg(cs_list_price) AS agg2,
	avg(cs_coupon_amt) AS agg3,
	avg(cs_sales_price) AS agg4
FROM
	catalog_sales,
	customer_demographics,
	date_dim,
	item,
	promotion
WHERE
	cs_sold_date_sk = d_date_sk
	AND cs_item_sk = i_item_sk
	AND cs_bill_cdemo_sk = cd_demo_sk
	AND cs_promo_sk = p_promo_sk
	AND cd_gender = 'M'
	AND cd_marital_status = 'W'
	AND cd_education_status = 'Unknown'
	AND (p_channel_email = 'N' OR p_channel_event = 'N')
	AND d_year = 2002
GROUP BY
	i_item_id
ORDER BY
	i_item_id
LIMIT
	100;
`

	// TODO(yuzefovich): modify it to be parsed by CRDB.
	query27 = `
select  i_item_id,
        s_state, grouping(s_state) g_state,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
 from store_sales, customer_demographics, date_dim, store, item
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_store_sk = s_store_sk and
       ss_cdemo_sk = cd_demo_sk and
       cd_gender = 'M' and
       cd_marital_status = 'W' and
       cd_education_status = 'Secondary' and
       d_year = 1999 and
       s_state in ('TN','TN', 'TN', 'TN', 'TN', 'TN')
 group by rollup (i_item_id, s_state)
 order by i_item_id
         ,s_state
 limit 100;
`

	query28 = `
SELECT
	*
FROM
	(
		SELECT
			avg(ss_list_price) AS b1_lp,
			count(ss_list_price) AS b1_cnt,
			count(DISTINCT ss_list_price) AS b1_cntd
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 0 AND 5
			AND (
					ss_list_price BETWEEN 107 AND (107 + 10)
					OR ss_coupon_amt BETWEEN 1319 AND (1319 + 1000)
					OR ss_wholesale_cost BETWEEN 60 AND (60 + 20)
				)
	)
		AS b1,
	(
		SELECT
			avg(ss_list_price) AS b2_lp,
			count(ss_list_price) AS b2_cnt,
			count(DISTINCT ss_list_price) AS b2_cntd
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 6 AND 10
			AND (
					ss_list_price BETWEEN 23 AND (23 + 10)
					OR ss_coupon_amt BETWEEN 825 AND (825 + 1000)
					OR ss_wholesale_cost BETWEEN 43 AND (43 + 20)
				)
	)
		AS b2,
	(
		SELECT
			avg(ss_list_price) AS b3_lp,
			count(ss_list_price) AS b3_cnt,
			count(DISTINCT ss_list_price) AS b3_cntd
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 11 AND 15
			AND (
					ss_list_price BETWEEN 74 AND (74 + 10)
					OR ss_coupon_amt BETWEEN 4381 AND (4381 + 1000)
					OR ss_wholesale_cost BETWEEN 57 AND (57 + 20)
				)
	)
		AS b3,
	(
		SELECT
			avg(ss_list_price) AS b4_lp,
			count(ss_list_price) AS b4_cnt,
			count(DISTINCT ss_list_price) AS b4_cntd
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 16 AND 20
			AND (
					ss_list_price BETWEEN 89 AND (89 + 10)
					OR ss_coupon_amt BETWEEN 3117 AND (3117 + 1000)
					OR ss_wholesale_cost BETWEEN 68 AND (68 + 20)
				)
	)
		AS b4,
	(
		SELECT
			avg(ss_list_price) AS b5_lp,
			count(ss_list_price) AS b5_cnt,
			count(DISTINCT ss_list_price) AS b5_cntd
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 21 AND 25
			AND (
					ss_list_price BETWEEN 58 AND (58 + 10)
					OR ss_coupon_amt BETWEEN 9402 AND (9402 + 1000)
					OR ss_wholesale_cost BETWEEN 38 AND (38 + 20)
				)
	)
		AS b5,
	(
		SELECT
			avg(ss_list_price) AS b6_lp,
			count(ss_list_price) AS b6_cnt,
			count(DISTINCT ss_list_price) AS b6_cntd
		FROM
			store_sales
		WHERE
			ss_quantity BETWEEN 26 AND 30
			AND (
					ss_list_price BETWEEN 64 AND (64 + 10)
					OR ss_coupon_amt BETWEEN 5792 AND (5792 + 1000)
					OR ss_wholesale_cost BETWEEN 73 AND (73 + 20)
				)
	)
		AS b6
LIMIT
	100;
`

	query29 = `
SELECT
	i_item_id,
	i_item_desc,
	s_store_id,
	s_store_name,
	max(ss_quantity) AS store_sales_quantity,
	max(sr_return_quantity) AS store_returns_quantity,
	max(cs_quantity) AS catalog_sales_quantity
FROM
	store_sales,
	store_returns,
	catalog_sales,
	date_dim AS d1,
	date_dim AS d2,
	date_dim AS d3,
	store,
	item
WHERE
	d1.d_moy = 4
	AND d1.d_year = 1998
	AND d1.d_date_sk = ss_sold_date_sk
	AND i_item_sk = ss_item_sk
	AND s_store_sk = ss_store_sk
	AND ss_customer_sk = sr_customer_sk
	AND ss_item_sk = sr_item_sk
	AND ss_ticket_number = sr_ticket_number
	AND sr_returned_date_sk = d2.d_date_sk
	AND d2.d_moy BETWEEN 4 AND (4 + 3)
	AND d2.d_year = 1998
	AND sr_customer_sk = cs_bill_customer_sk
	AND sr_item_sk = cs_item_sk
	AND cs_sold_date_sk = d3.d_date_sk
	AND d3.d_year IN (1998, 1998 + 1, 1998 + 2)
GROUP BY
	i_item_id, i_item_desc, s_store_id, s_store_name
ORDER BY
	i_item_id, i_item_desc, s_store_id, s_store_name
LIMIT
	100;
`

	query30 = `
WITH
	customer_total_return
		AS (
			SELECT
				wr_returning_customer_sk AS ctr_customer_sk,
				ca_state AS ctr_state,
				sum(wr_return_amt) AS ctr_total_return
			FROM
				web_returns, date_dim, customer_address
			WHERE
				wr_returned_date_sk = d_date_sk
				AND d_year = 2000
				AND wr_returning_addr_sk = ca_address_sk
			GROUP BY
				wr_returning_customer_sk, ca_state
		)
SELECT
	c_customer_id,
	c_salutation,
	c_first_name,
	c_last_name,
	c_preferred_cust_flag,
	c_birth_day,
	c_birth_month,
	c_birth_year,
	c_birth_country,
	c_login,
	c_email_address,
	c_last_review_date_sk,
	ctr_total_return
FROM
	customer_total_return AS ctr1,
	customer_address,
	customer
WHERE
	ctr1.ctr_total_return
	> (
			SELECT
				avg(ctr_total_return) * 1.2
			FROM
				customer_total_return AS ctr2
			WHERE
				ctr1.ctr_state = ctr2.ctr_state
		)
	AND ca_address_sk = c_current_addr_sk
	AND ca_state = 'AR'
	AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY
	c_customer_id,
	c_salutation,
	c_first_name,
	c_last_name,
	c_preferred_cust_flag,
	c_birth_day,
	c_birth_month,
	c_birth_year,
	c_birth_country,
	c_login,
	c_email_address,
	c_last_review_date_sk,
	ctr_total_return
LIMIT
	100;
`

	query31 = `
WITH
	ss
		AS (
			SELECT
				ca_county,
				d_qoy,
				d_year,
				sum(ss_ext_sales_price) AS store_sales
			FROM
				store_sales, date_dim, customer_address
			WHERE
				ss_sold_date_sk = d_date_sk
				AND ss_addr_sk = ca_address_sk
			GROUP BY
				ca_county, d_qoy, d_year
		),
	ws
		AS (
			SELECT
				ca_county,
				d_qoy,
				d_year,
				sum(ws_ext_sales_price) AS web_sales
			FROM
				web_sales, date_dim, customer_address
			WHERE
				ws_sold_date_sk = d_date_sk
				AND ws_bill_addr_sk = ca_address_sk
			GROUP BY
				ca_county, d_qoy, d_year
		)
SELECT
	ss1.ca_county,
	ss1.d_year,
	ws2.web_sales / ws1.web_sales AS web_q1_q2_increase,
	ss2.store_sales / ss1.store_sales
		AS store_q1_q2_increase,
	ws3.web_sales / ws2.web_sales AS web_q2_q3_increase,
	ss3.store_sales / ss2.store_sales
		AS store_q2_q3_increase
FROM
	ss AS ss1,
	ss AS ss2,
	ss AS ss3,
	ws AS ws1,
	ws AS ws2,
	ws AS ws3
WHERE
	ss1.d_qoy = 1
	AND ss1.d_year = 1999
	AND ss1.ca_county = ss2.ca_county
	AND ss2.d_qoy = 2
	AND ss2.d_year = 1999
	AND ss2.ca_county = ss3.ca_county
	AND ss3.d_qoy = 3
	AND ss3.d_year = 1999
	AND ss1.ca_county = ws1.ca_county
	AND ws1.d_qoy = 1
	AND ws1.d_year = 1999
	AND ws1.ca_county = ws2.ca_county
	AND ws2.d_qoy = 2
	AND ws2.d_year = 1999
	AND ws1.ca_county = ws3.ca_county
	AND ws3.d_qoy = 3
	AND ws3.d_year = 1999
	AND CASE
		WHEN ws1.web_sales > 0
		THEN ws2.web_sales / ws1.web_sales
		ELSE NULL
		END
		> CASE
			WHEN ss1.store_sales > 0
			THEN ss2.store_sales / ss1.store_sales
			ELSE NULL
			END
	AND CASE
		WHEN ws2.web_sales > 0
		THEN ws3.web_sales / ws2.web_sales
		ELSE NULL
		END
		> CASE
			WHEN ss2.store_sales > 0
			THEN ss3.store_sales / ss2.store_sales
			ELSE NULL
			END
ORDER BY
	store_q2_q3_increase;
`

	// NOTE: I added conversion of 90 days to an interval.
	query32 = `
SELECT
	sum(cs_ext_discount_amt) AS "excess discount amount"
FROM
	catalog_sales, item, date_dim
WHERE
	i_manufact_id = 722
	AND i_item_sk = cs_item_sk
	AND d_date BETWEEN '2001-03-09' AND (CAST('2001-03-09' AS DATE) + '90 days'::INTERVAL)
	AND d_date_sk = cs_sold_date_sk
	AND cs_ext_discount_amt
		> (
				SELECT
					1.3 * avg(cs_ext_discount_amt)
				FROM
					catalog_sales, date_dim
				WHERE
					cs_item_sk = i_item_sk
					AND d_date BETWEEN '2001-03-09' AND (CAST('2001-03-09' AS DATE) + '90 days'::INTERVAL)
					AND d_date_sk = cs_sold_date_sk
			)
LIMIT
	100;
`

	query33 = `
WITH
	ss
		AS (
			SELECT
				i_manufact_id,
				sum(ss_ext_sales_price) AS total_sales
			FROM
				store_sales,
				date_dim,
				customer_address,
				item
			WHERE
				i_manufact_id
				IN (
						SELECT
							i_manufact_id
						FROM
							item
						WHERE
							i_category IN ('Books',)
					)
				AND ss_item_sk = i_item_sk
				AND ss_sold_date_sk = d_date_sk
				AND d_year = 2001
				AND d_moy = 3
				AND ss_addr_sk = ca_address_sk
				AND ca_gmt_offset = -5
			GROUP BY
				i_manufact_id
		),
	cs
		AS (
			SELECT
				i_manufact_id,
				sum(cs_ext_sales_price) AS total_sales
			FROM
				catalog_sales,
				date_dim,
				customer_address,
				item
			WHERE
				i_manufact_id
				IN (
						SELECT
							i_manufact_id
						FROM
							item
						WHERE
							i_category IN ('Books',)
					)
				AND cs_item_sk = i_item_sk
				AND cs_sold_date_sk = d_date_sk
				AND d_year = 2001
				AND d_moy = 3
				AND cs_bill_addr_sk = ca_address_sk
				AND ca_gmt_offset = -5
			GROUP BY
				i_manufact_id
		),
	ws
		AS (
			SELECT
				i_manufact_id,
				sum(ws_ext_sales_price) AS total_sales
			FROM
				web_sales, date_dim, customer_address, item
			WHERE
				i_manufact_id
				IN (
						SELECT
							i_manufact_id
						FROM
							item
						WHERE
							i_category IN ('Books',)
					)
				AND ws_item_sk = i_item_sk
				AND ws_sold_date_sk = d_date_sk
				AND d_year = 2001
				AND d_moy = 3
				AND ws_bill_addr_sk = ca_address_sk
				AND ca_gmt_offset = -5
			GROUP BY
				i_manufact_id
		)
SELECT
	i_manufact_id, sum(total_sales) AS total_sales
FROM
	(
		SELECT * FROM ss UNION ALL SELECT * FROM cs
		UNION ALL SELECT * FROM ws
	)
		AS tmp1
GROUP BY
	i_manufact_id
ORDER BY
	total_sales
LIMIT
	100;
`

	query34 = `
SELECT
	c_last_name,
	c_first_name,
	c_salutation,
	c_preferred_cust_flag,
	ss_ticket_number,
	cnt
FROM
	(
		SELECT
			ss_ticket_number,
			ss_customer_sk,
			count(*) AS cnt
		FROM
			store_sales,
			date_dim,
			store,
			household_demographics
		WHERE
			store_sales.ss_sold_date_sk = date_dim.d_date_sk
			AND store_sales.ss_store_sk = store.s_store_sk
			AND store_sales.ss_hdemo_sk
				= household_demographics.hd_demo_sk
			AND (
					date_dim.d_dom BETWEEN 1 AND 3
					OR date_dim.d_dom BETWEEN 25 AND 28
				)
			AND (
					household_demographics.hd_buy_potential
					= '1001-5000'
					OR household_demographics.hd_buy_potential
						= '0-500'
				)
			AND household_demographics.hd_vehicle_count > 0
			AND (
					CASE
					WHEN household_demographics.hd_vehicle_count
					> 0
					THEN household_demographics.hd_dep_count
					/ household_demographics.hd_vehicle_count
					ELSE NULL
					END
				)
				> 1.2
			AND date_dim.d_year
				IN (2000, 2000 + 1, 2000 + 2)
			AND store.s_county
				IN (
						'Williamson County',
						'Williamson County',
						'Williamson County',
						'Williamson County',
						'Williamson County',
						'Williamson County',
						'Williamson County',
						'Williamson County'
					)
		GROUP BY
			ss_ticket_number, ss_customer_sk
	)
		AS dn,
	customer
WHERE
	ss_customer_sk = c_customer_sk AND cnt BETWEEN 15 AND 20
ORDER BY
	c_last_name,
	c_first_name,
	c_salutation,
	c_preferred_cust_flag DESC,
	ss_ticket_number;
`

	query35 = `
SELECT
	ca_state,
	cd_gender,
	cd_marital_status,
	cd_dep_count,
	count(*) AS cnt1,
	avg(cd_dep_count),
	stddev_samp(cd_dep_count),
	sum(cd_dep_count),
	cd_dep_employed_count,
	count(*) AS cnt2,
	avg(cd_dep_employed_count),
	stddev_samp(cd_dep_employed_count),
	sum(cd_dep_employed_count),
	cd_dep_college_count,
	count(*) AS cnt3,
	avg(cd_dep_college_count),
	stddev_samp(cd_dep_college_count),
	sum(cd_dep_college_count)
FROM
	customer AS c,
	customer_address AS ca,
	customer_demographics
WHERE
	c.c_current_addr_sk = ca.ca_address_sk
	AND cd_demo_sk = c.c_current_cdemo_sk
	AND EXISTS(
			SELECT
				*
			FROM
				store_sales, date_dim
			WHERE
				c.c_customer_sk = ss_customer_sk
				AND ss_sold_date_sk = d_date_sk
				AND d_year = 1999
				AND d_qoy < 4
		)
	AND (
			EXISTS(
				SELECT
					*
				FROM
					web_sales, date_dim
				WHERE
					c.c_customer_sk = ws_bill_customer_sk
					AND ws_sold_date_sk = d_date_sk
					AND d_year = 1999
					AND d_qoy < 4
			)
			OR EXISTS(
					SELECT
						*
					FROM
						catalog_sales, date_dim
					WHERE
						c.c_customer_sk
						= cs_ship_customer_sk
						AND cs_sold_date_sk = d_date_sk
						AND d_year = 1999
						AND d_qoy < 4
				)
		)
GROUP BY
	ca_state,
	cd_gender,
	cd_marital_status,
	cd_dep_count,
	cd_dep_employed_count,
	cd_dep_college_count
ORDER BY
	ca_state,
	cd_gender,
	cd_marital_status,
	cd_dep_count,
	cd_dep_employed_count,
	cd_dep_college_count
LIMIT
	100;
`

	// TODO(yuzefovich): modify it to be parsed by CRDB.
	query36 = `
select  
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
   ,i_category
   ,i_class
   ,grouping(i_category)+grouping(i_class) as lochierarchy
   ,rank() over (
 	partition by grouping(i_category)+grouping(i_class),
 	case when grouping(i_class) = 0 then i_category end 
 	order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
 from
    store_sales
   ,date_dim       d1
   ,item
   ,store
 where
    d1.d_year = 2000 
 and d1.d_date_sk = ss_sold_date_sk
 and i_item_sk  = ss_item_sk 
 and s_store_sk  = ss_store_sk
 and s_state in ('TN','TN','TN','TN',
                 'TN','TN','TN','TN')
 group by rollup(i_category,i_class)
 order by
   lochierarchy desc
  ,case when lochierarchy = 0 then i_category end
  ,rank_within_parent
  limit 100;
`

	// NOTE: I added conversion of 90 days to an interval.
	query37 = `
SELECT
	i_item_id, i_item_desc, i_current_price
FROM
	item, inventory, date_dim, catalog_sales
WHERE
	i_current_price BETWEEN 29 AND (29 + 30)
	AND inv_item_sk = i_item_sk
	AND d_date_sk = inv_date_sk
	AND d_date BETWEEN CAST('2002-03-29' AS DATE) AND (CAST('2002-03-29' AS DATE) + '60 days'::INTERVAL)
	AND i_manufact_id IN (705, 742, 777, 944)
	AND inv_quantity_on_hand BETWEEN 100 AND 500
	AND cs_item_sk = i_item_sk
GROUP BY
	i_item_id, i_item_desc, i_current_price
ORDER BY
	i_item_id
LIMIT
	100;
`

	query38 = `
SELECT
	count(*)
FROM
	(
		SELECT
			DISTINCT c_last_name, c_first_name, d_date
		FROM
			store_sales, date_dim, customer
		WHERE
			store_sales.ss_sold_date_sk = date_dim.d_date_sk
			AND store_sales.ss_customer_sk
				= customer.c_customer_sk
			AND d_month_seq BETWEEN 1189 AND (1189 + 11)
		INTERSECT
			SELECT
				DISTINCT c_last_name, c_first_name, d_date
			FROM
				catalog_sales, date_dim, customer
			WHERE
				catalog_sales.cs_sold_date_sk
				= date_dim.d_date_sk
				AND catalog_sales.cs_bill_customer_sk
					= customer.c_customer_sk
				AND d_month_seq BETWEEN 1189 AND (1189 + 11)
		INTERSECT
			SELECT
				DISTINCT c_last_name, c_first_name, d_date
			FROM
				web_sales, date_dim, customer
			WHERE
				web_sales.ws_sold_date_sk
				= date_dim.d_date_sk
				AND web_sales.ws_bill_customer_sk
					= customer.c_customer_sk
				AND d_month_seq BETWEEN 1189 AND (1189 + 11)
	)
		AS hot_cust
LIMIT
	100;
`

	query39 = `
WITH
	inv
		AS (
			SELECT
				w_warehouse_name,
				w_warehouse_sk,
				i_item_sk,
				d_moy,
				stdev,
				mean,
				CASE mean
				WHEN 0 THEN NULL
				ELSE stdev / mean
				END
					AS cov
			FROM
				(
					SELECT
						w_warehouse_name,
						w_warehouse_sk,
						i_item_sk,
						d_moy,
						stddev_samp(inv_quantity_on_hand)
							AS stdev,
						avg(inv_quantity_on_hand) AS mean
					FROM
						inventory, item, warehouse, date_dim
					WHERE
						inv_item_sk = i_item_sk
						AND inv_warehouse_sk
							= w_warehouse_sk
						AND inv_date_sk = d_date_sk
						AND d_year = 2000
					GROUP BY
						w_warehouse_name,
						w_warehouse_sk,
						i_item_sk,
						d_moy
				)
					AS foo
			WHERE
				CASE mean
				WHEN 0 THEN 0
				ELSE stdev / mean
				END
				> 1
		)
SELECT
	inv1.w_warehouse_sk,
	inv1.i_item_sk,
	inv1.d_moy,
	inv1.mean,
	inv1.cov,
	inv2.w_warehouse_sk,
	inv2.i_item_sk,
	inv2.d_moy,
	inv2.mean,
	inv2.cov
FROM
	inv AS inv1, inv AS inv2
WHERE
	inv1.i_item_sk = inv2.i_item_sk
	AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
	AND inv1.d_moy = 1
	AND inv2.d_moy = 1 + 1
ORDER BY
	inv1.w_warehouse_sk,
	inv1.i_item_sk,
	inv1.d_moy,
	inv1.mean,
	inv1.cov,
	inv2.d_moy,
	inv2.mean,
	inv2.cov;
WITH
	inv
		AS (
			SELECT
				w_warehouse_name,
				w_warehouse_sk,
				i_item_sk,
				d_moy,
				stdev,
				mean,
				CASE mean
				WHEN 0 THEN NULL
				ELSE stdev / mean
				END
					AS cov
			FROM
				(
					SELECT
						w_warehouse_name,
						w_warehouse_sk,
						i_item_sk,
						d_moy,
						stddev_samp(inv_quantity_on_hand)
							AS stdev,
						avg(inv_quantity_on_hand) AS mean
					FROM
						inventory, item, warehouse, date_dim
					WHERE
						inv_item_sk = i_item_sk
						AND inv_warehouse_sk
							= w_warehouse_sk
						AND inv_date_sk = d_date_sk
						AND d_year = 2000
					GROUP BY
						w_warehouse_name,
						w_warehouse_sk,
						i_item_sk,
						d_moy
				)
					AS foo
			WHERE
				CASE mean
				WHEN 0 THEN 0
				ELSE stdev / mean
				END
				> 1
		)
SELECT
	inv1.w_warehouse_sk,
	inv1.i_item_sk,
	inv1.d_moy,
	inv1.mean,
	inv1.cov,
	inv2.w_warehouse_sk,
	inv2.i_item_sk,
	inv2.d_moy,
	inv2.mean,
	inv2.cov
FROM
	inv AS inv1, inv AS inv2
WHERE
	inv1.i_item_sk = inv2.i_item_sk
	AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
	AND inv1.d_moy = 1
	AND inv2.d_moy = 1 + 1
	AND inv1.cov > 1.5
ORDER BY
	inv1.w_warehouse_sk,
	inv1.i_item_sk,
	inv1.d_moy,
	inv1.mean,
	inv1.cov,
	inv2.d_moy,
	inv2.mean,
	inv2.cov;
`

	// NOTE: I added conversion of 30 days to an interval.
	query40 = `
SELECT
	w_state,
	i_item_id,
	sum(
		CASE
		WHEN (
			CAST(d_date AS DATE)
			< CAST('2001-05-02' AS DATE)
		)
		THEN cs_sales_price - COALESCE(cr_refunded_cash, 0)
		ELSE 0
		END
	)
		AS sales_before,
	sum(
		CASE
		WHEN (
			CAST(d_date AS DATE)
			>= CAST('2001-05-02' AS DATE)
		)
		THEN cs_sales_price - COALESCE(cr_refunded_cash, 0)
		ELSE 0
		END
	)
		AS sales_after
FROM
	catalog_sales
	LEFT JOIN catalog_returns ON
			cs_order_number = cr_order_number
			AND cs_item_sk = cr_item_sk,
	warehouse,
	item,
	date_dim
WHERE
	i_current_price BETWEEN 0.99 AND 1.49
	AND i_item_sk = cs_item_sk
	AND cs_warehouse_sk = w_warehouse_sk
	AND cs_sold_date_sk = d_date_sk
	AND d_date BETWEEN (CAST('2001-05-02' AS DATE) - '30 days'::INTERVAL) AND (CAST('2001-05-02' AS DATE) + '30 days'::INTERVAL)
GROUP BY
	w_state, i_item_id
ORDER BY
	w_state, i_item_id
LIMIT
	100;
`

	query41 = `
SELECT
	DISTINCT i_product_name
FROM
	item AS i1
WHERE
	i_manufact_id BETWEEN 704 AND (704 + 40)
	AND (
			SELECT
				count(*) AS item_cnt
			FROM
				item
			WHERE
				(
					i_manufact = i1.i_manufact
					AND (
							(
								i_category = 'Women'
								AND (
										i_color = 'forest'
										OR i_color = 'lime'
									)
								AND (
										i_units = 'Pallet'
										OR i_units = 'Pound'
									)
								AND (
										i_size = 'economy'
										OR i_size = 'small'
									)
							)
							OR (
									i_category = 'Women'
									AND (
											i_color = 'navy'
											OR i_color
												= 'slate'
										)
									AND (
											i_units
											= 'Gross'
											OR i_units
												= 'Bunch'
										)
									AND (
											i_size
											= 'extra large'
											OR i_size
												= 'petite'
										)
								)
							OR (
									i_category = 'Men'
									AND (
											i_color
											= 'powder'
											OR i_color
												= 'sky'
										)
									AND (
											i_units
											= 'Dozen'
											OR i_units
												= 'Lb'
										)
									AND (
											i_size = 'N/A'
											OR i_size
												= 'large'
										)
								)
							OR (
									i_category = 'Men'
									AND (
											i_color
											= 'maroon'
											OR i_color
												= 'smoke'
										)
									AND (
											i_units
											= 'Ounce'
											OR i_units
												= 'Case'
										)
									AND (
											i_size
											= 'economy'
											OR i_size
												= 'small'
										)
								)
						)
				)
				OR (
						i_manufact = i1.i_manufact
						AND (
								(
									i_category = 'Women'
									AND (
											i_color = 'dark'
											OR i_color
												= 'aquamarine'
										)
									AND (
											i_units = 'Ton'
											OR i_units
												= 'Tbl'
										)
									AND (
											i_size
											= 'economy'
											OR i_size
												= 'small'
										)
								)
								OR (
										i_category = 'Women'
										AND (
												i_color
												= 'frosted'
												OR i_color
													= 'plum'
											)
										AND (
												i_units
												= 'Dram'
												OR i_units
													= 'Box'
											)
										AND (
												i_size
												= 'extra large'
												OR i_size
													= 'petite'
											)
									)
								OR (
										i_category = 'Men'
										AND (
												i_color
												= 'papaya'
												OR i_color
													= 'peach'
											)
										AND (
												i_units
												= 'Bundle'
												OR i_units
													= 'Carton'
											)
										AND (
												i_size
												= 'N/A'
												OR i_size
													= 'large'
											)
									)
								OR (
										i_category = 'Men'
										AND (
												i_color
												= 'firebrick'
												OR i_color
													= 'sienna'
											)
										AND (
												i_units
												= 'Cup'
												OR i_units
													= 'Each'
											)
										AND (
												i_size
												= 'economy'
												OR i_size
													= 'small'
											)
									)
							)
					)
		)
		> 0
ORDER BY
	i_product_name
LIMIT
	100;
`

	query42 = `
SELECT
	dt.d_year,
	item.i_category_id,
	item.i_category,
	sum(ss_ext_sales_price)
FROM
	date_dim AS dt, store_sales, item
WHERE
	dt.d_date_sk = store_sales.ss_sold_date_sk
	AND store_sales.ss_item_sk = item.i_item_sk
	AND item.i_manager_id = 1
	AND dt.d_moy = 11
	AND dt.d_year = 1998
GROUP BY
	dt.d_year, item.i_category_id, item.i_category
ORDER BY
	sum(ss_ext_sales_price) DESC,
	dt.d_year,
	item.i_category_id,
	item.i_category
LIMIT
	100;
`

	query43 = `
SELECT
	s_store_name,
	s_store_id,
	sum(
		CASE
		WHEN (d_day_name = 'Sunday') THEN ss_sales_price
		ELSE NULL
		END
	)
		AS sun_sales,
	sum(
		CASE
		WHEN (d_day_name = 'Monday') THEN ss_sales_price
		ELSE NULL
		END
	)
		AS mon_sales,
	sum(
		CASE
		WHEN (d_day_name = 'Tuesday') THEN ss_sales_price
		ELSE NULL
		END
	)
		AS tue_sales,
	sum(
		CASE
		WHEN (d_day_name = 'Wednesday') THEN ss_sales_price
		ELSE NULL
		END
	)
		AS wed_sales,
	sum(
		CASE
		WHEN (d_day_name = 'Thursday') THEN ss_sales_price
		ELSE NULL
		END
	)
		AS thu_sales,
	sum(
		CASE
		WHEN (d_day_name = 'Friday') THEN ss_sales_price
		ELSE NULL
		END
	)
		AS fri_sales,
	sum(
		CASE
		WHEN (d_day_name = 'Saturday') THEN ss_sales_price
		ELSE NULL
		END
	)
		AS sat_sales
FROM
	date_dim, store_sales, store
WHERE
	d_date_sk = ss_sold_date_sk
	AND s_store_sk = ss_store_sk
	AND s_gmt_offset = -5
	AND d_year = 2000
GROUP BY
	s_store_name, s_store_id
ORDER BY
	s_store_name,
	s_store_id,
	sun_sales,
	mon_sales,
	tue_sales,
	wed_sales,
	thu_sales,
	fri_sales,
	sat_sales
LIMIT
	100;
`

	query44 = `
SELECT
	asceding.rnk,
	i1.i_product_name AS best_performing,
	i2.i_product_name AS worst_performing
FROM
	(
		SELECT
			*
		FROM
			(
				SELECT
					item_sk,
					rank() OVER (ORDER BY rank_col ASC)
						AS rnk
				FROM
					(
						SELECT
							ss_item_sk AS item_sk,
							avg(ss_net_profit) AS rank_col
						FROM
							store_sales AS ss1
						WHERE
							ss_store_sk = 4
						GROUP BY
							ss_item_sk
						HAVING
							avg(ss_net_profit)
							> 0.9
								* (
										SELECT
											avg(
												ss_net_profit
											)
												AS rank_col
										FROM
											store_sales
										WHERE
											ss_store_sk = 4
											AND ss_hdemo_sk
												IS NULL
										GROUP BY
											ss_store_sk
									)
					)
						AS v1
			)
				AS v11
		WHERE
			rnk < 11
	)
		AS asceding,
	(
		SELECT
			*
		FROM
			(
				SELECT
					item_sk,
					rank() OVER (ORDER BY rank_col DESC)
						AS rnk
				FROM
					(
						SELECT
							ss_item_sk AS item_sk,
							avg(ss_net_profit) AS rank_col
						FROM
							store_sales AS ss1
						WHERE
							ss_store_sk = 4
						GROUP BY
							ss_item_sk
						HAVING
							avg(ss_net_profit)
							> 0.9
								* (
										SELECT
											avg(
												ss_net_profit
											)
												AS rank_col
										FROM
											store_sales
										WHERE
											ss_store_sk = 4
											AND ss_hdemo_sk
												IS NULL
										GROUP BY
											ss_store_sk
									)
					)
						AS v2
			)
				AS v21
		WHERE
			rnk < 11
	)
		AS descending,
	item AS i1,
	item AS i2
WHERE
	asceding.rnk = descending.rnk
	AND i1.i_item_sk = asceding.item_sk
	AND i2.i_item_sk = descending.item_sk
ORDER BY
	asceding.rnk
LIMIT
	100;
`

	query45 = `
SELECT
	ca_zip, ca_city, sum(ws_sales_price)
FROM
	web_sales, customer, customer_address, date_dim, item
WHERE
	ws_bill_customer_sk = c_customer_sk
	AND c_current_addr_sk = ca_address_sk
	AND ws_item_sk = i_item_sk
	AND (
			substr(ca_zip, 1, 5)
			IN (
					'85669',
					'86197',
					'88274',
					'83405',
					'86475',
					'85392',
					'85460',
					'80348',
					'81792'
				)
			OR i_item_id
				IN (
						SELECT
							i_item_id
						FROM
							item
						WHERE
							i_item_sk
							IN (
									2,
									3,
									5,
									7,
									11,
									13,
									17,
									19,
									23,
									29
								)
					)
		)
	AND ws_sold_date_sk = d_date_sk
	AND d_qoy = 1
	AND d_year = 2000
GROUP BY
	ca_zip, ca_city
ORDER BY
	ca_zip, ca_city
LIMIT
	100;
`

	query46 = `
SELECT
	c_last_name,
	c_first_name,
	ca_city,
	bought_city,
	ss_ticket_number,
	amt,
	profit
FROM
	(
		SELECT
			ss_ticket_number,
			ss_customer_sk,
			ca_city AS bought_city,
			sum(ss_coupon_amt) AS amt,
			sum(ss_net_profit) AS profit
		FROM
			store_sales,
			date_dim,
			store,
			household_demographics,
			customer_address
		WHERE
			store_sales.ss_sold_date_sk = date_dim.d_date_sk
			AND store_sales.ss_store_sk = store.s_store_sk
			AND store_sales.ss_hdemo_sk
				= household_demographics.hd_demo_sk
			AND store_sales.ss_addr_sk
				= customer_address.ca_address_sk
			AND (
					household_demographics.hd_dep_count = 8
					OR household_demographics.hd_vehicle_count
						= 0
				)
			AND date_dim.d_dow IN (6, 0)
			AND date_dim.d_year
				IN (2000, 2000 + 1, 2000 + 2)
			AND store.s_city
				IN (
						'Midway',
						'Fairview',
						'Fairview',
						'Fairview',
						'Fairview'
					)
		GROUP BY
			ss_ticket_number,
			ss_customer_sk,
			ss_addr_sk,
			ca_city
	)
		AS dn,
	customer,
	customer_address AS current_addr
WHERE
	ss_customer_sk = c_customer_sk
	AND customer.c_current_addr_sk
		= current_addr.ca_address_sk
	AND current_addr.ca_city != bought_city
ORDER BY
	c_last_name,
	c_first_name,
	ca_city,
	bought_city,
	ss_ticket_number
LIMIT
	100;
`

	query47 = `
WITH
	v1
		AS (
			SELECT
				i_category,
				i_brand,
				s_store_name,
				s_company_name,
				d_year,
				d_moy,
				sum(ss_sales_price) AS sum_sales,
				avg(sum(ss_sales_price)) OVER (
					PARTITION BY
						i_category,
						i_brand,
						s_store_name,
						s_company_name,
						d_year
				)
					AS avg_monthly_sales,
				rank() OVER (
					PARTITION BY
						i_category,
						i_brand,
						s_store_name,
						s_company_name
					ORDER BY
						d_year, d_moy
				)
					AS rn
			FROM
				item, store_sales, date_dim, store
			WHERE
				ss_item_sk = i_item_sk
				AND ss_sold_date_sk = d_date_sk
				AND ss_store_sk = s_store_sk
				AND (
						d_year = 2000
						OR (
								d_year = 2000 - 1
								AND d_moy = 12
							)
						OR (d_year = 2000 + 1 AND d_moy = 1)
					)
			GROUP BY
				i_category,
				i_brand,
				s_store_name,
				s_company_name,
				d_year,
				d_moy
		),
	v2
		AS (
			SELECT
				v1.s_store_name,
				v1.s_company_name,
				v1.d_year,
				v1.avg_monthly_sales,
				v1.sum_sales,
				v1_lag.sum_sales AS psum,
				v1_lead.sum_sales AS nsum
			FROM
				v1, v1 AS v1_lag, v1 AS v1_lead
			WHERE
				v1.i_category = v1_lag.i_category
				AND v1.i_category = v1_lead.i_category
				AND v1.i_brand = v1_lag.i_brand
				AND v1.i_brand = v1_lead.i_brand
				AND v1.s_store_name = v1_lag.s_store_name
				AND v1.s_store_name = v1_lead.s_store_name
				AND v1.s_company_name
					= v1_lag.s_company_name
				AND v1.s_company_name
					= v1_lead.s_company_name
				AND v1.rn = v1_lag.rn + 1
				AND v1.rn = v1_lead.rn - 1
		)
SELECT
	*
FROM
	v2
WHERE
	d_year = 2000
	AND avg_monthly_sales > 0
	AND CASE
		WHEN avg_monthly_sales > 0
		THEN abs(sum_sales - avg_monthly_sales)
		/ avg_monthly_sales
		ELSE NULL
		END
		> 0.1
ORDER BY
	sum_sales - avg_monthly_sales, nsum
LIMIT
	100;
`

	query48 = `
SELECT
	sum(ss_quantity)
FROM
	store_sales,
	store,
	customer_demographics,
	customer_address,
	date_dim
WHERE
	s_store_sk = ss_store_sk
	AND ss_sold_date_sk = d_date_sk
	AND d_year = 2001
	AND (
			(
				cd_demo_sk = ss_cdemo_sk
				AND cd_marital_status = 'S'
				AND cd_education_status = 'Secondary'
				AND ss_sales_price BETWEEN 100.00 AND 150.00
			)
			OR (
					cd_demo_sk = ss_cdemo_sk
					AND cd_marital_status = 'M'
					AND cd_education_status = '2 yr Degree'
					AND ss_sales_price BETWEEN 50.00 AND 100.00
				)
			OR (
					cd_demo_sk = ss_cdemo_sk
					AND cd_marital_status = 'D'
					AND cd_education_status
						= 'Advanced Degree'
					AND ss_sales_price BETWEEN 150.00 AND 200.00
				)
		)
	AND (
			(
				ss_addr_sk = ca_address_sk
				AND ca_country = 'United States'
				AND ca_state IN ('ND', 'NY', 'SD')
				AND ss_net_profit BETWEEN 0 AND 2000
			)
			OR (
					ss_addr_sk = ca_address_sk
					AND ca_country = 'United States'
					AND ca_state IN ('MD', 'GA', 'KS')
					AND ss_net_profit BETWEEN 150 AND 3000
				)
			OR (
					ss_addr_sk = ca_address_sk
					AND ca_country = 'United States'
					AND ca_state IN ('CO', 'MN', 'NC')
					AND ss_net_profit BETWEEN 50 AND 25000
				)
		);
`

	query49 = `
SELECT
	channel, item, return_ratio, return_rank, currency_rank
FROM
	(
		SELECT
			'web' AS channel,
			web.item,
			web.return_ratio,
			web.return_rank,
			web.currency_rank
		FROM
			(
				SELECT
					item,
					return_ratio,
					currency_ratio,
					rank() OVER (ORDER BY return_ratio)
						AS return_rank,
					rank() OVER (ORDER BY currency_ratio)
						AS currency_rank
				FROM
					(
						SELECT
							ws.ws_item_sk AS item,
							CAST(
								sum(
									COALESCE(
										wr.wr_return_quantity,
										0
									)
								)
									AS DECIMAL(15,4)
							)
							/ CAST(
									sum(
										COALESCE(
											ws.ws_quantity,
											0
										)
									)
										AS DECIMAL(15,4)
								)
								AS return_ratio,
							CAST(
								sum(
									COALESCE(
										wr.wr_return_amt,
										0
									)
								)
									AS DECIMAL(15,4)
							)
							/ CAST(
									sum(
										COALESCE(
											ws.ws_net_paid,
											0
										)
									)
										AS DECIMAL(15,4)
								)
								AS currency_ratio
						FROM
							web_sales AS ws
							LEFT JOIN web_returns AS wr ON
									ws.ws_order_number
									= wr.wr_order_number
									AND ws.ws_item_sk
										= wr.wr_item_sk,
							date_dim
						WHERE
							wr.wr_return_amt > 10000
							AND ws.ws_net_profit > 1
							AND ws.ws_net_paid > 0
							AND ws.ws_quantity > 0
							AND ws_sold_date_sk = d_date_sk
							AND d_year = 1998
							AND d_moy = 11
						GROUP BY
							ws.ws_item_sk
					)
						AS in_web
			)
				AS web
		WHERE
			web.return_rank <= 10 OR web.currency_rank <= 10
		UNION
			SELECT
				'catalog' AS channel,
				catalog.item,
				catalog.return_ratio,
				catalog.return_rank,
				catalog.currency_rank
			FROM
				(
					SELECT
						item,
						return_ratio,
						currency_ratio,
						rank() OVER (ORDER BY return_ratio)
							AS return_rank,
						rank() OVER (
							ORDER BY currency_ratio
						)
							AS currency_rank
					FROM
						(
							SELECT
								cs.cs_item_sk AS item,
								CAST(
									sum(
										COALESCE(
											cr.cr_return_quantity,
											0
										)
									)
										AS DECIMAL(15,4)
								)
								/ CAST(
										sum(
											COALESCE(
												cs.cs_quantity,
												0
											)
										)
											AS DECIMAL(15,4)
									)
									AS return_ratio,
								CAST(
									sum(
										COALESCE(
											cr.cr_return_amount,
											0
										)
									)
										AS DECIMAL(15,4)
								)
								/ CAST(
										sum(
											COALESCE(
												cs.cs_net_paid,
												0
											)
										)
											AS DECIMAL(15,4)
									)
									AS currency_ratio
							FROM
								catalog_sales AS cs
								LEFT JOIN catalog_returns
										AS cr ON
										cs.cs_order_number
										= cr.cr_order_number
										AND cs.cs_item_sk
											= cr.cr_item_sk,
								date_dim
							WHERE
								cr.cr_return_amount > 10000
								AND cs.cs_net_profit > 1
								AND cs.cs_net_paid > 0
								AND cs.cs_quantity > 0
								AND cs_sold_date_sk
									= d_date_sk
								AND d_year = 1998
								AND d_moy = 11
							GROUP BY
								cs.cs_item_sk
						)
							AS in_cat
				)
					AS catalog
			WHERE
				catalog.return_rank <= 10
				OR catalog.currency_rank <= 10
		UNION
			SELECT
				'store' AS channel,
				store.item,
				store.return_ratio,
				store.return_rank,
				store.currency_rank
			FROM
				(
					SELECT
						item,
						return_ratio,
						currency_ratio,
						rank() OVER (ORDER BY return_ratio)
							AS return_rank,
						rank() OVER (
							ORDER BY currency_ratio
						)
							AS currency_rank
					FROM
						(
							SELECT
								sts.ss_item_sk AS item,
								CAST(
									sum(
										COALESCE(
											sr.sr_return_quantity,
											0
										)
									)
										AS DECIMAL(15,4)
								)
								/ CAST(
										sum(
											COALESCE(
												sts.ss_quantity,
												0
											)
										)
											AS DECIMAL(15,4)
									)
									AS return_ratio,
								CAST(
									sum(
										COALESCE(
											sr.sr_return_amt,
											0
										)
									)
										AS DECIMAL(15,4)
								)
								/ CAST(
										sum(
											COALESCE(
												sts.ss_net_paid,
												0
											)
										)
											AS DECIMAL(15,4)
									)
									AS currency_ratio
							FROM
								store_sales AS sts
								LEFT JOIN store_returns
										AS sr ON
										sts.ss_ticket_number
										= sr.sr_ticket_number
										AND sts.ss_item_sk
											= sr.sr_item_sk,
								date_dim
							WHERE
								sr.sr_return_amt > 10000
								AND sts.ss_net_profit > 1
								AND sts.ss_net_paid > 0
								AND sts.ss_quantity > 0
								AND ss_sold_date_sk
									= d_date_sk
								AND d_year = 1998
								AND d_moy = 11
							GROUP BY
								sts.ss_item_sk
						)
							AS in_store
				)
					AS store
			WHERE
				store.return_rank <= 10
				OR store.currency_rank <= 10
	)
ORDER BY
	1, 4, 5, 2
LIMIT
	100;
`

	query50 = `
SELECT
	s_store_name,
	s_company_id,
	s_street_number,
	s_street_name,
	s_street_type,
	s_suite_number,
	s_city,
	s_county,
	s_state,
	s_zip,
	sum(
		CASE
		WHEN (sr_returned_date_sk - ss_sold_date_sk <= 30)
		THEN 1
		ELSE 0
		END
	)
		AS "30 days",
	sum(
		CASE
		WHEN sr_returned_date_sk - ss_sold_date_sk > 30
		AND sr_returned_date_sk - ss_sold_date_sk <= 60
		THEN 1
		ELSE 0
		END
	)
		AS "31-60 days",
	sum(
		CASE
		WHEN sr_returned_date_sk - ss_sold_date_sk > 60
		AND sr_returned_date_sk - ss_sold_date_sk <= 90
		THEN 1
		ELSE 0
		END
	)
		AS "61-90 days",
	sum(
		CASE
		WHEN sr_returned_date_sk - ss_sold_date_sk > 90
		AND sr_returned_date_sk - ss_sold_date_sk <= 120
		THEN 1
		ELSE 0
		END
	)
		AS "91-120 days",
	sum(
		CASE
		WHEN (sr_returned_date_sk - ss_sold_date_sk > 120)
		THEN 1
		ELSE 0
		END
	)
		AS ">120 days"
FROM
	store_sales,
	store_returns,
	store,
	date_dim AS d1,
	date_dim AS d2
WHERE
	d2.d_year = 2001
	AND d2.d_moy = 8
	AND ss_ticket_number = sr_ticket_number
	AND ss_item_sk = sr_item_sk
	AND ss_sold_date_sk = d1.d_date_sk
	AND sr_returned_date_sk = d2.d_date_sk
	AND ss_customer_sk = sr_customer_sk
	AND ss_store_sk = s_store_sk
GROUP BY
	s_store_name,
	s_company_id,
	s_street_number,
	s_street_name,
	s_street_type,
	s_suite_number,
	s_city,
	s_county,
	s_state,
	s_zip
ORDER BY
	s_store_name,
	s_company_id,
	s_street_number,
	s_street_name,
	s_street_type,
	s_suite_number,
	s_city,
	s_county,
	s_state,
	s_zip
LIMIT
	100;
`

	query51 = `
WITH
	web_v1
		AS (
			SELECT
				ws_item_sk AS item_sk,
				d_date,
				sum(sum(ws_sales_price)) OVER (
					PARTITION BY
						ws_item_sk
					ORDER BY
						d_date
					ROWS
						BETWEEN
							UNBOUNDED PRECEDING
						AND
							CURRENT ROW
				)
					AS cume_sales
			FROM
				web_sales, date_dim
			WHERE
				ws_sold_date_sk = d_date_sk
				AND d_month_seq BETWEEN 1212 AND (1212 + 11)
				AND ws_item_sk IS NOT NULL
			GROUP BY
				ws_item_sk, d_date
		),
	store_v1
		AS (
			SELECT
				ss_item_sk AS item_sk,
				d_date,
				sum(sum(ss_sales_price)) OVER (
					PARTITION BY
						ss_item_sk
					ORDER BY
						d_date
					ROWS
						BETWEEN
							UNBOUNDED PRECEDING
						AND
							CURRENT ROW
				)
					AS cume_sales
			FROM
				store_sales, date_dim
			WHERE
				ss_sold_date_sk = d_date_sk
				AND d_month_seq BETWEEN 1212 AND (1212 + 11)
				AND ss_item_sk IS NOT NULL
			GROUP BY
				ss_item_sk, d_date
		)
SELECT
	*
FROM
	(
		SELECT
			item_sk,
			d_date,
			web_sales,
			store_sales,
			max(web_sales) OVER (
				PARTITION BY
					item_sk
				ORDER BY
					d_date
				ROWS
					BETWEEN
						UNBOUNDED PRECEDING
					AND
						CURRENT ROW
			)
				AS web_cumulative,
			max(store_sales) OVER (
				PARTITION BY
					item_sk
				ORDER BY
					d_date
				ROWS
					BETWEEN
						UNBOUNDED PRECEDING
					AND
						CURRENT ROW
			)
				AS store_cumulative
		FROM
			(
				SELECT
					CASE
					WHEN web.item_sk IS NOT NULL
					THEN web.item_sk
					ELSE store.item_sk
					END
						AS item_sk,
					CASE
					WHEN web.d_date IS NOT NULL
					THEN web.d_date
					ELSE store.d_date
					END
						AS d_date,
					web.cume_sales AS web_sales,
					store.cume_sales AS store_sales
				FROM
					web_v1 AS web
					FULL JOIN store_v1 AS store ON
							web.item_sk = store.item_sk
							AND web.d_date = store.d_date
			)
				AS x
	)
		AS y
WHERE
	web_cumulative > store_cumulative
ORDER BY
	item_sk, d_date
LIMIT
	100;
`

	query52 = `
SELECT
	dt.d_year,
	item.i_brand_id AS brand_id,
	item.i_brand AS brand,
	sum(ss_ext_sales_price) AS ext_price
FROM
	date_dim AS dt, store_sales, item
WHERE
	dt.d_date_sk = store_sales.ss_sold_date_sk
	AND store_sales.ss_item_sk = item.i_item_sk
	AND item.i_manager_id = 1
	AND dt.d_moy = 12
	AND dt.d_year = 2000
GROUP BY
	dt.d_year, item.i_brand, item.i_brand_id
ORDER BY
	dt.d_year, ext_price DESC, brand_id
LIMIT
	100;
`

	query53 = `
SELECT
	*
FROM
	(
		SELECT
			i_manufact_id,
			sum(ss_sales_price) AS sum_sales,
			avg(sum(ss_sales_price)) OVER (
				PARTITION BY i_manufact_id
			)
				AS avg_quarterly_sales
		FROM
			item, store_sales, date_dim, store
		WHERE
			ss_item_sk = i_item_sk
			AND ss_sold_date_sk = d_date_sk
			AND ss_store_sk = s_store_sk
			AND d_month_seq
				IN (
						1186,
						1186 + 1,
						1186 + 2,
						1186 + 3,
						1186 + 4,
						1186 + 5,
						1186 + 6,
						1186 + 7,
						1186 + 8,
						1186 + 9,
						1186 + 10,
						1186 + 11
					)
			AND (
					(
						i_category
						IN (
								'Books',
								'Children',
								'Electronics'
							)
						AND i_class
							IN (
									'personal',
									'portable',
									'reference',
									'self-help'
								)
						AND i_brand
							IN (
									'scholaramalgamalg #14',
									'scholaramalgamalg #7',
									'exportiunivamalg #9',
									'scholaramalgamalg #9'
								)
					)
					OR (
							i_category
							IN ('Women', 'Music', 'Men')
							AND i_class
								IN (
										'accessories',
										'classical',
										'fragrances',
										'pants'
									)
							AND i_brand
								IN (
										'amalgimporto #1',
										'edu packscholar #1',
										'exportiimporto #1',
										'importoamalg #1'
									)
						)
				)
		GROUP BY
			i_manufact_id, d_qoy
	)
		AS tmp1
WHERE
	CASE
	WHEN avg_quarterly_sales > 0
	THEN abs(sum_sales - avg_quarterly_sales)
	/ avg_quarterly_sales
	ELSE NULL
	END
	> 0.1
ORDER BY
	avg_quarterly_sales, sum_sales, i_manufact_id
LIMIT
	100;
`

	query54 = `
WITH
	my_customers
		AS (
			SELECT
				DISTINCT c_customer_sk, c_current_addr_sk
			FROM
				(
					SELECT
						cs_sold_date_sk AS sold_date_sk,
						cs_bill_customer_sk AS customer_sk,
						cs_item_sk AS item_sk
					FROM
						catalog_sales
					UNION ALL
						SELECT
							ws_sold_date_sk AS sold_date_sk,
							ws_bill_customer_sk
								AS customer_sk,
							ws_item_sk AS item_sk
						FROM
							web_sales
				)
					AS cs_or_ws_sales,
				item,
				date_dim,
				customer
			WHERE
				sold_date_sk = d_date_sk
				AND item_sk = i_item_sk
				AND i_category = 'Music'
				AND i_class = 'country'
				AND c_customer_sk
					= cs_or_ws_sales.customer_sk
				AND d_moy = 1
				AND d_year = 1999
		),
	my_revenue
		AS (
			SELECT
				c_customer_sk,
				sum(ss_ext_sales_price) AS revenue
			FROM
				my_customers,
				store_sales,
				customer_address,
				store,
				date_dim
			WHERE
				c_current_addr_sk = ca_address_sk
				AND ca_county = s_county
				AND ca_state = s_state
				AND ss_sold_date_sk = d_date_sk
				AND c_customer_sk = ss_customer_sk
				AND d_month_seq BETWEEN (SELECT DISTINCT d_month_seq + 1 FROM date_dim WHERE (d_year = 1999) AND (d_moy = 1)) AND (SELECT DISTINCT d_month_seq + 3 FROM date_dim WHERE (d_year = 1999) AND (d_moy = 1))
			GROUP BY
				c_customer_sk
		),
	segments
		AS (
			SELECT
				CAST((revenue / 50) AS INT8) AS segment
			FROM
				my_revenue
		)
SELECT
	segment,
	count(*) AS num_customers,
	segment * 50 AS segment_base
FROM
	segments
GROUP BY
	segment
ORDER BY
	segment, num_customers
LIMIT
	100;
`

	query55 = `
SELECT
	i_brand_id AS brand_id,
	i_brand AS brand,
	sum(ss_ext_sales_price) AS ext_price
FROM
	date_dim, store_sales, item
WHERE
	d_date_sk = ss_sold_date_sk
	AND ss_item_sk = i_item_sk
	AND i_manager_id = 52
	AND d_moy = 11
	AND d_year = 2000
GROUP BY
	i_brand, i_brand_id
ORDER BY
	ext_price DESC, i_brand_id
LIMIT
	100;
`

	query56 = `
WITH
	ss
		AS (
			SELECT
				i_item_id,
				sum(ss_ext_sales_price) AS total_sales
			FROM
				store_sales,
				date_dim,
				customer_address,
				item
			WHERE
				i_item_id
				IN (
						SELECT
							i_item_id
						FROM
							item
						WHERE
							i_color
							IN ('powder', 'orchid', 'pink')
					)
				AND ss_item_sk = i_item_sk
				AND ss_sold_date_sk = d_date_sk
				AND d_year = 2000
				AND d_moy = 3
				AND ss_addr_sk = ca_address_sk
				AND ca_gmt_offset = -6
			GROUP BY
				i_item_id
		),
	cs
		AS (
			SELECT
				i_item_id,
				sum(cs_ext_sales_price) AS total_sales
			FROM
				catalog_sales,
				date_dim,
				customer_address,
				item
			WHERE
				i_item_id
				IN (
						SELECT
							i_item_id
						FROM
							item
						WHERE
							i_color
							IN ('powder', 'orchid', 'pink')
					)
				AND cs_item_sk = i_item_sk
				AND cs_sold_date_sk = d_date_sk
				AND d_year = 2000
				AND d_moy = 3
				AND cs_bill_addr_sk = ca_address_sk
				AND ca_gmt_offset = -6
			GROUP BY
				i_item_id
		),
	ws
		AS (
			SELECT
				i_item_id,
				sum(ws_ext_sales_price) AS total_sales
			FROM
				web_sales, date_dim, customer_address, item
			WHERE
				i_item_id
				IN (
						SELECT
							i_item_id
						FROM
							item
						WHERE
							i_color
							IN ('powder', 'orchid', 'pink')
					)
				AND ws_item_sk = i_item_sk
				AND ws_sold_date_sk = d_date_sk
				AND d_year = 2000
				AND d_moy = 3
				AND ws_bill_addr_sk = ca_address_sk
				AND ca_gmt_offset = -6
			GROUP BY
				i_item_id
		)
SELECT
	i_item_id, sum(total_sales) AS total_sales
FROM
	(
		SELECT * FROM ss UNION ALL SELECT * FROM cs
		UNION ALL SELECT * FROM ws
	)
		AS tmp1
GROUP BY
	i_item_id
ORDER BY
	total_sales, i_item_id
LIMIT
	100;
`

	query57 = `
WITH
	v1
		AS (
			SELECT
				i_category,
				i_brand,
				cc_name,
				d_year,
				d_moy,
				sum(cs_sales_price) AS sum_sales,
				avg(sum(cs_sales_price)) OVER (
					PARTITION BY
						i_category, i_brand, cc_name, d_year
				)
					AS avg_monthly_sales,
				rank() OVER (
					PARTITION BY
						i_category, i_brand, cc_name
					ORDER BY
						d_year, d_moy
				)
					AS rn
			FROM
				item, catalog_sales, date_dim, call_center
			WHERE
				cs_item_sk = i_item_sk
				AND cs_sold_date_sk = d_date_sk
				AND cc_call_center_sk = cs_call_center_sk
				AND (
						d_year = 2001
						OR (
								d_year = 2001 - 1
								AND d_moy = 12
							)
						OR (d_year = 2001 + 1 AND d_moy = 1)
					)
			GROUP BY
				i_category, i_brand, cc_name, d_year, d_moy
		),
	v2
		AS (
			SELECT
				v1.i_category,
				v1.i_brand,
				v1.cc_name,
				v1.d_year,
				v1.avg_monthly_sales,
				v1.sum_sales,
				v1_lag.sum_sales AS psum,
				v1_lead.sum_sales AS nsum
			FROM
				v1, v1 AS v1_lag, v1 AS v1_lead
			WHERE
				v1.i_category = v1_lag.i_category
				AND v1.i_category = v1_lead.i_category
				AND v1.i_brand = v1_lag.i_brand
				AND v1.i_brand = v1_lead.i_brand
				AND v1.cc_name = v1_lag.cc_name
				AND v1.cc_name = v1_lead.cc_name
				AND v1.rn = v1_lag.rn + 1
				AND v1.rn = v1_lead.rn - 1
		)
SELECT
	*
FROM
	v2
WHERE
	d_year = 2001
	AND avg_monthly_sales > 0
	AND CASE
		WHEN avg_monthly_sales > 0
		THEN abs(sum_sales - avg_monthly_sales)
		/ avg_monthly_sales
		ELSE NULL
		END
		> 0.1
ORDER BY
	sum_sales - avg_monthly_sales, avg_monthly_sales
LIMIT
	100;
`

	query58 = `
WITH
	ss_items
		AS (
			SELECT
				i_item_id AS item_id,
				sum(ss_ext_sales_price) AS ss_item_rev
			FROM
				store_sales, item, date_dim
			WHERE
				ss_item_sk = i_item_sk
				AND d_date
					IN (
							SELECT
								d_date
							FROM
								date_dim
							WHERE
								d_week_seq
								= (
										SELECT
											d_week_seq
										FROM
											date_dim
										WHERE
											d_date
											= '2001-06-16'
									)
						)
				AND ss_sold_date_sk = d_date_sk
			GROUP BY
				i_item_id
		),
	cs_items
		AS (
			SELECT
				i_item_id AS item_id,
				sum(cs_ext_sales_price) AS cs_item_rev
			FROM
				catalog_sales, item, date_dim
			WHERE
				cs_item_sk = i_item_sk
				AND d_date
					IN (
							SELECT
								d_date
							FROM
								date_dim
							WHERE
								d_week_seq
								= (
										SELECT
											d_week_seq
										FROM
											date_dim
										WHERE
											d_date
											= '2001-06-16'
									)
						)
				AND cs_sold_date_sk = d_date_sk
			GROUP BY
				i_item_id
		),
	ws_items
		AS (
			SELECT
				i_item_id AS item_id,
				sum(ws_ext_sales_price) AS ws_item_rev
			FROM
				web_sales, item, date_dim
			WHERE
				ws_item_sk = i_item_sk
				AND d_date
					IN (
							SELECT
								d_date
							FROM
								date_dim
							WHERE
								d_week_seq
								= (
										SELECT
											d_week_seq
										FROM
											date_dim
										WHERE
											d_date
											= '2001-06-16'
									)
						)
				AND ws_sold_date_sk = d_date_sk
			GROUP BY
				i_item_id
		)
SELECT
	ss_items.item_id,
	ss_item_rev,
	ss_item_rev
	/ ((ss_item_rev + cs_item_rev + ws_item_rev) / 3)
	* 100
		AS ss_dev,
	cs_item_rev,
	cs_item_rev
	/ ((ss_item_rev + cs_item_rev + ws_item_rev) / 3)
	* 100
		AS cs_dev,
	ws_item_rev,
	ws_item_rev
	/ ((ss_item_rev + cs_item_rev + ws_item_rev) / 3)
	* 100
		AS ws_dev,
	(ss_item_rev + cs_item_rev + ws_item_rev) / 3 AS average
FROM
	ss_items, cs_items, ws_items
WHERE
	ss_items.item_id = cs_items.item_id
	AND ss_items.item_id = ws_items.item_id
	AND ss_item_rev BETWEEN (0.9 * cs_item_rev) AND (1.1 * cs_item_rev)
	AND ss_item_rev BETWEEN (0.9 * ws_item_rev) AND (1.1 * ws_item_rev)
	AND cs_item_rev BETWEEN (0.9 * ss_item_rev) AND (1.1 * ss_item_rev)
	AND cs_item_rev BETWEEN (0.9 * ws_item_rev) AND (1.1 * ws_item_rev)
	AND ws_item_rev BETWEEN (0.9 * ss_item_rev) AND (1.1 * ss_item_rev)
	AND ws_item_rev BETWEEN (0.9 * cs_item_rev) AND (1.1 * cs_item_rev)
ORDER BY
	item_id, ss_item_rev
LIMIT
	100;
`

	query59 = `
WITH
	wss
		AS (
			SELECT
				d_week_seq,
				ss_store_sk,
				sum(
					CASE
					WHEN (d_day_name = 'Sunday')
					THEN ss_sales_price
					ELSE NULL
					END
				)
					AS sun_sales,
				sum(
					CASE
					WHEN (d_day_name = 'Monday')
					THEN ss_sales_price
					ELSE NULL
					END
				)
					AS mon_sales,
				sum(
					CASE
					WHEN (d_day_name = 'Tuesday')
					THEN ss_sales_price
					ELSE NULL
					END
				)
					AS tue_sales,
				sum(
					CASE
					WHEN (d_day_name = 'Wednesday')
					THEN ss_sales_price
					ELSE NULL
					END
				)
					AS wed_sales,
				sum(
					CASE
					WHEN (d_day_name = 'Thursday')
					THEN ss_sales_price
					ELSE NULL
					END
				)
					AS thu_sales,
				sum(
					CASE
					WHEN (d_day_name = 'Friday')
					THEN ss_sales_price
					ELSE NULL
					END
				)
					AS fri_sales,
				sum(
					CASE
					WHEN (d_day_name = 'Saturday')
					THEN ss_sales_price
					ELSE NULL
					END
				)
					AS sat_sales
			FROM
				store_sales, date_dim
			WHERE
				d_date_sk = ss_sold_date_sk
			GROUP BY
				d_week_seq, ss_store_sk
		)
SELECT
	s_store_name1,
	s_store_id1,
	d_week_seq1,
	sun_sales1 / sun_sales2,
	mon_sales1 / mon_sales2,
	tue_sales1 / tue_sales2,
	wed_sales1 / wed_sales2,
	thu_sales1 / thu_sales2,
	fri_sales1 / fri_sales2,
	sat_sales1 / sat_sales2
FROM
	(
		SELECT
			s_store_name AS s_store_name1,
			wss.d_week_seq AS d_week_seq1,
			s_store_id AS s_store_id1,
			sun_sales AS sun_sales1,
			mon_sales AS mon_sales1,
			tue_sales AS tue_sales1,
			wed_sales AS wed_sales1,
			thu_sales AS thu_sales1,
			fri_sales AS fri_sales1,
			sat_sales AS sat_sales1
		FROM
			wss, store, date_dim AS d
		WHERE
			d.d_week_seq = wss.d_week_seq
			AND ss_store_sk = s_store_sk
			AND d_month_seq BETWEEN 1195 AND (1195 + 11)
	)
		AS y,
	(
		SELECT
			s_store_name AS s_store_name2,
			wss.d_week_seq AS d_week_seq2,
			s_store_id AS s_store_id2,
			sun_sales AS sun_sales2,
			mon_sales AS mon_sales2,
			tue_sales AS tue_sales2,
			wed_sales AS wed_sales2,
			thu_sales AS thu_sales2,
			fri_sales AS fri_sales2,
			sat_sales AS sat_sales2
		FROM
			wss, store, date_dim AS d
		WHERE
			d.d_week_seq = wss.d_week_seq
			AND ss_store_sk = s_store_sk
			AND d_month_seq BETWEEN (1195 + 12) AND (1195 + 23)
	)
		AS x
WHERE
	s_store_id1 = s_store_id2
	AND d_week_seq1 = d_week_seq2 - 52
ORDER BY
	s_store_name1, s_store_id1, d_week_seq1
LIMIT
	100;
`

	query60 = `
WITH
	ss
		AS (
			SELECT
				i_item_id,
				sum(ss_ext_sales_price) AS total_sales
			FROM
				store_sales,
				date_dim,
				customer_address,
				item
			WHERE
				i_item_id
				IN (
						SELECT
							i_item_id
						FROM
							item
						WHERE
							i_category IN ('Jewelry',)
					)
				AND ss_item_sk = i_item_sk
				AND ss_sold_date_sk = d_date_sk
				AND d_year = 2000
				AND d_moy = 10
				AND ss_addr_sk = ca_address_sk
				AND ca_gmt_offset = -5
			GROUP BY
				i_item_id
		),
	cs
		AS (
			SELECT
				i_item_id,
				sum(cs_ext_sales_price) AS total_sales
			FROM
				catalog_sales,
				date_dim,
				customer_address,
				item
			WHERE
				i_item_id
				IN (
						SELECT
							i_item_id
						FROM
							item
						WHERE
							i_category IN ('Jewelry',)
					)
				AND cs_item_sk = i_item_sk
				AND cs_sold_date_sk = d_date_sk
				AND d_year = 2000
				AND d_moy = 10
				AND cs_bill_addr_sk = ca_address_sk
				AND ca_gmt_offset = -5
			GROUP BY
				i_item_id
		),
	ws
		AS (
			SELECT
				i_item_id,
				sum(ws_ext_sales_price) AS total_sales
			FROM
				web_sales, date_dim, customer_address, item
			WHERE
				i_item_id
				IN (
						SELECT
							i_item_id
						FROM
							item
						WHERE
							i_category IN ('Jewelry',)
					)
				AND ws_item_sk = i_item_sk
				AND ws_sold_date_sk = d_date_sk
				AND d_year = 2000
				AND d_moy = 10
				AND ws_bill_addr_sk = ca_address_sk
				AND ca_gmt_offset = -5
			GROUP BY
				i_item_id
		)
SELECT
	i_item_id, sum(total_sales) AS total_sales
FROM
	(
		SELECT * FROM ss UNION ALL SELECT * FROM cs
		UNION ALL SELECT * FROM ws
	)
		AS tmp1
GROUP BY
	i_item_id
ORDER BY
	i_item_id, total_sales
LIMIT
	100;
`

	query61 = `
SELECT
	promotions,
	total,
	CAST(promotions AS DECIMAL(15,4))
	/ CAST(total AS DECIMAL(15,4))
	* 100
FROM
	(
		SELECT
			sum(ss_ext_sales_price) AS promotions
		FROM
			store_sales,
			store,
			promotion,
			date_dim,
			customer,
			customer_address,
			item
		WHERE
			ss_sold_date_sk = d_date_sk
			AND ss_store_sk = s_store_sk
			AND ss_promo_sk = p_promo_sk
			AND ss_customer_sk = c_customer_sk
			AND ca_address_sk = c_current_addr_sk
			AND ss_item_sk = i_item_sk
			AND ca_gmt_offset = -7
			AND i_category = 'Home'
			AND (
					p_channel_dmail = 'Y'
					OR p_channel_email = 'Y'
					OR p_channel_tv = 'Y'
				)
			AND s_gmt_offset = -7
			AND d_year = 2000
			AND d_moy = 12
	)
		AS promotional_sales,
	(
		SELECT
			sum(ss_ext_sales_price) AS total
		FROM
			store_sales,
			store,
			date_dim,
			customer,
			customer_address,
			item
		WHERE
			ss_sold_date_sk = d_date_sk
			AND ss_store_sk = s_store_sk
			AND ss_customer_sk = c_customer_sk
			AND ca_address_sk = c_current_addr_sk
			AND ss_item_sk = i_item_sk
			AND ca_gmt_offset = -7
			AND i_category = 'Home'
			AND s_gmt_offset = -7
			AND d_year = 2000
			AND d_moy = 12
	)
		AS all_sales
ORDER BY
	promotions, total
LIMIT
	100;
`

	query62 = `
SELECT
	substr(w_warehouse_name, 1, 20),
	sm_type,
	web_name,
	sum(
		CASE
		WHEN (ws_ship_date_sk - ws_sold_date_sk <= 30)
		THEN 1
		ELSE 0
		END
	)
		AS "30 days",
	sum(
		CASE
		WHEN ws_ship_date_sk - ws_sold_date_sk > 30
		AND ws_ship_date_sk - ws_sold_date_sk <= 60
		THEN 1
		ELSE 0
		END
	)
		AS "31-60 days",
	sum(
		CASE
		WHEN ws_ship_date_sk - ws_sold_date_sk > 60
		AND ws_ship_date_sk - ws_sold_date_sk <= 90
		THEN 1
		ELSE 0
		END
	)
		AS "61-90 days",
	sum(
		CASE
		WHEN ws_ship_date_sk - ws_sold_date_sk > 90
		AND ws_ship_date_sk - ws_sold_date_sk <= 120
		THEN 1
		ELSE 0
		END
	)
		AS "91-120 days",
	sum(
		CASE
		WHEN (ws_ship_date_sk - ws_sold_date_sk > 120)
		THEN 1
		ELSE 0
		END
	)
		AS ">120 days"
FROM
	web_sales, warehouse, ship_mode, web_site, date_dim
WHERE
	d_month_seq BETWEEN 1223 AND (1223 + 11)
	AND ws_ship_date_sk = d_date_sk
	AND ws_warehouse_sk = w_warehouse_sk
	AND ws_ship_mode_sk = sm_ship_mode_sk
	AND ws_web_site_sk = web_site_sk
GROUP BY
	substr(w_warehouse_name, 1, 20), sm_type, web_name
ORDER BY
	substr(w_warehouse_name, 1, 20), sm_type, web_name
LIMIT
	100;
`

	query63 = `
SELECT
	*
FROM
	(
		SELECT
			i_manager_id,
			sum(ss_sales_price) AS sum_sales,
			avg(sum(ss_sales_price)) OVER (
				PARTITION BY i_manager_id
			)
				AS avg_monthly_sales
		FROM
			item, store_sales, date_dim, store
		WHERE
			ss_item_sk = i_item_sk
			AND ss_sold_date_sk = d_date_sk
			AND ss_store_sk = s_store_sk
			AND d_month_seq
				IN (
						1222,
						1222 + 1,
						1222 + 2,
						1222 + 3,
						1222 + 4,
						1222 + 5,
						1222 + 6,
						1222 + 7,
						1222 + 8,
						1222 + 9,
						1222 + 10,
						1222 + 11
					)
			AND (
					(
						i_category
						IN (
								'Books',
								'Children',
								'Electronics'
							)
						AND i_class
							IN (
									'personal',
									'portable',
									'reference',
									'self-help'
								)
						AND i_brand
							IN (
									'scholaramalgamalg #14',
									'scholaramalgamalg #7',
									'exportiunivamalg #9',
									'scholaramalgamalg #9'
								)
					)
					OR (
							i_category
							IN ('Women', 'Music', 'Men')
							AND i_class
								IN (
										'accessories',
										'classical',
										'fragrances',
										'pants'
									)
							AND i_brand
								IN (
										'amalgimporto #1',
										'edu packscholar #1',
										'exportiimporto #1',
										'importoamalg #1'
									)
						)
				)
		GROUP BY
			i_manager_id, d_moy
	)
		AS tmp1
WHERE
	CASE
	WHEN avg_monthly_sales > 0
	THEN abs(sum_sales - avg_monthly_sales)
	/ avg_monthly_sales
	ELSE NULL
	END
	> 0.1
ORDER BY
	i_manager_id, avg_monthly_sales, sum_sales
LIMIT
	100;
`

	query64 = `
WITH
	cs_ui
		AS (
			SELECT
				cs_item_sk,
				sum(cs_ext_list_price) AS sale,
				sum(
					cr_refunded_cash
					+ cr_reversed_charge
					+ cr_store_credit
				)
					AS refund
			FROM
				catalog_sales, catalog_returns
			WHERE
				cs_item_sk = cr_item_sk
				AND cs_order_number = cr_order_number
			GROUP BY
				cs_item_sk
			HAVING
				sum(cs_ext_list_price)
				> 2
					* sum(
							cr_refunded_cash
							+ cr_reversed_charge
							+ cr_store_credit
						)
		),
	cross_sales
		AS (
			SELECT
				i_product_name AS product_name,
				i_item_sk AS item_sk,
				s_store_name AS store_name,
				s_zip AS store_zip,
				ad1.ca_street_number AS b_street_number,
				ad1.ca_street_name AS b_street_name,
				ad1.ca_city AS b_city,
				ad1.ca_zip AS b_zip,
				ad2.ca_street_number AS c_street_number,
				ad2.ca_street_name AS c_street_name,
				ad2.ca_city AS c_city,
				ad2.ca_zip AS c_zip,
				d1.d_year AS syear,
				d2.d_year AS fsyear,
				d3.d_year AS s2year,
				count(*) AS cnt,
				sum(ss_wholesale_cost) AS s1,
				sum(ss_list_price) AS s2,
				sum(ss_coupon_amt) AS s3
			FROM
				store_sales,
				store_returns,
				cs_ui,
				date_dim AS d1,
				date_dim AS d2,
				date_dim AS d3,
				store,
				customer,
				customer_demographics AS cd1,
				customer_demographics AS cd2,
				promotion,
				household_demographics AS hd1,
				household_demographics AS hd2,
				customer_address AS ad1,
				customer_address AS ad2,
				income_band AS ib1,
				income_band AS ib2,
				item
			WHERE
				ss_store_sk = s_store_sk
				AND ss_sold_date_sk = d1.d_date_sk
				AND ss_customer_sk = c_customer_sk
				AND ss_cdemo_sk = cd1.cd_demo_sk
				AND ss_hdemo_sk = hd1.hd_demo_sk
				AND ss_addr_sk = ad1.ca_address_sk
				AND ss_item_sk = i_item_sk
				AND ss_item_sk = sr_item_sk
				AND ss_ticket_number = sr_ticket_number
				AND ss_item_sk = cs_ui.cs_item_sk
				AND c_current_cdemo_sk = cd2.cd_demo_sk
				AND c_current_hdemo_sk = hd2.hd_demo_sk
				AND c_current_addr_sk = ad2.ca_address_sk
				AND c_first_sales_date_sk = d2.d_date_sk
				AND c_first_shipto_date_sk = d3.d_date_sk
				AND ss_promo_sk = p_promo_sk
				AND hd1.hd_income_band_sk
					= ib1.ib_income_band_sk
				AND hd2.hd_income_band_sk
					= ib2.ib_income_band_sk
				AND cd1.cd_marital_status
					!= cd2.cd_marital_status
				AND i_color
					IN (
							'orange',
							'lace',
							'lawn',
							'misty',
							'blush',
							'pink'
						)
				AND i_current_price BETWEEN 48 AND (48 + 10)
				AND i_current_price BETWEEN (48 + 1) AND (48 + 15)
			GROUP BY
				i_product_name,
				i_item_sk,
				s_store_name,
				s_zip,
				ad1.ca_street_number,
				ad1.ca_street_name,
				ad1.ca_city,
				ad1.ca_zip,
				ad2.ca_street_number,
				ad2.ca_street_name,
				ad2.ca_city,
				ad2.ca_zip,
				d1.d_year,
				d2.d_year,
				d3.d_year
		)
SELECT
	cs1.product_name,
	cs1.store_name,
	cs1.store_zip,
	cs1.b_street_number,
	cs1.b_street_name,
	cs1.b_city,
	cs1.b_zip,
	cs1.c_street_number,
	cs1.c_street_name,
	cs1.c_city,
	cs1.c_zip,
	cs1.syear,
	cs1.cnt,
	cs1.s1 AS s11,
	cs1.s2 AS s21,
	cs1.s3 AS s31,
	cs2.s1 AS s12,
	cs2.s2 AS s22,
	cs2.s3 AS s32,
	cs2.syear,
	cs2.cnt
FROM
	cross_sales AS cs1, cross_sales AS cs2
WHERE
	cs1.item_sk = cs2.item_sk
	AND cs1.syear = 1999
	AND cs2.syear = 1999 + 1
	AND cs2.cnt <= cs1.cnt
	AND cs1.store_name = cs2.store_name
	AND cs1.store_zip = cs2.store_zip
ORDER BY
	cs1.product_name,
	cs1.store_name,
	cs2.cnt,
	cs1.s1,
	cs2.s1;
`

	query65 = `
SELECT
	s_store_name,
	i_item_desc,
	sc.revenue,
	i_current_price,
	i_wholesale_cost,
	i_brand
FROM
	store,
	item,
	(
		SELECT
			ss_store_sk, avg(revenue) AS ave
		FROM
			(
				SELECT
					ss_store_sk,
					ss_item_sk,
					sum(ss_sales_price) AS revenue
				FROM
					store_sales, date_dim
				WHERE
					ss_sold_date_sk = d_date_sk
					AND d_month_seq BETWEEN 1176 AND (1176 + 11)
				GROUP BY
					ss_store_sk, ss_item_sk
			)
				AS sa
		GROUP BY
			ss_store_sk
	)
		AS sb,
	(
		SELECT
			ss_store_sk,
			ss_item_sk,
			sum(ss_sales_price) AS revenue
		FROM
			store_sales, date_dim
		WHERE
			ss_sold_date_sk = d_date_sk
			AND d_month_seq BETWEEN 1176 AND (1176 + 11)
		GROUP BY
			ss_store_sk, ss_item_sk
	)
		AS sc
WHERE
	sb.ss_store_sk = sc.ss_store_sk
	AND sc.revenue <= 0.1 * sb.ave
	AND s_store_sk = sc.ss_store_sk
	AND i_item_sk = sc.ss_item_sk
ORDER BY
	s_store_name, i_item_desc
LIMIT
	100;
`

	query66 = `
SELECT
	w_warehouse_name,
	w_warehouse_sq_ft,
	w_city,
	w_county,
	w_state,
	w_country,
	ship_carriers,
	year,
	sum(jan_sales) AS jan_sales,
	sum(feb_sales) AS feb_sales,
	sum(mar_sales) AS mar_sales,
	sum(apr_sales) AS apr_sales,
	sum(may_sales) AS may_sales,
	sum(jun_sales) AS jun_sales,
	sum(jul_sales) AS jul_sales,
	sum(aug_sales) AS aug_sales,
	sum(sep_sales) AS sep_sales,
	sum(oct_sales) AS oct_sales,
	sum(nov_sales) AS nov_sales,
	sum(dec_sales) AS dec_sales,
	sum(jan_sales / w_warehouse_sq_ft)
		AS jan_sales_per_sq_foot,
	sum(feb_sales / w_warehouse_sq_ft)
		AS feb_sales_per_sq_foot,
	sum(mar_sales / w_warehouse_sq_ft)
		AS mar_sales_per_sq_foot,
	sum(apr_sales / w_warehouse_sq_ft)
		AS apr_sales_per_sq_foot,
	sum(may_sales / w_warehouse_sq_ft)
		AS may_sales_per_sq_foot,
	sum(jun_sales / w_warehouse_sq_ft)
		AS jun_sales_per_sq_foot,
	sum(jul_sales / w_warehouse_sq_ft)
		AS jul_sales_per_sq_foot,
	sum(aug_sales / w_warehouse_sq_ft)
		AS aug_sales_per_sq_foot,
	sum(sep_sales / w_warehouse_sq_ft)
		AS sep_sales_per_sq_foot,
	sum(oct_sales / w_warehouse_sq_ft)
		AS oct_sales_per_sq_foot,
	sum(nov_sales / w_warehouse_sq_ft)
		AS nov_sales_per_sq_foot,
	sum(dec_sales / w_warehouse_sq_ft)
		AS dec_sales_per_sq_foot,
	sum(jan_net) AS jan_net,
	sum(feb_net) AS feb_net,
	sum(mar_net) AS mar_net,
	sum(apr_net) AS apr_net,
	sum(may_net) AS may_net,
	sum(jun_net) AS jun_net,
	sum(jul_net) AS jul_net,
	sum(aug_net) AS aug_net,
	sum(sep_net) AS sep_net,
	sum(oct_net) AS oct_net,
	sum(nov_net) AS nov_net,
	sum(dec_net) AS dec_net
FROM
	(
		SELECT
			w_warehouse_name,
			w_warehouse_sq_ft,
			w_city,
			w_county,
			w_state,
			w_country,
			'ORIENTAL' || ',' || 'BOXBUNDLES'
				AS ship_carriers,
			d_year AS year,
			sum(
				CASE
				WHEN d_moy = 1
				THEN ws_ext_sales_price * ws_quantity
				ELSE 0
				END
			)
				AS jan_sales,
			sum(
				CASE
				WHEN d_moy = 2
				THEN ws_ext_sales_price * ws_quantity
				ELSE 0
				END
			)
				AS feb_sales,
			sum(
				CASE
				WHEN d_moy = 3
				THEN ws_ext_sales_price * ws_quantity
				ELSE 0
				END
			)
				AS mar_sales,
			sum(
				CASE
				WHEN d_moy = 4
				THEN ws_ext_sales_price * ws_quantity
				ELSE 0
				END
			)
				AS apr_sales,
			sum(
				CASE
				WHEN d_moy = 5
				THEN ws_ext_sales_price * ws_quantity
				ELSE 0
				END
			)
				AS may_sales,
			sum(
				CASE
				WHEN d_moy = 6
				THEN ws_ext_sales_price * ws_quantity
				ELSE 0
				END
			)
				AS jun_sales,
			sum(
				CASE
				WHEN d_moy = 7
				THEN ws_ext_sales_price * ws_quantity
				ELSE 0
				END
			)
				AS jul_sales,
			sum(
				CASE
				WHEN d_moy = 8
				THEN ws_ext_sales_price * ws_quantity
				ELSE 0
				END
			)
				AS aug_sales,
			sum(
				CASE
				WHEN d_moy = 9
				THEN ws_ext_sales_price * ws_quantity
				ELSE 0
				END
			)
				AS sep_sales,
			sum(
				CASE
				WHEN d_moy = 10
				THEN ws_ext_sales_price * ws_quantity
				ELSE 0
				END
			)
				AS oct_sales,
			sum(
				CASE
				WHEN d_moy = 11
				THEN ws_ext_sales_price * ws_quantity
				ELSE 0
				END
			)
				AS nov_sales,
			sum(
				CASE
				WHEN d_moy = 12
				THEN ws_ext_sales_price * ws_quantity
				ELSE 0
				END
			)
				AS dec_sales,
			sum(
				CASE
				WHEN d_moy = 1
				THEN ws_net_paid_inc_ship * ws_quantity
				ELSE 0
				END
			)
				AS jan_net,
			sum(
				CASE
				WHEN d_moy = 2
				THEN ws_net_paid_inc_ship * ws_quantity
				ELSE 0
				END
			)
				AS feb_net,
			sum(
				CASE
				WHEN d_moy = 3
				THEN ws_net_paid_inc_ship * ws_quantity
				ELSE 0
				END
			)
				AS mar_net,
			sum(
				CASE
				WHEN d_moy = 4
				THEN ws_net_paid_inc_ship * ws_quantity
				ELSE 0
				END
			)
				AS apr_net,
			sum(
				CASE
				WHEN d_moy = 5
				THEN ws_net_paid_inc_ship * ws_quantity
				ELSE 0
				END
			)
				AS may_net,
			sum(
				CASE
				WHEN d_moy = 6
				THEN ws_net_paid_inc_ship * ws_quantity
				ELSE 0
				END
			)
				AS jun_net,
			sum(
				CASE
				WHEN d_moy = 7
				THEN ws_net_paid_inc_ship * ws_quantity
				ELSE 0
				END
			)
				AS jul_net,
			sum(
				CASE
				WHEN d_moy = 8
				THEN ws_net_paid_inc_ship * ws_quantity
				ELSE 0
				END
			)
				AS aug_net,
			sum(
				CASE
				WHEN d_moy = 9
				THEN ws_net_paid_inc_ship * ws_quantity
				ELSE 0
				END
			)
				AS sep_net,
			sum(
				CASE
				WHEN d_moy = 10
				THEN ws_net_paid_inc_ship * ws_quantity
				ELSE 0
				END
			)
				AS oct_net,
			sum(
				CASE
				WHEN d_moy = 11
				THEN ws_net_paid_inc_ship * ws_quantity
				ELSE 0
				END
			)
				AS nov_net,
			sum(
				CASE
				WHEN d_moy = 12
				THEN ws_net_paid_inc_ship * ws_quantity
				ELSE 0
				END
			)
				AS dec_net
		FROM
			web_sales,
			warehouse,
			date_dim,
			time_dim,
			ship_mode
		WHERE
			ws_warehouse_sk = w_warehouse_sk
			AND ws_sold_date_sk = d_date_sk
			AND ws_sold_time_sk = t_time_sk
			AND ws_ship_mode_sk = sm_ship_mode_sk
			AND d_year = 2001
			AND t_time BETWEEN 42970 AND (42970 + 28800)
			AND sm_carrier IN ('ORIENTAL', 'BOXBUNDLES')
		GROUP BY
			w_warehouse_name,
			w_warehouse_sq_ft,
			w_city,
			w_county,
			w_state,
			w_country,
			d_year
		UNION ALL
			SELECT
				w_warehouse_name,
				w_warehouse_sq_ft,
				w_city,
				w_county,
				w_state,
				w_country,
				'ORIENTAL' || ',' || 'BOXBUNDLES'
					AS ship_carriers,
				d_year AS year,
				sum(
					CASE
					WHEN d_moy = 1
					THEN cs_ext_list_price * cs_quantity
					ELSE 0
					END
				)
					AS jan_sales,
				sum(
					CASE
					WHEN d_moy = 2
					THEN cs_ext_list_price * cs_quantity
					ELSE 0
					END
				)
					AS feb_sales,
				sum(
					CASE
					WHEN d_moy = 3
					THEN cs_ext_list_price * cs_quantity
					ELSE 0
					END
				)
					AS mar_sales,
				sum(
					CASE
					WHEN d_moy = 4
					THEN cs_ext_list_price * cs_quantity
					ELSE 0
					END
				)
					AS apr_sales,
				sum(
					CASE
					WHEN d_moy = 5
					THEN cs_ext_list_price * cs_quantity
					ELSE 0
					END
				)
					AS may_sales,
				sum(
					CASE
					WHEN d_moy = 6
					THEN cs_ext_list_price * cs_quantity
					ELSE 0
					END
				)
					AS jun_sales,
				sum(
					CASE
					WHEN d_moy = 7
					THEN cs_ext_list_price * cs_quantity
					ELSE 0
					END
				)
					AS jul_sales,
				sum(
					CASE
					WHEN d_moy = 8
					THEN cs_ext_list_price * cs_quantity
					ELSE 0
					END
				)
					AS aug_sales,
				sum(
					CASE
					WHEN d_moy = 9
					THEN cs_ext_list_price * cs_quantity
					ELSE 0
					END
				)
					AS sep_sales,
				sum(
					CASE
					WHEN d_moy = 10
					THEN cs_ext_list_price * cs_quantity
					ELSE 0
					END
				)
					AS oct_sales,
				sum(
					CASE
					WHEN d_moy = 11
					THEN cs_ext_list_price * cs_quantity
					ELSE 0
					END
				)
					AS nov_sales,
				sum(
					CASE
					WHEN d_moy = 12
					THEN cs_ext_list_price * cs_quantity
					ELSE 0
					END
				)
					AS dec_sales,
				sum(
					CASE
					WHEN d_moy = 1
					THEN cs_net_paid * cs_quantity
					ELSE 0
					END
				)
					AS jan_net,
				sum(
					CASE
					WHEN d_moy = 2
					THEN cs_net_paid * cs_quantity
					ELSE 0
					END
				)
					AS feb_net,
				sum(
					CASE
					WHEN d_moy = 3
					THEN cs_net_paid * cs_quantity
					ELSE 0
					END
				)
					AS mar_net,
				sum(
					CASE
					WHEN d_moy = 4
					THEN cs_net_paid * cs_quantity
					ELSE 0
					END
				)
					AS apr_net,
				sum(
					CASE
					WHEN d_moy = 5
					THEN cs_net_paid * cs_quantity
					ELSE 0
					END
				)
					AS may_net,
				sum(
					CASE
					WHEN d_moy = 6
					THEN cs_net_paid * cs_quantity
					ELSE 0
					END
				)
					AS jun_net,
				sum(
					CASE
					WHEN d_moy = 7
					THEN cs_net_paid * cs_quantity
					ELSE 0
					END
				)
					AS jul_net,
				sum(
					CASE
					WHEN d_moy = 8
					THEN cs_net_paid * cs_quantity
					ELSE 0
					END
				)
					AS aug_net,
				sum(
					CASE
					WHEN d_moy = 9
					THEN cs_net_paid * cs_quantity
					ELSE 0
					END
				)
					AS sep_net,
				sum(
					CASE
					WHEN d_moy = 10
					THEN cs_net_paid * cs_quantity
					ELSE 0
					END
				)
					AS oct_net,
				sum(
					CASE
					WHEN d_moy = 11
					THEN cs_net_paid * cs_quantity
					ELSE 0
					END
				)
					AS nov_net,
				sum(
					CASE
					WHEN d_moy = 12
					THEN cs_net_paid * cs_quantity
					ELSE 0
					END
				)
					AS dec_net
			FROM
				catalog_sales,
				warehouse,
				date_dim,
				time_dim,
				ship_mode
			WHERE
				cs_warehouse_sk = w_warehouse_sk
				AND cs_sold_date_sk = d_date_sk
				AND cs_sold_time_sk = t_time_sk
				AND cs_ship_mode_sk = sm_ship_mode_sk
				AND d_year = 2001
				AND t_time BETWEEN 42970 AND (42970 + 28800)
				AND sm_carrier IN ('ORIENTAL', 'BOXBUNDLES')
			GROUP BY
				w_warehouse_name,
				w_warehouse_sq_ft,
				w_city,
				w_county,
				w_state,
				w_country,
				d_year
	)
		AS x
GROUP BY
	w_warehouse_name,
	w_warehouse_sq_ft,
	w_city,
	w_county,
	w_state,
	w_country,
	ship_carriers,
	year
ORDER BY
	w_warehouse_name
LIMIT
	100;
`

	query67 = `
SELECT
	*
FROM
	(
		SELECT
			i_category,
			i_class,
			i_brand,
			i_product_name,
			d_year,
			d_qoy,
			d_moy,
			s_store_id,
			sumsales,
			rank() OVER (
				PARTITION BY
					i_category
				ORDER BY
					sumsales DESC
			)
				AS rk
		FROM
			(
				SELECT
					i_category,
					i_class,
					i_brand,
					i_product_name,
					d_year,
					d_qoy,
					d_moy,
					s_store_id,
					sum(
						COALESCE(
							ss_sales_price * ss_quantity,
							0
						)
					)
						AS sumsales
				FROM
					store_sales, date_dim, store, item
				WHERE
					ss_sold_date_sk = d_date_sk
					AND ss_item_sk = i_item_sk
					AND ss_store_sk = s_store_sk
					AND d_month_seq BETWEEN 1217 AND (1217 + 11)
				GROUP BY
					rollup(
						i_category,
						i_class,
						i_brand,
						i_product_name,
						d_year,
						d_qoy,
						d_moy,
						s_store_id
					)
			)
				AS dw1
	)
		AS dw2
WHERE
	rk <= 100
ORDER BY
	i_category,
	i_class,
	i_brand,
	i_product_name,
	d_year,
	d_qoy,
	d_moy,
	s_store_id,
	sumsales,
	rk
LIMIT
	100;
`

	query68 = `
SELECT
	c_last_name,
	c_first_name,
	ca_city,
	bought_city,
	ss_ticket_number,
	extended_price,
	extended_tax,
	list_price
FROM
	(
		SELECT
			ss_ticket_number,
			ss_customer_sk,
			ca_city AS bought_city,
			sum(ss_ext_sales_price) AS extended_price,
			sum(ss_ext_list_price) AS list_price,
			sum(ss_ext_tax) AS extended_tax
		FROM
			store_sales,
			date_dim,
			store,
			household_demographics,
			customer_address
		WHERE
			store_sales.ss_sold_date_sk = date_dim.d_date_sk
			AND store_sales.ss_store_sk = store.s_store_sk
			AND store_sales.ss_hdemo_sk
				= household_demographics.hd_demo_sk
			AND store_sales.ss_addr_sk
				= customer_address.ca_address_sk
			AND date_dim.d_dom BETWEEN 1 AND 2
			AND (
					household_demographics.hd_dep_count = 3
					OR household_demographics.hd_vehicle_count
						= 4
				)
			AND date_dim.d_year
				IN (1998, 1998 + 1, 1998 + 2)
			AND store.s_city IN ('Fairview', 'Midway')
		GROUP BY
			ss_ticket_number,
			ss_customer_sk,
			ss_addr_sk,
			ca_city
	)
		AS dn,
	customer,
	customer_address AS current_addr
WHERE
	ss_customer_sk = c_customer_sk
	AND customer.c_current_addr_sk
		= current_addr.ca_address_sk
	AND current_addr.ca_city != bought_city
ORDER BY
	c_last_name, ss_ticket_number
LIMIT
	100;
`

	query69 = `
SELECT
	cd_gender,
	cd_marital_status,
	cd_education_status,
	count(*) AS cnt1,
	cd_purchase_estimate,
	count(*) AS cnt2,
	cd_credit_rating,
	count(*) AS cnt3
FROM
	customer AS c,
	customer_address AS ca,
	customer_demographics
WHERE
	c.c_current_addr_sk = ca.ca_address_sk
	AND ca_state IN ('IL', 'TX', 'ME')
	AND cd_demo_sk = c.c_current_cdemo_sk
	AND EXISTS(
			SELECT
				*
			FROM
				store_sales, date_dim
			WHERE
				c.c_customer_sk = ss_customer_sk
				AND ss_sold_date_sk = d_date_sk
				AND d_year = 2002
				AND d_moy BETWEEN 1 AND (1 + 2)
		)
	AND (
			NOT
				EXISTS(
					SELECT
						*
					FROM
						web_sales, date_dim
					WHERE
						c.c_customer_sk
						= ws_bill_customer_sk
						AND ws_sold_date_sk = d_date_sk
						AND d_year = 2002
						AND d_moy BETWEEN 1 AND (1 + 2)
				)
			AND NOT
					EXISTS(
						SELECT
							*
						FROM
							catalog_sales, date_dim
						WHERE
							c.c_customer_sk
							= cs_ship_customer_sk
							AND cs_sold_date_sk = d_date_sk
							AND d_year = 2002
							AND d_moy BETWEEN 1 AND (1 + 2)
					)
		)
GROUP BY
	cd_gender,
	cd_marital_status,
	cd_education_status,
	cd_purchase_estimate,
	cd_credit_rating
ORDER BY
	cd_gender,
	cd_marital_status,
	cd_education_status,
	cd_purchase_estimate,
	cd_credit_rating
LIMIT
	100;
`

	// TODO(yuzefovich): modify it to be parsed by CRDB.
	query70 = `
select  
    sum(ss_net_profit) as total_sum
   ,s_state
   ,s_county
   ,grouping(s_state)+grouping(s_county) as lochierarchy
   ,rank() over (
 	partition by grouping(s_state)+grouping(s_county),
 	case when grouping(s_county) = 0 then s_state end 
 	order by sum(ss_net_profit) desc) as rank_within_parent
 from
    store_sales
   ,date_dim       d1
   ,store
 where
    d1.d_month_seq between 1220 and 1220+11
 and d1.d_date_sk = ss_sold_date_sk
 and s_store_sk  = ss_store_sk
 and s_state in
             ( select s_state
               from  (select s_state as s_state,
 			    rank() over ( partition by s_state order by sum(ss_net_profit) desc) as ranking
                      from   store_sales, store, date_dim
                      where  d_month_seq between 1220 and 1220+11
 			    and d_date_sk = ss_sold_date_sk
 			    and s_store_sk  = ss_store_sk
                      group by s_state
                     ) tmp1 
               where ranking <= 5
             )
 group by rollup(s_state,s_county)
 order by
   lochierarchy desc
  ,case when lochierarchy = 0 then s_state end
  ,rank_within_parent
 limit 100;
`

	// NOTE: this query has been modified by appending two extra columns to
	// ORDER BY clause so that it had deterministic output.
	query71 = `
SELECT
	i_brand_id AS brand_id,
	i_brand AS brand,
	t_hour,
	t_minute,
	sum(ext_price) AS ext_price
FROM
	item,
	(
		SELECT
			ws_ext_sales_price AS ext_price,
			ws_sold_date_sk AS sold_date_sk,
			ws_item_sk AS sold_item_sk,
			ws_sold_time_sk AS time_sk
		FROM
			web_sales, date_dim
		WHERE
			d_date_sk = ws_sold_date_sk
			AND d_moy = 12
			AND d_year = 2002
		UNION ALL
			SELECT
				cs_ext_sales_price AS ext_price,
				cs_sold_date_sk AS sold_date_sk,
				cs_item_sk AS sold_item_sk,
				cs_sold_time_sk AS time_sk
			FROM
				catalog_sales, date_dim
			WHERE
				d_date_sk = cs_sold_date_sk
				AND d_moy = 12
				AND d_year = 2002
		UNION ALL
			SELECT
				ss_ext_sales_price AS ext_price,
				ss_sold_date_sk AS sold_date_sk,
				ss_item_sk AS sold_item_sk,
				ss_sold_time_sk AS time_sk
			FROM
				store_sales, date_dim
			WHERE
				d_date_sk = ss_sold_date_sk
				AND d_moy = 12
				AND d_year = 2002
	)
		AS tmp,
	time_dim
WHERE
	sold_item_sk = i_item_sk
	AND i_manager_id = 1
	AND time_sk = t_time_sk
	AND (
			t_meal_time = 'breakfast'
			OR t_meal_time = 'dinner'
		)
GROUP BY
	i_brand, i_brand_id, t_hour, t_minute
ORDER BY
	ext_price DESC, i_brand_id, t_hour, t_minute;
`

	query72 = `
SELECT
	i_item_desc,
	w_warehouse_name,
	d1.d_week_seq,
	sum(CASE WHEN p_promo_sk IS NULL THEN 1 ELSE 0 END)
		AS no_promo,
	sum(CASE WHEN p_promo_sk IS NOT NULL THEN 1 ELSE 0 END)
		AS promo,
	count(*) AS total_cnt
FROM
	catalog_sales
	JOIN inventory ON cs_item_sk = inv_item_sk
	JOIN warehouse ON w_warehouse_sk = inv_warehouse_sk
	JOIN item ON i_item_sk = cs_item_sk
	JOIN customer_demographics ON
			cs_bill_cdemo_sk = cd_demo_sk
	JOIN household_demographics ON
			cs_bill_hdemo_sk = hd_demo_sk
	JOIN date_dim AS d1 ON cs_sold_date_sk = d1.d_date_sk
	JOIN date_dim AS d2 ON inv_date_sk = d2.d_date_sk
	JOIN date_dim AS d3 ON cs_ship_date_sk = d3.d_date_sk
	LEFT JOIN promotion ON cs_promo_sk = p_promo_sk
	LEFT JOIN catalog_returns ON
			cr_item_sk = cs_item_sk
			AND cr_order_number = cs_order_number
WHERE
	d1.d_week_seq = d2.d_week_seq
	AND inv_quantity_on_hand < cs_quantity
	AND d3.d_date > d1.d_date + 5
	AND hd_buy_potential = '1001-5000'
	AND d1.d_year = 1998
	AND cd_marital_status = 'S'
GROUP BY
	i_item_desc, w_warehouse_name, d1.d_week_seq
ORDER BY
	total_cnt DESC,
	i_item_desc,
	w_warehouse_name,
	d_week_seq
LIMIT
	100;
`

	query73 = `
SELECT
	c_last_name,
	c_first_name,
	c_salutation,
	c_preferred_cust_flag,
	ss_ticket_number,
	cnt
FROM
	(
		SELECT
			ss_ticket_number,
			ss_customer_sk,
			count(*) AS cnt
		FROM
			store_sales,
			date_dim,
			store,
			household_demographics
		WHERE
			store_sales.ss_sold_date_sk = date_dim.d_date_sk
			AND store_sales.ss_store_sk = store.s_store_sk
			AND store_sales.ss_hdemo_sk
				= household_demographics.hd_demo_sk
			AND date_dim.d_dom BETWEEN 1 AND 2
			AND (
					household_demographics.hd_buy_potential
					= '1001-5000'
					OR household_demographics.hd_buy_potential
						= '5001-10000'
				)
			AND household_demographics.hd_vehicle_count > 0
			AND CASE
				WHEN household_demographics.hd_vehicle_count
				> 0
				THEN household_demographics.hd_dep_count
				/ household_demographics.hd_vehicle_count
				ELSE NULL
				END
				> 1
			AND date_dim.d_year
				IN (2000, 2000 + 1, 2000 + 2)
			AND store.s_county
				IN (
						'Williamson County',
						'Williamson County',
						'Williamson County',
						'Williamson County'
					)
		GROUP BY
			ss_ticket_number, ss_customer_sk
	)
		AS dj,
	customer
WHERE
	ss_customer_sk = c_customer_sk AND cnt BETWEEN 1 AND 5
ORDER BY
	cnt DESC, c_last_name ASC;
`

	query74 = `
WITH
	year_total
		AS (
			SELECT
				c_customer_id AS customer_id,
				c_first_name AS customer_first_name,
				c_last_name AS customer_last_name,
				d_year AS year,
				max(ss_net_paid) AS year_total,
				's' AS sale_type
			FROM
				customer, store_sales, date_dim
			WHERE
				c_customer_sk = ss_customer_sk
				AND ss_sold_date_sk = d_date_sk
				AND d_year IN (1999, 1999 + 1)
			GROUP BY
				c_customer_id,
				c_first_name,
				c_last_name,
				d_year
			UNION ALL
				SELECT
					c_customer_id AS customer_id,
					c_first_name AS customer_first_name,
					c_last_name AS customer_last_name,
					d_year AS year,
					max(ws_net_paid) AS year_total,
					'w' AS sale_type
				FROM
					customer, web_sales, date_dim
				WHERE
					c_customer_sk = ws_bill_customer_sk
					AND ws_sold_date_sk = d_date_sk
					AND d_year IN (1999, 1999 + 1)
				GROUP BY
					c_customer_id,
					c_first_name,
					c_last_name,
					d_year
		)
SELECT
	t_s_secyear.customer_id,
	t_s_secyear.customer_first_name,
	t_s_secyear.customer_last_name
FROM
	year_total AS t_s_firstyear,
	year_total AS t_s_secyear,
	year_total AS t_w_firstyear,
	year_total AS t_w_secyear
WHERE
	t_s_secyear.customer_id = t_s_firstyear.customer_id
	AND t_s_firstyear.customer_id = t_w_secyear.customer_id
	AND t_s_firstyear.customer_id
		= t_w_firstyear.customer_id
	AND t_s_firstyear.sale_type = 's'
	AND t_w_firstyear.sale_type = 'w'
	AND t_s_secyear.sale_type = 's'
	AND t_w_secyear.sale_type = 'w'
	AND t_s_firstyear.year = 1999
	AND t_s_secyear.year = 1999 + 1
	AND t_w_firstyear.year = 1999
	AND t_w_secyear.year = 1999 + 1
	AND t_s_firstyear.year_total > 0
	AND t_w_firstyear.year_total > 0
	AND CASE
		WHEN t_w_firstyear.year_total > 0
		THEN t_w_secyear.year_total
		/ t_w_firstyear.year_total
		ELSE NULL
		END
		> CASE
			WHEN t_s_firstyear.year_total > 0
			THEN t_s_secyear.year_total
			/ t_s_firstyear.year_total
			ELSE NULL
			END
ORDER BY
	1, 3, 2
LIMIT
	100;
`

	query75 = `
WITH
	all_sales
		AS (
			SELECT
				d_year,
				i_brand_id,
				i_class_id,
				i_category_id,
				i_manufact_id,
				sum(sales_cnt) AS sales_cnt,
				sum(sales_amt) AS sales_amt
			FROM
				(
					SELECT
						d_year,
						i_brand_id,
						i_class_id,
						i_category_id,
						i_manufact_id,
						cs_quantity
						- COALESCE(cr_return_quantity, 0)
							AS sales_cnt,
						cs_ext_sales_price
						- COALESCE(cr_return_amount, 0.0)
							AS sales_amt
					FROM
						catalog_sales
						JOIN item ON i_item_sk = cs_item_sk
						JOIN date_dim ON
								d_date_sk = cs_sold_date_sk
						LEFT JOIN catalog_returns ON
								cs_order_number
								= cr_order_number
								AND cs_item_sk = cr_item_sk
					WHERE
						i_category = 'Sports'
					UNION
						SELECT
							d_year,
							i_brand_id,
							i_class_id,
							i_category_id,
							i_manufact_id,
							ss_quantity
							- COALESCE(
									sr_return_quantity,
									0
								)
								AS sales_cnt,
							ss_ext_sales_price
							- COALESCE(sr_return_amt, 0.0)
								AS sales_amt
						FROM
							store_sales
							JOIN item ON
									i_item_sk = ss_item_sk
							JOIN date_dim ON
									d_date_sk
									= ss_sold_date_sk
							LEFT JOIN store_returns ON
									ss_ticket_number
									= sr_ticket_number
									AND ss_item_sk
										= sr_item_sk
						WHERE
							i_category = 'Sports'
					UNION
						SELECT
							d_year,
							i_brand_id,
							i_class_id,
							i_category_id,
							i_manufact_id,
							ws_quantity
							- COALESCE(
									wr_return_quantity,
									0
								)
								AS sales_cnt,
							ws_ext_sales_price
							- COALESCE(wr_return_amt, 0.0)
								AS sales_amt
						FROM
							web_sales
							JOIN item ON
									i_item_sk = ws_item_sk
							JOIN date_dim ON
									d_date_sk
									= ws_sold_date_sk
							LEFT JOIN web_returns ON
									ws_order_number
									= wr_order_number
									AND ws_item_sk
										= wr_item_sk
						WHERE
							i_category = 'Sports'
				)
					AS sales_detail
			GROUP BY
				d_year,
				i_brand_id,
				i_class_id,
				i_category_id,
				i_manufact_id
		)
SELECT
	prev_yr.d_year AS prev_year,
	curr_yr.d_year AS year,
	curr_yr.i_brand_id,
	curr_yr.i_class_id,
	curr_yr.i_category_id,
	curr_yr.i_manufact_id,
	prev_yr.sales_cnt AS prev_yr_cnt,
	curr_yr.sales_cnt AS curr_yr_cnt,
	curr_yr.sales_cnt - prev_yr.sales_cnt AS sales_cnt_diff,
	curr_yr.sales_amt - prev_yr.sales_amt AS sales_amt_diff
FROM
	all_sales AS curr_yr, all_sales AS prev_yr
WHERE
	curr_yr.i_brand_id = prev_yr.i_brand_id
	AND curr_yr.i_class_id = prev_yr.i_class_id
	AND curr_yr.i_category_id = prev_yr.i_category_id
	AND curr_yr.i_manufact_id = prev_yr.i_manufact_id
	AND curr_yr.d_year = 2002
	AND prev_yr.d_year = 2002 - 1
	AND CAST(curr_yr.sales_cnt AS DECIMAL(17,2))
		/ CAST(prev_yr.sales_cnt AS DECIMAL(17,2))
		< 0.9
ORDER BY
	sales_cnt_diff, sales_amt_diff
LIMIT
	100;
`

	query76 = `
SELECT
	channel,
	col_name,
	d_year,
	d_qoy,
	i_category,
	count(*) AS sales_cnt,
	sum(ext_sales_price) AS sales_amt
FROM
	(
		SELECT
			'store' AS channel,
			'ss_customer_sk' AS col_name,
			d_year,
			d_qoy,
			i_category,
			ss_ext_sales_price AS ext_sales_price
		FROM
			store_sales, item, date_dim
		WHERE
			ss_customer_sk IS NULL
			AND ss_sold_date_sk = d_date_sk
			AND ss_item_sk = i_item_sk
		UNION ALL
			SELECT
				'web' AS channel,
				'ws_promo_sk' AS col_name,
				d_year,
				d_qoy,
				i_category,
				ws_ext_sales_price AS ext_sales_price
			FROM
				web_sales, item, date_dim
			WHERE
				ws_promo_sk IS NULL
				AND ws_sold_date_sk = d_date_sk
				AND ws_item_sk = i_item_sk
		UNION ALL
			SELECT
				'catalog' AS channel,
				'cs_bill_customer_sk' AS col_name,
				d_year,
				d_qoy,
				i_category,
				cs_ext_sales_price AS ext_sales_price
			FROM
				catalog_sales, item, date_dim
			WHERE
				cs_bill_customer_sk IS NULL
				AND cs_sold_date_sk = d_date_sk
				AND cs_item_sk = i_item_sk
	)
		AS foo
GROUP BY
	channel, col_name, d_year, d_qoy, i_category
ORDER BY
	channel, col_name, d_year, d_qoy, i_category
LIMIT
	100;
`

	// NOTE: I added conversion of 30 days to an interval.
	query77 = `
WITH
	ss
		AS (
			SELECT
				s_store_sk,
				sum(ss_ext_sales_price) AS sales,
				sum(ss_net_profit) AS profit
			FROM
				store_sales, date_dim, store
			WHERE
				ss_sold_date_sk = d_date_sk
				AND d_date BETWEEN CAST('2000-08-10' AS DATE) AND (CAST('2000-08-10' AS DATE) + '30 days'::INTERVAL)
				AND ss_store_sk = s_store_sk
			GROUP BY
				s_store_sk
		),
	sr
		AS (
			SELECT
				s_store_sk,
				sum(sr_return_amt) AS returns,
				sum(sr_net_loss) AS profit_loss
			FROM
				store_returns, date_dim, store
			WHERE
				sr_returned_date_sk = d_date_sk
				AND d_date BETWEEN CAST('2000-08-10' AS DATE) AND (CAST('2000-08-10' AS DATE) + '30 days'::INTERVAL)
				AND sr_store_sk = s_store_sk
			GROUP BY
				s_store_sk
		),
	cs
		AS (
			SELECT
				cs_call_center_sk,
				sum(cs_ext_sales_price) AS sales,
				sum(cs_net_profit) AS profit
			FROM
				catalog_sales, date_dim
			WHERE
				cs_sold_date_sk = d_date_sk
				AND d_date BETWEEN CAST('2000-08-10' AS DATE) AND (CAST('2000-08-10' AS DATE) + '30 days'::INTERVAL)
			GROUP BY
				cs_call_center_sk
		),
	cr
		AS (
			SELECT
				cr_call_center_sk,
				sum(cr_return_amount) AS returns,
				sum(cr_net_loss) AS profit_loss
			FROM
				catalog_returns, date_dim
			WHERE
				cr_returned_date_sk = d_date_sk
				AND d_date BETWEEN CAST('2000-08-10' AS DATE) AND (CAST('2000-08-10' AS DATE) + '30 days'::INTERVAL)
			GROUP BY
				cr_call_center_sk
		),
	ws
		AS (
			SELECT
				wp_web_page_sk,
				sum(ws_ext_sales_price) AS sales,
				sum(ws_net_profit) AS profit
			FROM
				web_sales, date_dim, web_page
			WHERE
				ws_sold_date_sk = d_date_sk
				AND d_date BETWEEN CAST('2000-08-10' AS DATE) AND (CAST('2000-08-10' AS DATE) + '30 days'::INTERVAL)
				AND ws_web_page_sk = wp_web_page_sk
			GROUP BY
				wp_web_page_sk
		),
	wr
		AS (
			SELECT
				wp_web_page_sk,
				sum(wr_return_amt) AS returns,
				sum(wr_net_loss) AS profit_loss
			FROM
				web_returns, date_dim, web_page
			WHERE
				wr_returned_date_sk = d_date_sk
				AND d_date BETWEEN CAST('2000-08-10' AS DATE) AND (CAST('2000-08-10' AS DATE) + '30 days'::INTERVAL)
				AND wr_web_page_sk = wp_web_page_sk
			GROUP BY
				wp_web_page_sk
		)
SELECT
	channel,
	id,
	sum(sales) AS sales,
	sum(returns) AS returns,
	sum(profit) AS profit
FROM
	(
		SELECT
			'store channel' AS channel,
			ss.s_store_sk AS id,
			sales,
			COALESCE(returns, 0) AS returns,
			profit - COALESCE(profit_loss, 0) AS profit
		FROM
			ss LEFT JOIN sr ON ss.s_store_sk = sr.s_store_sk
		UNION ALL
			SELECT
				'catalog channel' AS channel,
				cs_call_center_sk AS id,
				sales,
				returns,
				profit - profit_loss AS profit
			FROM
				cs, cr
		UNION ALL
			SELECT
				'web channel' AS channel,
				ws.wp_web_page_sk AS id,
				sales,
				COALESCE(returns, 0) AS returns,
				profit - COALESCE(profit_loss, 0) AS profit
			FROM
				ws
				LEFT JOIN wr ON
						ws.wp_web_page_sk
						= wr.wp_web_page_sk
	)
		AS x
GROUP BY
	rollup(channel, id)
ORDER BY
	channel, id
LIMIT
	100;
`

	query78 = `
WITH
	ws
		AS (
			SELECT
				d_year AS ws_sold_year,
				ws_item_sk,
				ws_bill_customer_sk AS ws_customer_sk,
				sum(ws_quantity) AS ws_qty,
				sum(ws_wholesale_cost) AS ws_wc,
				sum(ws_sales_price) AS ws_sp
			FROM
				web_sales
				LEFT JOIN web_returns ON
						wr_order_number = ws_order_number
						AND ws_item_sk = wr_item_sk
				JOIN date_dim ON ws_sold_date_sk = d_date_sk
			WHERE
				wr_order_number IS NULL
			GROUP BY
				d_year, ws_item_sk, ws_bill_customer_sk
		),
	cs
		AS (
			SELECT
				d_year AS cs_sold_year,
				cs_item_sk,
				cs_bill_customer_sk AS cs_customer_sk,
				sum(cs_quantity) AS cs_qty,
				sum(cs_wholesale_cost) AS cs_wc,
				sum(cs_sales_price) AS cs_sp
			FROM
				catalog_sales
				LEFT JOIN catalog_returns ON
						cr_order_number = cs_order_number
						AND cs_item_sk = cr_item_sk
				JOIN date_dim ON cs_sold_date_sk = d_date_sk
			WHERE
				cr_order_number IS NULL
			GROUP BY
				d_year, cs_item_sk, cs_bill_customer_sk
		),
	ss
		AS (
			SELECT
				d_year AS ss_sold_year,
				ss_item_sk,
				ss_customer_sk,
				sum(ss_quantity) AS ss_qty,
				sum(ss_wholesale_cost) AS ss_wc,
				sum(ss_sales_price) AS ss_sp
			FROM
				store_sales
				LEFT JOIN store_returns ON
						sr_ticket_number = ss_ticket_number
						AND ss_item_sk = sr_item_sk
				JOIN date_dim ON ss_sold_date_sk = d_date_sk
			WHERE
				sr_ticket_number IS NULL
			GROUP BY
				d_year, ss_item_sk, ss_customer_sk
		)
SELECT
	ss_customer_sk,
	round(
		ss_qty
		/ (COALESCE(ws_qty, 0) + COALESCE(cs_qty, 0)),
		2
	)
		AS ratio,
	ss_qty AS store_qty,
	ss_wc AS store_wholesale_cost,
	ss_sp AS store_sales_price,
	COALESCE(ws_qty, 0) + COALESCE(cs_qty, 0)
		AS other_chan_qty,
	COALESCE(ws_wc, 0) + COALESCE(cs_wc, 0)
		AS other_chan_wholesale_cost,
	COALESCE(ws_sp, 0) + COALESCE(cs_sp, 0)
		AS other_chan_sales_price
FROM
	ss
	LEFT JOIN ws ON
			ws_sold_year = ss_sold_year
			AND ws_item_sk = ss_item_sk
			AND ws_customer_sk = ss_customer_sk
	LEFT JOIN cs ON
			cs_sold_year = ss_sold_year
			AND cs_item_sk = ss_item_sk
			AND cs_customer_sk = ss_customer_sk
WHERE
	(COALESCE(ws_qty, 0) > 0 OR COALESCE(cs_qty, 0) > 0)
	AND ss_sold_year = 1998
ORDER BY
	ss_customer_sk,
	ss_qty DESC,
	ss_wc DESC,
	ss_sp DESC,
	other_chan_qty,
	other_chan_wholesale_cost,
	other_chan_sales_price,
	ratio
LIMIT
	100;
`

	// NOTE: this query has been modified by appending one extra column to
	// ORDER BY clause so that it had deterministic output.
	query79 = `
SELECT
	c_last_name,
	c_first_name,
	substr(s_city, 1, 30),
	ss_ticket_number,
	amt,
	profit
FROM
	(
		SELECT
			ss_ticket_number,
			ss_customer_sk,
			store.s_city,
			sum(ss_coupon_amt) AS amt,
			sum(ss_net_profit) AS profit
		FROM
			store_sales,
			date_dim,
			store,
			household_demographics
		WHERE
			store_sales.ss_sold_date_sk = date_dim.d_date_sk
			AND store_sales.ss_store_sk = store.s_store_sk
			AND store_sales.ss_hdemo_sk
				= household_demographics.hd_demo_sk
			AND (
					household_demographics.hd_dep_count = 7
					OR household_demographics.hd_vehicle_count
						> -1
				)
			AND date_dim.d_dow = 1
			AND date_dim.d_year
				IN (2000, 2000 + 1, 2000 + 2)
			AND store.s_number_employees BETWEEN 200 AND 295
		GROUP BY
			ss_ticket_number,
			ss_customer_sk,
			ss_addr_sk,
			store.s_city
	)
		AS ms,
	customer
WHERE
	ss_customer_sk = c_customer_sk
ORDER BY
	c_last_name, c_first_name, substr(s_city, 1, 30), profit, ss_ticket_number
LIMIT
	100;
`

	// NOTE: I added conversion of 30 days to an interval.
	query80 = `
WITH
	ssr
		AS (
			SELECT
				s_store_id AS store_id,
				sum(ss_ext_sales_price) AS sales,
				sum(COALESCE(sr_return_amt, 0)) AS returns,
				sum(
					ss_net_profit - COALESCE(sr_net_loss, 0)
				)
					AS profit
			FROM
				store_sales
				LEFT JOIN store_returns ON
						ss_item_sk = sr_item_sk
						AND ss_ticket_number
							= sr_ticket_number,
				date_dim,
				store,
				item,
				promotion
			WHERE
				ss_sold_date_sk = d_date_sk
				AND d_date BETWEEN CAST('2002-08-14' AS DATE) AND (CAST('2002-08-14' AS DATE) + '30 days'::INTERVAL)
				AND ss_store_sk = s_store_sk
				AND ss_item_sk = i_item_sk
				AND i_current_price > 50
				AND ss_promo_sk = p_promo_sk
				AND p_channel_tv = 'N'
			GROUP BY
				s_store_id
		),
	csr
		AS (
			SELECT
				cp_catalog_page_id AS catalog_page_id,
				sum(cs_ext_sales_price) AS sales,
				sum(COALESCE(cr_return_amount, 0))
					AS returns,
				sum(
					cs_net_profit - COALESCE(cr_net_loss, 0)
				)
					AS profit
			FROM
				catalog_sales
				LEFT JOIN catalog_returns ON
						cs_item_sk = cr_item_sk
						AND cs_order_number
							= cr_order_number,
				date_dim,
				catalog_page,
				item,
				promotion
			WHERE
				cs_sold_date_sk = d_date_sk
				AND d_date BETWEEN CAST('2002-08-14' AS DATE) AND (CAST('2002-08-14' AS DATE) + '30 days'::INTERVAL)
				AND cs_catalog_page_sk = cp_catalog_page_sk
				AND cs_item_sk = i_item_sk
				AND i_current_price > 50
				AND cs_promo_sk = p_promo_sk
				AND p_channel_tv = 'N'
			GROUP BY
				cp_catalog_page_id
		),
	wsr
		AS (
			SELECT
				web_site_id,
				sum(ws_ext_sales_price) AS sales,
				sum(COALESCE(wr_return_amt, 0)) AS returns,
				sum(
					ws_net_profit - COALESCE(wr_net_loss, 0)
				)
					AS profit
			FROM
				web_sales
				LEFT JOIN web_returns ON
						ws_item_sk = wr_item_sk
						AND ws_order_number
							= wr_order_number,
				date_dim,
				web_site,
				item,
				promotion
			WHERE
				ws_sold_date_sk = d_date_sk
				AND d_date BETWEEN CAST('2002-08-14' AS DATE) AND (CAST('2002-08-14' AS DATE) + '30 days'::INTERVAL)
				AND ws_web_site_sk = web_site_sk
				AND ws_item_sk = i_item_sk
				AND i_current_price > 50
				AND ws_promo_sk = p_promo_sk
				AND p_channel_tv = 'N'
			GROUP BY
				web_site_id
		)
SELECT
	channel,
	id,
	sum(sales) AS sales,
	sum(returns) AS returns,
	sum(profit) AS profit
FROM
	(
		SELECT
			'store channel' AS channel,
			'store' || store_id AS id,
			sales,
			returns,
			profit
		FROM
			ssr
		UNION ALL
			SELECT
				'catalog channel' AS channel,
				'catalog_page' || catalog_page_id AS id,
				sales,
				returns,
				profit
			FROM
				csr
		UNION ALL
			SELECT
				'web channel' AS channel,
				'web_site' || web_site_id AS id,
				sales,
				returns,
				profit
			FROM
				wsr
	)
		AS x
GROUP BY
	rollup(channel, id)
ORDER BY
	channel, id
LIMIT
	100;
`

	query81 = `
WITH
	customer_total_return
		AS (
			SELECT
				cr_returning_customer_sk AS ctr_customer_sk,
				ca_state AS ctr_state,
				sum(cr_return_amt_inc_tax)
					AS ctr_total_return
			FROM
				catalog_returns, date_dim, customer_address
			WHERE
				cr_returned_date_sk = d_date_sk
				AND d_year = 2001
				AND cr_returning_addr_sk = ca_address_sk
			GROUP BY
				cr_returning_customer_sk, ca_state
		)
SELECT
	c_customer_id,
	c_salutation,
	c_first_name,
	c_last_name,
	ca_street_number,
	ca_street_name,
	ca_street_type,
	ca_suite_number,
	ca_city,
	ca_county,
	ca_state,
	ca_zip,
	ca_country,
	ca_gmt_offset,
	ca_location_type,
	ctr_total_return
FROM
	customer_total_return AS ctr1,
	customer_address,
	customer
WHERE
	ctr1.ctr_total_return
	> (
			SELECT
				avg(ctr_total_return) * 1.2
			FROM
				customer_total_return AS ctr2
			WHERE
				ctr1.ctr_state = ctr2.ctr_state
		)
	AND ca_address_sk = c_current_addr_sk
	AND ca_state = 'TN'
	AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY
	c_customer_id,
	c_salutation,
	c_first_name,
	c_last_name,
	ca_street_number,
	ca_street_name,
	ca_street_type,
	ca_suite_number,
	ca_city,
	ca_county,
	ca_state,
	ca_zip,
	ca_country,
	ca_gmt_offset,
	ca_location_type,
	ctr_total_return
LIMIT
	100;
`

	// NOTE: I added conversion of 60 days to an interval.
	query82 = `
SELECT
	i_item_id, i_item_desc, i_current_price
FROM
	item, inventory, date_dim, store_sales
WHERE
	i_current_price BETWEEN 58 AND (58 + 30)
	AND inv_item_sk = i_item_sk
	AND d_date_sk = inv_date_sk
	AND d_date BETWEEN CAST('2001-01-13' AS DATE) AND (CAST('2001-01-13' AS DATE) + '60 days'::INTERVAL)
	AND i_manufact_id IN (259, 559, 580, 485)
	AND inv_quantity_on_hand BETWEEN 100 AND 500
	AND ss_item_sk = i_item_sk
GROUP BY
	i_item_id, i_item_desc, i_current_price
ORDER BY
	i_item_id
LIMIT
	100;
`

	query83 = `
WITH
	sr_items
		AS (
			SELECT
				i_item_id AS item_id,
				sum(sr_return_quantity) AS sr_item_qty
			FROM
				store_returns, item, date_dim
			WHERE
				sr_item_sk = i_item_sk
				AND d_date
					IN (
							SELECT
								d_date
							FROM
								date_dim
							WHERE
								d_week_seq
								IN (
										SELECT
											d_week_seq
										FROM
											date_dim
										WHERE
											d_date
											IN (
													'2001-07-13',
													'2001-09-10',
													'2001-11-16'
												)
									)
						)
				AND sr_returned_date_sk = d_date_sk
			GROUP BY
				i_item_id
		),
	cr_items
		AS (
			SELECT
				i_item_id AS item_id,
				sum(cr_return_quantity) AS cr_item_qty
			FROM
				catalog_returns, item, date_dim
			WHERE
				cr_item_sk = i_item_sk
				AND d_date
					IN (
							SELECT
								d_date
							FROM
								date_dim
							WHERE
								d_week_seq
								IN (
										SELECT
											d_week_seq
										FROM
											date_dim
										WHERE
											d_date
											IN (
													'2001-07-13',
													'2001-09-10',
													'2001-11-16'
												)
									)
						)
				AND cr_returned_date_sk = d_date_sk
			GROUP BY
				i_item_id
		),
	wr_items
		AS (
			SELECT
				i_item_id AS item_id,
				sum(wr_return_quantity) AS wr_item_qty
			FROM
				web_returns, item, date_dim
			WHERE
				wr_item_sk = i_item_sk
				AND d_date
					IN (
							SELECT
								d_date
							FROM
								date_dim
							WHERE
								d_week_seq
								IN (
										SELECT
											d_week_seq
										FROM
											date_dim
										WHERE
											d_date
											IN (
													'2001-07-13',
													'2001-09-10',
													'2001-11-16'
												)
									)
						)
				AND wr_returned_date_sk = d_date_sk
			GROUP BY
				i_item_id
		)
SELECT
	sr_items.item_id,
	sr_item_qty,
	sr_item_qty
	/ (sr_item_qty + cr_item_qty + wr_item_qty)
	/ 3.0
	* 100
		AS sr_dev,
	cr_item_qty,
	cr_item_qty
	/ (sr_item_qty + cr_item_qty + wr_item_qty)
	/ 3.0
	* 100
		AS cr_dev,
	wr_item_qty,
	wr_item_qty
	/ (sr_item_qty + cr_item_qty + wr_item_qty)
	/ 3.0
	* 100
		AS wr_dev,
	(sr_item_qty + cr_item_qty + wr_item_qty) / 3.0
		AS average
FROM
	sr_items, cr_items, wr_items
WHERE
	sr_items.item_id = cr_items.item_id
	AND sr_items.item_id = wr_items.item_id
ORDER BY
	sr_items.item_id, sr_item_qty
LIMIT
	100;
`

	query84 = `
SELECT
	c_customer_id AS customer_id,
	COALESCE(c_last_name, '')
	|| ', '
	|| COALESCE(c_first_name, '')
		AS customername
FROM
	customer,
	customer_address,
	customer_demographics,
	household_demographics,
	income_band,
	store_returns
WHERE
	ca_city = 'Woodland'
	AND c_current_addr_sk = ca_address_sk
	AND ib_lower_bound >= 60306
	AND ib_upper_bound <= 60306 + 50000
	AND ib_income_band_sk = hd_income_band_sk
	AND cd_demo_sk = c_current_cdemo_sk
	AND hd_demo_sk = c_current_hdemo_sk
	AND sr_cdemo_sk = cd_demo_sk
ORDER BY
	c_customer_id
LIMIT
	100;
`

	query85 = `
SELECT
	substr(r_reason_desc, 1, 20),
	avg(ws_quantity),
	avg(wr_refunded_cash),
	avg(wr_fee)
FROM
	web_sales,
	web_returns,
	web_page,
	customer_demographics AS cd1,
	customer_demographics AS cd2,
	customer_address,
	date_dim,
	reason
WHERE
	ws_web_page_sk = wp_web_page_sk
	AND ws_item_sk = wr_item_sk
	AND ws_order_number = wr_order_number
	AND ws_sold_date_sk = d_date_sk
	AND d_year = 1998
	AND cd1.cd_demo_sk = wr_refunded_cdemo_sk
	AND cd2.cd_demo_sk = wr_returning_cdemo_sk
	AND ca_address_sk = wr_refunded_addr_sk
	AND r_reason_sk = wr_reason_sk
	AND (
			(
				cd1.cd_marital_status = 'D'
				AND cd1.cd_marital_status
					= cd2.cd_marital_status
				AND cd1.cd_education_status = 'Primary'
				AND cd1.cd_education_status
					= cd2.cd_education_status
				AND ws_sales_price BETWEEN 100.00 AND 150.00
			)
			OR (
					cd1.cd_marital_status = 'S'
					AND cd1.cd_marital_status
						= cd2.cd_marital_status
					AND cd1.cd_education_status = 'College'
					AND cd1.cd_education_status
						= cd2.cd_education_status
					AND ws_sales_price BETWEEN 50.00 AND 100.00
				)
			OR (
					cd1.cd_marital_status = 'U'
					AND cd1.cd_marital_status
						= cd2.cd_marital_status
					AND cd1.cd_education_status
						= 'Advanced Degree'
					AND cd1.cd_education_status
						= cd2.cd_education_status
					AND ws_sales_price BETWEEN 150.00 AND 200.00
				)
		)
	AND (
			(
				ca_country = 'United States'
				AND ca_state IN ('NC', 'TX', 'IA')
				AND ws_net_profit BETWEEN 100 AND 200
			)
			OR (
					ca_country = 'United States'
					AND ca_state IN ('WI', 'WV', 'GA')
					AND ws_net_profit BETWEEN 150 AND 300
				)
			OR (
					ca_country = 'United States'
					AND ca_state IN ('OK', 'VA', 'KY')
					AND ws_net_profit BETWEEN 50 AND 250
				)
		)
GROUP BY
	r_reason_desc
ORDER BY
	substr(r_reason_desc, 1, 20),
	avg(ws_quantity),
	avg(wr_refunded_cash),
	avg(wr_fee)
LIMIT
	100;
`

	// TODO(yuzefovich): modify it to be parsed by CRDB.
	query86 = `
select   
    sum(ws_net_paid) as total_sum
   ,i_category
   ,i_class
   ,grouping(i_category)+grouping(i_class) as lochierarchy
   ,rank() over (
 	partition by grouping(i_category)+grouping(i_class),
 	case when grouping(i_class) = 0 then i_category end 
 	order by sum(ws_net_paid) desc) as rank_within_parent
 from
    web_sales
   ,date_dim       d1
   ,item
 where
    d1.d_month_seq between 1186 and 1186+11
 and d1.d_date_sk = ws_sold_date_sk
 and i_item_sk  = ws_item_sk
 group by rollup(i_category,i_class)
 order by
   lochierarchy desc,
   case when lochierarchy = 0 then i_category end,
   rank_within_parent
 limit 100;
`

	query87 = `
SELECT
	count(*)
FROM
	(
		(
			SELECT
				DISTINCT c_last_name, c_first_name, d_date
			FROM
				store_sales, date_dim, customer
			WHERE
				store_sales.ss_sold_date_sk
				= date_dim.d_date_sk
				AND store_sales.ss_customer_sk
					= customer.c_customer_sk
				AND d_month_seq BETWEEN 1202 AND (1202 + 11)
		)
		EXCEPT
			(
				SELECT
					DISTINCT
					c_last_name,
					c_first_name,
					d_date
				FROM
					catalog_sales, date_dim, customer
				WHERE
					catalog_sales.cs_sold_date_sk
					= date_dim.d_date_sk
					AND catalog_sales.cs_bill_customer_sk
						= customer.c_customer_sk
					AND d_month_seq BETWEEN 1202 AND (1202 + 11)
			)
		EXCEPT
			(
				SELECT
					DISTINCT
					c_last_name,
					c_first_name,
					d_date
				FROM
					web_sales, date_dim, customer
				WHERE
					web_sales.ws_sold_date_sk
					= date_dim.d_date_sk
					AND web_sales.ws_bill_customer_sk
						= customer.c_customer_sk
					AND d_month_seq BETWEEN 1202 AND (1202 + 11)
			)
	)
		AS cool_cust;
`

	query88 = `
SELECT
	*
FROM
	(
		SELECT
			count(*) AS h8_30_to_9
		FROM
			store_sales,
			household_demographics,
			time_dim,
			store
		WHERE
			ss_sold_time_sk = time_dim.t_time_sk
			AND ss_hdemo_sk
				= household_demographics.hd_demo_sk
			AND ss_store_sk = s_store_sk
			AND time_dim.t_hour = 8
			AND time_dim.t_minute >= 30
			AND (
					(
						household_demographics.hd_dep_count
						= 0
						AND household_demographics.hd_vehicle_count
							<= 0 + 2
					)
					OR (
							household_demographics.hd_dep_count
							= -1
							AND household_demographics.hd_vehicle_count
								<= -1 + 2
						)
					OR (
							household_demographics.hd_dep_count
							= 3
							AND household_demographics.hd_vehicle_count
								<= 3 + 2
						)
				)
			AND store.s_store_name = 'ese'
	)
		AS s1,
	(
		SELECT
			count(*) AS h9_to_9_30
		FROM
			store_sales,
			household_demographics,
			time_dim,
			store
		WHERE
			ss_sold_time_sk = time_dim.t_time_sk
			AND ss_hdemo_sk
				= household_demographics.hd_demo_sk
			AND ss_store_sk = s_store_sk
			AND time_dim.t_hour = 9
			AND time_dim.t_minute < 30
			AND (
					(
						household_demographics.hd_dep_count
						= 0
						AND household_demographics.hd_vehicle_count
							<= 0 + 2
					)
					OR (
							household_demographics.hd_dep_count
							= -1
							AND household_demographics.hd_vehicle_count
								<= -1 + 2
						)
					OR (
							household_demographics.hd_dep_count
							= 3
							AND household_demographics.hd_vehicle_count
								<= 3 + 2
						)
				)
			AND store.s_store_name = 'ese'
	)
		AS s2,
	(
		SELECT
			count(*) AS h9_30_to_10
		FROM
			store_sales,
			household_demographics,
			time_dim,
			store
		WHERE
			ss_sold_time_sk = time_dim.t_time_sk
			AND ss_hdemo_sk
				= household_demographics.hd_demo_sk
			AND ss_store_sk = s_store_sk
			AND time_dim.t_hour = 9
			AND time_dim.t_minute >= 30
			AND (
					(
						household_demographics.hd_dep_count
						= 0
						AND household_demographics.hd_vehicle_count
							<= 0 + 2
					)
					OR (
							household_demographics.hd_dep_count
							= -1
							AND household_demographics.hd_vehicle_count
								<= -1 + 2
						)
					OR (
							household_demographics.hd_dep_count
							= 3
							AND household_demographics.hd_vehicle_count
								<= 3 + 2
						)
				)
			AND store.s_store_name = 'ese'
	)
		AS s3,
	(
		SELECT
			count(*) AS h10_to_10_30
		FROM
			store_sales,
			household_demographics,
			time_dim,
			store
		WHERE
			ss_sold_time_sk = time_dim.t_time_sk
			AND ss_hdemo_sk
				= household_demographics.hd_demo_sk
			AND ss_store_sk = s_store_sk
			AND time_dim.t_hour = 10
			AND time_dim.t_minute < 30
			AND (
					(
						household_demographics.hd_dep_count
						= 0
						AND household_demographics.hd_vehicle_count
							<= 0 + 2
					)
					OR (
							household_demographics.hd_dep_count
							= -1
							AND household_demographics.hd_vehicle_count
								<= -1 + 2
						)
					OR (
							household_demographics.hd_dep_count
							= 3
							AND household_demographics.hd_vehicle_count
								<= 3 + 2
						)
				)
			AND store.s_store_name = 'ese'
	)
		AS s4,
	(
		SELECT
			count(*) AS h10_30_to_11
		FROM
			store_sales,
			household_demographics,
			time_dim,
			store
		WHERE
			ss_sold_time_sk = time_dim.t_time_sk
			AND ss_hdemo_sk
				= household_demographics.hd_demo_sk
			AND ss_store_sk = s_store_sk
			AND time_dim.t_hour = 10
			AND time_dim.t_minute >= 30
			AND (
					(
						household_demographics.hd_dep_count
						= 0
						AND household_demographics.hd_vehicle_count
							<= 0 + 2
					)
					OR (
							household_demographics.hd_dep_count
							= -1
							AND household_demographics.hd_vehicle_count
								<= -1 + 2
						)
					OR (
							household_demographics.hd_dep_count
							= 3
							AND household_demographics.hd_vehicle_count
								<= 3 + 2
						)
				)
			AND store.s_store_name = 'ese'
	)
		AS s5,
	(
		SELECT
			count(*) AS h11_to_11_30
		FROM
			store_sales,
			household_demographics,
			time_dim,
			store
		WHERE
			ss_sold_time_sk = time_dim.t_time_sk
			AND ss_hdemo_sk
				= household_demographics.hd_demo_sk
			AND ss_store_sk = s_store_sk
			AND time_dim.t_hour = 11
			AND time_dim.t_minute < 30
			AND (
					(
						household_demographics.hd_dep_count
						= 0
						AND household_demographics.hd_vehicle_count
							<= 0 + 2
					)
					OR (
							household_demographics.hd_dep_count
							= -1
							AND household_demographics.hd_vehicle_count
								<= -1 + 2
						)
					OR (
							household_demographics.hd_dep_count
							= 3
							AND household_demographics.hd_vehicle_count
								<= 3 + 2
						)
				)
			AND store.s_store_name = 'ese'
	)
		AS s6,
	(
		SELECT
			count(*) AS h11_30_to_12
		FROM
			store_sales,
			household_demographics,
			time_dim,
			store
		WHERE
			ss_sold_time_sk = time_dim.t_time_sk
			AND ss_hdemo_sk
				= household_demographics.hd_demo_sk
			AND ss_store_sk = s_store_sk
			AND time_dim.t_hour = 11
			AND time_dim.t_minute >= 30
			AND (
					(
						household_demographics.hd_dep_count
						= 0
						AND household_demographics.hd_vehicle_count
							<= 0 + 2
					)
					OR (
							household_demographics.hd_dep_count
							= -1
							AND household_demographics.hd_vehicle_count
								<= -1 + 2
						)
					OR (
							household_demographics.hd_dep_count
							= 3
							AND household_demographics.hd_vehicle_count
								<= 3 + 2
						)
				)
			AND store.s_store_name = 'ese'
	)
		AS s7,
	(
		SELECT
			count(*) AS h12_to_12_30
		FROM
			store_sales,
			household_demographics,
			time_dim,
			store
		WHERE
			ss_sold_time_sk = time_dim.t_time_sk
			AND ss_hdemo_sk
				= household_demographics.hd_demo_sk
			AND ss_store_sk = s_store_sk
			AND time_dim.t_hour = 12
			AND time_dim.t_minute < 30
			AND (
					(
						household_demographics.hd_dep_count
						= 0
						AND household_demographics.hd_vehicle_count
							<= 0 + 2
					)
					OR (
							household_demographics.hd_dep_count
							= -1
							AND household_demographics.hd_vehicle_count
								<= -1 + 2
						)
					OR (
							household_demographics.hd_dep_count
							= 3
							AND household_demographics.hd_vehicle_count
								<= 3 + 2
						)
				)
			AND store.s_store_name = 'ese'
	)
		AS s8;
`

	query89 = `
SELECT
	*
FROM
	(
		SELECT
			i_category,
			i_class,
			i_brand,
			s_store_name,
			s_company_name,
			d_moy,
			sum(ss_sales_price) AS sum_sales,
			avg(sum(ss_sales_price)) OVER (
				PARTITION BY
					i_category,
					i_brand,
					s_store_name,
					s_company_name
			)
				AS avg_monthly_sales
		FROM
			item, store_sales, date_dim, store
		WHERE
			ss_item_sk = i_item_sk
			AND ss_sold_date_sk = d_date_sk
			AND ss_store_sk = s_store_sk
			AND d_year IN (2001,)
			AND (
					(
						i_category
						IN (
								'Books',
								'Children',
								'Electronics'
							)
						AND i_class
							IN (
									'history',
									'school-uniforms',
									'audio'
								)
					)
					OR (
							i_category
							IN ('Men', 'Sports', 'Shoes')
							AND i_class
								IN (
										'pants',
										'tennis',
										'womens'
									)
						)
				)
		GROUP BY
			i_category,
			i_class,
			i_brand,
			s_store_name,
			s_company_name,
			d_moy
	)
		AS tmp1
WHERE
	CASE
	WHEN (avg_monthly_sales != 0)
	THEN (
		abs(sum_sales - avg_monthly_sales)
		/ avg_monthly_sales
	)
	ELSE NULL
	END
	> 0.1
ORDER BY
	sum_sales - avg_monthly_sales, s_store_name
LIMIT
	100;
`

	query90 = `
SELECT
	CAST(amc AS DECIMAL(15,4)) / CAST(pmc AS DECIMAL(15,4))
		AS am_pm_ratio
FROM
	(
		SELECT
			count(*) AS amc
		FROM
			web_sales,
			household_demographics,
			time_dim,
			web_page
		WHERE
			ws_sold_time_sk = time_dim.t_time_sk
			AND ws_ship_hdemo_sk
				= household_demographics.hd_demo_sk
			AND ws_web_page_sk = web_page.wp_web_page_sk
			AND time_dim.t_hour BETWEEN 12 AND (12 + 1)
			AND household_demographics.hd_dep_count = 6
			AND web_page.wp_char_count BETWEEN 5000 AND 5200
	)
		AS at,
	(
		SELECT
			count(*) AS pmc
		FROM
			web_sales,
			household_demographics,
			time_dim,
			web_page
		WHERE
			ws_sold_time_sk = time_dim.t_time_sk
			AND ws_ship_hdemo_sk
				= household_demographics.hd_demo_sk
			AND ws_web_page_sk = web_page.wp_web_page_sk
			AND time_dim.t_hour BETWEEN 14 AND (14 + 1)
			AND household_demographics.hd_dep_count = 6
			AND web_page.wp_char_count BETWEEN 5000 AND 5200
	)
		AS pt
ORDER BY
	am_pm_ratio
LIMIT
	100;
`

	query91 = `
SELECT
	cc_call_center_id AS call_center,
	cc_name AS call_center_name,
	cc_manager AS manager,
	sum(cr_net_loss) AS returns_loss
FROM
	call_center,
	catalog_returns,
	date_dim,
	customer,
	customer_address,
	customer_demographics,
	household_demographics
WHERE
	cr_call_center_sk = cc_call_center_sk
	AND cr_returned_date_sk = d_date_sk
	AND cr_returning_customer_sk = c_customer_sk
	AND cd_demo_sk = c_current_cdemo_sk
	AND hd_demo_sk = c_current_hdemo_sk
	AND ca_address_sk = c_current_addr_sk
	AND d_year = 2000
	AND d_moy = 12
	AND (
			(
				cd_marital_status = 'M'
				AND cd_education_status = 'Unknown'
			)
			OR (
					cd_marital_status = 'W'
					AND cd_education_status
						= 'Advanced Degree'
				)
		)
	AND hd_buy_potential LIKE 'Unknown%'
	AND ca_gmt_offset = -7
GROUP BY
	cc_call_center_id,
	cc_name,
	cc_manager,
	cd_marital_status,
	cd_education_status
ORDER BY
	sum(cr_net_loss) DESC;
`

	// NOTE: I added conversion of 90 days to an interval.
	query92 = `
SELECT
	sum(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM
	web_sales, item, date_dim
WHERE
	i_manufact_id = 714
	AND i_item_sk = ws_item_sk
	AND d_date BETWEEN '2000-02-01' AND (CAST('2000-02-01' AS DATE) + '90 days'::INTERVAL)
	AND d_date_sk = ws_sold_date_sk
	AND ws_ext_discount_amt
		> (
				SELECT
					1.3 * avg(ws_ext_discount_amt)
				FROM
					web_sales, date_dim
				WHERE
					ws_item_sk = i_item_sk
					AND d_date BETWEEN '2000-02-01' AND (CAST('2000-02-01' AS DATE) + '90 days'::INTERVAL)
					AND d_date_sk = ws_sold_date_sk
			)
ORDER BY
	sum(ws_ext_discount_amt)
LIMIT
	100;
`

	query93 = `
SELECT
	ss_customer_sk, sum(act_sales) AS sumsales
FROM
	(
		SELECT
			ss_item_sk,
			ss_ticket_number,
			ss_customer_sk,
			CASE
			WHEN sr_return_quantity IS NOT NULL
			THEN (ss_quantity - sr_return_quantity)
			* ss_sales_price
			ELSE (ss_quantity * ss_sales_price)
			END
				AS act_sales
		FROM
			store_sales
			LEFT JOIN store_returns ON
					sr_item_sk = ss_item_sk
					AND sr_ticket_number = ss_ticket_number,
			reason
		WHERE
			sr_reason_sk = r_reason_sk
			AND r_reason_desc = 'reason 58'
	)
		AS t
GROUP BY
	ss_customer_sk
ORDER BY
	sumsales, ss_customer_sk
LIMIT
	100;
`

	// NOTE: I added conversion of 60 days to an interval.
	query94 = `
SELECT
	count(DISTINCT ws_order_number) AS "order count",
	sum(ws_ext_ship_cost) AS "total shipping cost",
	sum(ws_net_profit) AS "total net profit"
FROM
	web_sales AS ws1, date_dim, customer_address, web_site
WHERE
	d_date BETWEEN '2002-5-01' AND (CAST('2002-5-01' AS DATE) + '60 days'::INTERVAL)
	AND ws1.ws_ship_date_sk = d_date_sk
	AND ws1.ws_ship_addr_sk = ca_address_sk
	AND ca_state = 'OK'
	AND ws1.ws_web_site_sk = web_site_sk
	AND web_company_name = 'pri'
	AND EXISTS(
			SELECT
				*
			FROM
				web_sales AS ws2
			WHERE
				ws1.ws_order_number = ws2.ws_order_number
				AND ws1.ws_warehouse_sk
					!= ws2.ws_warehouse_sk
		)
	AND NOT
			EXISTS(
				SELECT
					*
				FROM
					web_returns AS wr1
				WHERE
					ws1.ws_order_number
					= wr1.wr_order_number
			)
ORDER BY
	count(DISTINCT ws_order_number)
LIMIT
	100;
`

	// NOTE: I added conversion of 60 days to an interval.
	query95 = `
WITH
	ws_wh
		AS (
			SELECT
				ws1.ws_order_number,
				ws1.ws_warehouse_sk AS wh1,
				ws2.ws_warehouse_sk AS wh2
			FROM
				web_sales AS ws1, web_sales AS ws2
			WHERE
				ws1.ws_order_number = ws2.ws_order_number
				AND ws1.ws_warehouse_sk
					!= ws2.ws_warehouse_sk
		)
SELECT
	count(DISTINCT ws_order_number) AS "order count",
	sum(ws_ext_ship_cost) AS "total shipping cost",
	sum(ws_net_profit) AS "total net profit"
FROM
	web_sales AS ws1, date_dim, customer_address, web_site
WHERE
	d_date BETWEEN '2001-4-01' AND (CAST('2001-4-01' AS DATE) + '60 days'::INTERVAL)
	AND ws1.ws_ship_date_sk = d_date_sk
	AND ws1.ws_ship_addr_sk = ca_address_sk
	AND ca_state = 'VA'
	AND ws1.ws_web_site_sk = web_site_sk
	AND web_company_name = 'pri'
	AND ws1.ws_order_number
		IN (SELECT ws_order_number FROM ws_wh)
	AND ws1.ws_order_number
		IN (
				SELECT
					wr_order_number
				FROM
					web_returns, ws_wh
				WHERE
					wr_order_number = ws_wh.ws_order_number
			)
ORDER BY
	count(DISTINCT ws_order_number)
LIMIT
	100;
`

	query96 = `
SELECT
	count(*)
FROM
	store_sales, household_demographics, time_dim, store
WHERE
	ss_sold_time_sk = time_dim.t_time_sk
	AND ss_hdemo_sk = household_demographics.hd_demo_sk
	AND ss_store_sk = s_store_sk
	AND time_dim.t_hour = 8
	AND time_dim.t_minute >= 30
	AND household_demographics.hd_dep_count = 0
	AND store.s_store_name = 'ese'
ORDER BY
	count(*)
LIMIT
	100;
`

	query97 = `
WITH
	ssci
		AS (
			SELECT
				ss_customer_sk AS customer_sk,
				ss_item_sk AS item_sk
			FROM
				store_sales, date_dim
			WHERE
				ss_sold_date_sk = d_date_sk
				AND d_month_seq BETWEEN 1199 AND (1199 + 11)
			GROUP BY
				ss_customer_sk, ss_item_sk
		),
	csci
		AS (
			SELECT
				cs_bill_customer_sk AS customer_sk,
				cs_item_sk AS item_sk
			FROM
				catalog_sales, date_dim
			WHERE
				cs_sold_date_sk = d_date_sk
				AND d_month_seq BETWEEN 1199 AND (1199 + 11)
			GROUP BY
				cs_bill_customer_sk, cs_item_sk
		)
SELECT
	sum(
		CASE
		WHEN ssci.customer_sk IS NOT NULL
		AND csci.customer_sk IS NULL
		THEN 1
		ELSE 0
		END
	)
		AS store_only,
	sum(
		CASE
		WHEN ssci.customer_sk IS NULL
		AND csci.customer_sk IS NOT NULL
		THEN 1
		ELSE 0
		END
	)
		AS catalog_only,
	sum(
		CASE
		WHEN ssci.customer_sk IS NOT NULL
		AND csci.customer_sk IS NOT NULL
		THEN 1
		ELSE 0
		END
	)
		AS store_and_catalog
FROM
	ssci
	FULL JOIN csci ON
			ssci.customer_sk = csci.customer_sk
			AND ssci.item_sk = csci.item_sk
LIMIT
	100;
`

	// NOTE: I added conversion of 30 days to an interval.
	query98 = `
SELECT
	i_item_id,
	i_item_desc,
	i_category,
	i_class,
	i_current_price,
	sum(ss_ext_sales_price) AS itemrevenue,
	sum(ss_ext_sales_price) * 100
	/ sum(sum(ss_ext_sales_price)) OVER (
			PARTITION BY i_class
		)
		AS revenueratio
FROM
	store_sales, item, date_dim
WHERE
	ss_item_sk = i_item_sk
	AND i_category IN ('Men', 'Sports', 'Jewelry')
	AND ss_sold_date_sk = d_date_sk
	AND d_date BETWEEN CAST('1999-02-05' AS DATE) AND (CAST('1999-02-05' AS DATE) + '30 days'::INTERVAL)
GROUP BY
	i_item_id,
	i_item_desc,
	i_category,
	i_class,
	i_current_price
ORDER BY
	i_category,
	i_class,
	i_item_id,
	i_item_desc,
	revenueratio;
`

	query99 = `
SELECT
	substr(w_warehouse_name, 1, 20),
	sm_type,
	cc_name,
	sum(
		CASE
		WHEN (cs_ship_date_sk - cs_sold_date_sk <= 30)
		THEN 1
		ELSE 0
		END
	)
		AS "30 days",
	sum(
		CASE
		WHEN cs_ship_date_sk - cs_sold_date_sk > 30
		AND cs_ship_date_sk - cs_sold_date_sk <= 60
		THEN 1
		ELSE 0
		END
	)
		AS "31-60 days",
	sum(
		CASE
		WHEN cs_ship_date_sk - cs_sold_date_sk > 60
		AND cs_ship_date_sk - cs_sold_date_sk <= 90
		THEN 1
		ELSE 0
		END
	)
		AS "61-90 days",
	sum(
		CASE
		WHEN cs_ship_date_sk - cs_sold_date_sk > 90
		AND cs_ship_date_sk - cs_sold_date_sk <= 120
		THEN 1
		ELSE 0
		END
	)
		AS "91-120 days",
	sum(
		CASE
		WHEN (cs_ship_date_sk - cs_sold_date_sk > 120)
		THEN 1
		ELSE 0
		END
	)
		AS ">120 days"
FROM
	catalog_sales,
	warehouse,
	ship_mode,
	call_center,
	date_dim
WHERE
	d_month_seq BETWEEN 1194 AND (1194 + 11)
	AND cs_ship_date_sk = d_date_sk
	AND cs_warehouse_sk = w_warehouse_sk
	AND cs_ship_mode_sk = sm_ship_mode_sk
	AND cs_call_center_sk = cc_call_center_sk
GROUP BY
	substr(w_warehouse_name, 1, 20), sm_type, cc_name
ORDER BY
	substr(w_warehouse_name, 1, 20), sm_type, cc_name
LIMIT
	100;
`
)
