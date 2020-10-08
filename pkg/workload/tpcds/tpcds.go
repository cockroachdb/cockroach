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

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

type tpcds struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	queriesToRunRaw  string
	queriesToOmitRaw string
	queryTimeLimit   time.Duration
	selectedQueries  []int
	vectorize        string
}

func init() {
	workload.Register(tpcdsMeta)
}

var tpcdsMeta = workload.Meta{
	Name:        `tpcds`,
	Description: `TPC-DS is a read-only workload of "decision support" queries on large datasets.`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		g := &tpcds{}
		g.flags.FlagSet = pflag.NewFlagSet(`tpcds`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`queries-to-omit`:  {RuntimeOnly: true},
			`queries-to-run`:   {RuntimeOnly: true},
			`query-time-limit`: {RuntimeOnly: true},
			`vectorize`:        {RuntimeOnly: true},
		}

		// NOTE: we're skipping queries 27, 36, 70, and 86 by default at the moment
		// because they require some modifications.
		g.flags.StringVar(&g.queriesToOmitRaw, `queries-to-omit`,
			`27,36,70,86`,
			`Queries not to run (i.e. all others will be run). Use a comma separated list of query numbers`)
		g.flags.StringVar(&g.queriesToRunRaw, `queries`,
			``,
			`Queries to run. Use a comma separated list of query numbers. If omitted, all queries are run. `+
				`Note that --queries-to-omit flag has a higher precedence`)
		g.flags.DurationVar(&g.queryTimeLimit, `query-time-limit`, 5*time.Minute,
			`Time limit for a single run of a query`)
		g.flags.StringVar(&g.vectorize, `vectorize`, `on`,
			`Set vectorize session variable`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*tpcds) Meta() workload.Meta { return tpcdsMeta }

// Flags implements the Flagser interface.
func (w *tpcds) Flags() workload.Flags { return w.flags }

// Hooks implements the Hookser interface.
func (w *tpcds) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			if w.queryTimeLimit <= 0 {
				return errors.Errorf("non-positive query time limit was set: %s", w.queryTimeLimit)
			}
			skipQuery := make([]bool, NumQueries+1)
			for _, queryName := range strings.Split(w.queriesToOmitRaw, `,`) {
				queryNum, err := strconv.Atoi(queryName)
				if err != nil {
					return err
				}
				if queryNum < 1 || queryNum > NumQueries {
					return errors.Errorf("unknown query %d (only queries in range [1, %d] are supported)",
						queryNum, NumQueries)
				}
				skipQuery[queryNum] = true
			}
			if w.queriesToRunRaw != `` {
				for _, queryName := range strings.Split(w.queriesToRunRaw, `,`) {
					queryNum, err := strconv.Atoi(queryName)
					if err != nil {
						return err
					}
					if _, ok := QueriesByNumber[queryNum]; !ok {
						return errors.Errorf(`unknown query: %s (probably, the query needs modifications, `+
							`so it is disabled for now)`, queryName)
					}
					if !skipQuery[queryNum] {
						w.selectedQueries = append(w.selectedQueries, queryNum)
					}
				}
				return nil
			}
			for queryNum := 1; queryNum <= NumQueries; queryNum++ {
				if !skipQuery[queryNum] {
					w.selectedQueries = append(w.selectedQueries, queryNum)
				}
			}
			return nil
		},
	}
}

// Tables implements the Generator interface.
func (w *tpcds) Tables() []workload.Table {
	// Note: we specify InitialRows for the Tables with non-zero count of tuples
	// so that `workload init` returns an error that tpcds doesn't support init.
	return []workload.Table{
		{
			Name:        `call_center`,
			Schema:      tpcdsCallCenterSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `catalog_page`,
			Schema:      tpcdsCatalogPageSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `catalog_returns`,
			Schema:      tpcdsCatalogReturnsSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `catalog_sales`,
			Schema:      tpcdsCatalogSalesSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `customer`,
			Schema:      tpcdsCustomerSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `customer_address`,
			Schema:      tpcdsCustomerAddressSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `customer_demographics`,
			Schema:      tpcdsCustomerDemographicsSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `date_dim`,
			Schema:      tpcdsDateDimSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `dbgen_version`,
			Schema:      tpcdsDbgenVersionSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `household_demographics`,
			Schema:      tpcdsHouseholdDemographicsSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `income_band`,
			Schema:      tpcdsIncomeBandSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `inventory`,
			Schema:      tpcdsInventorySchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `item`,
			Schema:      tpcdsItemSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `promotion`,
			Schema:      tpcdsPromotionSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `reason`,
			Schema:      tpcdsReasonSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `ship_mode`,
			Schema:      tpcdsShipModeSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `store`,
			Schema:      tpcdsStoreSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `store_returns`,
			Schema:      tpcdsStoreReturnsSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `store_sales`,
			Schema:      tpcdsStoreSalesSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `time_dim`,
			Schema:      tpcdsTimeDimSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `warehouse`,
			Schema:      tpcdsWarehouseSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `web_page`,
			Schema:      tpcdsWebPageSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `web_returns`,
			Schema:      tpcdsWebReturnsSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `web_sales`,
			Schema:      tpcdsWebSalesSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
		{
			Name:        `web_site`,
			Schema:      tpcdsWebSiteSchema,
			InitialRows: workload.Tuples(-1, nil),
		},
	}
}

// Ops implements the Opser interface.
func (w *tpcds) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(w, w.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(w.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(w.connFlags.Concurrency + 1)

	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < w.connFlags.Concurrency; i++ {
		worker := &worker{
			config: w,
			db:     db,
		}
		ql.WorkerFns = append(ql.WorkerFns, worker.run)
	}
	return ql, nil
}

type worker struct {
	config *tpcds
	db     *gosql.DB
	ops    int
}

func (w *worker) run(ctx context.Context) error {
	queryNum := w.config.selectedQueries[w.ops%len(w.config.selectedQueries)]
	w.ops++

	prep := fmt.Sprintf("SET statement_timeout='%s'; SET vectorize=%s;",
		w.config.queryTimeLimit, w.config.vectorize)
	_, err := w.db.Exec(prep)
	if err != nil {
		return err
	}
	query := QueriesByNumber[queryNum]

	var rows *gosql.Rows
	start := timeutil.Now()
	err = func() error {
		done := make(chan error, 1)
		go func(context.Context) {
			var err error
			rows, err = w.db.Query(query)
			done <- err
		}(ctx)
		select {
		case <-time.After(w.config.queryTimeLimit * 2):
			return errors.Errorf("[q%d] timed out, but did not cancel execution", queryNum)
		case err := <-done:
			return err
		}
	}()
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		log.Infof(ctx, "[q%d] error: %s", queryNum, err)
		return err
	}
	var numRows int
	for rows.Next() {
		numRows++
	}
	if err := rows.Err(); err != nil {
		log.Infof(ctx, "[q%d] error: %s", queryNum, err)
		return err
	}
	elapsed := timeutil.Since(start)
	// TODO(yuzefovich): at the moment, we're not printing out the histograms
	// since that would just be too much noise; however, having the percentiles
	// in the output would also be useful.
	log.Infof(ctx, "[q%d] returned %d rows after %.2f seconds",
		queryNum, numRows, elapsed.Seconds())
	return nil
}

const (
	tpcdsDbgenVersionSchema = `(
	dv_version      VARCHAR(16),
	dv_create_date  DATE,
	dv_create_time  TIME,
	dv_cmdline_args VARCHAR(200)
)`

	tpcdsCustomerAddressSchema = `(
	ca_address_sk    INT8 NOT NULL,
	ca_address_id    CHAR(16) NOT NULL,
	ca_street_number CHAR(10),
	ca_street_name   VARCHAR(60),
	ca_street_type   CHAR(15),
	ca_suite_number  CHAR(10),
	ca_city          VARCHAR(60),
	ca_county        VARCHAR(30),
	ca_state         CHAR(2),
	ca_zip           CHAR(10),
	ca_country       VARCHAR(20),
	ca_gmt_offset    DECIMAL(5,2),
	ca_location_type CHAR(20),
	PRIMARY KEY (ca_address_sk)
)`

	tpcdsCustomerDemographicsSchema = `(
	cd_demo_sk            INT8 NOT NULL,
	cd_gender             CHAR,
	cd_marital_status     CHAR,
	cd_education_status   CHAR(20),
	cd_purchase_estimate  INT8,
	cd_credit_rating      CHAR(10),
	cd_dep_count          INT8,
	cd_dep_employed_count INT8,
	cd_dep_college_count  INT8,
	PRIMARY KEY (cd_demo_sk)
)`

	tpcdsDateDimSchema = `(
	d_date_sk           INT8 NOT NULL,
	d_date_id           CHAR(16) NOT NULL,
	d_date              DATE,
	d_month_seq         INT8,
	d_week_seq          INT8,
	d_quarter_seq       INT8,
	d_year              INT8,
	d_dow               INT8,
	d_moy               INT8,
	d_dom               INT8,
	d_qoy               INT8,
	d_fy_year           INT8,
	d_fy_quarter_seq    INT8,
	d_fy_week_seq       INT8,
	d_day_name          CHAR(9),
	d_quarter_name      CHAR(6),
	d_holiday           CHAR,
	d_weekend           CHAR,
	d_following_holiday CHAR,
	d_first_dom         INT8,
	d_last_dom          INT8,
	d_same_day_ly       INT8,
	d_same_day_lq       INT8,
	d_current_day       CHAR,
	d_current_week      CHAR,
	d_current_month     CHAR,
	d_current_quarter   CHAR,
	d_current_year      CHAR,
	PRIMARY KEY (d_date_sk)
)`

	tpcdsWarehouseSchema = `(
	w_warehouse_sk    INT8 NOT NULL,
	w_warehouse_id    CHAR(16) NOT NULL,
	w_warehouse_name  VARCHAR(20),
	w_warehouse_sq_ft INT8,
	w_street_number   CHAR(10),
	w_street_name     VARCHAR(60),
	w_street_type     CHAR(15),
	w_suite_number    CHAR(10),
	w_city            VARCHAR(60),
	w_county          VARCHAR(30),
	w_state           CHAR(2),
	w_zip             CHAR(10),
	w_country         VARCHAR(20),
	w_gmt_offset      DECIMAL(5,2),
	PRIMARY KEY (w_warehouse_sk)
)`

	tpcdsShipModeSchema = `(
	sm_ship_mode_sk INT8 NOT NULL,
	sm_ship_mode_id CHAR(16) NOT NULL,
	sm_type         CHAR(30),
	sm_code         CHAR(10),
	sm_carrier      CHAR(20),
	sm_contract     CHAR(20),
	PRIMARY KEY (sm_ship_mode_sk)
)`

	tpcdsTimeDimSchema = `(
	t_time_sk   INT8 NOT NULL,
	t_time_id   CHAR(16) NOT NULL,
	t_time      INT8,
	t_hour      INT8,
	t_minute    INT8,
	t_second    INT8,
	t_am_pm     CHAR(2),
	t_shift     CHAR(20),
	t_sub_shift CHAR(20),
	t_meal_time CHAR(20),
	PRIMARY KEY (t_time_sk)
)`

	tpcdsReasonSchema = `(
	r_reason_sk   INT8 NOT NULL,
	r_reason_id   CHAR(16) NOT NULL,
	r_reason_desc CHAR(100),
	PRIMARY KEY (r_reason_sk)
)`

	tpcdsIncomeBandSchema = `(
	ib_income_band_sk INT8 NOT NULL,
	ib_lower_bound    INT8,
	ib_upper_bound    INT8,
	PRIMARY KEY (ib_income_band_sk)
)`

	tpcdsItemSchema = `(
	i_item_sk        INT8 NOT NULL,
	i_item_id        CHAR(16) NOT NULL,
	i_rec_start_date DATE,
	i_rec_end_date   DATE,
	i_item_desc      VARCHAR(200),
	i_current_price  DECIMAL(7,2),
	i_wholesale_cost DECIMAL(7,2),
	i_brand_id       INT8,
	i_brand          CHAR(50),
	i_class_id       INT8,
	i_class          CHAR(50),
	i_category_id    INT8,
	i_category       CHAR(50),
	i_manufact_id    INT8,
	i_manufact       CHAR(50),
	i_size           CHAR(20),
	i_formulation    CHAR(20),
	i_color          CHAR(20),
	i_units          CHAR(10),
	i_container      CHAR(10),
	i_manager_id     INT8,
	i_product_name   CHAR(50),
	PRIMARY KEY (i_item_sk)
)`

	tpcdsStoreSchema = `(
	s_store_sk         INT8 NOT NULL,
	s_store_id         CHAR(16) NOT NULL,
	s_rec_start_date   DATE,
	s_rec_end_date     DATE,
	s_closed_date_sk   INT8,
	s_store_name       VARCHAR(50),
	s_number_employees INT8,
	s_floor_space      INT8,
	s_hours            CHAR(20),
	s_manager          VARCHAR(40),
	s_market_id        INT8,
	s_geography_class  VARCHAR(100),
	s_market_desc      VARCHAR(100),
	s_market_manager   VARCHAR(40),
	s_division_id      INT8,
	s_division_name    VARCHAR(50),
	s_company_id       INT8,
	s_company_name     VARCHAR(50),
	s_street_number    VARCHAR(10),
	s_street_name      VARCHAR(60),
	s_street_type      CHAR(15),
	s_suite_number     CHAR(10),
	s_city             VARCHAR(60),
	s_county           VARCHAR(30),
	s_state            CHAR(2),
	s_zip              CHAR(10),
	s_country          VARCHAR(20),
	s_gmt_offset       DECIMAL(5,2),
	s_tax_precentage   DECIMAL(5,2),
	PRIMARY KEY (s_store_sk)
)`

	tpcdsCallCenterSchema = `(
	cc_call_center_sk INT8 NOT NULL,
	cc_call_center_id CHAR(16) NOT NULL,
	cc_rec_start_date DATE,
	cc_rec_end_date   DATE,
	cc_closed_date_sk INT8,
	cc_open_date_sk   INT8,
	cc_name           VARCHAR(50),
	cc_class          VARCHAR(50),
	cc_employees      INT8,
	cc_sq_ft          INT8,
	cc_hours          CHAR(20),
	cc_manager        VARCHAR(40),
	cc_mkt_id         INT8,
	cc_mkt_class      CHAR(50),
	cc_mkt_desc       VARCHAR(100),
	cc_market_manager VARCHAR(40),
	cc_division       INT8,
	cc_division_name  VARCHAR(50),
	cc_company        INT8,
	cc_company_name   CHAR(50),
	cc_street_number  CHAR(10),
	cc_street_name    VARCHAR(60),
	cc_street_type    CHAR(15),
	cc_suite_number   CHAR(10),
	cc_city           VARCHAR(60),
	cc_county         VARCHAR(30),
	cc_state          CHAR(2),
	cc_zip            CHAR(10),
	cc_country        VARCHAR(20),
	cc_gmt_offset     DECIMAL(5,2),
	cc_tax_percentage DECIMAL(5,2),
	PRIMARY KEY (cc_call_center_sk)
)`

	tpcdsCustomerSchema = `(
	c_customer_sk          INT8 NOT NULL,
	c_customer_id          CHAR(16) NOT NULL,
	c_current_cdemo_sk     INT8,
	c_current_hdemo_sk     INT8,
	c_current_addr_sk      INT8,
	c_first_shipto_date_sk INT8,
	c_first_sales_date_sk  INT8,
	c_salutation           CHAR(10),
	c_first_name           CHAR(20),
	c_last_name            CHAR(30),
	c_preferred_cust_flag  CHAR,
	c_birth_day            INT8,
	c_birth_month          INT8,
	c_birth_year           INT8,
	c_birth_country        VARCHAR(20),
	c_login                CHAR(13),
	c_email_address        CHAR(50),
	c_last_review_date     CHAR(10),
	PRIMARY KEY (c_customer_sk)
)`

	tpcdsWebSiteSchema = `(
	web_site_sk        INT8 NOT NULL,
	web_site_id        CHAR(16) NOT NULL,
	web_rec_start_date DATE,
	web_rec_end_date   DATE,
	web_name           VARCHAR(50),
	web_open_date_sk   INT8,
	web_close_date_sk  INT8,
	web_class          VARCHAR(50),
	web_manager        VARCHAR(40),
	web_mkt_id         INT8,
	web_mkt_class      VARCHAR(50),
	web_mkt_desc       VARCHAR(100),
	web_market_manager VARCHAR(40),
	web_company_id     INT8,
	web_company_name   CHAR(50),
	web_street_number  CHAR(10),
	web_street_name    VARCHAR(60),
	web_street_type    CHAR(15),
	web_suite_number   CHAR(10),
	web_city           VARCHAR(60),
	web_county         VARCHAR(30),
	web_state          CHAR(2),
	web_zip            CHAR(10),
	web_country        VARCHAR(20),
	web_gmt_offset     DECIMAL(5,2),
	web_tax_percentage DECIMAL(5,2),
	PRIMARY KEY (web_site_sk)
)`

	tpcdsStoreReturnsSchema = `(
	sr_returned_date_sk   INT8,
	sr_return_time_sk     INT8,
	sr_item_sk            INT8 NOT NULL,
	sr_customer_sk        INT8,
	sr_cdemo_sk           INT8,
	sr_hdemo_sk           INT8,
	sr_addr_sk            INT8,
	sr_store_sk           INT8,
	sr_reason_sk          INT8,
	sr_ticket_number      INT8 NOT NULL,
	sr_return_quantity    INT8,
	sr_return_amt         DECIMAL(7,2),
	sr_return_tax         DECIMAL(7,2),
	sr_return_amt_inc_tax DECIMAL(7,2),
	sr_fee                DECIMAL(7,2),
	sr_return_ship_cost   DECIMAL(7,2),
	sr_refunded_cash      DECIMAL(7,2),
	sr_reversed_charge    DECIMAL(7,2),
	sr_store_credit       DECIMAL(7,2),
	sr_net_loss           DECIMAL(7,2),
	PRIMARY KEY (sr_item_sk, sr_ticket_number)
)`

	tpcdsHouseholdDemographicsSchema = `(
	hd_demo_sk        INT8 NOT NULL,
	hd_income_band_sk INT8,
	hd_buy_potential  CHAR(15),
	hd_dep_count      INT8,
	hd_vehicle_count  INT8,
	PRIMARY KEY (hd_demo_sk)
)`

	tpcdsWebPageSchema = `(
	wp_web_page_sk      INT8 NOT NULL,
	wp_web_page_id      CHAR(16) NOT NULL,
	wp_rec_start_date   DATE,
	wp_rec_end_date     DATE,
	wp_creation_date_sk INT8,
	wp_access_date_sk   INT8,
	wp_autogen_flag     CHAR,
	wp_customer_sk      INT8,
	wp_url              VARCHAR(100),
	wp_type             CHAR(50),
	wp_char_count       INT8,
	wp_link_count       INT8,
	wp_image_count      INT8,
	wp_max_ad_count     INT8,
	PRIMARY KEY (wp_web_page_sk)
)`

	tpcdsPromotionSchema = `(
	p_promo_sk        INT8 NOT NULL,
	p_promo_id        CHAR(16) NOT NULL,
	p_start_date_sk   INT8,
	p_end_date_sk     INT8,
	p_item_sk         INT8,
	p_cost            DECIMAL(15,2),
	p_response_target INT8,
	p_promo_name      CHAR(50),
	p_channel_dmail   CHAR,
	p_channel_email   CHAR,
	p_channel_catalog CHAR,
	p_channel_tv      CHAR,
	p_channel_radio   CHAR,
	p_channel_press   CHAR,
	p_channel_event   CHAR,
	p_channel_demo    CHAR,
	p_channel_details VARCHAR(100),
	p_purpose         CHAR(15),
	p_discount_active CHAR,
	PRIMARY KEY (p_promo_sk)
)`

	tpcdsCatalogPageSchema = `(
	cp_catalog_page_sk     INT8 NOT NULL,
	cp_catalog_page_id     CHAR(16) NOT NULL,
	cp_start_date_sk       INT8,
	cp_end_date_sk         INT8,
	cp_department          VARCHAR(50),
	cp_catalog_number      INT8,
	cp_catalog_page_number INT8,
	cp_description         VARCHAR(100),
	cp_type                VARCHAR(100),
	PRIMARY KEY (cp_catalog_page_sk)
)`

	tpcdsInventorySchema = `(
	inv_date_sk          INT8 NOT NULL,
	inv_item_sk          INT8 NOT NULL,
	inv_warehouse_sk     INT8 NOT NULL,
	inv_quantity_on_hand INT8,
	PRIMARY KEY (inv_date_sk, inv_item_sk, inv_warehouse_sk)
)`

	tpcdsCatalogReturnsSchema = `(
	cr_returned_date_sk      INT8,
	cr_returned_time_sk      INT8,
	cr_item_sk               INT8 NOT NULL,
	cr_refunded_customer_sk  INT8,
	cr_refunded_cdemo_sk     INT8,
	cr_refunded_hdemo_sk     INT8,
	cr_refunded_addr_sk      INT8,
	cr_returning_customer_sk INT8,
	cr_returning_cdemo_sk    INT8,
	cr_returning_hdemo_sk    INT8,
	cr_returning_addr_sk     INT8,
	cr_call_center_sk        INT8,
	cr_catalog_page_sk       INT8,
	cr_ship_mode_sk          INT8,
	cr_warehouse_sk          INT8,
	cr_reason_sk             INT8,
	cr_order_number          INT8 NOT NULL,
	cr_return_quantity       INT8,
	cr_return_amount         DECIMAL(7,2),
	cr_return_tax            DECIMAL(7,2),
	cr_return_amt_inc_tax    DECIMAL(7,2),
	cr_fee                   DECIMAL(7,2),
	cr_return_ship_cost      DECIMAL(7,2),
	cr_refunded_cash         DECIMAL(7,2),
	cr_reversed_charge       DECIMAL(7,2),
	cr_store_credit          DECIMAL(7,2),
	cr_net_loss              DECIMAL(7,2),
	PRIMARY KEY (cr_item_sk, cr_order_number)
)`

	tpcdsWebReturnsSchema = `(
	wr_returned_date_sk      INT8,
	wr_returned_time_sk      INT8,
	wr_item_sk               INT8 NOT NULL,
	wr_refunded_customer_sk  INT8,
	wr_refunded_cdemo_sk     INT8,
	wr_refunded_hdemo_sk     INT8,
	wr_refunded_addr_sk      INT8,
	wr_returning_customer_sk INT8,
	wr_returning_cdemo_sk    INT8,
	wr_returning_hdemo_sk    INT8,
	wr_returning_addr_sk     INT8,
	wr_web_page_sk           INT8,
	wr_reason_sk             INT8,
	wr_order_number          INT8 NOT NULL,
	wr_return_quantity       INT8,
	wr_return_amt            DECIMAL(7,2),
	wr_return_tax            DECIMAL(7,2),
	wr_return_amt_inc_tax    DECIMAL(7,2),
	wr_fee                   DECIMAL(7,2),
	wr_return_ship_cost      DECIMAL(7,2),
	wr_refunded_cash         DECIMAL(7,2),
	wr_reversed_charge       DECIMAL(7,2),
	wr_account_credit        DECIMAL(7,2),
	wr_net_loss              DECIMAL(7,2),
	PRIMARY KEY (wr_item_sk, wr_order_number)
)`

	tpcdsWebSalesSchema = `(
	ws_sold_date_sk          INT8,
	ws_sold_time_sk          INT8,
	ws_ship_date_sk          INT8,
	ws_item_sk               INT8 NOT NULL,
	ws_bill_customer_sk      INT8,
	ws_bill_cdemo_sk         INT8,
	ws_bill_hdemo_sk         INT8,
	ws_bill_addr_sk          INT8,
	ws_ship_customer_sk      INT8,
	ws_ship_cdemo_sk         INT8,
	ws_ship_hdemo_sk         INT8,
	ws_ship_addr_sk          INT8,
	ws_web_page_sk           INT8,
	ws_web_site_sk           INT8,
	ws_ship_mode_sk          INT8,
	ws_warehouse_sk          INT8,
	ws_promo_sk              INT8,
	ws_order_number          INT8 NOT NULL,
	ws_quantity              INT8,
	ws_wholesale_cost        DECIMAL(7,2),
	ws_list_price            DECIMAL(7,2),
	ws_sales_price           DECIMAL(7,2),
	ws_ext_discount_amt      DECIMAL(7,2),
	ws_ext_sales_price       DECIMAL(7,2),
	ws_ext_wholesale_cost    DECIMAL(7,2),
	ws_ext_list_price        DECIMAL(7,2),
	ws_ext_tax               DECIMAL(7,2),
	ws_coupon_amt            DECIMAL(7,2),
	ws_ext_ship_cost         DECIMAL(7,2),
	ws_net_paid              DECIMAL(7,2),
	ws_net_paid_inc_tax      DECIMAL(7,2),
	ws_net_paid_inc_ship     DECIMAL(7,2),
	ws_net_paid_inc_ship_tax DECIMAL(7,2),
	ws_net_profit            DECIMAL(7,2),
	PRIMARY KEY (ws_item_sk, ws_order_number)
)`

	tpcdsCatalogSalesSchema = `(
	cs_sold_date_sk          INT8,
	cs_sold_time_sk          INT8,
	cs_ship_date_sk          INT8,
	cs_bill_customer_sk      INT8,
	cs_bill_cdemo_sk         INT8,
	cs_bill_hdemo_sk         INT8,
	cs_bill_addr_sk          INT8,
	cs_ship_customer_sk      INT8,
	cs_ship_cdemo_sk         INT8,
	cs_ship_hdemo_sk         INT8,
	cs_ship_addr_sk          INT8,
	cs_call_center_sk        INT8,
	cs_catalog_page_sk       INT8,
	cs_ship_mode_sk          INT8,
	cs_warehouse_sk          INT8,
	cs_item_sk               INT8 NOT NULL,
	cs_promo_sk              INT8,
	cs_order_number          INT8 NOT NULL,
	cs_quantity              INT8,
	cs_wholesale_cost        DECIMAL(7,2),
	cs_list_price            DECIMAL(7,2),
	cs_sales_price           DECIMAL(7,2),
	cs_ext_discount_amt      DECIMAL(7,2),
	cs_ext_sales_price       DECIMAL(7,2),
	cs_ext_wholesale_cost    DECIMAL(7,2),
	cs_ext_list_price        DECIMAL(7,2),
	cs_ext_tax               DECIMAL(7,2),
	cs_coupon_amt            DECIMAL(7,2),
	cs_ext_ship_cost         DECIMAL(7,2),
	cs_net_paid              DECIMAL(7,2),
	cs_net_paid_inc_tax      DECIMAL(7,2),
	cs_net_paid_inc_ship     DECIMAL(7,2),
	cs_net_paid_inc_ship_tax DECIMAL(7,2),
	cs_net_profit            DECIMAL(7,2),
	PRIMARY KEY (cs_item_sk, cs_order_number)
)`

	tpcdsStoreSalesSchema = `(
	ss_sold_date_sk       INT8,
	ss_sold_time_sk       INT8,
	ss_item_sk            INT8 NOT NULL,
	ss_customer_sk        INT8,
	ss_cdemo_sk           INT8,
	ss_hdemo_sk           INT8,
	ss_addr_sk            INT8,
	ss_store_sk           INT8,
	ss_promo_sk           INT8,
	ss_ticket_number      INT8 NOT NULL,
	ss_quantity           INT8,
	ss_wholesale_cost     DECIMAL(7,2),
	ss_list_price         DECIMAL(7,2),
	ss_sales_price        DECIMAL(7,2),
	ss_ext_discount_amt   DECIMAL(7,2),
	ss_ext_sales_price    DECIMAL(7,2),
	ss_ext_wholesale_cost DECIMAL(7,2),
	ss_ext_list_price     DECIMAL(7,2),
	ss_ext_tax            DECIMAL(7,2),
	ss_coupon_amt         DECIMAL(7,2),
	ss_net_paid           DECIMAL(7,2),
	ss_net_paid_inc_tax   DECIMAL(7,2),
	ss_net_profit         DECIMAL(7,2),
	PRIMARY KEY (ss_item_sk, ss_ticket_number)
)`
)
