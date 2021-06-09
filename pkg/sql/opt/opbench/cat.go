// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opbench

import "github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"

// TODO(justin): pull schema definitions like this into a file that can be
// imported here as well as in the data-driven tests.

// MakeTPCHCatalog returns a test catalog loaded with the TPCH schema
// and statistics for scale factor 2.
//
// If you need to re-collect these stats, you can load the TPC-H dataset into a
// CockroachDB cluster and run `EXPLAIN (opt, env)` with a query that joins all
// 8 tables in the TPC-H database. This will produce the stats without
// histograms.
func MakeTPCHCatalog() *testcat.Catalog {
	cat := testcat.New()

	if err := cat.ExecuteMultipleDDL(`
CREATE TABLE region (
	r_regionkey INT8 NOT NULL,
	r_name CHAR(25) NOT NULL,
	r_comment VARCHAR(152) NULL,
	CONSTRAINT "primary" PRIMARY KEY (r_regionkey ASC),
	FAMILY "primary" (r_regionkey, r_name, r_comment)
);

ALTER TABLE region INJECT STATISTICS '[
    {
        "columns": [
            "r_regionkey"
        ],
        "created_at": "2021-06-22 20:49:36.59663",
        "distinct_count": 5,
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 5
    },
    {
        "columns": [
            "r_name"
        ],
        "created_at": "2021-06-22 20:49:36.59663",
        "distinct_count": 5,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 5
    },
    {
        "columns": [
            "r_comment"
        ],
        "created_at": "2021-06-22 20:49:36.59663",
        "distinct_count": 5,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 5
    }
]';

CREATE TABLE nation (
	n_nationkey INT8 NOT NULL,
	n_name CHAR(25) NOT NULL,
	n_regionkey INT8 NOT NULL,
	n_comment VARCHAR(152) NULL,
	CONSTRAINT "primary" PRIMARY KEY (n_nationkey ASC),
	CONSTRAINT nation_fkey_region FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey) NOT VALID,
	INDEX n_rk (n_regionkey ASC),
	FAMILY "primary" (n_nationkey, n_name, n_regionkey, n_comment)
);

ALTER TABLE nation INJECT STATISTICS '[
    {
        "columns": [
            "n_nationkey"
        ],
        "created_at": "2021-06-22 20:49:36.446819",
        "distinct_count": 25,
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 25
    },
    {
        "columns": [
            "n_regionkey"
        ],
        "created_at": "2021-06-22 20:49:36.446819",
        "distinct_count": 5,
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 25
    },
    {
        "columns": [
            "n_name"
        ],
        "created_at": "2021-06-22 20:49:36.446819",
        "distinct_count": 25,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 25
    },
    {
        "columns": [
            "n_comment"
        ],
        "created_at": "2021-06-22 20:49:36.446819",
        "distinct_count": 25,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 25
    }
]';

CREATE TABLE customer (
	c_custkey INT8 NOT NULL,
	c_name VARCHAR(25) NOT NULL,
	c_address VARCHAR(40) NOT NULL,
	c_nationkey INT8 NOT NULL,
	c_phone CHAR(15) NOT NULL,
	c_acctbal FLOAT8 NOT NULL,
	c_mktsegment CHAR(10) NOT NULL,
	c_comment VARCHAR(117) NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (c_custkey ASC),
	CONSTRAINT customer_fkey_nation FOREIGN KEY (c_nationkey) REFERENCES nation(n_nationkey) NOT VALID,
	INDEX c_nk (c_nationkey ASC),
	FAMILY "primary" (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
);

ALTER TABLE customer INJECT STATISTICS '[
    {
        "columns": [
            "c_custkey"
        ],
        "created_at": "2021-06-22 20:49:46.176247",
        "distinct_count": 298313,
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 300000
    },
    {
        "columns": [
            "c_nationkey"
        ],
        "created_at": "2021-06-22 20:49:46.176247",
        "distinct_count": 25,
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 300000
    },
    {
        "columns": [
            "c_name"
        ],
        "created_at": "2021-06-22 20:49:46.176247",
        "distinct_count": 299605,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 300000
    },
    {
        "columns": [
            "c_address"
        ],
        "created_at": "2021-06-22 20:49:46.176247",
        "distinct_count": 303317,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 300000
    },
    {
        "columns": [
            "c_phone"
        ],
        "created_at": "2021-06-22 20:49:46.176247",
        "distinct_count": 302786,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 300000
    },
    {
        "columns": [
            "c_acctbal"
        ],
        "created_at": "2021-06-22 20:49:46.176247",
        "distinct_count": 266432,
        "histo_col_type": "FLOAT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 300000
    },
    {
        "columns": [
            "c_mktsegment"
        ],
        "created_at": "2021-06-22 20:49:46.176247",
        "distinct_count": 5,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 300000
    },
    {
        "columns": [
            "c_comment"
        ],
        "created_at": "2021-06-22 20:49:46.176247",
        "distinct_count": 297046,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 300000
    }
]';

CREATE TABLE orders (
	o_orderkey INT8 NOT NULL,
	o_custkey INT8 NOT NULL,
	o_orderstatus CHAR NOT NULL,
	o_totalprice FLOAT8 NOT NULL,
	o_orderdate DATE NOT NULL,
	o_orderpriority CHAR(15) NOT NULL,
	o_clerk CHAR(15) NOT NULL,
	o_shippriority INT8 NOT NULL,
	o_comment VARCHAR(79) NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (o_orderkey ASC),
	CONSTRAINT orders_fkey_customer FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey) NOT VALID,
	INDEX o_ck (o_custkey ASC),
	INDEX o_od (o_orderdate ASC),
	FAMILY "primary" (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment)
);

ALTER TABLE orders INJECT STATISTICS '[
    {
        "columns": [
            "o_orderkey"
        ],
        "created_at": "2021-06-22 20:52:21.194289",
        "distinct_count": 3025927,
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 3000000
    },
    {
        "columns": [
            "o_custkey"
        ],
        "created_at": "2021-06-22 20:52:21.194289",
        "distinct_count": 201250,
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 3000000
    },
    {
        "columns": [
            "o_orderdate"
        ],
        "created_at": "2021-06-22 20:52:21.194289",
        "distinct_count": 2406,
        "histo_col_type": "DATE",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 3000000
    },
    {
        "columns": [
            "o_orderstatus"
        ],
        "created_at": "2021-06-22 20:52:21.194289",
        "distinct_count": 3,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 3000000
    },
    {
        "columns": [
            "o_totalprice"
        ],
        "created_at": "2021-06-22 20:52:21.194289",
        "distinct_count": 2836859,
        "histo_col_type": "FLOAT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 3000000
    },
    {
        "columns": [
            "o_orderpriority"
        ],
        "created_at": "2021-06-22 20:52:21.194289",
        "distinct_count": 4,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 3000000
    },
    {
        "columns": [
            "o_clerk"
        ],
        "created_at": "2021-06-22 20:52:21.194289",
        "distinct_count": 2000,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 3000000
    },
    {
        "columns": [
            "o_shippriority"
        ],
        "created_at": "2021-06-22 20:52:21.194289",
        "distinct_count": 1,
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 3000000
    },
    {
        "columns": [
            "o_comment"
        ],
        "created_at": "2021-06-22 20:52:21.194289",
        "distinct_count": 2914644,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 3000000
    }
]';

CREATE TABLE part (
	p_partkey INT8 NOT NULL,
	p_name VARCHAR(55) NOT NULL,
	p_mfgr CHAR(25) NOT NULL,
	p_brand CHAR(10) NOT NULL,
	p_type VARCHAR(25) NOT NULL,
	p_size INT8 NOT NULL,
	p_container CHAR(10) NOT NULL,
	p_retailprice FLOAT8 NOT NULL,
	p_comment VARCHAR(23) NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (p_partkey ASC),
	FAMILY "primary" (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)
);

ALTER TABLE part INJECT STATISTICS '[
    {
        "columns": [
            "p_partkey"
        ],
        "created_at": "2021-06-22 20:49:54.752688",
        "distinct_count": 396158,
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 400000
    },
    {
        "columns": [
            "p_name"
        ],
        "created_at": "2021-06-22 20:49:54.752688",
        "distinct_count": 120,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 400000
    },
    {
        "columns": [
            "p_mfgr"
        ],
        "created_at": "2021-06-22 20:49:54.752688",
        "distinct_count": 5,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 400000
    },
    {
        "columns": [
            "p_brand"
        ],
        "created_at": "2021-06-22 20:49:54.752688",
        "distinct_count": 25,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 400000
    },
    {
        "columns": [
            "p_type"
        ],
        "created_at": "2021-06-22 20:49:54.752688",
        "distinct_count": 25,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 400000
    },
    {
        "columns": [
            "p_size"
        ],
        "created_at": "2021-06-22 20:49:54.752688",
        "distinct_count": 50,
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 400000
    },
    {
        "columns": [
            "p_container"
        ],
        "created_at": "2021-06-22 20:49:54.752688",
        "distinct_count": 7,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 400000
    },
    {
        "columns": [
            "p_retailprice"
        ],
        "created_at": "2021-06-22 20:49:54.752688",
        "distinct_count": 22034,
        "histo_col_type": "FLOAT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 400000
    },
    {
        "columns": [
            "p_comment"
        ],
        "created_at": "2021-06-22 20:49:54.752688",
        "distinct_count": 367577,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 400000
    }
]';

CREATE TABLE supplier (
	s_suppkey INT8 NOT NULL,
	s_name CHAR(25) NOT NULL,
	s_address VARCHAR(40) NOT NULL,
	s_nationkey INT8 NOT NULL,
	s_phone CHAR(15) NOT NULL,
	s_acctbal FLOAT8 NOT NULL,
	s_comment VARCHAR(101) NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (s_suppkey ASC),
	CONSTRAINT supplier_fkey_nation FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey) NOT VALID,
	INDEX s_nk (s_nationkey ASC),
	FAMILY "primary" (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)
);

ALTER TABLE supplier INJECT STATISTICS '[
    {
        "columns": [
            "s_suppkey"
        ],
        "created_at": "2021-06-22 20:49:44.692152",
        "distinct_count": 20053,
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 20000
    },
    {
        "columns": [
            "s_nationkey"
        ],
        "created_at": "2021-06-22 20:49:44.692152",
        "distinct_count": 25,
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 20000
    },
    {
        "columns": [
            "s_name"
        ],
        "created_at": "2021-06-22 20:49:44.692152",
        "distinct_count": 19976,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 20000
    },
    {
        "columns": [
            "s_address"
        ],
        "created_at": "2021-06-22 20:49:44.692152",
        "distinct_count": 20219,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 20000
    },
    {
        "columns": [
            "s_phone"
        ],
        "created_at": "2021-06-22 20:49:44.692152",
        "distinct_count": 19914,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 20000
    },
    {
        "columns": [
            "s_acctbal"
        ],
        "created_at": "2021-06-22 20:49:44.692152",
        "distinct_count": 19990,
        "histo_col_type": "FLOAT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 20000
    },
    {
        "columns": [
            "s_comment"
        ],
        "created_at": "2021-06-22 20:49:44.692152",
        "distinct_count": 19753,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 20000
    }
]';

CREATE TABLE partsupp (
	ps_partkey INT8 NOT NULL,
	ps_suppkey INT8 NOT NULL,
	ps_availqty INT8 NOT NULL,
	ps_supplycost FLOAT8 NOT NULL,
	ps_comment VARCHAR(199) NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (ps_partkey ASC, ps_suppkey ASC),
	CONSTRAINT partsupp_fkey_part FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey) NOT VALID,
	CONSTRAINT partsupp_fkey_supplier FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey) NOT VALID,
	INDEX ps_sk (ps_suppkey ASC),
	FAMILY "primary" (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)
);

ALTER TABLE partsupp INJECT STATISTICS '[
    {
        "columns": [
            "ps_partkey"
        ],
        "created_at": "2021-06-22 20:49:52.837276",
        "distinct_count": 396158,
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 1600000
    },
    {
        "columns": [
            "ps_suppkey"
        ],
        "created_at": "2021-06-22 20:49:52.837276",
        "distinct_count": 20053,
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 1600000
    },
    {
        "columns": [
            "ps_partkey",
            "ps_suppkey"
        ],
        "created_at": "2021-06-22 20:49:52.837276",
        "distinct_count": 1590650,
        "histo_col_type": "",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 1600000
    },
    {
        "columns": [
            "ps_availqty"
        ],
        "created_at": "2021-06-22 20:49:52.837276",
        "distinct_count": 9920,
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 1600000
    },
    {
        "columns": [
            "ps_supplycost"
        ],
        "created_at": "2021-06-22 20:49:52.837276",
        "distinct_count": 1000,
        "histo_col_type": "FLOAT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 1600000
    },
    {
        "columns": [
            "ps_comment"
        ],
        "created_at": "2021-06-22 20:49:52.837276",
        "distinct_count": 1592939,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 1600000
    }
]';

CREATE TABLE lineitem (
	l_orderkey INT8 NOT NULL,
	l_partkey INT8 NOT NULL,
	l_suppkey INT8 NOT NULL,
	l_linenumber INT8 NOT NULL,
	l_quantity FLOAT8 NOT NULL,
	l_extendedprice FLOAT8 NOT NULL,
	l_discount FLOAT8 NOT NULL,
	l_tax FLOAT8 NOT NULL,
	l_returnflag CHAR NOT NULL,
	l_linestatus CHAR NOT NULL,
	l_shipdate DATE NOT NULL,
	l_commitdate DATE NOT NULL,
	l_receiptdate DATE NOT NULL,
	l_shipinstruct CHAR(25) NOT NULL,
	l_shipmode CHAR(10) NOT NULL,
	l_comment VARCHAR(44) NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (l_orderkey ASC, l_linenumber ASC),
	CONSTRAINT lineitem_fkey_orders FOREIGN KEY (l_orderkey) REFERENCES orders(o_orderkey) NOT VALID,
	CONSTRAINT lineitem_fkey_part FOREIGN KEY (l_partkey) REFERENCES part(p_partkey) NOT VALID,
	CONSTRAINT lineitem_fkey_supplier FOREIGN KEY (l_suppkey) REFERENCES supplier(s_suppkey) NOT VALID,
	INDEX l_ok (l_orderkey ASC),
	INDEX l_pk (l_partkey ASC),
	INDEX l_sk (l_suppkey ASC),
	INDEX l_sd (l_shipdate ASC),
	INDEX l_cd (l_commitdate ASC),
	INDEX l_rd (l_receiptdate ASC),
	INDEX l_pk_sk (l_partkey ASC, l_suppkey ASC),
	INDEX l_sk_pk (l_suppkey ASC, l_partkey ASC),
	FAMILY "primary" (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
);

ALTER TABLE lineitem INJECT STATISTICS '[
    {
        "columns": [
            "l_orderkey"
        ],
        "created_at": "2021-06-22 20:52:03.751363",
        "distinct_count": 3025927,
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 12002964
    },
    {
        "columns": [
            "l_linenumber"
        ],
        "created_at": "2021-06-22 20:52:03.751363",
        "distinct_count": 7,
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 12002964
    },
    {
        "columns": [
            "l_orderkey",
            "l_linenumber"
        ],
        "created_at": "2021-06-22 20:52:03.751363",
        "distinct_count": 12172069,
        "histo_col_type": "",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 12002964
    },
    {
        "columns": [
            "l_partkey"
        ],
        "created_at": "2021-06-22 20:52:03.751363",
        "distinct_count": 396158,
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 12002964
    },
    {
        "columns": [
            "l_suppkey"
        ],
        "created_at": "2021-06-22 20:52:03.751363",
        "distinct_count": 20053,
        "histo_col_type": "INT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 12002964
    },
    {
        "columns": [
            "l_shipdate"
        ],
        "created_at": "2021-06-22 20:52:03.751363",
        "distinct_count": 2526,
        "histo_col_type": "DATE",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 12002964
    },
    {
        "columns": [
            "l_commitdate"
        ],
        "created_at": "2021-06-22 20:52:03.751363",
        "distinct_count": 2466,
        "histo_col_type": "DATE",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 12002964
    },
    {
        "columns": [
            "l_receiptdate"
        ],
        "created_at": "2021-06-22 20:52:03.751363",
        "distinct_count": 2554,
        "histo_col_type": "DATE",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 12002964
    },
    {
        "columns": [
            "l_partkey",
            "l_suppkey"
        ],
        "created_at": "2021-06-22 20:52:03.751363",
        "distinct_count": 1590197,
        "histo_col_type": "",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 12002964
    },
    {
        "columns": [
            "l_quantity"
        ],
        "created_at": "2021-06-22 20:52:03.751363",
        "distinct_count": 50,
        "histo_col_type": "FLOAT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 12002964
    },
    {
        "columns": [
            "l_extendedprice"
        ],
        "created_at": "2021-06-22 20:52:03.751363",
        "distinct_count": 1023542,
        "histo_col_type": "FLOAT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 12002964
    },
    {
        "columns": [
            "l_discount"
        ],
        "created_at": "2021-06-22 20:52:03.751363",
        "distinct_count": 11,
        "histo_col_type": "FLOAT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 12002964
    },
    {
        "columns": [
            "l_tax"
        ],
        "created_at": "2021-06-22 20:52:03.751363",
        "distinct_count": 9,
        "histo_col_type": "FLOAT8",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 12002964
    },
    {
        "columns": [
            "l_returnflag"
        ],
        "created_at": "2021-06-22 20:52:03.751363",
        "distinct_count": 3,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 12002964
    },
    {
        "columns": [
            "l_linestatus"
        ],
        "created_at": "2021-06-22 20:52:03.751363",
        "distinct_count": 2,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 12002964
    },
    {
        "columns": [
            "l_shipinstruct"
        ],
        "created_at": "2021-06-22 20:52:03.751363",
        "distinct_count": 4,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 12002964
    },
    {
        "columns": [
            "l_shipmode"
        ],
        "created_at": "2021-06-22 20:52:03.751363",
        "distinct_count": 7,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 12002964
    },
    {
        "columns": [
            "l_comment"
        ],
        "created_at": "2021-06-22 20:52:03.751363",
        "distinct_count": 9911253,
        "histo_col_type": "STRING",
        "name": "__auto__",
        "null_count": 0,
        "row_count": 12002964
    }
]';
`); err != nil {
		panic(err)
	}

	return cat
}
