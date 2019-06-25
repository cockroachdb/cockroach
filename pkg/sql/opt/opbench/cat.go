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
// and statistics.
func MakeTPCHCatalog() *testcat.Catalog {
	cat := testcat.New()

	if err := cat.ExecuteMultipleDDL(`
CREATE TABLE public.region (
    r_regionkey INT8 PRIMARY KEY,
    r_name CHAR(25) NOT NULL,
    r_comment VARCHAR(152)
);

ALTER TABLE region INJECT STATISTICS '[
	{
			"columns": [
					"r_regionkey"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 5,
			"histo_col_type": "int",
			"name": "_",
			"null_count": 0,
			"row_count": 5
	},
	{
			"columns": [
					"r_name"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 5,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 5
	},
	{
			"columns": [
					"r_comment"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 5,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 5
	}
]';

CREATE TABLE public.nation (
    n_nationkey INT8 PRIMARY KEY,
    n_name CHAR(25) NOT NULL,
    n_regionkey INT8 NOT NULL,
    n_comment VARCHAR(152),
    INDEX n_rk (n_regionkey ASC),
    CONSTRAINT nation_fkey_region FOREIGN KEY (n_regionkey) REFERENCES public.region (r_regionkey)
);

ALTER TABLE nation INJECT STATISTICS '[
	{
			"columns": [
					"n_nationkey"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 25,
			"histo_col_type": "int",
			"name": "_",
			"null_count": 0,
			"row_count": 25
	},
	{
			"columns": [
					"n_regionkey"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 5,
			"histo_col_type": "int",
			"name": "_",
			"null_count": 0,
			"row_count": 25
	},
	{
			"columns": [
					"n_name"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 25,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 25
	},
	{
			"columns": [
					"n_comment"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 25,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 25
	}
]';

CREATE TABLE public.supplier (
    s_suppkey INT8 PRIMARY KEY,
    s_name CHAR(25) NOT NULL,
    s_address VARCHAR(40) NOT NULL,
    s_nationkey INT8 NOT NULL,
    s_phone CHAR(15) NOT NULL,
    s_acctbal FLOAT8 NOT NULL,
    s_comment VARCHAR(101) NOT NULL,
    INDEX s_nk (s_nationkey ASC),
    CONSTRAINT supplier_fkey_nation FOREIGN KEY (s_nationkey) REFERENCES public.nation (n_nationkey)
);

ALTER TABLE supplier INJECT STATISTICS '[
	{
			"columns": [
					"s_suppkey"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 10034,
			"histo_col_type": "int",
			"name": "_",
			"null_count": 0,
			"row_count": 10000
	},
	{
			"columns": [
					"s_nationkey"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 25,
			"histo_col_type": "int",
			"name": "_",
			"null_count": 0,
			"row_count": 10000
	},
	{
			"columns": [
					"s_name"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 9990,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 10000
	},
	{
			"columns": [
					"s_address"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 10027,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 10000
	},
	{
			"columns": [
					"s_phone"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 10021,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 10000
	},
	{
			"columns": [
					"s_acctbal"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 9967,
			"histo_col_type": "float",
			"name": "_",
			"null_count": 0,
			"row_count": 10000
	},
	{
			"columns": [
					"s_comment"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 9934,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 10000
	}
]';

CREATE TABLE public.part (
    p_partkey INT8 PRIMARY KEY,
    p_name VARCHAR(55) NOT NULL,
    p_mfgr CHAR(25) NOT NULL,
    p_brand CHAR(10) NOT NULL,
    p_type VARCHAR(25) NOT NULL,
    p_size INT8 NOT NULL,
    p_container CHAR(10) NOT NULL,
    p_retailprice FLOAT8 NOT NULL,
    p_comment VARCHAR(23) NOT NULL
);

ALTER TABLE part INJECT STATISTICS '[
	{
			"columns": [
					"p_partkey"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 199810,
			"histo_col_type": "int",
			"name": "_",
			"null_count": 0,
			"row_count": 200000
	},
	{
			"columns": [
					"p_name"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 198131,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 200000
	},
	{
			"columns": [
					"p_mfgr"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 5,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 200000
	},
	{
			"columns": [
					"p_brand"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 25,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 200000
	},
	{
			"columns": [
					"p_type"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 150,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 200000
	},
	{
			"columns": [
					"p_size"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 50,
			"histo_col_type": "int",
			"name": "_",
			"null_count": 0,
			"row_count": 200000
	},
	{
			"columns": [
					"p_container"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 40,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 200000
	},
	{
			"columns": [
					"p_retailprice"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 20831,
			"histo_col_type": "float",
			"name": "_",
			"null_count": 0,
			"row_count": 200000
	},
	{
			"columns": [
					"p_comment"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 132344,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 200000
	}
]';

CREATE TABLE public.partsupp (
    ps_partkey INT8 NOT NULL,
    ps_suppkey INT8 NOT NULL,
    ps_availqty INT8 NOT NULL,
    ps_supplycost FLOAT8 NOT NULL,
    ps_comment VARCHAR(199) NOT NULL,
    PRIMARY KEY (ps_partkey, ps_suppkey),
    INDEX ps_sk (ps_suppkey ASC),
    CONSTRAINT partsupp_fkey_part FOREIGN KEY (ps_partkey) REFERENCES public.part (p_partkey),
    CONSTRAINT partsupp_fkey_supplier FOREIGN KEY (ps_suppkey) REFERENCES public.supplier (s_suppkey)
);

ALTER TABLE partsupp INJECT STATISTICS '[
	{
			"columns": [
					"ps_partkey"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 199810,
			"histo_col_type": "int",
			"name": "_",
			"null_count": 0,
			"row_count": 800000
	},
	{
			"columns": [
					"ps_suppkey"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 10034,
			"histo_col_type": "int",
			"name": "_",
			"null_count": 0,
			"row_count": 800000
	},
	{
			"columns": [
					"ps_availqty"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 10032,
			"histo_col_type": "int",
			"name": "_",
			"null_count": 0,
			"row_count": 800000
	},
	{
			"columns": [
					"ps_supplycost"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 100379,
			"histo_col_type": "float",
			"name": "_",
			"null_count": 0,
			"row_count": 800000
	},
	{
			"columns": [
					"ps_comment"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 799641,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 800000
	}
]';

CREATE TABLE public.customer (
    c_custkey INT8 PRIMARY KEY,
    c_name VARCHAR(25) NOT NULL,
    c_address VARCHAR(40) NOT NULL,
    c_nationkey INT8 NOT NULL,
    c_phone CHAR(15) NOT NULL,
    c_acctbal FLOAT8 NOT NULL,
    c_mktsegment CHAR(10) NOT NULL,
    c_comment VARCHAR(117) NOT NULL,
    INDEX c_nk (c_nationkey ASC),
    CONSTRAINT customer_fkey_nation FOREIGN KEY (c_nationkey) REFERENCES public.nation (n_nationkey)
);

ALTER TABLE customer INJECT STATISTICS '[
	{
			"columns": [
					"c_custkey"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 150097,
			"histo_col_type": "int",
			"name": "_",
			"null_count": 0,
			"row_count": 150000
	},
	{
			"columns": [
					"c_nationkey"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 25,
			"histo_col_type": "int",
			"name": "_",
			"null_count": 0,
			"row_count": 150000
	},
	{
			"columns": [
					"c_name"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 151126,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 150000
	},
	{
			"columns": [
					"c_address"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 149937,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 150000
	},
	{
			"columns": [
					"c_phone"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 150872,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 150000
	},
	{
			"columns": [
					"c_acctbal"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 140628,
			"histo_col_type": "float",
			"name": "_",
			"null_count": 0,
			"row_count": 150000
	},
	{
			"columns": [
					"c_mktsegment"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 5,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 150000
	},
	{
			"columns": [
					"c_comment"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 149323,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 150000
	}
]';

CREATE TABLE public.orders (
    o_orderkey INT8 PRIMARY KEY,
    o_custkey INT8 NOT NULL,
    o_orderstatus CHAR NOT NULL,
    o_totalprice FLOAT8 NOT NULL,
    o_orderdate DATE NOT NULL,
    o_orderpriority CHAR(15) NOT NULL,
    o_clerk CHAR(15) NOT NULL,
    o_shippriority INT8 NOT NULL,
    o_comment VARCHAR(79) NOT NULL,
    INDEX o_ck (o_custkey ASC),
    INDEX o_od (o_orderdate ASC),
    CONSTRAINT orders_fkey_customer FOREIGN KEY (o_custkey) REFERENCES public.customer (c_custkey)
);

ALTER TABLE orders INJECT STATISTICS '[
	{
			"columns": [
					"o_orderkey"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 1508717,
			"histo_col_type": "int",
			"name": "_",
			"null_count": 0,
			"row_count": 1500000
	},
	{
			"columns": [
					"o_custkey"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 99837,
			"histo_col_type": "int",
			"name": "_",
			"null_count": 0,
			"row_count": 1500000
	},
	{
			"columns": [
					"o_orderdate"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 2406,
			"histo_col_type": "date",
			"name": "_",
			"null_count": 0,
			"row_count": 1500000
	},
	{
			"columns": [
					"o_orderstatus"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 3,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 1500000
	},
	{
			"columns": [
					"o_totalprice"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 1459167,
			"histo_col_type": "float",
			"name": "_",
			"null_count": 0,
			"row_count": 1500000
	},
	{
			"columns": [
					"o_orderpriority"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 5,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 1500000
	},
	{
			"columns": [
					"o_clerk"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 1000,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 1500000
	},
	{
			"columns": [
					"o_shippriority"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 1,
			"histo_col_type": "int",
			"name": "_",
			"null_count": 0,
			"row_count": 1500000
	},
	{
			"columns": [
					"o_comment"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 1469402,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 1500000
	}
]';

CREATE TABLE public.lineitem (
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
    PRIMARY KEY (l_orderkey, l_linenumber),
    INDEX l_ok (l_orderkey ASC),
    INDEX l_pk (l_partkey ASC),
    INDEX l_sk (l_suppkey ASC),
    INDEX l_sd (l_shipdate ASC),
    INDEX l_cd (l_commitdate ASC),
    INDEX l_rd (l_receiptdate ASC),
    INDEX l_pk_sk (l_partkey ASC, l_suppkey ASC),
    INDEX l_sk_pk (l_suppkey ASC, l_partkey ASC),
    CONSTRAINT lineitem_fkey_orders FOREIGN KEY (l_orderkey) REFERENCES public.orders (o_orderkey),
    CONSTRAINT lineitem_fkey_part FOREIGN KEY (l_partkey) REFERENCES public.part (p_partkey),
    CONSTRAINT lineitem_fkey_supplier FOREIGN KEY (l_suppkey) REFERENCES public.supplier (s_suppkey),
    CONSTRAINT lineitem_fkey_partsupp FOREIGN KEY (l_partkey, l_suppkey) REFERENCES public.partsupp (ps_partkey, ps_suppkey)
);

ALTER TABLE lineitem INJECT STATISTICS '[
	{
			"columns": [
					"l_orderkey"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 1508717,
			"histo_col_type": "int",
			"name": "_",
			"null_count": 0,
			"row_count": 6001215
	},
	{
			"columns": [
					"l_partkey"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 199810,
			"histo_col_type": "int",
			"name": "_",
			"null_count": 0,
			"row_count": 6001215
	},
	{
			"columns": [
					"l_suppkey"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 10034,
			"histo_col_type": "int",
			"name": "_",
			"null_count": 0,
			"row_count": 6001215
	},
	{
			"columns": [
					"l_shipdate"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 2526,
			"histo_col_type": "date",
			"name": "_",
			"null_count": 0,
			"row_count": 6001215
	},
	{
			"columns": [
					"l_commitdate"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 2466,
			"histo_col_type": "date",
			"name": "_",
			"null_count": 0,
			"row_count": 6001215
	},
	{
			"columns": [
					"l_receiptdate"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 2554,
			"histo_col_type": "date",
			"name": "_",
			"null_count": 0,
			"row_count": 6001215
	},
	{
			"columns": [
					"l_linenumber"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 7,
			"histo_col_type": "int",
			"name": "_",
			"null_count": 0,
			"row_count": 6001215
	},
	{
			"columns": [
					"l_quantity"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 50,
			"histo_col_type": "float",
			"name": "_",
			"null_count": 0,
			"row_count": 6001215
	},
	{
			"columns": [
					"l_extendedprice"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 925955,
			"histo_col_type": "float",
			"name": "_",
			"null_count": 0,
			"row_count": 6001215
	},
	{
			"columns": [
					"l_discount"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 11,
			"histo_col_type": "float",
			"name": "_",
			"null_count": 0,
			"row_count": 6001215
	},
	{
			"columns": [
					"l_tax"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 9,
			"histo_col_type": "float",
			"name": "_",
			"null_count": 0,
			"row_count": 6001215
	},
	{
			"columns": [
					"l_returnflag"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 3,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 6001215
	},
	{
			"columns": [
					"l_linestatus"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 2,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 6001215
	},
	{
			"columns": [
					"l_shipinstruct"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 4,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 6001215
	},
	{
			"columns": [
					"l_shipmode"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 7,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 6001215
	},
	{
			"columns": [
					"l_comment"
			],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"distinct_count": 4643303,
			"histo_col_type": "string",
			"name": "_",
			"null_count": 0,
			"row_count": 6001215
	}
]';
`); err != nil {
		panic(err)
	}

	return cat
}
