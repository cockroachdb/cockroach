// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bench

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

// A query can be issued using the "simple protocol" or the "prepare protocol".
//
// With the simple protocol, all arguments are inlined in the SQL string; the
// query goes through all phases of planning on each execution. Only these
// phases are valid with the simple protocol:
//   - Parse
//   - OptBuildNoNorm
//   - OptBuildNorm
//   - Explore
//   - ExecBuild
//
// With the prepare protocol, the query is built at prepare time (with
// normalization rules turned on) and the resulting memo is saved and reused. On
// each execution, placeholders are assigned before exploration. Only these
// phases are valid with the prepare protocol:
//   - AssignPlaceholdersNoNorm
//   - AssignPlaceholdersNorm
//   - Explore
//   - ExecBuild
type Phase int

const (
	// Parse creates the AST from the SQL string.
	Parse Phase = iota

	// OptBuildNoNorm constructs the Memo from the AST, with normalization rules
	// disabled. OptBuildNoNorm includes the time to Parse.
	OptBuildNoNorm

	// OptBuildNorm constructs the Memo from the AST, with normalization rules
	// enabled. OptBuildNorm includes the time to Parse.
	OptBuildNorm

	// AssignPlaceholdersNoNorm uses a prepared Memo and assigns placeholders,
	// with normalization rules disabled.
	AssignPlaceholdersNoNorm

	// AssignPlaceholdersNorm uses a prepared Memo and assigns placeholders, with
	// normalization rules enabled.
	AssignPlaceholdersNorm

	// Explore constructs the Memo (either by building it from the statement or by
	// assigning placeholders to a prepared Memo) and enables all normalization
	// and exploration rules. The Memo is fully optimized. Explore includes the
	// time to OptBuildNorm or AssignPlaceholdersNorm.
	Explore

	// ExecBuild calls a stub factory to construct a dummy plan from the optimized
	// Memo. Since the factory is not creating a real plan, only a part of the
	// execbuild time is captured. ExecBuild includes the time to Explore.
	ExecBuild
)

// SimplePhases are the legal phases when running a query that was not prepared.
var SimplePhases = []Phase{Parse, OptBuildNoNorm, OptBuildNorm, Explore, ExecBuild}

// PreparedPhases are the legal phases when running a query that was prepared.
var PreparedPhases = []Phase{AssignPlaceholdersNoNorm, AssignPlaceholdersNorm, Explore, ExecBuild}

func (bt Phase) String() string {
	var strTab = [...]string{
		Parse:                    "Parse",
		OptBuildNoNorm:           "OptBuildNoNorm",
		OptBuildNorm:             "OptBuildNorm",
		AssignPlaceholdersNoNorm: "AssignPlaceholdersNoNorm",
		AssignPlaceholdersNorm:   "AssignPlaceholdersNorm",
		Explore:                  "Explore",
		ExecBuild:                "ExecBuild",
	}
	return strTab[bt]
}

type benchQuery struct {
	name    string
	query   string
	args    []interface{}
	cleanup string
}

var schemas = []string{
	`CREATE TABLE kv (k BIGINT NOT NULL PRIMARY KEY, v BYTES NOT NULL)`,
	`
	CREATE TABLE customer
	(
		c_id           integer        not null,
		c_d_id         integer        not null,
		c_w_id         integer        not null,
		c_first        varchar(16),
		c_middle       char(2),
		c_last         varchar(16),
		c_street_1     varchar(20),
		c_street_2     varchar(20),
		c_city         varchar(20),
		c_state        char(2),
		c_zip          char(9),
		c_phone        char(16),
		c_since        timestamp,
		c_credit       char(2),
		c_credit_lim   decimal(12,2),
		c_discount     decimal(4,4),
		c_balance      decimal(12,2),
		c_ytd_payment  decimal(12,2),
		c_payment_cnt  integer,
		c_delivery_cnt integer,
		c_data         varchar(500),
		primary key (c_w_id, c_d_id, c_id),
		index customer_idx (c_w_id, c_d_id, c_last, c_first)
	)
	`,
	`
	CREATE TABLE new_order
	(
		no_o_id  integer   not null,
		no_d_id  integer   not null,
		no_w_id  integer   not null,
		primary key (no_w_id, no_d_id, no_o_id DESC)
	)
	`,
	`
	CREATE TABLE stock
	(
		s_i_id       integer       not null,
		s_w_id       integer       not null,
		s_quantity   integer,
		s_dist_01    char(24),
		s_dist_02    char(24),
		s_dist_03    char(24),
		s_dist_04    char(24),
		s_dist_05    char(24),
		s_dist_06    char(24),
		s_dist_07    char(24),
		s_dist_08    char(24),
		s_dist_09    char(24),
		s_dist_10    char(24),
		s_ytd        integer,
		s_order_cnt  integer,
		s_remote_cnt integer,
		s_data       varchar(50),
		primary key (s_w_id, s_i_id)
	)
	`,
	`
	CREATE TABLE order_line
	(
		ol_o_id         integer   not null,
		ol_d_id         integer   not null,
		ol_w_id         integer   not null,
		ol_number       integer   not null,
		ol_i_id         integer   not null,
		ol_supply_w_id  integer,
		ol_delivery_d   timestamp,
		ol_quantity     integer,
		ol_amount       decimal(6,2),
		ol_dist_info    char(24),
		primary key (ol_w_id, ol_d_id, ol_o_id DESC, ol_number),
		index order_line_fk (ol_supply_w_id, ol_i_id),
		foreign key (ol_supply_w_id, ol_i_id) references stock (s_w_id, s_i_id)
	)
	`,
	`
	CREATE TABLE j
	(
		a INT PRIMARY KEY,
		b INT,
		INDEX b_idx (b)
	)
	`,
	`
	CREATE TABLE comp
	(
		a INT,
		b INT,
		c INT,
		d INT,
		e INT,
		f INT,
		a1 INT AS (a+1) STORED,
		b1 INT AS (b+1) STORED,
		c1 INT AS (c+1) STORED,
		d1 INT AS (d+1) VIRTUAL,
		e1 INT AS (e+1) VIRTUAL,
		f1 INT AS (f+1) VIRTUAL,
		shard INT AS (mod(fnv32(crdb_internal.datums_to_bytes(a, b, c, d, e)), 8)) VIRTUAL,
		CHECK (shard IN (0, 1, 2, 3, 4, 5, 6, 7)),
		PRIMARY KEY (shard, a, b, c, d, e),
		INDEX (a, b, a1),
		INDEX (c1, a, c),
		INDEX (f),
		INDEX (d1, d, e)
	)
	`,
	`
	CREATE TABLE json_table
	(
		k INT PRIMARY KEY,
		i INT,
		j JSON
	)
	`,
	`
	CREATE TABLE json_comp
	(
		k UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		i INT,
		j1 JSON,
		j2 JSON,
		j3 JSON,
		j4 INT AS ((j1->'foo'->'bar'->'int')::INT) STORED,
		j5 INT AS ((j1->'foo'->'bar'->'int2')::INT) STORED,
		j6 STRING AS ((j2->'str')::STRING) STORED
	)
	`,
	`
		CREATE TABLE single_col_histogram (k TEXT PRIMARY KEY);
	`,
	`
		ALTER TABLE single_col_histogram INJECT STATISTICS '[
		  {
		    "columns": ["k"],
		    "created_at": "2018-01-01 1:00:00.00000+00:00",
		    "row_count": 30000,
		    "distinct_count": 1100,
		    "null_count": 0,
		    "avg_size": 2,
		    "histo_col_type": "STRING",
		    "histo_buckets": [
		      {"num_eq": 0, "num_range": 0, "distinct_range": 0, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________0"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________100"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________200"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________300"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________400"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________500"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________600"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________700"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________800"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________900"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________1000"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________1200"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________1300"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________1400"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________1500"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________1600"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________1700"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________1800"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________1900"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________2000"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________2100"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________2200"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________2300"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________2400"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________2500"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________2600"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________2700"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________2800"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________2900"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________3000"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________3100"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________3200"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________3300"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________3400"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________3500"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________3500"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________3600"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________3700"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________3800"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________3900"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________4000"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________4100"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________4200"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________4300"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________4400"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________4500"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________4600"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________4700"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________4800"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________4900"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________5100"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________5200"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________5300"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________5400"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________5500"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________5600"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________5700"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________5800"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________5900"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________6000"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________6000"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________6100"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________6200"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________6300"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________6400"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________6500"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________6600"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________6700"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________6800"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________6900"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________7000"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________7100"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________7200"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________7300"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________7400"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________7500"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________7600"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________7700"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________7800"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________7900"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________8000"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________8100"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________8200"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________8300"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________8400"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________8500"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________8600"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________8700"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________8800"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________8900"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________9000"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________9100"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________9200"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________9300"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________9400"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________9500"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________9600"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________9700"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________9800"},
		      {"num_eq": 10, "num_range": 10, "distinct_range": 10, "upper_bound": "abcdefghijklmnopqrstuvwxyz___________________9900"}
		    ]
		  }
		]'
	`,
}

var queries = [...]benchQuery{
	// 1. Table with small number of columns.
	// 2. Table with no indexes.
	// 3. Very simple query that returns single row based on key filter.
	{
		name:  "kv-read",
		query: `SELECT k, v FROM kv WHERE k IN ($1)`,
		args:  []interface{}{1},
	},

	// 1. PREPARE with constant filter value (no placeholders).
	{
		name:  "kv-read-const",
		query: `SELECT k, v FROM kv WHERE k IN (1)`,
		args:  []interface{}{},
	},

	// 1. Table with many columns.
	// 2. Multi-column primary key.
	// 3. Mutiple indexes to consider.
	// 4. Multiple placeholder values.
	{
		name: "tpcc-new-order",
		query: `
			SELECT c_discount, c_last, c_credit
			FROM customer
			WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3
		`,
		args: []interface{}{10, 100, 50},
	},

	// 1. ORDER BY clause.
	// 2. LIMIT clause.
	// 3. Best plan requires reverse scan.
	{
		name: "tpcc-delivery",
		query: `
			SELECT no_o_id
			FROM new_order
			WHERE no_w_id = $1 AND no_d_id = $2
			ORDER BY no_o_id ASC
			LIMIT 1
			FOR UPDATE
		`,
		args: []interface{}{10, 100},
	},

	// 1. Count and Distinct aggregate functions.
	// 2. Simple join.
	// 3. Best plan requires lookup join.
	// 4. Placeholders used in larger constant expressions.
	{
		name: "tpcc-stock-level",
		query: `
			SELECT count(DISTINCT s_i_id)
			FROM order_line
			JOIN stock
			ON s_i_id=ol_i_id AND s_w_id=ol_w_id
			WHERE ol_w_id = $1
				AND ol_d_id = $2
				AND ol_o_id BETWEEN $3 - 20 AND $3 - 1
				AND s_quantity < $4
		`,
		args: []interface{}{10, 100, 1000, 15},
	},

	// 1. Table with more than 15 columns (triggers slow path for FastIntMap).
	// 2. Table with many indexes.
	// 3. Query with a single fixed column that can't use any of the indexes.
	{
		name: "many-columns-and-indexes-a",
		query: `
			SELECT id FROM k
			WHERE x = $1
		`,
		args: []interface{}{1},
	},

	// 1. Table with more than 15 columns (triggers slow path for FastIntMap).
	// 2. Table with many indexes.
	// 3. Query that can't use any of the indexes with a more complex filter.
	{
		name: "many-columns-and-indexes-b",
		query: `
			SELECT id FROM k
			WHERE x = $1 AND y = $2 AND z = $3
		`,
		args: []interface{}{1, 2, 3},
	},

	// Query that initializes column IDs as high as ~320.
	{
		name: "many-columns-and-indexes-c",
		query: `
			SELECT * FROM
				k k1, k k2, k k3, k k4, k k5, k k6, k k7, k k8, k k9, k k10, k k11
			WHERE
				k1.x = $1 AND k2.y = $2 AND k3.z = $3 AND
				k1.a = k2.a AND
				k2.a = k3.a AND
				k3.a = k4.a AND
				k5.a = k6.a AND
				k6.z = k7.z AND
				k7.z = k8.z AND
				k8.m = k9.m AND
				k10.m = k11.m
		`,
		args: []interface{}{1, 2, 3},
	},

	// Query with high column IDs and an aggregation.
	{
		name: "many-columns-and-indexes-d",
		query: `
			SELECT
				k1.a, k2.a, k3.a, k4.a, k5.a, k6.a, k7.a, k8.a, k9.a, k10.a, k11.a,
				min(k1.b), min(k2.b), min(k3.b), min(k4.b), min(k5.b), min(k6.b),
				min(k7.b), min(k8.b), min(k9.b), min(k10.b), min(k11.b),
				min(k1.c), min(k2.c), min(k3.c), min(k4.c), min(k5.c), min(k6.c),
				min(k7.c), min(k8.c), min(k9.c), min(k10.c), min(k11.c),
				min(k1.d), min(k2.d), min(k3.d), min(k4.d), min(k5.d), min(k6.d),
				min(k7.d), min(k8.d), min(k9.d), min(k10.d), min(k11.d)
			FROM
			  k k1, k k2, k k3, k k4, k k5, k k6, k k7, k k8, k k9, k k10, k k11
			WHERE k1.s = $1 AND k2.t = $2
			GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
		`,
		args: []interface{}{1, 2},
	},
	{
		name:  "comp-pk",
		query: "SELECT * FROM comp WHERE a = $1 AND b = $2 AND c = $3 AND d = $4 AND e = $5",
		args:  []interface{}{1, 2, 3, 4, 5},
	},
	{
		name:  "comp-insert-on-conflict",
		query: "INSERT INTO comp VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (shard, a, b, c, d, e) DO UPDATE SET f = excluded.f + 1",
		args:  []interface{}{1, 2, 3, 4, 5, 6},
	},
	{
		name:  "single-col-histogram-range",
		query: "SELECT * FROM single_col_histogram WHERE k >= $1",
		args:  []interface{}{"'abc'"},
	},
	{
		name:  "single-col-histogram-bounded-range-small",
		query: "SELECT * FROM single_col_histogram WHERE k >= $1 and k < $2",
		args: []interface{}{
			"'abcdefghijklmnopqrstuvwxyz___________________7325'",
			"'abcdefghijklmnopqrstuvwxyz___________________7350'",
		},
	},
	{
		name:  "single-col-histogram-bounded-range-big",
		query: "SELECT * FROM single_col_histogram WHERE k >= $1 and k < $2",
		args: []interface{}{
			"'abcdefghijklmnopqrstuvwxyz___________________7325'",
			"'abcdefghijklmnopqrstuvwxyz___________________9000'",
		},
	},
	{
		name:    "json-insert",
		query:   `INSERT INTO json_table(k, i, j) VALUES ($1, $2, $3)`,
		args:    []interface{}{1, 10, `'{"a": "foo", "b": "bar", "c": [2, 3, "baz", true, false, null]}'`},
		cleanup: "TRUNCATE TABLE json_table",
	},
	{
		name:  "json-comp-insert",
		query: `INSERT INTO json_comp(i, j1, j2, j3) VALUES ($1, $2, $3, $4)`,
		args:  []interface{}{10, `'{"foo": {"bar": {"int": 12345, "int2": 1}}, "baz": false}'`, `'{"str": "hello world"}'`, `'{"c": [2, 3, "baz", true, false, null]}'`},
	},
	{
		name: "batch-insert-one",
		query: `
		INSERT INTO customer(c_id,c_d_id,c_w_id,c_first,c_middle,c_last,c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_since,c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment,c_payment_cnt,c_delivery_cnt,c_data) VALUES
		(1,1,0,'ImoTe4PiKWXD','OE','BARBARBAR','EDWcyUYqVzgyFfZ079y','JJusaryB4vhd9f9mR0R','fn7QCDRbuMdQKKyF','VJ',074111111,'6767660292945378','2006-01-02 15:04:05','GC',50000,0.3751,-10,10,1,0,'sIfe3YGrHlrJnaR0wr2qvuan0VBADnKYYF5o3vrNPF3SE8s9Io5vM9rPTaaPQ7hKv7AXuCHcotXSMwthyG3wek6ddPsFhqJuNzixMlN77YrUO3HDzs5M3HFcTSd2J1jcncpaB2WAN2QfIITOe4ZQqKHGQyVLOAYOYS0YpzRfe0xj4h6X19y35QCL9V2lRzqzMBNTZ9l2aj2Q46mELTJ1LhsKxxEbgHTprfCdzTJdDxV2FVX4UhqgJVn1BFjSKvrPuhWWiAHOeWN2zkDKvNrYQk8Gmqzx7m4bPdpgmzTRf4uweQJjhCtCLKIyxB95Wuh0Di1g1TLdgvWgWujOBdKZgPhNIFskWEH6t3WqOpGlCRwnBeNTElFPcAAajhkkHaxPIj98d3bYARff')
        `,
		args:    []interface{}{},
		cleanup: "TRUNCATE TABLE customer",
	},
	{
		name: "batch-insert-many",
		query: `
		INSERT INTO customer(c_id,c_d_id,c_w_id,c_first,c_middle,c_last,c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_since,c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment,c_payment_cnt,c_delivery_cnt,c_data) VALUES
		(1,1,0,'ImoTe4PiKWXD','OE','BARBARBAR','EDWcyUYqVzgyFfZ079y','JJusaryB4vhd9f9mR0R','fn7QCDRbuMdQKKyF','VJ',074111111,'6767660292945378','2006-01-02 15:04:05','GC',50000,0.3751,-10,10,1,0,'sIfe3YGrHlrJnaR0wr2qvuan0VBADnKYYF5o3vrNPF3SE8s9Io5vM9rPTaaPQ7hKv7AXuCHcotXSMwthyG3wek6ddPsFhqJuNzixMlN77YrUO3HDzs5M3HFcTSd2J1jcncpaB2WAN2QfIITOe4ZQqKHGQyVLOAYOYS0YpzRfe0xj4h6X19y35QCL9V2lRzqzMBNTZ9l2aj2Q46mELTJ1LhsKxxEbgHTprfCdzTJdDxV2FVX4UhqgJVn1BFjSKvrPuhWWiAHOeWN2zkDKvNrYQk8Gmqzx7m4bPdpgmzTRf4uweQJjhCtCLKIyxB95Wuh0Di1g1TLdgvWgWujOBdKZgPhNIFskWEH6t3WqOpGlCRwnBeNTElFPcAAajhkkHaxPIj98d3bYARff')
	   ,(2,1,0,'ZZkuR43WxYZS6aOW','OE','BARBAROUGHT','Pxi54S73Lhl7Iu6pxC','dcR385vdXwl6zCs','8vr3WsbyLCPx','SE',419211111,'1495184815919142','2006-01-02 15:04:05','GC',50000,0.4998,-10,10,1,0,'w8vcZdB7i7atNhngDu9JpF3JZ9wCQCvsZgEHg7EdEVhhklF8Rrbikj6piyIUvYx9B7VKStRcpZeNo6d2SX5bJUYTP3Wjg5c8eilAGMHJHegUaRG4J4b1vFLEOAnFhANK6NW1dFiBD5nkhiLPBG53hZBa2zmwyyaubeGa1Nh1045BDL4sHSYyuWcwbjTLg0uAXMgLyFw65WmoD8dL05qxObW3q4UJbPlsW4TNVCnTyXUm7Z1U4E9fqitWzeoCGI1lhOzZQC9Xu2WWJFIYjbvfNDEoL7P9i2H5lHsvTiNXwO8VtKjW3X1wRWtTAhlU18iQWRA2ZcehURhiI75OGZtnKX1yrH6GPNPM4lkO2dEKNz0cnQNqVFN')
	   ,(3,1,0,'g6Yp2m4FjibHNQM','OE','BARBARABLE','VNdeadCuVKJei','ml0QcBg7AptFtw','ynpjD9h72F','WL',808211111,'2429470478005498','2006-01-02 15:04:05','GC',50000,0.1184,-10,10,1,0,'Dhhk0BmuymPCbvhyWj032mN8FhlKXzMLKdgAJScqmeeKt5b3VrCLx68Ti0PZGtlelFHgKKz5uzto86uuZupwDQ8PNpCyyqnqL7xRLcIT9YTFZEJfQ4cbpN05eKB4c2mzYvjj7OOsUnPjo5tuq2S2aPJxfZJrM12grcT7vsZGM3Si52TykUMrIa1NDH7CwpbUpLhAIv7xdaeXsJUZVLbVCLSwpQSaF1RMF1c71FSkwqjohpG69b4j15g7y26bN4Z0fAkCt5kcAcl4xwCyedJTKn7uUaUJfXji35IBVdxUdeUMeQqRAbb6ZWcBOlxgMZ51F3hACruAsaqlIvQDZl1IqukF3OXg1eHTXkqhhO5AKR1ztPMZswWNKuJKyihEwHgsF4KzYaY5R5tuyb8AIfiHnigkYJwWXOT0UoNcQVCg2')
	   ,(4,1,0,'Ts74DITqt2a','OE','BARBARPRI','7wjX2OnlAGVF76FTkoKt','okMTEGvIHTgV','jPYCRyK2XQUAJTor','WB',067711111,'8352532367491630','2006-01-02 15:04:05','GC',50000,0.3902,-10,10,1,0,'ALHHY6uMbfRGrm7d0nysFVPkGcSFb9ihRhOSLOoetJZS89tiJ6aHREKcHxhiC4D23aj3Pq2xLQeDfsWADf4ZFa1SMDLXI3kK5vZ8DMJPHeYz6Ar1WdftULyGVpMX4om4wWutNKWMBSAywz2K7C8sVS52vSZMIeg4dmSfLdBIfVuQh7Hs8LgTOAevrbb9DmDx6B1tATnUdNKwns2tGXVJ4xWSPEwOyJBeLOrmmcY0as5TT0PzKPPza6Xe2B8dP4trfoILqqMoEiX5hOcgIRkFwCyZHBz8I0zGlzVTp3WOosuqp1SRjBJqA945WtePzKDkiEVGq1Y1mTDuOw1EVcZd8MMQsKt6rdvmJ4yY0Hz24E18n58okO7b2VuJAhhKJctkxmXiODNArwtsdLPm6sMgwcn69Gsntk6TP')
	   ,(5,1,0,'pNka6iGqjrb4r','OE','BARBARPRES','LCbqe6dvmKxmZSQH','QTMd9TmOsOKtE1hH','k39gv2H9gCGWV57','GA',806111111,'2484610956752362','2006-01-02 15:04:05','GC',50000,0.3245,-10,10,1,0,'dGDTU4dMtAZhWzRIS48gBbGygJYmMW0vU9fHlWMFLOnGP8iuQKHrE4ZT8FZ0l7udUaOYSGy23h9Pqdnah2RP7VxB6EGLMXRohTmbvqgrIM7pmLU9hLfnfvx8k3INQ74K815wcNtXwFNe1OgqXfI4k1dxTFLpFkYwEUkEDImfjqumAXLaawFdLwXJkyoC1KyRuWbzMVNHG5OXRBgiMEXENDSMxzs9JT9XCl4Un2NC1LTNLGcq7uGreTmZowCqNzwZIBAOQjR4mBCS45hOBjB0cSDiu7Y4DbASEldzDxCvQGqc8OxJP9r5ZQHwdE00nsBE7ci7ugGh4jFM9lpmX')
	   ,(6,1,0,'roqjdr2aUSfDl','OE','BARBARESE','WUb8c97ajSgZe4Dhmxmi','9oV5eteTdk4OJDT','VjPATvr5omZlRKblc3b','HO',061611111,'4706546294584946','2006-01-02 15:04:05','GC',50000,0.079,-10,10,1,0,'hhb0nIage382Qy65rAZqiqjwi5MuSenNHQ8LVxHHyP0uiGoTjGfMaPFTc36z1gqeDVG8ihoIlO8IHcvDnFz0Uefd1EClT7K0ZBr7xTk4CDvUphSbL9jPJVk250WMOQTxeFIpL3AFJFvyXlvQfCePRMhgzqdMnqt1EZoDEnwGlEsas5rTolSlr2ACg9UjzOlc83kcCyfDYwv4WxOCkRG7UAK39ARqnbRUA4dxJ7RMEz6rK78mB0xeAb489ASJlLbaM1JUxSorvJ6kV3902iAqdEgMdtfILNEFaDDiLgAc5VKInsadI2SI5HUdkDDLBwsrsbP5QwzavpwdlnIao9odi')
	   ,(7,1,0,'E2xcSjIcR7reV','OE','BARBARANTI','uoEptvbIAox9obdH6','cHolUYwRmXd','MxkjHj3IEJzg3o','BO',076411111,'3098892017539424','2006-01-02 15:04:05','GC',50000,0.2241,-10,10,1,0,'P4A0cWSoiFviwAV72wnf8uAjllyNxlwq9AWIfjxewuIDXfLSZs0qudKSeGmEhBc37MaSQ7T1woi4P1ZghwfEXRzkTlOlEsU9SiWauKXULVLStQyaCx8cp0iwervospbPwwWJVGieDlS2SVZTpbDVyhlUg43LVGDFQAeFKoD5qa7nDlxtStc6B3Pfo6zListwYpRgdBsgUFl07Qwy2NdeTeUd71XBfAMSEGrpcfZR2QKs25jAdpxLXDKOVSZFeI4QkSM4TCvhJwr16KN6aZMxK0KjEf4erp2opKJsX87xrYSkVpdubJcCrAm1NL5z5tOfdneiK9gdVSnXLm1q5JZpbEBJeYRtnjbryfO8VF4I36BDM7xRTZGzkvHO6On9XVDTttjvOSi14b0TXjmAwLJWpE8cgZSPItomMELzRJJkgls4CTE4ooKNGK1rCaxNASF5n2d64AUWGE04QTvLAu1uKGBVK3gsy9Kg')
	   ,(8,1,0,'ZufWMKUE','OE','BARBARCALLY','sjgJemf8OHsv1w','u5P1nyXhBIhVQG1Y2bS','zpR43j5zxKls','CF',681011111,'1322205623955156','2006-01-02 15:04:05','GC',50000,0.0121,-10,10,1,0,'y699Ta85k3rtx73GgBJiP9H9C6g8GYk3JfqojDIHIUMcTEjvIZcRRKcx8dR9d2NKkKskOfUrHZP0lvfFyKGFA50BgbdxDscqICxpAkxgFflZcjcxtnT9uuomSyFvztHR0xfgTtuSxWmlRka0n5ostCdcTPuWQ9sk4t4cVWbZRshPyQ8l5K1QL48sSZblQYhAklAqFRBMdFZMMqCpIF4bmM1r7Veak4Q1a2AV23CqA8tb5oEy85KnFifvscpAWRIqfefs83N78Om4aoJQoz9XNIQGkcVAUymnf44hWARwEFEaMPHXWtIKvQZZxBtn9UsUShZM2mN6AorIlqBn47FoH55DzscTtl3h7OaQvdD21asRDGEgziBDsm4I4grag8SZKtQkGxi6F7FENWylPT39PKnTl8RH3yaEyxqW8mera')
	   ,(9,1,0,'VG9qzEG0b1','OE','BARBARATION','YbJEAteVpF8jeYT','SP6rjowsrR1','UazL3UGFouEZCd4fgS','VH',011011111,'5344779729978484','2006-01-02 15:04:05','GC',50000,0.0382,-10,10,1,0,'MsGtTGA00d0mNP01ERgwKkZQ3RvxpfVW6msFuWRsO0t0yY37XFDLWlPHONoGvkc2qtGx5PQ7kF07zzZhkmUNVqcWzcSoRSGbKJp1gZ5opDYfuRMANkS0HFXQO2c5X87XENzekOwI82RzrDjaEospPL0yUGVf0n4GwpvHT74Fcx1UI262383xe51QO5Mlqe35eGoY3ZWAknTrfSsDBggJSRgwg82zczLHH4aqe48ErTrnFQQ4DDYP6fxyiuPv5mkjGnEMdN5b6KmcQpVfrKDz2Kn047uRaN2PMMhrHayvxuuy3dQyawzYalV34FrqVDYbZEuEAl7FAe4NDNl5eSSEpF3kco1q7PZJXRCXQ2I0Nb8O33JHohpDpfNIX408hdguYXwV4ALHevmEzp6ICbzjvQmRpUOfZKRfTW89uDl1IwBlsqUeXXAmEplzz72qdLImdTdY0dL')
	   ,(10,1,0,'gVCjITvzIMM7ut','OE','BARBAREING','KbWLwwSxyh4R1x','KTo1vvH5nr5f','873LcDDDllmHsE7mbm1','SO',060111111,'4916403310232207','2006-01-02 15:04:05','GC',50000,0.1422,-10,10,1,0,'ZYHfBJ4w7PMMq8j2o94s7gq9X5ncY3evjlAxt4f3cFFRjLpeUwIoewQuE3VE0SiqWnL0zaQF2nbAC67Drr4db8GHAR8wkULNCRHMIn5vpGe4yV9051sxg8H9qjt1IOAhk28wtXcNunpOcEPQ1sByQmwoJ1rIh2YqsOcLHcxCA4m0j6O0juTSKsURqcSbHygvo0XMUor4o3rzceDvlKOuJqZVKfCs1GmboDNCXIZA503o0tgcfDkZbf32Fknw1RTncnuLEW3BX8y0OzXJ945eWiOqC2EbOPJOzi8ixZeVoFmBHDH9YKagtypDaxiYKuVzJ7YkrSqQpccXkoBlGuQa29mOwWHTAfWLe5ziJcuZ')
	   ,(11,1,0,'0tmnyaNY','OE','BAROUGHTBAR','tLS0XpSVtO1I0XyiUXr','axsc5slYvl3IzMXP','NIbDLRrpDITpcHU','CX',309611111,'5062353230282514','2006-01-02 15:04:05','GC',50000,0.4309,-10,10,1,0,'0W8QFKb4sWKonyWrMWny69By8g8GuoNUIe7FKPcCNu8GMIeqrZa5Z6cTpZZYjqJZtdWG9ZdQcJM7NGwBdNJY5GYf60ZS0t0ii6dXK4j9m9PSZujJW7slJesQNWS7ONSQeqqHlfDnFcn7YM4sCEwHVqsvau8KWsqTqaexgIUuir9wYRH69K7mtp1aiSgVWmrJIjelAnPKbqUA3623rwvNnhELw2iWbLAAIIW18yrqFxP0EyFjJGxJUlIci0W67OdL3dabzSW75xrkp5ek5XcsXCh8hBdyWZY3BERKQS9cTivMHX4rKYEulXcobEKFMBvuH51Mz2qEigcY6c3I0OXFhoHU5esLD0KQNcylJiXQsXpoUbWurQHyuYR4Jsy6GvHlWnJS0i4SKKP558RTTHzgGq6uTQnIVDtfsNZ7bHjRh9Q5pmdYPi4')
	   ,(12,1,0,'mk0U9W99Bom','OE','BAROUGHTOUGHT','fHfIjrJnL3HqoTpxj','jDXBgLxyG9nQ','BniF44xiZx456','AW',976811111,'2708972002120126','2006-01-02 15:04:05','GC',50000,0.2895,-10,10,1,0,'UFw4Z0ghkdbmPjuKhRHlNgLWfAd8r7XffwgtAC9Q6MkbuahkDvkQskZlmciVgFTvRFveKqG6I2RZ8VuB0FiRz9k2gc2s5tsfhi1HEtD0ormA2zti9HVWqsICwWsxnliexqvmEWaBLKX9WhzO03gHzzWjDW0XGew4J895tLZch6xR3zUCYLdQ7ONonskST7Darlx8xRLqi9bhLqfh1Z6v7GHi05WEMOsm7b6gXukj1DhTXV3UjnwDX9Nlk8tNaottAoSnJLPc4DiPiMnT7TMc0ITryRJkp2HFpQRX5b0EM9Sys6h5IOR8BnCKhwi')
	   ,(13,1,0,'j7DCT2Gup','OE','BAROUGHTABLE','lRvJMgOp43SMv2Fk','7jRSZ7kal5AD0Imc4o7N','mgVkdoqJ2dsWfTOiz','KV',767811111,'1766347829489314','2006-01-02 15:04:05','BC',50000,0.0099,-10,10,1,0,'wCW4hRWwFp3E9nHmtYc0VOKbkKBu2bk5fZMVJAfc91oE5Z5jzP9FI1qgrsRkVNvrm8YrUnQunobIxE91BDYTLu4QkJirSLuvdOD6WSJTVy22ushQmYp7NCEhwGDz8RaFKhrdZlllkubF8o3FZ80McRsyut8wpemyaml3z6FVWbqQEeYugOLStc2H2ZeywfARLnvXF0GLUayoLBs7Nuk6uw2jHqPRfbKItpD8joIhskfqwPQIH035gcNnKJxoQHb4cavDzzKJ3ezzO1br46sDTcAfVDU0kwjSczZ2sO63fpRpZusN3sfXmAXzi6w8piiCMVzp3UayiMGavqyuSWGJoFte7negfV4Ua5qYGhxnKUEfjbljyiKYKMitkH06Gr7V3zgxPuw9gNSZutwKWQgqlAAqt9h6Z4VyLZ3VePvLJou1DcwxIYQqLlOxD7oNjpdCGrK5tsol2qcWM5RgSEQCDOKRsQRboIacnabUeJmzPX5wF9dKc7ll')
	   ,(14,1,0,'hQcYZWmW1ig','OE','BAROUGHTPRI','vbL4mJRcrnVaUQfRw','gDcYCUWaSGtlA1cakH8','f6xiCh6uU7MVFL1cNqs7','KZ',275211111,'6877221791613589','2006-01-02 15:04:05','GC',50000,0.0197,-10,10,1,0,'sSqxqnnWYhoUVEPNrb2YtZQdtN6kHa3aAehHY0cuht5o895imQMeixa3UAyuy16Ml4BLoXWBAOKJHJ71D3SOEtHDzXSm2AGBe9wGjSFazVyza0UA0l4xVvVG4p7pqTolvYyLPsO0BKApn0IDNO1TbulOnhdXakKezx8ETLhjyPxbVmYJfZj2PqFr2be8XRvMbDBbzVzOslQv2zE14KcWWZ8wAbA7GJWwvaOEZVMBjdQmKm2V9dSgkTFMJm6RvVKEZdIyBWDLf1nb7hxUg4rgANK2LZxiNe7xOShLrbWYABt9Kw3jOw8lVEp3OA5WHcWhGadUaEspa11aeWdVwmpJmSYhPY0stcijvU5P1JzLorGQehobHBj7oNVPaL4UHKXubfCb')
	   ,(15,1,0,'qrV1aYMvg','OE','BAROUGHTPRES','GPUEowg0sYl','5tnT3e3kSgn0I7D','L17PTV7GSbBL','UQ',068611111,'7973964276836760','2006-01-02 15:04:05','BC',50000,0.142,-10,10,1,0,'6qMssBGaG8ryYG29EDsvCEYj5ydM5Ft17HxupTghzkORjUvmlfRT68ERwyt0tRWjxbKpzS8mJ1EtSqaCoiPRZlOrw50V8Qeg5LrkGOjp6vVc5kgxUCwJzbATpYUYB3ZmCu4C85ffK8WM6BDwELIGUzDLn3HVV0PyklIlCEMQmkCbQ6hNgN895p847512TCTa0LkKy6L0QkixMH9sU6q7q0VGM3wRTYiOKLQxBpKLVqivfhsbLdpfmoaZuIib4H1xJLi70JyV5DhSsBG4Ar1qJ5y9L8mCwv9DKtnrOMjco9mbXFITl2be4x2EXUjx28tB0t8x0zptSBvePPsWLWb09YvpxqkuSlg5ar3AGUIcsXZEuRdzeJee5jdrsJHtoW7ezBHHr6O85lDgHauNn9TtoyFl7X7RRqmJn')
	   ,(16,1,0,'9jtoeyJ0jX','OE','BAROUGHTESE','wseqWXnpsaiGgawJeV8c','SCjgKr2Rv2DHpacw','fPlKZGimkQruC0','ME',258011111,'2024059767903759','2006-01-02 15:04:05','BC',50000,0.307,-10,10,1,0,'7lCsh1AV9CR92aAoqNMR1ZDaNbPnZkM4pUdyqcIlVfMP4PUUheTHfYFDB4y2JP9SMX2RkpCgwUgs7dnYO8Xswcjk7Tp3cXlkaGzprndmcryDY3mMM0YIjJvsKt5v4AAuzT8MF86C2tW9rDC6pPdK7ghhkBFVK0wokXVukQOYlgbjLfazfN6SSyP3BFiWKJ0yu713Wo2uytAvEnw2H0lC5XmuLoU22wxbOgnPO0I3EsdiVVIKgbMNcV7aFnZjNrrZv5NxphN6OR0FHb8srLhI4lDyU3SPZW6qf8gtPXBLJ08pnDvVr3p0Oa7Xf0cyjDIfB242mNzm13g7Bu5fdQ4iQWtQmJ8eirKvXFDON5Oqe5qON6pr3s87hQC4CLlVByZxd7uW0U3GIp4cvixLvmmGuRY7HTRNDclKsgKxa4zdEi1UerBPs1AdhTDOVj5xz9tiRWZbKTO')
	   ,(17,1,0,'9qOY4wW3NN2p2','OE','BAROUGHTANTI','7ztCDMXX3lWVfE','pUZ4hwZWF1fm','Is37pjRRHs','FZ',447711111,'0830586597157581','2006-01-02 15:04:05','GC',50000,0.2993,-10,10,1,0,'uvAT17rGNGctNkks2nqekcWneMKJ6BOHO1WUMPhYb9D7zHHsh9yoVciXBdPTiUZtJgMRTT6TtKYpE999hHri4FZfGG20AktDh3rsUgfYoZDb5Pm8N1o8fpdAki7AXEyKFnuz8fDSsZ0zn3uTWWlPLcLPOYZHgw7zkbuUR94GqBzmSdQpj6F6iNRnJJ5hFv0J5apZmskfpnUD1qgWisNplpR7cQdFnvPlMBykvOQwjaNOZFrm5Ohvl2sLsdNglE0mZg6igAfMlBjkW0xoFOTRjlBF1FdbCef8ElZDYb3iKeqQXKdMgFteWVA2uLfFJbHrngCMc3QzdqyKwF088FSuQpo7QK1Aua3kADpkZHtL9VJZeRZh4RBrp9rHi8evp3tn4z6m0CxH7pIWtOQ09QclG3GVMw3PqvJhdB9SM')
	   ,(18,1,0,'3paMvaSaZcygoa','OE','BAROUGHTCALLY','MYTIsSiMlxSdyhh5c','a6M2AssRBAJCToLrD1J6','vKYKfZx5YS','CC',758611111,'3868848936189017','2006-01-02 15:04:05','GC',50000,0.2287,-10,10,1,0,'9PJAooHop3yUA7ndRYvy3yUm976sWWLnoaBbaiNWmrQO1UHIxlnbQLjYOLBkAXu14GwP9WtbfzY22VwmVXZdggdeTounFkwhrsvoGo7T3CR4BJFqUxRIykLVynIvFoQpE7Pzc7xWsFL5E1XlzmsNOogoYKM9QGh9qdLvT5BLXwBJvMpsRMtJ1mzTPwg9bFskfRtQYuej2LmIlxvJXn59Cvltkm6vztW2W3TYFHm53gCnVXbfvKUSzyGDxH7kdEc9DJ5yLE4I7ylb8FTgU4lEJ17SYYXGqT6U6RblKkH1zkQzjd3xwFMPmqYIY1sl2jARjZIxuuqG9Ppd84dHVHNSZXc3ric6cuVGc3Q7kElPnjxD0DGJM7abyQPBIvEStEODT4kkxycSvCCngdpqv8pUlVuph3KeDFycyFO3OH4Drc0ktkDe5TW4a8hwqykZ5tFzSl1BwvF')
	   ,(19,1,0,'A75aStydXhI5','OE','BAROUGHTATION','MLpXQGWePIBmjg73SSqy','RNcaWZC66vxd9Go8gE','xggWGCugEwRw','NP',828011111,'5789858987082543','2006-01-02 15:04:05','GC',50000,0.1086,-10,10,1,0,'srMB1cTRzM5GcLmvpD3YTYrpRMeENP9zu9J07GPzXArAC6rVRdvy0opU7RgZE0ku737ucrVQ2B2nvCuyqIt6QhMjBipJQVIb80jaC1Rp9uD1aaM751UFnOtVK10B5jXPMmEFXAM6Tz1XQkXmJT7RJKPKpDtPrVvn8ti7Q9W8rKzzKnVxRpK9g4loTzgQ7VsTmjDqH3gYRiwHqeSTYWtagEt1vBmV0oXzYOXU8Zs7XsTSxcRywvOS1kTVsjVA4IFsHCzAd5sE02mCiCuRUSKXUboOcX7PxYD6iSbdTL6UBCzdH7nSiSueBseLK13sx5EjisIn57LyW5lZ7o4wZFQ4cYeJkhiV9qkwH2QYTesrHJli8aojQin9IpQ0zYB2I')
	   ,(20,1,0,'krnjabjuHXISa','OE','BAROUGHTEING','NPAxB0FweHN1U','MtSbEhTmfuXT','dn71n4Pvou','XM',382111111,'9371143296871736','2006-01-02 15:04:05','GC',50000,0.3232,-10,10,1,0,'Bw8pTGBw6XDOVP4iLVKNIvJCtmEwHq0OLPX48KtWK6gnnAVQKmfelIZRRkRGIONs1eW8uM2BnbbsR7aV73bJMCVOLsD0z8wN15zlpzYDfd0oY3Z5HViOTUjAYZAiMm2SIvivUgqDnL5mwqq4DY7S9jMz6H4VFCkdLiwiKxYANANbu84JcWX5yHgogiFnOExRm3XX5GoNLjno3om4ynag9LrsWcViVyeQ7dcMeqdyM2n55jlsXLJT6LswuzMdCqJjzfcDn0UqIYp7k6AzptDT6wS71SlfICdS7s7xXieGh3Vk8USqs2zih42BTq42lCbjekA1C4VxNVc3TsdU0WYfOFUuRV3ghyex')
	   ,(21,1,0,'6RtBoePRV','OE','BARABLEBAR','XUyGy1gnKbcMoa3riMj','M6YZcddWhM9k36t','gB3MOsJO4oGlPvgAwMo','ZD',213511111,'2969911116701468','2006-01-02 15:04:05','GC',50000,0.4873,-10,10,1,0,'IUTArNJTaMEpAsHAHXnso9JWq6cXU2GdWNOjyRsA4BVe8jY1YP5EkB5rm7MfUnu6DlzTuzSJX4KRDu1Oqd3eab6n1OFm4pH7HD8Biyl8iiqVSzmVTTxnVsn9636ZpLBOEla7OgFVcCvn1ngEli1bdl6BFU07w9skwCjhDg60HLf2NfjyNU9FQg3VDd14Fojcq0Xx9mmScV4YKItoeQ3qTVS08v1US5YhHWFnjN4xmoDRUP4EnhNhatvm2PjvDDNN7cUlf9iikn1kj1CHYyJL7aTmASU4cqTNbRM62Ke6HxjD9wURAb2GLmM9yddk0CaIuqZ8ftXc3IVaTi1unBc0VfmM6v8ZYLYetSzJr2YRgx1hWaBWYWQY1vyq7SzJGSkvEfWdj')
	   ,(22,1,0,'Ydm4tAgXwIMur','OE','BARABLEOUGHT','nOnOLUSHpZk8BP0Agnx','CEoEjlour3HwXIN52s','Strp7gVRB7aoJJ','PC',818711111,'4409642226224022','2006-01-02 15:04:05','GC',50000,0.391,-10,10,1,0,'Pbv3kHA2eoyE7P1XYql4B8bS7YxDGaLMk9ximdDs3nbjowBPzqI6ftQjnDxkRSQyIgo3WJoEUGaNn3D7alarDQKX5Xxn36wVhs3nB1yg4ovNbe81F4QaoOcIOICcjwe81rKtWkpALyitctZG0OHZNdMgiJ7qeoxB5cEgUpRPVvQsG9ayJ8EccFVNrLGVbpH7vNQzXChptnbx3gmZEQ9wse0iNvBceTQpTa6IURVjNay6f97trE0N67fCxt7BaCD96ujjIJVjgCIOvLroIQmqsNrzVBkOPth85rvf38GeJK2G2jSIi2fo5OetkyAjmEvocvATNhfI8X0QzWwIPq351U3dmCBWUUM0ihX4OO6bFOBqRdc02hJozqFGCwR7q6ePaK3WgwZ4TXz6K8VMJWruFn28tMNZ5e9AAYnMUjAWKh6')
	   ,(23,1,0,'Cqp8FpgejqT1U','OE','BARABLEABLE','RZ2pYuNoqc3dEM9UufpR','7fS1cTVeehXXAlXL','TFVcyzYnxu4W3GxeH','QF',330611111,'5076901522490179','2006-01-02 15:04:05','GC',50000,0.2881,-10,10,1,0,'zSXHfWpNHXxllDZLQwWsb8m0XyL8fULqJu29I8Epc4SVZZcq5jWYoDnHXgDEniOlUABjEdqQi9cBaS2DMzZYtbHagcCqJ8pI6xCIFBns9nYNu314i8prQn7oqWq3JxTDE7tRJD5JG7fXFdvg9hLowrRVMc2SQLCrp1Qy98MAfOtt8uH8sVJsovH1SP7Bnk6jwYVXRUSaOC5fBRdfb2QilFZEwFbqhvBkz1eoQWq4IjYOFGxkMwfkrAAGNfJCuiExypFo5ONZCrYlIyKqHB9tEzz5dsnEyCPY8IA9fFLltfAIeRedrNeIns9AWXJC5BZzl0iSg')
	   ,(24,1,0,'ljRaI5LB','OE','BARABLEPRI','KT24izxzZPHYIgky','99fbZSSoyPRNvx','nKPHEySHx2kvQ5BQzaf','RD',170211111,'9915629111216303','2006-01-02 15:04:05','GC',50000,0.2203,-10,10,1,0,'kwB2uVv28UaFJOAUUV1d4eaIsvhezsRtmvC89m9ZKpdDfdh7A8PHNeNR1CSvIUVZS9jDC9JZzSUpfZ2R1oy8cat3dPgLDDrsoydMrZIYvzp6NNQmZmi2reoeSdos1gEk3PjvsJNRN2yCfUf08HAIDLxRnKEog5tCYxsdw5dEK6qYrRIA8LNHG7zFlzB5lEJMhFPxZrxr6Nvd3lm1QsyR2U3p0cnWDg1uTAnJ5SeIoF0OVAeNakSkvZjXrxbsXh5KJzZ3CHaY3Qkq6rsHe2n5DSLik8saLKgPuP9z0pEHeUJzdMgvNdjtf5B')
	   ,(25,1,0,'VmsS2r9FW','OE','BARABLEPRES','g0HvtrrxpZFZHt','rqzIHXkPzE3','QJvBQQm4QPZapEQhUI','TZ',367711111,'0108261223619014','2006-01-02 15:04:05','GC',50000,0.4403,-10,10,1,0,'UDTm5r0IMISoukoT3jR4YJUuhs7K8sNJx8YwXoT76vUBPT0mW9tWBwobSge07TCOzZbpwDqSlDd65qqQTudpelwPoj9mOxiZvFLxeJHzzzEqYIWNUEBxAG9GHA9ius712WtnzDgBFk1yCmz45sbIfKXhficL9AQOjaNap9oWsNbwLI3cbDCgV4VC0GEZRe0wdoYlTZnKgnGYRN59n1eRF7G94UKM0VzDIki8it4ysmDISkz0b5fNXyz64OE4l4Z0echaeSn7gcFTu3TOCAXq7OeREU0jl59OzLzXSQqKIDn0KpjRmCJp1cE7v080qoGtM036ziGcmZ8tekVlPOfZtZOgxuYwDG74IiVj9kjukMRzWaftxXUQryawUQCZj3auGbsTe066fj8CdUYfqsQEP0fo7kGQOYBuEpu5RsyExC12uYNQ')
	   ,(26,1,0,'Vhhdt8mEn','OE','BARABLEESE','eOehF0gmFXGULS','XM22rUdEKrOA','04f9vRCXNwha','XU',842411111,'0339030981809850','2006-01-02 15:04:05','GC',50000,0.197,-10,10,1,0,'VnPETIsgo2Rc0a9IVMgrTouXZvQRSVjFpNl97YPLVWlyBW9zPTQirXfp7wnJamhy1r60RA8oxJLSBn9ZaNfF9FcFm6p5asfkv3mGMw19hsxoOeHjHRpWbobKSSCoLgmPSAJDrmhF50u7PFhiIgdWrYXSdr0O0jj4kFwn4pHa76RoRaeB55DlNIDyee8KLtMFoCO7mqT1PMARuOScDZpZJ6wGQDuckHY0gHSQdbo5dyg76YqGlSIweP6kIU2FR3PMA0ncGeZyQT0nZdLj9OoP6kOd0asRHBYOMV8iDFAcNc7U3mHYhjKxZ9DiFsSdj0z8kdkorzUmHZTETDU1Hqo74UdSKUECx6fiQZLhIfGhnA2v8EaxceFN7yMVMl938LlRbFtNvybRo7raSZeX52fU6ZB6Iz4O4MEwwdANTkTlZ')
	   ,(27,1,0,'lB6Ble7Pa28f2','OE','BARABLEANTI','KbdJExH82WcryPCHoz','ntTxFsWbS57boP7','vXE34BiVY3MzEM','EJ',659511111,'0565551803369495','2006-01-02 15:04:05','GC',50000,0.2164,-10,10,1,0,'ddKyIMD6oo6IR8AeQ2zsk7A9PjRd0OXbTRO0uh2DU4xlnzFBbPl9fPPdZ4O7l4GJ6oU58B452IiWcREcJLdAhnzDbXUr5EelT7FkYqKqXBDtD1VXfvu24tD2fIeg8nbmdzCJ8wFYKxO6bZARMKAzN7cAErlZl6dY1Wk2dXEiiv31AFazJwjWN7feCuCHfWdpAqvQ7mikXOffL4pkLXos0vj0DanA7BVdVclZy20JKLQD6b08oAHKLPQDouHu0kveiGkESCWJvraj3xZYWOJbVNRzF1GIc5zdb1ihi1H2PaWAeAEcZtNbGbJjF4Amju5micRx2heV8TR6CuMc482NdFlUAJAWwDP')
	   ,(28,1,0,'gO4WY8yQZ2wB','OE','BARABLECALLY','iq1OgilWq37s2','8eMQ4J1S4EwxM9UM','fvJI2y4QAMo2Rxb2j3','NQ',214611111,'1656788920645654','2006-01-02 15:04:05','GC',50000,0.0635,-10,10,1,0,'8LFp89htLFfAP3Zu7xAXC3g2G4wM9BIweaUqrWMxGY8kq9mqA8jXrlZr1XP2LV5q3lEmjYqcxK9JrWyo9NQn9Ece0c9DntGaWK4TxRYmKYddnCs2eHThn4HmkeRSehvEEcbXos73MWOlpy0N6GRbyc1pFgfVlrTlOUcki1V6NgDtAoGBy8ufgTXFtJCwZYnUNGy3Yao439FMAoaq0TtEeNBENueG7yIGjpp57DAOqXqTvvJvBX9BxRvlBk8sgwbOSHPGLxS33lm3tEj3tzcXRYS3w7Z7s8tRADAEUQ2r67t5Hs6v7ANNrglr8hpxXmbMVRi08kmNhL9G3xocxoeuKMYQJUSTml2DPrglrVIr')
	   ,(29,1,0,'xNxyBRWAQ9vp','OE','BARABLEATION','0VwataHLptcSheNPJ6','oHg5g3eeSCRqp2fbu','V4dB1oca9Hy3wcpJ','ZE',204111111,'9266020472909983','2006-01-02 15:04:05','GC',50000,0.3531,-10,10,1,0,'hahYFhPJDNveyLtY6pDrQjLEjBXkW3iELzHLMOcCxbCsB15kupeCImP5XZEuT0lSY1udqFX2d3uxsIx08CE9TFpueeT0YMqsVVyzW6XHYXrPrwoay1aBXUukK85kkLXPEz7UlZdy977XI9zaOOuCwYekdE6lN1D0tR6ylPjwJcGWYfeGeyKfiZZtd5UEPEO6V6MBXTGrqmYalC10t45EvKc1qbi0eIo1o8qNAbKM9kfbcEBbEbJx5sDC48MY2AOgZZPmuQt29u39BMsPUNmp2mZSPZhXgxxIf2iecutY3gcRPB2FGCMdSSzlRhngaDbc9gjRH0Fdhuo2pderBKuNKQ3KPqN65Rylaktu5oiQGfuQTift6kpaTZCS7wtA5FpiEWCrneEOGiqu8hdju1Ly3jfjV9JPvmCawuHnOJPwNnvnJQOiVbJsvbJBt3R6ppIXl5NDbzTwL9qP5ti5c5nK')
	   ,(30,1,0,'euBrXHQs','OE','BARABLEEING','XkFFadtf5oCcFni2Y','dO1viSVVZLVsiQTU','teKs7Z2sMwnG','BB',377211111,'9672609884830805','2006-01-02 15:04:05','GC',50000,0.3568,-10,10,1,0,'R5MVRk2X1fWvSlfxHeS5H36Rz1c6v7d6p2XRyT0VLdGKMZ8mDIebohbR4kqHz1Unouo9s0FmDuBBAaEDRpfuneI2KRRBW1OTpZRyNTrZNJyGp9B39rB9A9ujZZ9P223Q9iadw2nQlyD2rrS53SBukkYX8x4kyumgHxnKI7rBp7LlvAouKssEjBqQ9Nt8OyxLybpvctDftcAE73YiBixhMQwOgkFwBXFCEsVsKAYuNTftdA5yL0ULUs23Ci37jKjfKpElITKZfaon2IRC595VUWSeLfdd7jnP8FmW6KxxQ31MqopFygYyEohN1B1jW7Eqbz468OTtxNYAXocI64huWxx4SJjHFDsXsIvgb3GywhhtEDGc4864DOnq2MOHmV985eKQZQbwzpytsBITrMwL8oIM3kJVllE2IzVVa4q2h4IhigjMXDSF9AAEJ')
	   ,(31,1,0,'dBIFQ7eA','OE','BARPRIBAR','ZeIsZtB5uHGDTh','YbvF7gPu9PKzEFdtS','lvaW3kdMUWd3XlCnXNuh','ZN',549511111,'6929904909072923','2006-01-02 15:04:05','GC',50000,0.1039,-10,10,1,0,'cEpQocaLqESn5ZUvgy7q74J3egP2pp6BTtYHmRcez3hPZElvnxBVRUFKkjNIvqCIz8zf7TBOQb0HouUAFPXEMkn9F31RL2ZZ0C206I5nxaAqHyW4RMsMBxWrwkouk6AF0fWYZy4HPXVjbPXdDKPLCAa5rMaTNNjgNR0YHT2IWsv7XYzAF1toppdc9p8a3zB9yd2GjnviY7tUmlFKRlvQmcPuJGLnyCsm5xE1h8twOcJvYZtyfyRxDo2pQ08g0uz5dQfzGtLKdAPC1o0nNyHJkutbbSyKcdp2TobNLI0SvzFAuSUok04mtFsTjS5HHngjfXiUMJnLDetnRz1PbIEjPWZz153Im')
	   ,(32,1,0,'QuqAHlQWq0','OE','BARPRIOUGHT','W05pWtcVwzuqpWd8qP','cX2QegGSeoniF8lWy8Zx','byGhVHwfI0kI','KT',672911111,'2224246770583209','2006-01-02 15:04:05','GC',50000,0.3182,-10,10,1,0,'d7OgXYCZRuvwr7zEfOPDBQ8XuEIe2FGGMLGhnVr6Ah5rgInW5Z3tQVPnbP9Qwwh0ek6UlPFzMUhIwhW6YJ0UAEJPrBuKDk7Yj3c9Uk9t7gnBx3feA9way09EGskCIk7Kv33zbonxKGxNnR1OLguZG8th9pfecVwKuOMIJ7Y4V4kooQjOvf3IfXw8aufrFuBTJZvMvbnYOFNYrIevvONlLdjPx9phZYpnPywTerBn6uVW99i8TAGwNWs50eu7s9lksMz5ETr1o5krQMynhy7fuslnQw8N8StmVBY5WJMxrrJs1P4zcqwppnyc05QI2J0nQkB3cuzZsMw6C4f7G2DoQ7B3ZfJzVKvZ9UW5Tq40NPFkfNwr57XvQ8vC2MqHfJ6Uz')
	   ,(33,1,0,'7mVCqPAONtOip894','OE','BARPRIABLE','VYdEWO5c0O6R4rfs5DsS','04lEIf6lwfr0ylVh','rRYx9DOxWqhgYJYTJe3','LD',882911111,'6494293642410336','2006-01-02 15:04:05','GC',50000,0.1241,-10,10,1,0,'irnYBWx1AwCYgDN6TOs451n5MS9vKsy31zGIuvTECM6zpgj2w9dkrOEP04L0jwlUYv9ewAckPVFx28WdI9HkOkLRwWN5yw5pzV8SjQbI13gVS9HsfTtGSueTVYh4l6qdjVjnkA1vkcBENmmLQemEjRvPLi84ycbf84WIW4wgsuYIsqiP20B4Lfa0yNlA3ScdonOn3npiVKSo1cEUEjJiuHxtyvpWnuXA9Ph3tXNI4BwC9vjqgJv38jfpZPmpssqfmesExAQmXlNEzsZDaqnD3GZjkaOhBPbKTLAjdjd5epdcnhykmVz1MJYd7llVV1ClflMHXe4bUspoLu8p3BR5tCqYA4A5SJbbDW3Z7ziH1rGcBT0PIrScdf80T0iTjLCrJ49lO99Pi95G1roJmwCGiWyHPuW4rG9WlbvBcCGaH0knnOroCa1OouV4dE09airCdQrdoHH4dtia2hy0FhcUWs9XDJGD0STVMCVmMFM0PQnjKHaV')
	   ,(34,1,0,'HTg12mBMZu43H3Q','OE','BARPRIPRI','oWGKATvcNkjQbcgRZ7xx','sgDe6IPnsTRqkwb9woe','akQOzTCRWyS9A4H4zYD','KC',737611111,'1191904167148280','2006-01-02 15:04:05','GC',50000,0.2115,-10,10,1,0,'eCdixRJh0eGfogRRsPl2PHvwETQTNXQjtVpYMeu1JXec1avUIzMNxy1sDXnNeoZhO7IR7d3a0a4iyW8k6btlQM1bJFTTm8MLi1Uj5HJi3Q9zS9uq4KUkpR8IBxlul9wqrtQknt5iiFSMslkLLg16biPmsAXY5358ilhKHKlML7ME7sLD8jFpkhJbIx2gm12GHZvGDF1mUoqU4RjMFj1YS36cAQArZKRSvxPB2vt9eLfYuaIbpZ1p02S6SqbuNFrD5pKhJ5vnNfllDG2RF1NQKUayL8sRGhNGYBPX7flu6lue4R7Tcyar1YmQaWJE4CKiVEKWyiprCG9Xl2sTizoW4HwXp2CIdSol0v5SdzmZddqJJgwUi9iqlvoGXA7RATbsoKH23wrxXbpiGY8u6xrKQbIiQuZkiB9GjrzNWsOM8Ezjq8tA6OlqYVpCZd7B')
	   ,(35,1,0,'NwuaOEmjo2EJe','OE','BARPRIPRES','iBvwyLhDHuKpjnvrE','9fy0QxsQLPXcglECeu','0e3Lz4rRYaLF','RY',813311111,'5161756815845025','2006-01-02 15:04:05','BC',50000,0.288,-10,10,1,0,'QKvqUUbi13paLtQLiylPtPG32gK1dFZWeWYROObZsy3k662ONszj8HhLphacQWmZwRfyvHinmiukmFJBDNv1U6PPUxH1ikIowUQISV4wpqODwXTvEjYs1AWSUBKRZo6oSPUl0Ptww7JoCdlNGbIhHDo8eJ5IBAfpNumiOlLe3IBrPYfVv5hhXBNxEzE5fj2khPQmN5nfERLPeTuVBBST3ZNANrJmp4SG713dJYbcxxlGYHKDU0Eost54h3VHS5LVg4qLOBWe4MJFHTTQg1sJECOQpByx2y0JFjW1fKCfHKC8FCulga2xVB8bvuNS9j4TxH9QipkM2T0oKVvgiIslKGbMd3NkY4esAZorgCAUqol7E5fOxPP12XOKr7qcEOMHFiiJ2stnq3ndsLLcB8fohXYiMee9Dm8RasryIeG5kqHnBjBFCAxoyoURBKPAy79ya1PFeD2vxUOfC5bnBDBQBUza')
	   ,(36,1,0,'N76NxtuIfdGb9dz','OE','BARPRIESE','zZJ1QbjIVwDSc7','WsZPcwvQnZH','RCeFBlUjkhU','MF',836411111,'0439490613416278','2006-01-02 15:04:05','GC',50000,0.0739,-10,10,1,0,'NvZwemOPjl6Y8of4j3qzawdUdySPvw8x1TFuiORUCLvvnwtqNH4oEXbbOEWa6JPZFq2mCCDlQMDG7Nsr21emBsRhAjxsF4K58kel9vtnX5pfg73Jur42ZCz9THwBVn90q8opCaOyuug0unTqqPfPw0vWph8e0sJ87geclfqCIYiF7ZlJX9BDsxiS2jMAvSgU5Kce9hr5v4PLET7bMrQ1nPervvQAIcV4FXb7OUYPnWtK7Wf5C9wq50snyRh8Q2Zb485e36WOCab2vEkJLlGcpURKFbnEL7qAqswe7KYYUFM98qnenKrnmTyXLRn9U0Bk3nbA7oCg28brDKo5r2MQIB09uBAIgx')
	   ,(37,1,0,'ZrWIPWCXipH','OE','BARPRIANTI','TC47nK1duZdQUUIyuoG','rtFEMiJHE0S59V1b2','gaiGtfmHLnzU89D','KJ',796011111,'2172082079225344','2006-01-02 15:04:05','GC',50000,0.0443,-10,10,1,0,'Qy8VocBC90Sc4AKQ8yUli2TY0HDr5WplKqmP9OhrjDh8NkESRHdMqUlDioz2NjZD4qvdWiDPgdXj6lSQZxHRf1YXH5XRpofnIbI4sHIR0ag44V1hpwQlO4MmJibhr7F0JbVQAtREzvzvsBzS8D1UulXz5wMzlrTAZD5az52oeoQ4neiipnxVkq2QeOW5THFjZ44AQXUQS39QcIDqedWVLEkpkG9pMimaraOELKuqScIBJR8PPdGYR0YXIPd4pHc4XkBRAGY98wd370zO2VC2NVvRv1ZkzZhhcwi7khgOboaE7thHsKKzxn6WuX1htAxLOTGvkmPpGlxDheSOJxkkmw4zeHujrNa1tqRnVZUzZJ447iI72jErZtiZwZmV7lfj')
	   ,(38,1,0,'3T7FxAnDVU75irqi','OE','BARPRICALLY','IUtVPZNpyMF1eY3kSHw','Vt7oHk1TRB','Xzl7EcVhByKYIivSZ','QO',164111111,'4055118939974604','2006-01-02 15:04:05','GC',50000,0.3809,-10,10,1,0,'QfYjiDcYUkWxwqICnSQ9jbIDmSE1j3zLaBIcyfmk5GFXeJguYvbQry533KwTwZbR34LSjlqdPyvbOkXB6R8ueY6s2Xj06V0vGV0DPhF9svwRvy5wqTa3qHDJikbe1wdZje2PwaHhynMY3zWInH2r6ocAXdCMU6q2XPkuimdXD55KqtEXZ5B2URZrgBh0GMLvy5Tx0FMmURn9ZRMXjJvkQAvHpIwhJPhOCUpbqR8If6TMDaZ0tkz4YEa4CoOWeqao1rlDXaFDozvnBOupviEHeMuUj69mpS46OCzgXL5G2eKR5wGqZcKOqMD8ujvl3Sw9ctZlGiqvIQtbBmWRSAx2Um60PiHKjjjdbd46JfAA4CfyP9sB5mRPhQLsQqijpazVKYV4QB9l8P79anz4UjDFFg')
	   ,(39,1,0,'VkGthSm69bO8sr','OE','BARPRIATION','0I6jfL77TWeWPMQVA','QZR6ciJgpscF','ojBrfxDRafAA','EZ',664911111,'2379624215087102','2006-01-02 15:04:05','GC',50000,0.0783,-10,10,1,0,'rU5jG9IKsm3xlpwyeH9rmJ6CnJPCwR18Lc9IcViUzBU79enHk9cf5R2Y7Jv8WbqD9TEm5ygEkhFDwrQXJLAcX62LLSa9AV94QDk0rTRGJY6s8w8pPM7uqCVGIGuV0QkCD41SJvVR0b8fHNhvqpqfKq3AneVs2DYlemSutlfLDaqLr1UswtXb3qXvZLRVkdVgjlC6hhL4tTVoKlVQ3f1LpBCVGzWgPeVFyidXwytfgGI8bvtAt6UGFhjrg6tCCe7a3A0Y2mt5eDxEv4KSQ0Ol0sOR6Bx4lOjr8uYAeaDwt7oEUBj65iMK2yNZiy7ugSTAD9QM3bok9M0PEaILLIxKYwjtFAZCbBnxAaLpafeoEzkb139KQSTkli949xYBVlVrBgrpILIpbNgPEiKjMRJSOoYlWXK2MySRclLXuxsAfDQmokMkv55dttF5X8jG5YEdtYGJRJPkmKVNTYlsQtsM')
	   ,(40,1,0,'jFZ3XmOCLYeIbg','OE','BARPRIEING','iLkBOgwztv4MMg','H8lf7V6Fg0','qyuiiAprLXJbTZn','WZ',052211111,'2412386774766953','2006-01-02 15:04:05','GC',50000,0.0545,-10,10,1,0,'uzwHgirvt871x7aiqTxXyBOjSptyXKPrZlkeRMfEXsaFtRw8YWuAFfmUeZuGQ3Jv8BDvW9Qv1p5wpWZ6IpWj3FTOXINOqW4s1vuTrWF0R5qBjj5xxFXxI1QVJI1g4DgSQ9rrq8hqkpqDlxRm95heAf7mdtLG4mU9YcSSqg42dkNohUaCfnPHwUr4qNjFb38SYjFV6uYVY3euLLw2HZok571mCxdt7xMMIMgnwnw1pwImt0ynKgkCtmVKrwNU4ortRFNMLt1nJRWM2k8vH2ZhOVnd1Sd8NStrouq5UyW9Kcib4qSXJ4gEGkO0CNS9tj5P22pP419WDSZ0PPknc311m9LXLXKTXuXrq2maMK72KSOoniiFh8SobyDnxl47eckjIwgUacZovjeoOPXqbjxgSqyuNIo3TjDVyAssfq85XR0vPdsS1XxFhaqPjDr6BPtJPf2Tn22Za1uTnNfT2K6eKGQGA2bOg79PsMs4uieDwY')
	   ,(41,1,0,'wnRhFuKnTobY2Ii','OE','BARPRESBAR','UaIbwVP5dda829','7uZwzYFwAXnVCbLoYT1q','F7D5DbcG6DukyjPCvi','BW',012311111,'7917850669653242','2006-01-02 15:04:05','GC',50000,0.1562,-10,10,1,0,'M1ha0gJSm0vvRVEr9TIIADMzx80Et5SHN6OVAb0MMICqSNe2hBZ3x9QsbA2e2oFwOlLy8841VLXmX4eQPSen0UQFDkdwKmJI4DNGdbbkIiXW5TCyuofJvhbiEPsK88Hx5zewbRdABzK2MAvZ6lmsvTSj1mbSWZTRslaXg6IzQQ9pthU6tmihmEkmaYuKee0FVoYUAMpatbPt5oJbCOXsGkJsqsTL0Jse51xfSXzTev1tnF58WoIbVmfzwAxTQKfUgOoEFccHXXs87NlgFDfxuOMnvWskGpBr4Y5E47tAEaQkFhKWDpO8dNwnGw6mmqPm20P')
	   ,(42,1,0,'8ZVWHKFer','OE','BARPRESOUGHT','mu8uDeXWgGMAU0oJ2Nsy','zbKFuOgOkl9hpQVAVu2l','kRSODBfMHuoeDeU','DJ',165211111,'2081288842627077','2006-01-02 15:04:05','GC',50000,0.4843,-10,10,1,0,'cJZaHv4eoHAxcAw8H0vBGFtYRU84kgXUq9pyXasIRhXKnlHpAn2iseSfUE3Ei4d4UNs30vVEw1YyfxyrRaUwTayPKzE3XHrm4hzohqGNMeOyIIhmX1OWG1YSCZYr4WJZb1rBWgj9MiaeKI2PdLMjey3Uu9bSOzK472yrAyWCx6lq8znaB0rjDdsr3946vIcEcB7rJMD0g17gGEFGLj1WR0z9CsiyJ4QJb4CmBU5DSMzAvQvHEzASkSWdkmK29rRWM4vwkwAUS8OmrgMnhhVZSnYuor5aKK7pBiGzBFQ28DF1MKCi9uFJGZiUl8QVGVIAOCJ3XvDjDswD4ET9f67LLSFKOvGKnZdRCSV1ppxdwIOV9HDIwcoKTpDFC5PSBFd3AJJdxWYhUfiTDCXbEYgXKvxxovS8M1LVDBPv7Bf5gispnIwvhF4iE')
	   ,(43,1,0,'g3VPZ1biu','OE','BARPRESABLE','NEXmEJgdj7YGGMF','hWo7jZYTwaCv8','xsEOSQaA47iCMGk','LB',682411111,'2816349734217749','2006-01-02 15:04:05','GC',50000,0.2435,-10,10,1,0,'EHLSRNj1D0R5wyh6D6xhSMAZ9nGDukZtSh9t5ejISk6nJpRsR2cPjhtcOdPtGJdKHFooREgOm1xeQdAZDNI4SyzB4VF66D6DURhcSHbJVIPWhsvJddXrAWZWpUHKyaK0ViKqD248VBfrQ3gf2ILKKrNHwINUOejvYTmcTVt0hqWoVwtvgQtIbdXDTnDFT9z9kAkGhlv8o6jryaL9mb42A1LgwMtnGJtdgB6UBK0mKW06NQgHTS29Yh0fdKbm6qZIouv04U038skXswe9pwEDMcjltFzOm5aL5JVtnme5vJxvqckoqizfRE61gTcwy9aMUABBfDYMJiAecQsYOSSXdNUVyWTSBYA1KkKld9E0O91rULOSV78bbrh7ZnRllJWbAbHqfdvs5occk51TU')
	   ,(44,1,0,'rb0MAxy3OkE','OE','BARPRESPRI','Jms26eDUuDzDKuo','M4BeTN5JwOI','GebMRyBn7u9Iocgp','GF',959811111,'1822326510150499','2006-01-02 15:04:05','GC',50000,0.4343,-10,10,1,0,'cI3qxgYyxag4QBuULPbBK9enOoJhkBZ2hlaOtBjg4sxVNOgf9ZSUhkL7OIDYNcsGzqjHWQoLl2geYAO7WUIjXsMd0d8aIfmo2PoNJ84x3XmU8eJdqbTCPXftthQMUvozxAnBnVgPffJT5U7rqXEXDniBlzJ6vZSq2ekZZgTzqz32D2AOPyxf8vIGIJlDwn8jVUgLXqSCHSItPnVkIuC6Lg0vwbnRFsbDVrNEv6tueWyIOVjRORkUY8HOORQy09cZWOfDAZ45lJx2M8PFPslxN4TIdChARxNvDx00G2Ss0aG3mZTw1k7DxH3PX2Su4bYLkjlO9ct2uPkB0P7i5pZq4y8CfVkhiptwwvTTUIrGRPiCTq6jd8PrPj0friNS9pzG60bMA982LOlP3f1QbyKyVQiSZ9fjMEF')
	   ,(45,1,0,'D2zhdIDPlJVj','OE','BARPRESPRES','oV7t806HidXnxFJ3','1Rik0NgUMJn1age4','nyE1aI8z3vgz5ZV7','GN',822011111,'4103966963495863','2006-01-02 15:04:05','GC',50000,0.2695,-10,10,1,0,'DNJy0PqyXPaBNb0IYHFzg5esZWrFZYRGcPV8c4ZJHjaG2cxt3snSuiAx8w7vOfXnx0LnoLcORXTlEDVIVmeGWEuLWR1ICLI0igAIF4U8RXEmuSVvAmOZJB95avUrSYW4VoXs5HgcubL8SYo1yO2rwdsvwOTjWa3MzeDZQjMp7j7ud9ruH7c4Joyq80MNPTfIFKkfZwQeBi7H8Z49199uHi4G3BL1XRzeSpZLuuBqly1PJeobEzdaj1gcSQsqNO0gNL38SjQgeO1mL0633uDbpyr4UkV9UlIIh9b2KecYb4ycTFppmL8AiXdw20cMLYv')
	   ,(46,1,0,'e8SavKlMK1qp','OE','BARPRESESE','y1aVIp7ysRvZ','IibgxpBLbDW','hq3lpvwDRIF6','NU',465011111,'0069155090098016','2006-01-02 15:04:05','GC',50000,0.2543,-10,10,1,0,'rgzfU3ph8iO46lpD8FKBcSe8VqlODFFqQmd2npHECCKpEDVxYQaAvdSejh3LULPTum6oWmNd5wUDS0fzJR54jfCQ6K6PXF5b0WUI0S54PQkaBNTUJuRVd0IIOCNdHGKdzrmgPslxbz86MAcLUSScRgq1d2Fn0b6iweEYfV79h5DZnmEUwLR6fIt9Zp4Sp4PBIiGbzO90zipwdm0ggW1z3EHkG4lUdwtqlKgH66TZS7Ifg2Fvadb1GwuA93tT5Iqv9iBwfgbo4ANGVWmt8u3MCTDDwJjsIS6z8zqjkoUJTnLVzlucTpSd8rOgawnI5q97SEBu1GPHLLfVKet7zh8Y89058zAVYRJciw6VpV8Rd93qaBnbV0')
	   ,(47,1,0,'7cmSHWqy5LKeVH','OE','BARPRESANTI','0t9cCg4240GgmThwSLq','qkvd0xUuJI','KR2NcIwkVbfALU6eCIH','DC',450811111,'1381465021530329','2006-01-02 15:04:05','GC',50000,0.1467,-10,10,1,0,'7l4cmhNPbG4SJYuzRdmRfQTrn8Jx5leFmgp0xhON47IyW7wganIfGqqdgsSIU6PL5L27XEhPkeTpnaXjuLCG4TSKmsRyT31LwoCH8GOxKYWdGCQ0swJxdTzKCnNKmROYVzecWTqixh0SUyFbJAKbWnRPng6aJrt8clHGRY6CsTQ9mLuUDS3lYZ4KCHNjicT1Q0wUW87dbalK7DKDOnzyMvg6otsL6ZT8Ai5oDzTXts9hrYIH8ALHOKzrznqlBAXwSwKE0kZS80nUTP2A09JkpsW41wGsNEySCrX3AiwXFb0A2hkgBuDsEkmxK1bD3JkHISJjXzwXwfh9ACjGoui0')
	   ,(48,1,0,'3VLjB2CKeaiARnx','OE','BARPRESCALLY','JliuWLKD5l4LAaLD','IeyGRKJJIELuLz4n','33jF55Mndhyk','DQ',066811111,'2070315468060620','2006-01-02 15:04:05','GC',50000,0.4437,-10,10,1,0,'j31c3Bh8UY8in1C3H7bFeFzh51WjmHH0wQ7aWNR1mhWCQHJUOQbPWJLHtztf4ioSaQLBHBorhmOFSbv79GW9S8lJm9HCBiUdAXuw5Whfd0DXSzdeMLPwHlPQG63aseH2wK1jd0ZvCf80Zexhj3lJPloHSdPG9fhBXJIVISc77EXXwOlVsx3dQixtgcB2qFZaOW8UdshX3gHVhPQRWO0gphKWiqhtCNWrO6NHVcRZ2N7ucKA2zHEFW5cjMIW8x1ZpxfAE4OyoOI9YR1ekv8Blru8eDxkH7047uo6r1M4CYsAmuXhSP0ESxYNBICnjKPotjkRKdvJySbeFmYiY0uNOMd96daO6ox0GGsqqOLjaweH08fp2dkM0vpF3b0aDV38aFmcuJ8IsaN2grLQAPfI')
	   ,(49,1,0,'7mqTj1qrXjM7Qxh8','OE','BARPRESATION','S3ihsfRnrH','I4pfglbeTkSWJy4BkVZb','sKL27pmWVDFdFx0','BX',567011111,'5379846594157743','2006-01-02 15:04:05','GC',50000,0.3867,-10,10,1,0,'EgM1WCmqtkCmlBrziJZsIeS132D7DuyeZQsXAfH5L3TFmUfjEAOMpgtWPvIF46o6BRDD3VA4jnqvcasfEt0KskFXU0fItVxTKaILtA4ABqBVuAlkNrqOYwEfTaSRaj2VOpFAIWvWdQb1gnSEDcJH0iyLbeS8XBQ0sNMQG3X5lC1RsSUmEtQgrHfQDnuDyPJrLShiQOgejY6ZNKUlAoPvk6WcnmoaHE4g1O9ChEt0R7XGEZmuRMictkGxZjKM1mRfimmA7CyJNnifJZLUp62lzG93qqe799X4I3ZmkMVYlm7GwuJ25ZvcTigYaxV1c9ZSXjkqLtwBxzBXMly3SStxhAHBK7d4qwb2EuQHeX6A12KMvqjmIXwO0WuIWO4aSigapVNne0LYcWCe3Id2MJKUvXM6NDFtptIFMlSJaQGlmCMil6awnMVUwh7')
	   ,(50,1,0,'Jk2WX4Htt','OE','BARPRESEING','aS6iiOwovir28','fpzz8qPohj8wLF','yrp16iobGdnl','DT',659111111,'7801150246618688','2006-01-02 15:04:05','BC',50000,0.0649,-10,10,1,0,'KIACKe3UCfEDmTg3WeANsGtoyNFWbVVAdOMebruYhRTvqCu979MbfWYVAMEuNvtwGhh58Eo8SdU4Vt5g0aTe2CuUBrr38uIgXrOZLbiptFVkytMnMVLYprWd5Rt931IURFO4zFjeSXpJA2y1uyVpiLru0eCEGedDFUQTh5XHsGwaXMUaRGBoisb45GOIvUlfkqFvx4sEq69EZJ6dvm0ZhvepC8aEmUFVmSRuun7pkef919Xn9tTpfob0MP7IxODRbqsnQvPnjeTKhOmJQQp4N3Foxavdlt2XF1jJAlv8WhX59Pe4vZPXonq1s0YI0p5zCp8r63qqLM6m7TyLHcF6dHedkxh1g8T2HvKvp8G3IacFWYD7MWi3dQ7hmexudoDVXxKXamYjwDJoWl4s4YtNpiukcHEdQLi8iQZh')
	   ,(51,1,0,'z5Dw6kkYBi2ampPk','OE','BARESEBAR','e5ux1uEkZWL5pgS','ukbSmvRxjZ','Xs8p2aNx7lqAV','EQ',947511111,'3750584645097786','2006-01-02 15:04:05','GC',50000,0.3199,-10,10,1,0,'mHQIQiIHX2DF7QQuk5Lciijy1W6wUUYnzacLd2wM6CWX3q1nLFm6KgzRpTWGvRHiQGr5lmnF0bK2l2AypYGT1xiyyXEDCp2uMWRA5tn49j6Dd84rFjM1rJl1D7801ZJm5DeVbqGpFImYjiwCdW6zNUvkzTxwPHW9oT1YmIdIxz0SohDecoVzpxLPi9ZSOJ8tYmUp3CT5pAEYVrYxfINFUh6PULxzkH1TWcE9Jb3CfOEwkXhuHW15udMoewWkgM33y0n0tSem7JnGTlJw1S4Rwt4HnrsPNmQKnQIjv4umJfspSNdt')
	   ,(52,1,0,'8FyKWA9fBM','OE','BARESEOUGHT','ltfOJGQgtKi37ye1','OXqTMqTk8ACtT18x','z06UdfF1ZyO','AC',914511111,'4938259593253414','2006-01-02 15:04:05','GC',50000,0.0589,-10,10,1,0,'FOaLz8vudFx0kE70q0TRuxIvm0bvznVbKDx1PUKMQPqIYaHZMbT55Pdy33UWmHoxtMFneDmEGk9KfZ3XnpaP5OE0Gx2An4Hh67G7IRCfjlywv8foLQST4kt26hLtisucq5K7mKwVj4XO0UtU5wihkne5TBm28LoieSaYBStZmIUucaUpxTDcoB0c3In6Cc5ZriVZwUD1fCnDLoZiwhS2VAPOIS1OeYu8rsyqQVr4WNFNRatL72ln5PJsb1D8csdCqJg3lxmvzpb6hdLZBdVxtNQf2yl5gZbo3Aj8ltxikvZCSN0pTzYFndGfej6f1BXmpHs5X2Y7Z6ZNcSPZmLkObIxiBu0GU9t5IpEE3znQWQFn3oODHGR2h7yXCXRksIP5aVnWpqIsIkmdxiEgBEpmKsGmBWBU7W6d8VYZaxLGTFS9paj1NWZ7DlsP')
	   ,(53,1,0,'gLeZcnl5UvF','OE','BARESEABLE','fDydusp9ZTRKkweAeo9','eft9ndfO1VeR','4js4z03FlOOKsX','OH',529211111,'6284792302211929','2006-01-02 15:04:05','GC',50000,0.2234,-10,10,1,0,'EO99Pk4thDM8sBu9TLfEIwwFjJfjQ6YPxILEMJfyNKg8hSP2SHSsZ6NsKQMYm9YisgtBVeXOvBmT5ZpTNmkEmkILv4AOWui0Q7I3s7Y3ASzOY04yLon64xxKRJcDQuFlYxs6UOfzrPAjq5xJKEWQ6RsNUWauywEKWKir7ARDEvh4K441WidXhTwL4rzYKRzHheM35OLGZj9I9uCKIadNsFw6yjofWYbYMSgznW97gMRuvkZbobH5NA00fRnhtQ1m5iaJls86xDLyRP4Hhr4fSCEMU7IejVbIX38nnxiWjYgV9uaN9uY343p1PFvSCTyQlrBJEaXr34RBrh9hjVw1IOnkZQZBVX6rvCMnpICjqASXYRy6a6Z90phipH4UDqSEy28NRzJzgVNmyWk67aNXoOB7onEu756doAQv2KQ1')
	   ,(54,1,0,'mvr4fA2RgxVAqh','OE','BARESEPRI','jiVxWWgDnOKF8Yt8R','nFV99maevdQ33fgQ','TbQJdPCH2C','PB',850511111,'9510675428116148','2006-01-02 15:04:05','GC',50000,0.1564,-10,10,1,0,'x1jeMuRH2jpS4Il3SNnit6sHXp9Y1OTa42w10EpeE4R76Shod0WY8Ld9CYRc8zNxt3gk8ACjOh2odSDL6ttb9Qs7vpbSxz3WuBTiRc9z6tNCMi3gTP3A8kIKcLTtIxSMxVCJxBHeLTRuF0AiiBaekUIvPkfRJYik5BbpCx3bqXyy8wimOIpy62FxjbSzF0NA3WxxHzuIsRYcX7VolxuQgH61HE1aizHb5WtE7XajejFEn8cgLrdZvvnjdXAA49v0dFw17Mq4HoRND5f4gi7m1UdcgTzOGCpJz1RZ9nH5cLw8vY8jIB2yCp0M04d4cPFgaPR2mlwb3IyzvrHCxF2OcI1ziTWdvvytWYztJk0p2SQLhfcVuBSLnAFDbQpkFV33NFFYOiFPwlyYeGRSnveqBrrV4rodc6RZWsQBNNUtMIdVjZ4erblye00fkgCoYQheD803Kjz6GAA')
	   ,(55,1,0,'tObw1Pmu','OE','BARESEPRES','MzPGpRUrlETHnsuW6Fq','pBkHjQ71V7sjzCfrx9','jZ8kZSTkl449c','BE',088111111,'5159194113904352','2006-01-02 15:04:05','GC',50000,0.4394,-10,10,1,0,'uJWvRZvdzh2cmlHM2Ujb3AUfRcefYNQMgs7tPcqzAN5Hko5SuwSymgwyiSJwH4qEIE9UfQJxCOLFoh707Tn2QUEYFFnYd6NPqQgDC0wSNk2Ka62xsBzPe8DclulS30JuE0O3y71TEsVGHKhgsPg7JgGMpzlbMoF0k3zcy6xXIfuPUZRwy7ajTCHoT0V65cVYzdLZKoiw1SRlyr4TLxd1gKPutZpsPdh7emrikQnCQts8vCDymLJOId4QBlJwJRnXqS1GoumXmkSgfZlsryfQmnumcIEwbg7ZfWFjk8HoJ3agkz6TzALhBT1OXrKnHnToJo77rTtcaMKOnM0nPzn0Wjz9a8h2fM7MW36VUSv2V2ul9KvDeR6deA2yMN')
	   ,(56,1,0,'Q9LHoEMUP8E1BGJS','OE','BARESEESE','oDz9GIDrHNLW1NhXrzFy','XdjBXnluCnX8QmLvq3','AKM7b2nHyvl','CM',295411111,'2373441334445020','2006-01-02 15:04:05','GC',50000,0.2946,-10,10,1,0,'x2Ly0lKIjnaWkRhQfCPjv6cstilrmsWOOD8AbXzDqUU9KdDufMDow4H18KnD741zE8z4JWwyJJl9lAXHNndOc76SVv65Jop4mycXdunLBKLEbzVGpGAzO6g5RY59Bo5PYxBpBlmtVAS3ZglZe30rbKmWu63VjEYb3Z5jZPmAT99KevXraEl9JJR4263ZY0wLG02gKzT048oMj24epytZnSrtoLTDYOnqRzjFhyK51PXT31glVYLZgyVONHQiCpfXqnEyzjCYXSRQXzk9Ogxh8ep1lnjK1QtwgqBpyZ6mlSlKnJgsT0iSnRjYxo2BzfrntZHxphQ64IAVRfoPhkiXlCE7UKzrH2Ugv2ESPk4a5bEeLVO5mEwyqFSzkamkI4glPX4ahZRwKHsjqNbgXGEEeNEBQ1h6ehqoxJnby9ArIZeaRPTy516fHSxK2ufAs49LJKRQjtYriY3iHi')
	   ,(57,1,0,'nhLeTJLyze60','OE','BARESEANTI','JX4bvozBks','lhvBkiD9ibJf','DZOLTR3kRnQZJjsYS7','AK',561811111,'9545252770071401','2006-01-02 15:04:05','GC',50000,0.3021,-10,10,1,0,'6WrojzwJNS9rtaN3DPICnkcaGGCKPaVmbrKkNgJ3rXBSOEacr0ZqhEJ4TVlAbmjBMGTGf8T9MkTwJ0OGYHb0ZOSDkigtPwvzrxi4vjQ98i58WnanjXjbn3NQO8Y0pbedEbnzbrzbba2Ytu2P3BAln7rQkLBUOn5J4U5O8xUtWkCZe1eznhaauMpmDdcUuwzYYUMwdn42wZg8Sg8w3T0MpaM5M7d2C78jwWaiySo4GLKsVl90nl0HTZFJV50deuAFLEj5Cdt3AvH5tYfBpsgfAZQPVpAAf6o5K4YcKLV81jumADf8813xrc1lFXDHi3KYqDEmPq5yRzA3lzvifS31EjmKjZsunoq0Md0ovNJ4M4rOFoBLipD2hbtCulauw3DnH9sFptIJJlxIPspUiM')
	   ,(58,1,0,'JDVwWMAh','OE','BARESECALLY','id9bT4gYy1rI','gGtx3XTwgr5AgiCVxL','85ndb09fjrMX','DQ',153011111,'6611623740210652','2006-01-02 15:04:05','GC',50000,0.435,-10,10,1,0,'z799my2BAbcJiQtbmPgOfdZS2QbLPjJHV1QQdNUSJWfImwXRSfmLcNSpFqHO4K0AIGDEIiRZ4K3KoxkaJD2geyAHK2kugPWXG9ik87ZsTdYKd7dgdwgWGZuQ0JyhIAq5u6bqYT1sghMsYfUOiGmzxNeyfC5OryD4Ryh6O4IpT3OZx3SlvpA8YnpsTJxVYT3q2hn5sAvGD99e9O2DaTQVEZayOFV1MWhi3nAAIwbqvThWwjbbkbn2TX7HJ9Jla8ZFXUnyYQRux0ot2EnJhIk6q1ID4jNpCRHe7ZLExxY0jOTZNcvPFTiq5pfXSsfC69iFtdjAD40FvB0LKqCSAkUBgDTO4')
	   ,(59,1,0,'YFwS80QZKv76bh4K','OE','BARESEATION','rd16WbR9QHRT','ViibHHIq7Yhfw','Nm43kkaOI9','JR',693711111,'3894533091286986','2006-01-02 15:04:05','BC',50000,0.3029,-10,10,1,0,'p1wRowrTfPhWXxvRqaFxkjmNF4Q3k6NSMTWe8XKjQLxg1JlZWjkt6q6CmcutyA3Ora9sm6wLTuBSbRloPeanHQauPoGxsvg3hN3zUfzNTrPikAtTFtxwXD8CoxHTGzUIxAafpB5jlUbIFZKuS9OR2fuOEZqIEaeh4o0XzICPBCXgXBkAuBX7U9ZNwh5Z95jP7skCZw3Lfu3MiNUQhEpB2KMUAQSKzopEvsuCWmoLCG2UWMU4RIUVjBmdB558MuuMJfx2NOndsojiI1cd2I8cD01p1pLbJdfzDZeBArlITrzEkZ3mfRslt2hEi5ye6hQtze7qeJxzMMA8vWDsH4qxTdKUQP8tqoyd2Cz27Q0oULaUNIfPp2CKr06H1OJdEP5qPIpboqBOja2uS5cnrobXnxriHMqyIUkrAFuYT3qt2tIdCbJdRcp7K3tl2XrDXiWiPOrqktgyWqxUxBVrrjSZ92qSmHSpAg')
	   ,(60,1,0,'x5fqWGgv3PK6Pl','OE','BARESEEING','FHVorKrGrT10TUp','m2rial1ZxV4axRqldOw','TJIO421JddI77Ol','BH',103011111,'6022014205144262','2006-01-02 15:04:05','GC',50000,0.2723,-10,10,1,0,'xWTYcUdn3sZ1YFZ9Y4H8YlE4HM4PaHFyURzHCSh5LERbXNyYzSPK7Pew1K9iuFhzBiNrpPwa6xx1hJgmUyNNDQz13lqhRORBrnUIHtmqEnQI9aB8urdT71vzS1FSl5HyRYxG5RPHC9aaDdCE4A5hZqAc0eRfWtVr02ifahRqp9p39CAneldaBDvcL7uflJljlf6St1TGJL1d66UG7Xi4QoMNn17aCmCPS8pZ5fuYXzdCkRmUCQOXmbz2KOKDjlGPxqv8UNhYcNVmJ6NmLhwEQRsY7bSc4qxT7yn2Kvr3VkpeTqVMHtGBIrpc5fEHoN2a10WU0AEgWpZjOdWF3L9GBuxOkpnDuqEFpOFr81Tue6T')
	   ,(61,1,0,'upfwMao1fn','OE','BARANTIBAR','MmqrrPNwqLG','WYrVqPGC75D','hvxAkhIgiR86H','SR',523811111,'1095030083241105','2006-01-02 15:04:05','GC',50000,0.1806,-10,10,1,0,'pvSbCvGbGdQKM4oq20O1vXKyb3TlOeFU930BiEjmWQb9S450t142vkdyoCYAez0ejpdMVoskwm392XzlJ6EixlOYa1hJbBsDswvfL3pDsWL6uxqOH9yYi1HxNjyeLNx6oR2JvT39LetQIZAwkAebXCpvjBThIpeJDatPTkvpa5isMy9oFGp60dc9horyWLuBzUq67KTlaQ0VViA560Y6tlc4NBbf7mBf2ihtoV19cr5OUKNgHgEEDHeFMFAyjcGCVsOZstaWxvIYxosBEEX5Q9sFADy7NwYdLoQFtd8C4yIGGj9nzGt6zi7nPqKB3YQlyt18Iz5whGAPIjxgVAEb2OnTJIrcb1m4GMHzACvn0hckFTUjlW675NXkmXQwdndEqiknThsUZs3tuyHDpbCoLY3JjtFavucCTcbRLoJy2XW0DFaPRrc5B7To2mEpOgeQFtDoNa8tw2kQKMkPQpF')
	   ,(62,1,0,'bs86NlqrlQSk852y','OE','BARANTIOUGHT','zOUNraZPkZK66vjw','3UIkGLoc6DmqSR71','9iSc5z8xmevKI8Z','AH',174711111,'4902231462743370','2006-01-02 15:04:05','GC',50000,0.0285,-10,10,1,0,'KsvFv0W926zF558ueWDMa3vOo4NW9FPkk8cvbrQhYNShiARBMACA6xyNRXog26oge3lA2nbYueRqPpbDRaimbyeZI2lVzp7lvtk0GxWDFlNR7SsCPSrNmC58mUcrwXVDLymjy4odHo8wYBU7qcavOXRy00CiZM6VKP3gJdjxICT7Tt8FwvFXZhd92cosUgSG2gm0PrNlJdDTZo3mLZ2LOFCJAV11W5JBICQgqVS69HpGCOm1uhC0riphYb0mCrlD9uz1Ajj14xk2Mk3l7TyPSfYSbjXcoCFd2lo8dI6YrRrb69bi4BM2OtqDCjmFCuSh6hoxg5P4dllkUmSpSrIOU6lBSTNnWYlm2')
	   ,(63,1,0,'dLktpDWCNOn7','OE','BARANTIABLE','JPjfG4rgLaMroedzoe','sFZONET0ernaP9o5MJ','mzbOph3Fk4hb','PE',844611111,'3950357648923725','2006-01-02 15:04:05','GC',50000,0.4226,-10,10,1,0,'tcpiZyTwWTzL0KhkHnOdiln2h5I42bdu1DktnH421WaJmRBLqgD18o7263SqRbpXP5efW1GKLZkqzmdCnqDO72L7mqppJXJFPjRrsSU2OTYeDRDHrsNBFBEbB5NawoUzAu5u5zs9XkxKgLC41wsbxfU2lmFciLaY3kKYttIdW3OF2avgoFgUgq7vXt75WHggJzgrxedLbjvn0eSshSS4FOZPEgN3DwBS129GucGkPVXILHES63u2022qm8trfhPUIfchmaQjQuAKi2eXGnTILDga00krGmviA3axGUHz9EVW6zoaRO45qCH5mCHP8mQ2RdRxJH2Y9D53MpC9Szl0KjmbwA7lmqmqS6wkiW')
	   ,(64,1,0,'43G1ygZdmD','OE','BARANTIPRI','JC5FJArnrO5','swcmn3b4jiQQv4sW9','38hloES8ctNR5K','QZ',198611111,'9605043606628367','2006-01-02 15:04:05','GC',50000,0.3005,-10,10,1,0,'8SF8Dn2kjjD8LV9AjqZ13RepyiFviyrGPBOCLQCm3YJdAaBUcbK9rVEFL4IdRTaudXxP99sqf5SPGdZditXgMFI5fvkr8nWPYnnmQPSZOfUsX6YHstDYY4a6vnSf9bF7CbIt7m7cBfIkcVyEDYAUgDF8uQ3ewUHoP5WkpSXeTb8G0ysF8SW07WszSDu0FtvOfRCUy2BBPGxVJfwVwwBYA7dDQyY2hBZ4XcY8wujgC376U9PXfIhrQYTuWSH5IvhsKy8Z3AADLsvVJhQgPPFHAiPyBBW3Baqa2e31mfCBNTaYnFQnrYYoIkhvcmeKBDUr7mGLh2WryVBvuaqcf6z3H0tyDI9AWVaiZBnQ2iMODFBANbovP8HAkVya7wXOVCTEcw5tqksSUkkmy0FNJmXf3BPM8tAzi4MMOygPXnjpFyR8quzP60vwqfWKL8SDhDCtD74cFgiAYLBSGo')
	   ,(65,1,0,'MKDnPVdzgsyqb','OE','BARANTIPRES','Mp6Y4fTKL6lUflzPtNZ','7OHJXgY1c7G4iOk','FtE800dSfRdut5','AO',820111111,'9212151655693796','2006-01-02 15:04:05','GC',50000,0.1203,-10,10,1,0,'TCL9UD5zgITlRD2AsGQi8CazlufC7AVJyvYIdV250HFlCgY754c3uJbnTI12eyhAF5EHmSljxPqey6w0V8YHY3G5WtxifM8oWGylOlmVDKvlfSQ4Jto25xrIB82WelSSEN85B66S6CCRlwfpKNQOia9n4hkbc95zQunGz5aTK6TP7DaBMGOyRblrUj2xyJVFDPAAcxDdbuG8JoH7KcYz5739IJyjwuOEfUwcuXzgv6BzeXuRAvPIby7xLMkrNNw69b1l7Jk0K8TqRoXpvItTyIwPz3DfG2JsrpCAeMqe7rMVoPTHp6ov2lbAr5vwM3JAmqMLa0OQ1pgp0TUH1qX7IDTbmnDBjZuaSWWcbDgWQLH6tm69BZfXNxCg6amAcZaeN0MI1kzwDZYwTvHjr')
	   ,(66,1,0,'TPzeTihg','OE','BARANTIESE','YVbHSaetOxwN','AFuXy8ZVB987jwafEo01','kUMWRdymNTDHpU','JR',283411111,'9910221751217681','2006-01-02 15:04:05','GC',50000,0.3522,-10,10,1,0,'StMV727Pnh4qCYnCFXg2bm87Gmmis1clX2hJ8dXXbObCmxs1mGWgVCbD2qi80zUSxPmj7tH0Pr6yitj8snpzFBIfQnMUouNrOidl2mQHt1wnEZhZdMpkLS8qIIOE0BnGzA1YH6zl5bpMGx5TDb2sfWbCudTuroxiIDqAMzlOF2T67N28g2vh5sQ1SZUbeW9qC1HzwzmFmkUUFFI2mjqh5GHK5SNUuiNLjg31LzQrQFgRbYLJANTeOtsr4FUOwRVEClw4H2YlOp5aOb6gdQFG3e48NgGJINvLvGEOWV35yEOYN8pTl7iZRWBlvXhFhKXsmQIYBsAI')
	   ,(67,1,0,'mS0YXOPnMSTv','OE','BARANTIANTI','4tsWbiDtBURrIv82N3','5dg42NNLGPWbErkPUtpt','byoK1gt8MDuX9','ZX',087611111,'1314155122508731','2006-01-02 15:04:05','GC',50000,0.2062,-10,10,1,0,'JPHqheJ0oFJnsK4Hv8VWFdq7jHo25VGlw0HPKBuKq2QHRj1TM1YvsHkBGrIJst77SCCZVYQfHz6o8MnW1V1RfQIUROxBTYam5AP0KyOahBzB5E6EbOlM6NXM0TsKCxrN5yzOwJ6q0HKrSRHKg9SRhIpoIu9L5ZPMv5EqnHG3wkhe6lX84BKJpiAH981F7s0GJO9PblGm1t6ZoYGtVs2vXckl4idMETjBQJXF7mf7RFhnHittqfWZ4LpXxV0Fpq4cIl9Ok71xX05gkHiHnfUoBUYXr2XmuvWUR0FcM1rUWowqCh5h3soemm7raDvf9I6xPa0SvGiRYy9cJkJbSS5U3Eg9r5GESncZmaytGZJExJJBtf74DIr')
	   ,(68,1,0,'vATWeP2qB2p3l','OE','BARANTICALLY','yXQxqiFICRSEmkA8sv','rqyMhTyIFg8Vqxmoq','ci3xoN3t6Ll','HN',096311111,'3903860507048022','2006-01-02 15:04:05','GC',50000,0.269,-10,10,1,0,'2jBW1LAJMTGVfYl6qtSCtcOkp1CZVjg1mIMH1mhVjqeRlHCYt3g8SMRNCmgsoJixPQ27Lvadq77792hAMVBgDCcSLpwW3OjDzXY9IUkrNQxkEbUpVCc7aB16Rx9l8JidAZeXKxO59kMb9MKvtSL6gRo7aXADvC2gYcKSvV7U4xXURcKBDlZpr3blZZg7QJALx9glEfQHA46h5HOd2VONlMKqSg0bYNsZeuJvu1WCucU784yKLlnYUSJLUWy8rqaKHalScaapG2IUgWZCEnBfPp9d4lPXUMwBN7YrJrTtpWdOvgiSVbln3VRw8OX8cuMHRDQdYhlo1JLMpjyNt6QvEnuNfCBhWqU5NAsS3UsyGOxHNbRW8i8oPgIOm1WsZaj9X3TunflNYrMVGQILqOQfwUvqe40aX6')
	   ,(69,1,0,'F22ghmABySJlVl','OE','BARANTIATION','eAhYvAvq0vTMFE6','12Lf5fJgeycTy6','5GI73EYCdx17','XH',341111111,'3292519426418049','2006-01-02 15:04:05','GC',50000,0.0196,-10,10,1,0,'uzHspg0jMntGBZ2Pfgrsa6wglhSw7ZY02AIngwakpZ1KssklPLnfkpy2C4juRgXmC6ouGgFBcsRE7D6w2DhE6df51MGl8kV6Gb1DSya9QOjhmoVSRH65W5wJnG67l0nt2JSE4Fm7BjOUZ5sKPVAUZR6oG6XqXC22ZB85ROX3HYtPdlsfLqBuXMl0Z8Z5vjA68sJk6bOUCC14OOZmgufIL3PQ3fHpjfq3Idu6DWBwnO8dlvtKSAZAwbFD98qEMXxl4zsJZCiHPIylX7Xh9QlDcgBU2rn266aQ3u8rEZSDa2Oj7uY5iuEnVGo7rhvENhcWYZlX')
	   ,(70,1,0,'wv4qGChWzxFUT','OE','BARANTIEING','U1joy9kfeIMilJmQe','YE1g4GTEi7MJ0','TmqISVy7HOBn','OC',318311111,'0435612375458620','2006-01-02 15:04:05','GC',50000,0.1934,-10,10,1,0,'LXoUAK5CVGFfhWuQkGqAVE7F4LIghHnyEW9Srvh9BfDT82eMeKmKY8IRM1b2GAOcqgBGNLVUBTAMmVRz7PgQm59anViFHrebfUuEOz0aPeklPy3FCT2VigNnsaJS9njRTzTMnmbMAwaAAvYbnwnKQVFeJ85ev13BZ68JNEoOx8WFKLDnIkyXjngtzjSNgBeHsm9OnP0TVdc0fS9UAPDL46mSnB3mMAU0KHAZvHBaulVgkUwDwGvRpS1hX0A5hrGD4GoZxj7w8YRwGrLtGsslot6T740yhlPksQRlIGyBFTeqFhD3bO5j5ceWDw9oNuAph9HxKarxpvs')
	   ,(71,1,0,'Twfk5Zrz5jzG2E','OE','BARCALLYBAR','rhaltU62hbWVBiNY','vkL1zxwQ7FJqfw7V1vSA','Mn3H0LD1kCSN7LiHswp','VH',709811111,'8798368015114240','2006-01-02 15:04:05','GC',50000,0.1411,-10,10,1,0,'UkeeX8j38c7emwhEecAjWO0LFz8QQQFgn7UgQvTJaV7y7FcdGC1aOHxF3KedlQuau2UZpQxjiCKsXyGKxRMKPb7ZpCP69VUWDNxSy7wJUFCgPXCkOG8rOwwS1AhthIbe6NGplmAQRAaaFIYDTQkMzLCYwo1hlo53OuVjoo4iEc1WfXgbO674zNo1tw64a6J0LYkRJiRudGOw9pUvwRPyXfORX48OSWuXXAAsN4zizmLzESR3xQkQJe58aW1Zuhiv0B9wlzVpM63UoxyN4VqWysLG18S9nxbDxJCkaHVfmSQ9qzPTp3NjYtG5LLktwv4y3IOmnnlj97hc6nbj2UROXXsZwWZFeq7feKpW8Cjk5I1WNK2l1b7IOckaJCqDhM1iiy8MVqqq5HOGksizY3mbP0G9hPTlWJRIXmaQ6hYiOCb')
	   ,(72,1,0,'eumzhlrwdKa','OE','BARCALLYOUGHT','29MBztxK8B','lI2IWarX4ycSPUfYzV','nSEcwBnS2XnTr','RW',748311111,'9889486001978064','2006-01-02 15:04:05','GC',50000,0.0779,-10,10,1,0,'PxdznCYeFZDYhxqQtIyotjURpcFXaxuaxcDRXEMOjV7GczmVvjg1ZeaVWlyqdVmGhQisNZblzkIItRGOm7O9yHOxVG2iX0e05reWt4YCL5Qp1tZc3jVagCScz5cMK2mHBiQehLgK3yMjTF2l9kQxI8yJigTiY9IZnDtBqGSBQahOzP9GKqkcnAVPP35bKry9Kp1YYU1JoRNvFH7Nx2K95HpdJGqQ94uyv5C9wj0r0ZzvMTDF3oOBF8Xj2TSv4LPGEFXd68lWLPh7CnHFTWySsjulQSPUjarrM2I9aB1s16wItPUtngwYzB2zRVCUx8c3IP55pouLMjtYDOlPnZHD7XFIqtgGuhG1TtpMxu2ZWHWEbNRqur58HIPlTnBHnGscx3NhJJNTHk9pto1C6e158VU33zslU4WlG1GG5mhKWupRd79')
	   ,(73,1,0,'mMim6lDn9Y','OE','BARCALLYABLE','zplbXEumLBmSgY','YIze0ICyRXSt','KxRkXUbbpoaRvZi9tXVi','HG',998111111,'9025047296834234','2006-01-02 15:04:05','GC',50000,0.4776,-10,10,1,0,'OEOToNZuW6BkLZPklgmHYlPmVBX73bk9NRxEUHqBVAYlPMJlSfppbGgdbu0mE76kbhRiuQAmrSpNX6qg8UKgKdJTJ4LZcomV5V10yRgn5KXYxgHITicf3psX3wMdtY0QAimW5QSD6xsOkESBjIsy65BbaEV8hduDR75idy6J3qG8giW5bJK4fMBbPh41Tf87c0yXx4oOXopckHEyPtBLLjzLQU5DgnFFs2tl1kGzNM78l6KIOSxqdc7JfUAL776HUH93Mxill84BCkJ93xitJjPyMi20yBLD5aQBpzEjUQeiYA9cD1U509mnteXjdgJ4vQwodBZTXDZ2yjyBiYoI1aauY5oRiI8mbgHjVR9c5eytLz6E4p8GRdCpmk1xP6fF0JA1anYKBNN1kPMxfsF3GvyfA8qYsfz6NEk5GpQ5Qjk7VPFromFjAh4pRxeqqwTj')
	   ,(74,1,0,'yvz3gDNxxn','OE','BARCALLYPRI','MbFFPif0XyzlepL5YOe9','d0UKyTZFl1I7fBpbUjK','wqqyYDYQAEVTqiLt','EH',023611111,'5338789453001193','2006-01-02 15:04:05','GC',50000,0.409,-10,10,1,0,'xkAdgogM6lYOfAAWjxloEqW5Jq5VRsxYhr1LZeSUhyYRHDNutyCEtKSY51fOQ1ylLa5UFBlPvUnnfXGYShsoMttB5UBPbrtTxIlqRPc7FmQavFaneYLnXCwhqY5AXiUfYNCXBXJ7IPYOLbUIkvQmLU7f7qWq9ll6Bzafk41GuYYDmBvW3Cpp9pLSN8XqAPbesCLPCdWRqtWCQzgLqXLoM73lMwlJDt24yWyijVFFQmEVv4MKKCcwNwUctxfw57EVDfeZRwccaNUOxjjAjbE2PQvElM6TglM6qApDzvBtsn31T2m8xU0n12fNtkioqDxH2d4yv4710fY7gia8iByccfKYc73enBniqNh8OZSYJOAEY1PrUZz5lRN')
	   ,(75,1,0,'YqBjxMKg','OE','BARCALLYPRES','E8A22l2crSpm','iF5AQtpYHpuKbvSJPV','CmwvC5gYsiJIKe','XN',949311111,'8824959943713584','2006-01-02 15:04:05','GC',50000,0.4894,-10,10,1,0,'sfIBhIsLBwVfF9xlVPrap9DDH4qVjjlJesGwFm7wiL82rJ4ySyKHaKptSXtAPaoZD8SAKaIGo7Ul6OjzBlQ8pp7WDhpFZzqTnbffHOgRvFB0M8CVHJLPsFHMAhDotBlVLXovNdcCMt82HksWwyyUJb4wgIFDqjPE01I6FMnxFBZKnBUreDxgWlNXIIzn73BSL8DX3VR57I4WGiNWa7Q7IOv1hxP4hF6irCINX1EsXcAfFPWuOIEux2r7NHJuKKYADzFsiZdAp2e1gzePmlDvHZMhn5jk3f46ZHr1b8FNXofZFdmfZ1IFHOfyWiOgVgdlkEZV0vvgV9dsOqQ1WrnjeJfZ0XKnnRtyQmMwvt2kHeu3BRJ3gxKGwzGCCTMIThAj9as6I3dffCBfKeCCjOkhqiXDHPjMmInsAeaT4SAcxKykjMN2e4HNhfHEVS6n6MSaC6ue80g0FFWpebFV3SkUrqsI7Z5mQD8NZW2WdHKB')
	   ,(76,1,0,'AR9Qs8chHUfa8U','OE','BARCALLYESE','50f5nCu0ccsid','bVcBTbmY6mUZN','SxjYRZEP6J7RE5TFZ7g','LL',106811111,'5218944881863257','2006-01-02 15:04:05','GC',50000,0.1668,-10,10,1,0,'4g8gdiTDRzdo7RxXRhgOJsWL7VMgpdO86uRsQxlHlU3gkK1eb9m2tYXwbY1D0KqOjLTdTtVOdyL4gKEhGHHtGVlW4y2gfW45z1fSiYN0As1dpt8NCgcq0ZF4iNdnHAuZ3k0esmV29nQFkwyoHcJKKEKHg8TgPnVfGfbYXrS612RAw7qramHAW74QaX6PYOksWJdbwswAmUCkFNDFYuiXCUZPKkpyLD3z4PGSzsMuQPCuKtPK1wyAvZOeEOHO7tW2LXbvCe1Aqqs6d6qK1yyv8tcubpUOXUfZnmxpBKQ0DaJRGuSYjxOpitCzAt3VjeENLrXmYYBIKhLuFmPdDQeoQYQGWSEv0kWPKtqEOZ38V5PDvUNQm')
	   ,(77,1,0,'hsnLMX9DdS','OE','BARCALLYANTI','ROZeBxIYad7','EE3hrymARtDYP','KKTALJEuo16fCOKN8','YA',932711111,'1659837293114926','2006-01-02 15:04:05','GC',50000,0.0497,-10,10,1,0,'a6yhm8pow7erADnkrmkg3Vhjf8yXnL5BgzeUJTvjuAsxfmThdyX0PZIjJZiYI5DuShC95r7G6wEuooW9hFeuxZVsotlhGpnL2QHzetXxA0VYLQrJcYa6ZUDRrDpMgqAqbekcfABa7UszTlTbi4wWAbqV9FWoCEJjAVguAqv4Y5Bav8BWR8ZVBBYTxb3G8RH922REETXuOeLOJLRKc5UktkbZV5EciTB95EyNnATLjBYagRa2TjAZOACaYNgfaadTWxseHIkEzbTm4kSSwp4oMed2qHDDKnwxjwDx1TTsZLgUjBj79R9JueXs2zgFjlQYjZldPIPyZ0mfcY5P3h4z1KpUBKigDEav0g3vQYaDVzYWJbeITOjpnR27KZJnR9I8gVnrIhzghMdGsOfXPCA8Qh4eGLPLlKVEaBtyN1sFc3JPEbu6IUQzxUpLOBQjfliOk5zACA6XtfFnpVZ5Ng')
	   ,(78,1,0,'52hfmAnQhll8v','OE','BARCALLYCALLY','QHL2ThAfzPSk','fdgllrSnzdCz9VVr','hoYpRJjuPq9YSPkOR5OE','SW',150411111,'0950180568950141','2006-01-02 15:04:05','GC',50000,0.3525,-10,10,1,0,'S7yUdBYpacrranmPpEO93ckMh8butjGmla5v0zSUX2hL3Q7SsQi4LroeudXJOEZJKQOsGilSgW6fzhmcwDAi7WzQ9IYkv3kOzAgMu2S1gfp77qyJzV0V5hzsoLOYya5319H3aeWPnxhEWlaf6QMlMdiTRjfQIyprYxTjBj1oi6gkoGPMCQbZ7Vgbxu3eZFY8Z1nt1SagJ1NRLmt1ksrL1jzJBNMasDCV5nI9p89nREsHuGW6EoObEwwgnCCalHdpZIeaN23gZmDUyXVGrh6smzeaQRNzSZmswnvxht3WaK6Y8xoHmnTPQrDqr6wGKK4X10yFiCDZhx5dgg6xW5cWN0FbBXeTPFZwCFyz15vSUUZQHCws')
	   ,(79,1,0,'nnAG4yzUEf7ht','OE','BARCALLYATION','xszZ3WEn4BwdwHfvxYr','01zCDl114L8Jh8n','YTtvzIfODhJ5qw','DX',813911111,'5435494970003800','2006-01-02 15:04:05','GC',50000,0.3704,-10,10,1,0,'yzODe0FKulTihL6ZtPKk1W2CoZuHHVsRooB9MI7b1Q2OvAMKsJKZTcOtirhDvq9tFPyzlkBaFAnuzC7Iu3XVBEQCn9xyKtS5eP2iV6cMFDK5HKvk4Le4m6mw3BJtTiyLcq6sYUtYSGl4kQDh2oWGQOuiPdnzpDdRef71ld2eCpC9EDNUegZeMGsQzmMioQoFwvW2ZKhGlbgh2YrPb9HkeOc0fYOkIEmypo8eMsaIgkglAp4oRBFOU7vu7NPLDFAKC8Cj1zSqtuWPQ6yVcXxJv36bBtqFYt7cug2KnVCDMZ2Crm5wyWKRoz4QlrXR2CuQTHeoCjQOjdboESLZwvikwbaMsugmy9tKq6ceLLU9bbgX6c7v4vis4QjKJUyIgmcMQnhZXQjYwxX7z8IZLoW0IOsyXvyca9463ncHd6TqB3DpA5kPgX5hhFokAIGuUFbJI0EcLWUbweCezDMK064OPOJD0sCI89klQhfA')
	   ,(80,1,0,'JdUQVjk9r','OE','BARCALLYEING','h4TVzcYz65dB5i0R3o','37IaJIwhI82q3','xfUKmRggkgo1IvSWx','UV',996011111,'8343721827360481','2006-01-02 15:04:05','GC',50000,0.259,-10,10,1,0,'GiYOV9a32jMFTmDe5obm8u5FXdkQRgtzyU0M80mbYkk1JtYcDlVuWNp2e42Ac8WwHMIlSjJutTYYP0pAxS8SVtlpcYZC9pdEYhlxpdC9N9jaNXW0NXE7GmXQ8DaSx8mWodJhYpu74QdjiUO0Cd4gQ5jL4O6VhKD6xlkWfPY6oSy9CqalmKnzeaABMUc2amg8g0LoOwucT4YohoOw3I3I4eXPtCxppxUjDtnlwMnAo5vRQ2hkq6bhlwRsHzg6QV0lJQiiOSaYrsfi2VY4L5xQ3CxEELf6XOXgNvUCXysvOMC260AecfQwxaWIKhbe2aMdGpCSvhRfAsPr5PWLZWZAHg4kaGgRE1odfokrzFYnDC9GefteQt1WlLwKh98iK6i1jaUVEXf1VIcVQx7sJheECPKBpVvmUPDbHGq7SufoAQtfHD7oZWgTLO2L5WuQvoJynlIeu7oA7QPWfzdHlE4')
	   ,(81,1,0,'Thnk1xbzHW','OE','BARATIONBAR','PKd3fAeqS3ph','JBml1XZUwIfe2G4lQjIT','3lr1CXTdoN7Az','ZD',821611111,'7157404299688226','2006-01-02 15:04:05','GC',50000,0.358,-10,10,1,0,'puBieDw95OLnauBUxepYk8bhEq4L0eixf0DXRpyzUQCF6CmtA3GLu6PerULAIyG8VrK01363grhAgMsaKrOhOKyFRZO6VUxMjTqWHCz7Jc5vGySNjTK8dSBTE4ftWLaa3kbJ9k8f1TDp0n3KgcqgrtL6UwbivUbNFHeU8CfnnjPgfrfMj28waRsjpLSLQamQb3laFVbHnmIRSUob63OLwSSDXvvM3DJrUVDY0jjZwi5L7mPrfHW4mXbvDQ56rUQWtLu6DfhhGxkb6AjA9RROvBnDCm3XJLF37qA5Fbdkyxs9UwUB4EdwEAnI7yZIhMC9LS9OCgSJQmhrtbWaBuvFXCPcGC0HmF4MPmUGdL0AFVDbqWE7kqUuOGdBCWFLOLeYdBw22SCcHqSGY2FiaxrTcZxajf3VMFcgdH0VlxKQMdkVu6hMVBEmt2J7wm5BK2u4wQ3SbyOJRvQlcvJN4UlwSinddUACxb1DAMlRj1O')
	   ,(82,1,0,'ICcmHukkF','OE','BARATIONOUGHT','ihnTHbHR044yfoTkJ9','2JE9KPXb255','9wv03qWhzchvExgZ5Z','VU',449711111,'9248893756512833','2006-01-02 15:04:05','GC',50000,0.1022,-10,10,1,0,'pLWHk6SRt3152Qcn7ZmlHscXilUI2joDNbP0v2Dz9hAIPfP93NpoQ304DeTDRzuDfzhK4LeAIa6Xh3pzc14ERJcRVHWUUgRr3uYoAIwfL7QDqrD9v3DMyQPNwI1qDp77DPGptbn0NRXg8pFyYtFEL7O7yCghCXOAWauzW5ujEQodVG8APqL6GPQB0UWyrsPWVojYPonbJnz5RPLUo8EH8jglroUejlHoP0P2WejBfxEgAsWgVwvzqSApFMxMT742hej1uyNUAn8GUWjmO411c6Y6FumrLVJr9NKGU2mD4qsr40lh4UzORcEYZkL2VF1nIwFmFeXD4Gnf29vdXX1ae89G1VI3k6BvbFZClNmDVEfA53w9qOtoITekHp9l7oXOCnFiZgmSmv8w08q')
	   ,(83,1,0,'9LOGeuZFQ','OE','BARATIONABLE','n4CYj93gG58WGMZD','xkrBsrjvXdMu82Pyg','pK4QwNCFA3WH','PG',876411111,'0679272635441813','2006-01-02 15:04:05','GC',50000,0.2692,-10,10,1,0,'2YoYZiH4XBzZwIHmNIM8AZKsyH12TkhKZT0AyIfhFPll0ewzntvzEV7kV8bBWdda0fKkEUDu5PqjBwrYFDOQlHCUGcwrcqAviEBJZyQOJmkTlwhQ5A9pw2NcifPhCJOKt94VPrxOQAGmhvlmOwVlBtMGa5Kgu097OMe0jbs7VNqKnNrRBj0Z0OV4XMTryuKYIFqIC3YzCpfhZ202uXe6MIbzJmPdBjUeFH4jhnqOv5NIKJAwsSthgVyLY4LwxENlih3zicpNme9M75EfdCU3UoIMapnbojI3MAB5h5GWuhbVpxBQFtzO2vsZDW8Y7TsGn4IIyc7vcAMLkb0FG5nOwT0owd2Wxfw7Np')
	   ,(84,1,0,'ir4718tjxWKUTynZ','OE','BARATIONPRI','u9crsicIqBqGpWMa','eIH13ZseCFAv4QOiMI7r','gEd7TSwm6PtJ','FR',116411111,'3677489328869900','2006-01-02 15:04:05','GC',50000,0.2618,-10,10,1,0,'V27oWs3l90NLfvkiAZKBndtOxpMkECZuYuEzpwwafYBecaQWdtcr7uMBnXqjBmcW4DgZG1tev7V11HdCn1MkmpLTw2gFVV5XKK7yGwwdanwyCDGBPlxCdndWkiBTpvidVt9WYGM4Sk4n9WLNewzb8ericej9SXWI1tUTcU3LVNW2ou4p9Dnn9NQKqCOXcuCwyDTYLy4BDSBCgg6PMKZMpv86XFGpM1ufBI9EwBiAFnkAJBT2CmvdyDhde6gj60OKZ301C8DMTd6orE5z6k1mKM44rbMTbO8SBxRweEB7sVWXr1Lb7jdPYI94b0yIx09MyNvcy7KInYe1weCSodD6XkLn19Qdnkh3bRfjMcdblL6nvMTTgwN2bcwULm4yYKlL6ldHHpCRffU0uYUFwELg4H5lPLUnIDxXTkoYjat3vhbT3Lq1MteLCrrEL01Wfy2V1w6gs2iC6yBAazTj3S2vO7OQ6oNpwZwz')
	   ,(85,1,0,'gwbrqq9mh','OE','BARATIONPRES','AM1ZLuVL1dyuXHe','VMXQDRUhmQuc3apq','RIwtodZQV4E','HO',723711111,'8867096275543945','2006-01-02 15:04:05','GC',50000,0.3503,-10,10,1,0,'jUrx0nf02H51stXvStv0uhf5M43fTLRulRq3ZwfCuS6kOdclJjqolOtIkDJTyxqGPKisn5OzpWTzOj5IcKV3nydBIbp4yExo39Ca1jRPEtolkxKO0Csh5kLniAzITM5KMuszbTROSzKUDUyYAGPI6ZsqP9xtnKXH8oY8tvLrLuhQTiBFB2CP3oXvZWAKc9ABWrFeAqAttfi15lkPjucWuQOi9HyspP3TkR4QT1oLCGVqI9e81bbNlJlxmvAqcSoEUQUkWgqA96vErvjH0goLWhLImb7DrclnIcxSAAliXTdXdwjHljITXs7WDo7lMHD6wzwY1dHKKXmIwvf')
	   ,(86,1,0,'w5nXMY7j0','OE','BARATIONESE','0qqi2PfpHzm','eB5HM3uhV1nbZ5A','3BGkIoMfWQAw','BK',337611111,'5515140285646454','2006-01-02 15:04:05','GC',50000,0.3161,-10,10,1,0,'7umECldyRJrH3CHwk4ylRSaWtnWaRwtsCECMV9d6ukVAD5J99bxTiJ0fPcSjVytXbzkbV7l4EcMlohZtBOeyTgdXjH3ti056VOpi6NRxNI1nfKXJXgzff91cUCbxnLedVefjT2TQdPiCxfr7SgdQN6skHyVBbib8JDMSDTp7mDQWYXNmRRDQudrYFkCF7QCnlPBWnAkR4z1MPEd11tbM2kbcIPFe17tarmKPqBggYj01bKvRWMoWM67OFTHaE4TF468vgEyaPVeD9P8cpdpSHc55KdMTf4QATOoKdQi9KD3zD0IECXdfJnoiYFW7I4shRJ79uD3wgxTw2k0dqWW0AsMUWtu31nDgHcCJv84bfDji3ChLN5dV')
	   ,(87,1,0,'gun2wld1avJT','OE','BARATIONANTI','Y3uD3Da3sRqT388pS','c5FaUrrA3CIjv7','ZuW84xLw5I4pDHHHHCU','MM',075011111,'0611467989805008','2006-01-02 15:04:05','GC',50000,0.1204,-10,10,1,0,'6CHX9TrPVBb0zWPAFg1Cpk5GTD0YN1H0feLvfYxaY4hfWupFGrtdkgarVCJpgLsfs3m0NoBzMVsK0K9p6rGmgyKsBrVqPi9zqteTaApP73bkUZaihQd1ZV0HszsZkguy3YGa1ob6VWkgKKCV5yDpoiCOrreYlsyHN94lksuRnmZEZEmUHgfEYRLPAF7pZgKY8x7BNxZAh6lkLR9eSsg70OE4wFM6Gwnk5noSN24g7FUPTuZBN4TNBkmXvA08SMSQ09LC97pcH3KaKEE5W9GQFYA8RzKBwDcKgJhHfiBcdfnBZU3FJIOsXZdumrI32H9zknV9wmvYV46emXbEr6t8newjsaM3qEOYdoqgcpxNZhXYeLA9QC2R9CHtEg1ZjtkP5R61CVPLd05tud1YpYU1hsL0LG2LeoHD')
	   ,(88,1,0,'LjWrcxns','OE','BARATIONCALLY','V5e9YrsmEoYzt','Oeo8ZdOlujkda4U','5p4WTyPEEs4V','WC',809611111,'9697205942447975','2006-01-02 15:04:05','GC',50000,0.0571,-10,10,1,0,'T1tP5NYeJU3lzP3HkByeF8ClloH0nXjaINcpRCzsrzgFIV4P2mw1Tlo8HQ59BRZZm6IUAF5wy5y5BQIuwLi0eem1pK5D75KX0T1LFgTxv0VyxsWEdX66XQqPDSgLr00N54h34OGo7VffZ4yPMJFaqY8GwAwmJjNixWO7lVe6DayeMpb4KhgQAWxcIRmV5f1mcBCSGjQHxHEP9JH33Dpgtan6CBvkrZXc68jtmk85bHlhURzahfpE9EgfVa07NKogAGKyWTslY5GETDcWipEsIfXewo6eb2CvR37EiaTjUZNb6BKpIuIAmigXiPH0PQwIL46P5Ok2BzdAJunmdxXV8ihTnEY3Ogy3jXp0QSFwdb7vA2KnF')
	   ,(89,1,0,'Yn9dPKvEO8v4p','OE','BARATIONATION','8J4BCmcdzz','jRtDPE8Q0AUOO7Z','soTgFficPJ','HY',879311111,'4702751091866107','2006-01-02 15:04:05','BC',50000,0.1583,-10,10,1,0,'AW3dz09x8AyP9mvUttZZEO0Kl749xJAWNisZiQ799NDW5zae7hmLZ2mbLSKNJlacaDY5ucmZKzRTKEVnPCM4GG05EHljfZwEveT5EUDdsKBOHKhoWrzuerLrcTbCrHqXntMoZdQf3ibslbNmOhpNnyR80AgMfRpce5ducSzTkQCfQoFFMIrH1V6j5RJJbhgJ1iYI6ypNovdPZazYaSBvYqfOzRT9vYk0GFwonyk3UjHb2oZYGfSLdkh2xZsChFGtna45yC6BFyocMjJUhDJW9kOmaBiEooltynk8PxtFZgP9ekCWzoSW1wF2CsbX8EJMnt4YIbKq7fT8Qkzb4KK3KTZKZif4DCJKyP4eKaQOCZfQWdw4voG70VSO7VbUYWy6qSoILpCHlernI0mOVsx2RXI')
	   ,(90,1,0,'Mj0oWMZscO','OE','BARATIONEING','0aiahYsLeW','pXkiGLJVlVH6yiSd81','zm08TOrNEmK6','WY',282711111,'1240166140830735','2006-01-02 15:04:05','GC',50000,0.1365,-10,10,1,0,'5oDYgUx6jwmH2k9kGrM9Lw4GtmPN27LT4Nmy2mVIwhGROKQJy8fgUTRJFA9gzhNxM1pe1F9y6EWcSiNPYciDdDfuxk25uYZF5fM98MzQ5JVrgYPqjPGltFnNZYYUN91HgXamWbdYn24lEwRKL1KtXAnnY9aNbMN4URsxF5kXZuaAtczYqepUP2wIanfImQ54Kik1wRBa9pp55ks85KGs8xyApEuEdKqeKeTmlA8q71ppLDDJoWytgWRZQS9yNqxLjyCANkKqPr2KoZTOO5OIAkzM7lrn131w4P9ccsIy05S6lPPE4VtV81uZgnosnJ96nNqiTKSewEjyAlxQ0cG9bmuiyz9hr7GYHHjPE3S4j5pIGv07vr66yeh50hJMjDoJQOeTo5MXw6rvGMjETfogQaqogMfdikU9dy5qqZJQ9lmRA10gVl')
	   ,(91,1,0,'kCyRzcO3pZ','OE','BAREINGBAR','8dmh0Rt0UK4XOyVzREA','oTZk0Lw6g8','j9MvzM7UzVuKQYBcw0Q','ND',888511111,'0287145305737379','2006-01-02 15:04:05','GC',50000,0.3343,-10,10,1,0,'Aatnvc4wvaJz16kf9uEwmXZNVQ7jD8I1RwxTBBMIOm2zl9MwZlihiMzhlyV7HUYE4KQNkNEpL7pRzdxZReTTx3q5DZNTyfBFI6ZsnebeiFVaww0fWIeEff2IHQVikKEcEWom1dNHTTXA4BfmtcPCj8Uv8kcMNxjR2OVmUXGbk38Rvk71XgHFpw4mwk65IhrrirTR4gmMHyyC58ZIwOzNpqEECM9oVMw1CHyCMeUWkNnak6lTtoSzc7fP05sOhU0OiM1Ej987ukED1mUhJ9l0ipNaFjvr214Au1oMPiQzkcOuG4mcilz99Ec')
	   ,(92,1,0,'Zvqwz6CKdW','OE','BAREINGOUGHT','LQ2lGCFGOwA','Apo6bTW1RcJ','sPSPOrrJAcG','ST',934211111,'1669356053314940','2006-01-02 15:04:05','GC',50000,0.4271,-10,10,1,0,'3XRRUjK8MZrLJA8RPoXaVjTqoV3Dxj9YJtRA1Y18W17jm8WTHN94PQGtdiZvYxosZewL3DkPjfrJ0bDhz1Vyk7CUZPqKtfQsSYSk0UU9JQJr8CaS76GNHmLgjRy4dKZ2x7aMIyjQrNqdTcZiara2QFg0kbt2YDJUM4ov6yXRbmT2kC1znuzYRVLUaYW8ySrlXR8LAu9lJB0WtAQHg9aZ9sDVs9i7hqHY1GUDH84ffZrAxtATQ8jMBxpQpZEiRSQZR0hGmhTgJ33IP0lgld6ZYxIRg0LojupCJp6yizL81lbH1AEVFMdyYTeSzJDNFryprDDofURJzH4YSkxtexj1R6sQnX1xRKDFLld7mnH9EHCOEUl3JUDjs8Ed7SRk3vMPJJRRwsRMqaMfolGOPCBEZ3qwdzIkM2ugrpaQUUQRHGAUUTqhgqrZKm4siMl6NUWP3VQ3ATEy5N0Vhs5bF0xwNvyLiCUN2eyuPRZC76qzfAAM')
	   ,(93,1,0,'9NwXRUKWL4','OE','BAREINGABLE','pkGpmk2cHlVrJcO1pD','OK6Q64xdEOkx','ymg6d4LOoZFyDpSlIE','FG',779111111,'6013822974289013','2006-01-02 15:04:05','GC',50000,0.0285,-10,10,1,0,'LYYnnqzXoVEra8bnjvQRvKIe6xBGpfWc49p44FDzAWNqiSxkXd7BWTu9XYAHTkqjs8g4o5124fMektIDxv1MVSeuADDDtQbds6aGxzt0Gkf7R6aOkKlFGwVhRXya06ZyrIsjA7R2iyNDTKxxldb1LdtGav5ks6FM7TXaNnxGBOPkzLOIF8c6yAxqnfueKHKiLjxBLeshv8eLyjBt3H2tcDLm1F7UQy4CWwBXRZvjGHJK9KObWW2mFRuDMvxrJqLt0Xr8pr4rgQqnYhHxt0dCAaUFE8zr70dIxqfWooBhU4GFQsbJZpF684fRGUbfZwNWskP2r6S3YotmcunN80cZsYvCxjD3uLUVGfdAcrM3cLYVGP13SmoQUlqQOhNIG2752wij0dXAJA7rJUz7PnawfiO2UCarasqcTEULgLPxTm3JZUv0BX8hdcUGir7TzMJvrpOSA3Zfb6wCl2YyTWaQJlpoUgTTChYuSMbnmPAxuhz7pQVngiVb')
	   ,(94,1,0,'H8aI52oPxh9Scqs','OE','BAREINGPRI','g6p5ubNw9v2TxjFFz','HtYgptqtN30QrQDEk','1gmXvhabyCiNyHa','BS',847811111,'8531101734046531','2006-01-02 15:04:05','GC',50000,0.2502,-10,10,1,0,'kteEiOgPswBLVlZATmndQLJnZzCZwX2hmUyaLvFnBfb2rSRY9fhklKODsvUr2bT7WV32rbLma8QzY2P4Vgpo49F8xEiIGTLyIhDuj6g1kOmn2VN6AtvHv0qQllLOyqS6CTifkaF4z6tx9oe5c6BLpdwKvHCsYC0vRANqDmBRnrPZTsQ64jyoqO0LPTrB4l3c9LzPNmtaK94GlCKCpl7nWMVvL4knPLhYCmch34s9fXGCepiECXgsCW5fKwSOW52sXF04x07G6qccG4VnAfJuwzTU7GiGihEWEmNnXmdEKgvC13kg')
	   ,(95,1,0,'0LBQdyS8Vr12F6','OE','BAREINGPRES','2abp3hxCwkg3Mu4LObWI','XYDla9MAXP4JzbKvvPz','YSrrqPz9hU6b7QN6kZ','JP',160711111,'8383157797837149','2006-01-02 15:04:05','GC',50000,0.2294,-10,10,1,0,'1II6ZPDbU54r6w9IphYYJMITJ1jEftHn724OIUeylRSB40lB9WWoIzg5boCgRQasCRoovaiStKkuB4VhA9wZfvtsb8tTYPwRIonzy0bllrvonBQpXAWB9CLxb21HscV48sn1Y54w92UEIJLeTV8bIaTAVAUxWiEuPqujvOrjV93mFPgM00RZjYfOxwXYzG7GxAyeYlqX3sUCnOikAOZgk3WWkA1G3fMeLiGjc2aiFjyCzV8QeBnyVOVCIRDA3ppmeyD6Rc0KBso1Y3z8jCUI4Eeq3AWMwlL1GUcjsQjeCimRKHMOhdResS86kYKiToHGzpD6AjbFjRi4ZXUkq6KMiOwe7xEkp1E9lXJSuIOiSbsbiz71304yPcZVD1nnwLP84WbAiq2FH33XhSSxgqrXXRAfZGs0PEVvRDhJ3Rozb3nkHYqvn7py51gvz3At8J6Iw3dpl2CZHmOLrwxuEt5ruYlXQOGlHV9FIiw')
	   ,(96,1,0,'qx44OmsTDqyqOM','OE','BAREINGESE','qyr2aQpLAwuzxhY7tH7K','eMXp3oDdkT4','3hZmUkXE1O7h','SG',154111111,'8128330343303181','2006-01-02 15:04:05','GC',50000,0.4601,-10,10,1,0,'QXJQPsFOdTv9BI0LMMSnUTqGcJVQgJKVpTiBF8KQY2ypLOg8YMyHmyTe8Lit3pl93EwtPL4RVMBiTAaAjrenFaORO0HXpk1R80HCi9iesFKV4zTN81S3zMludsm3m8owtKb14EWHytLx7bsLTfLUOYS9hubQXnEPwU8ObquKRiSlmlkMAieTh1QoDh4UuQirMa7BlW5OJW7UFFbY41VoE8swe88Sxmt7f1atkxL4zoHdGNai6mvzK9dK7dyfEMKfFm6VWXVrR0zpxo3KTjlvLwu83zF8T4hRfbC7A6WO88rYNQ029fGaZDgbzxT1r1HFTz0Jgc4Chf3ZC6SujKc55sS5xZ3kgNJX0sarSby2jKzPZUtIYZYvkqocN5Nni7uegieIwyW94ULTHuQoOvSzUTwUBV9OmPCiX8BlzLQD9HCsHqUwIk1NuyvL3D6yMp9P')
	   ,(97,1,0,'WHECFjvc0K7RQaEV','OE','BAREINGANTI','TPH8138RW1','44okYshctlyujXkkSEJ','ixRE14WQHwp','VU',397911111,'2178441101288753','2006-01-02 15:04:05','GC',50000,0.2551,-10,10,1,0,'jcy5WnWGnpGYsHIQ0SCTWRb5rjju1ptk575yYni7D1U0WIwMWKdrMfLq7FnIyJSSqmRXFz8m4qzhToQkA8TMQXtTepkxQDTb8FAB5oGnyGGq5rssV8b5bkn2o5gRENq6eCSIadE0TqJQ0MwaOBgw35SclgMx2i5mctuDEvSAy7StgK7LBBOeDpXWt25GLrliQWIzF4mIje9JELBnCeuiDuNVhBL916Cf10ADy95aM1emEsiM0fI0D1UFx5QTRXZ6esbsYy8tILdnkngj7WLkYGTqFbhvHHcF0wVxOEclllo8ukzPUWvi43UW6LT5tmhvPvEeNPIn10ayEhuEBI4RnVXmeD9Hrf1dF')
	   ,(98,1,0,'jfNFl1Vs','OE','BAREINGCALLY','YM3MVKDwRQ2kmn','KW5fVzUvwN2BdySLF','cTqXEIXyXGyPbAjT4HP','DM',523411111,'3376497467537232','2006-01-02 15:04:05','GC',50000,0.1202,-10,10,1,0,'S7UHgCVmYEfJRQxUgy8cKkLH4MOrbnBGpxRrr4wjImDfhfFtnnmGybtcWs677QGiU8xmdiMH8K6b62C0CdYfGnssAlLcqztP9HfYV54NhVOH1LxHhznsjZMcWjSdECNn5PU4eQZ4FLGaOnhccFo1NAIl2ZqiRsf3QepWs3kDH1BQ48p9zJrqdZUZT6eKZgHWs8egyhUUJQ7sJZLs05loijHZuAb8CmiwILq9fxS9DFsenwazMfhiyyd8MDgCREWdJUuTufiUC9rvhuuF6pmOXyTEiZygR6HKMlgAPYeOaLjOuDmiVWeQ5M39KOHw8XgPxYLcNslvufTxqNe5eCqDX0Td9LPyrRMU2wHAuPw7c8Lh6C7CR7gHylftQB5f2DuC9TvcnK6DalNgczAoc93pmmqX7dLa1yRD7Tp5OAwKB3gfHOKNDUryazcfpg0qnMlVaG5YWdsRjEtRd2sR7')
	   ,(99,1,0,'xpYXCE4X1k','OE','BAREINGATION','JE3TXEzpDlkZD','K9z0vH0Vfx','kFxmDZ9Gc2KLyn','JH',645111111,'3781592849387929','2006-01-02 15:04:05','GC',50000,0.1187,-10,10,1,0,'8DFRxZ9H3kipDma9g77soRVBxf2gJxHA1Z1kl6VIzpMDuISx7iQMBO7mDnRlU9KFxeWYZ9v4IkGHdjvcMBRjXxfJ5nnGRoMsv7lYynwzUuNEjF0jvVGQjqRbEKRU7fJVIGllzl7vXTlx3dE1rwjGTWTh5RXVb3wCYsHjGJbs2chY3NZZIktioYbMzgd23IWhRdkxRy0bfQseb4BeprM0hHy48yuby9k7NYebJsqLq2x07cdBc67LhZPl6akCv0zrM87GZ7soe6nIqm3YJoBy3M7gdREGpzMlfBDFJmUhkwIwyLI5AoC5stsjMx1dJWomj8Sy6l90slNQCgf4X20bbCVTMdqv5iYoKmd4omb4NGm')
	   ,(100,1,0,'tJdFES91dg9sR3q','OE','BAREINGEING','FDVqahuqm49k','5WDuvl1DKMA304','2QVaosmCclE','BP',144511111,'3125394640256110','2006-01-02 15:04:05','GC',50000,0.0988,-10,10,1,0,'Q7Drh0haCoaMI1hoE0KjI6fjq5Jjr1LuNaUYTSotnbkZqFYmhEDXFrpZt9sPOteE8knQLY7CG4JSCNkNWddpHt4CzmmeaHxethuiydgklvbmJp1s948IJcW9Dssi8tZ5G3yFyBfbFOFDJjOrNW2z1SSGscFjPlGH0BebwsJxZ9aL3SrSF4f4kFEpK8Ny7ENTklUx4tM23KLFo5knYR3A47LI8kwul30PTcXTnKsyKVWFdJxS3tr6Lhm0ZiQ7KPtz86zjTJdyPKBQGcN1dQfXaOOookpElnKbtcUjPTfEtkqw3BnDwg1ak4n8e5icGbrZfD5secyWwupiQTg6gU9WZ66ipHG9XFCbmIkUsSOI2ylpVrKz9VjIz9boQIp7J2qpqhXLxNeurT5G8ssV7KsBBubnYi1yESJ8obbBuXoHDIpKc1RXR1pNpYQz8WccrvpGfhekuM9czt')
	   ,(101,1,0,'SDY0vRdiMto5jiSl','OE','OUGHTBARBAR','e6Saa5avKvGT7jNEZ','BTY6KJkKr1uFyHu','r1RI1fMaAj2at','FZ',347311111,'4364289038725247','2006-01-02 15:04:05','GC',50000,0.0267,-10,10,1,0,'Yh73BgiAse9etNnIKqtGxqdtlGmmQPqrcwvqfgrI1oTM62quHeGOYsEOsxrX2J0gT55xxpicZ3R8aWSROxZ4ByIAfwJoxRJ3pOqmn1yDCvevRR6s2NBvrJxGXaUp5jp3pwVFeyZsaPvw0rpISmnNSNLpgA6b3a9LQw71V87JriNozdQNnfLAvPBahGxv3TnCIQuRHNLptEuHEZATSegMGmdXYauF4XXq2iFAnZ7vhYayi7ITtKZJyWo8WeHLlz31mmpxEcoVMFNduk5tqxN5cXzstrURffvyj3P3qhVf2IpfG7I3zntlxV')
	   ,(102,1,0,'ljNPIv994L','OE','OUGHTBAROUGHT','joq8bO4luB5x','UyjjFLUrqy308S5vyU3','dRUz2rX7toRRTCKu','JO',019711111,'1061742120119477','2006-01-02 15:04:05','GC',50000,0.3055,-10,10,1,0,'934cYlDbbLxNVDQG3kaHrxUda43wGQX4U0IyHPINz03DBGv2vlPbpqA3jqkztypu5jlNXJ2j1xcg83AGJFqVE6KHjVXeNEVavgBbcGGDmq2oRvvgGkf4tsKp4BI6yLd1rpHEjAexFmmdYHnLemjq4K2xYmB5gZTBE9P5JUiFnUEtxUXdUeWNCANAavdEQMhulFoIwyF23V9uB4HD21qjgPqIqOccvpBjMipFMSonxf045eCCLKDScdTNAz3Rfkse8d7IbZ4kejj9yIRUy90N6QG1qjLlDDyOtIZO4frmBGu7Rg3lIr1XaY3VCVDvYD2aJoEFOQJRAGW9xeMjBkby0Jf7INNcTrmAYWc08SIsiVa4CIDOarQfx8wpygXguuHSkkYq')
	   ,(103,1,0,'S76uCOBW55JCPei','OE','OUGHTBARABLE','iM9fzFGgeQBE6Z','8kxI20o6AuKnsnHaeius','gLfyi59tWgms0anoq','OE',653211111,'0817692287901861','2006-01-02 15:04:05','GC',50000,0.2515,-10,10,1,0,'FGTJtuP6tse7Gp6HzvrJFujiYBhh9yjA36umexCpe2QZZy7bXhC7iAciW3J4gPT9Rbe1cz0B3KwAvvn5nxUos2j4knCcmxo8IqIi8fvL6b8IONHmXWaComgIb3s4KavlYHCKgF4ozDxVfIXsrxQVGkbfFHqCnLGNoV6IUprKoKOj25YmdExX1kdSTaQeZbSgDN0L700lxy8NLkSnAU4Dtjv5NQRBPROtnbGItIiuX6AFMfdnfxXH1AUhPzKbEDpGFZZ8ohbithaMcaVabfvfxDTKbibFzD61ghCVUpXAGInl3bkV4qHVnAz74R70buKEvmKrX2yuIQxdS742cxsMgOC0Vf0FdCLQG5K6LwYcXoN5MU270L8jHu9dPgucGSHdlNPkEvtd7KHbrH00gFerYohfS13aeMxAmxM1ndC6VJAA9DBuNcLRaDSMySAPSfwtZRecnv6bfVM1G9bfdaR4hKN03NLxAynQjjrjMcimwmktz6xSOly')
	   ,(104,1,0,'zwoOfChO','OE','OUGHTBARPRI','4rj9fTBj1yI','sEJ7jrO88B7tGH','zmRdF7Lt7pTOnB8FA','LG',485911111,'5437772972658502','2006-01-02 15:04:05','GC',50000,0.1886,-10,10,1,0,'oI7zRnDf4nQdObp65Fh3y9hKNlPFewvW7J5WKTrZaEV009cWDfoSDNC6wMtUlR3rV0H0Kc3v7jyAStkMHVIECg5EBNQ0DP0ktgPYN8NcrlvqEKX5J4Pw8MhnKLXmA8IkiGVudMMwrfqqpkdZhrps482wOlZ2jXNEIXzbZdDqzO6nQFdZ4WsLiLUNMy37IKCITgbmaKXqptaC06WGW52E2JznGhGraK6MH3B6D34KcXZUTvulAH28heOkZhsw05CxRHummhs5zrnixZMTRf9RFuKdgxO5HjYc4iNj0PdglNlzGfzR3sbNwJ3tvz9OXgdEHntbg0jQenLCFc9zd9x75CSZv7B95eEV2vN6ZaUsIkII1GapUzVhXzdNIsO65bExZinEzTYUinvMhyJDuQ2TxSR7xXXVtBVSaMKJw1Fxx4SYYpqQTvaGeI3vqCpqN11ONgZSH4ljbJZsomMdnSISXwto68xxT')
	   ,(105,1,0,'2J8pWE6uabafGxe','OE','OUGHTBARPRES','MGOCd6wDC9D','cA3CJuaK8kE','WBmcOdLdYhM8QzZmT','GL',343211111,'1351871137947644','2006-01-02 15:04:05','GC',50000,0.3889,-10,10,1,0,'EYChOMfqVojgaXaNBGGCL10u5cfOcNprqJgNyM2YWlxvPBml70wehvHJac8f8h0yq0n045JyBlON5mt7ZMmtZfodhrrO28W7Ae1Qf1Ojuz6Fu8QuBZNzUneLdVnEfCbtCHhdNou6ab4mCYyKWyjODRIjHKQTZzUzFtBVuOfUSZUqqVvTI9cLszRIukmLFWMSiXojTup7l21pu9Ghy4NsewM1e2OYGCPwvLMQI4iYkuiJoFkX4hZ6rmeBbdDc9ncIIsJfvBprab6twhUMO4R79HngcL81zs9ylqENEn0WzkxeHC95ZJTrSQFpg8uUkpk2q3P2xSzL6P7CnWk2H2glP6aUcNKiQu4jnZqG3RxHDd5cS5zrkSg6QpEQw71yFrvTrYVG2AdzFDSgIJn4PfC5exE2gHJU')
	   ,(106,1,0,'dg0GE6bEtfBkDLT','OE','OUGHTBARESE','WKHSX1Ejxo','eA2OXnPxzS','u8tcwg7DarDLuTwI8','HF',513211111,'8385389631778541','2006-01-02 15:04:05','GC',50000,0.3559,-10,10,1,0,'werBjthISbjv9KvUYHxDBXMqIzaxyynZJcLFYfb9NBrpeHqw6Rqoe7LsC8jHkOyQ32ryTlNiR0B9zYAQLpeHUpie3AGqjbSq75J9EOT5jL4poAiMqGMNa8UYSjU0jRI4R9tQTs2x10rIGS5Q7mO2otEIGeq3UpFYRArRM212wabzBHQmG9C31ftZNT6aSkE8ax2TlZt9qIyvQYuGFolMwz4QgtRO2HVZugOE0XANwJxWHI06cL7iAX8WqlQMUegD2lzNL9Zyxfoz8d6SzTJK36MUOCT7fo2gfGfwGdg08hRiJ959CpjyU6ZwnKLggr3n4dei3ua84sc0m9iYwmn10ySFpFiTf0a6YwimuNHtisBHUl07sEC3IJakjzJ6jGXnu1iGv77paXlw50h23fjJivXcwMxz6JoxN20nKD')
	   ,(107,1,0,'bVjmkDzV7JPj','OE','OUGHTBARANTI','EoIp77Pu4UC','vGs7cTpYyTGJY2YY','KRaQQ9wW0CpTFp5Xx','TL',721211111,'4781509599226529','2006-01-02 15:04:05','BC',50000,0.1606,-10,10,1,0,'YEMgJg6obKuZvqfpomCVGF6l6hGauPBEy4GXoC8psvQxWh1yWx61MtS4Dc4wdtaBXcb9lBdK4Ts1cbYAEUVUD3hEBGLj6OSJwfEMC3NNxkg7AEw2guATXAm1Ka75dRhQX76bG95aq8JFOKyNMAx3Q4LfmC5mwmiuvatP0QiC2TUEEaVJUnwqZMRwUDRI3HIcqn2TwPGFjWdywhikkfAD0080EqOYCzJw4oiSGViYs8ZeH7LuEjkxBCIMLklYJjAOy29cSBAT9uex03OkEwlYKFHVff5iDfmg7wOxU5uPvTADo2eWlkobrDMIMRmvEdZCnugSlS6l4rBFTLOW0tb50')
	   ,(108,1,0,'l9OQcfmSzj','OE','OUGHTBARCALLY','YmJNsUgxw8Ea3iEybk0m','lEqWZI2or6tjFW','GGwMg71lynnTY4B','WY',617711111,'7897423048725724','2006-01-02 15:04:05','GC',50000,0.3841,-10,10,1,0,'PnfGuhfak39BpBnR9iFOToqflT3Ry90Pk9gUClXa2gybRWmGWPcrP2NYZhEsym4WpK6r1v8JM3P5NU3yhtaodxFasCUldkt1vsKWZpR0rfIdCJLK5WtEvfNh60ZQfXk71ZTOVmBm5DaPcAxZaYFc6QD6e1Dp3VtEsPz6Cve2lczyw5sjDODrLzJ038xWtBatkB5JKsvPFYm2KyQN6FpbbloL6zVHkOH8zoaO1jVXFnaIo9in1MkpoQ9vFu8a0bX6GNQnEPSB1nuoHOxEWnJj06J9LYohGk3ReYnXdBcQp0r4MqbQUZUcNBRU3ZvtLnzX3zsBPFGFx6u6nIIe3H54')
	   ,(109,1,0,'sMRcTyMfn2','OE','OUGHTBARATION','RemJsmgQPkn','Kc2xvGYNJJvFXpvhZ7','Re6z5YeKf4Dfu3pJ','FY',971111111,'1493884301345979','2006-01-02 15:04:05','GC',50000,0.026,-10,10,1,0,'EPAOzPuC45eq4EeEr6bUBU2iYNi1Ax9eHObBPB5fEpwqo8bnaji7qdMGQSRzpcCRnD0CUGiIU1CsSHtCTQhr9TRT3LJErhPTRPtLPbe0L5Un0PnwSJtSKFjvv5P7ji6UBXsE5wSS6vTE8IlulWu49Hglf8cC2H4B1jGFeD5M0wgmSopqAf2ZxeMms9bnaLEDFOE8Z1Wl9NtWzZxwZJvGj1plYDd7azHBC3Z7futQyTvKoGL4SHm1qoMCiwzHrQbtd9LwQuDdNxIwaOtyuo1DWSDNSEQ2FLCwJY8CrasV4ggB6cF3euugoZwT3l4fDOi4P0XkwL2LulSKdR7FFRT3mb0yoiV3OVyDFidEkq2jvw1i51hIRdBKbPtpzGEuEsvSj')
	   ,(110,1,0,'2hpic2K5Ph','OE','OUGHTBAREING','T1jksAizrg759IMRvR','mtI3eU5VqnpiZ1wW','kDE1zzpPHJZBQeBTu','BA',317811111,'5849379800665698','2006-01-02 15:04:05','GC',50000,0.3138,-10,10,1,0,'xlzELufC8WjHaaGERyPjzoXmzhDhmeQi9gU07ZaCxzXrFabq6bItRwXulC4lLSQKfOmS8cCATeXK17ptuBf86hWhfrmT0YYD9q0NIVGyN7CwY4HhMHkZBJy7qns6q3NY24lKBk6tiFEE3sqyMxSpOzE1CmbxNWBkrE16OlQEea7SYCTVQqDEt3rYN6RVYSG9hQIe8sjLc6p9a5znPd5hevNCrrHYLEvUExYHGbdxXFJYbt92FocXyLVC45Le3qdWqjbs02FoQxYyBiROu5xTOvO9yC2sL7b0unpf8JTtp1cffGk21nt0ycQ1Xzb0HBampHGbZw1UkcncjncC83aGGHx0HIt0wsamrT7Ex')
	   ,(111,1,0,'RnxJ4fRhxSCZ2','OE','OUGHTOUGHTBAR','f8wO1J4EksextHD4AiV','LOwHLy0YFk','5NazgZ0ilw8i3m','TO',899511111,'3176105206236250','2006-01-02 15:04:05','GC',50000,0.2362,-10,10,1,0,'vNL0uZotzwjsPAjoXsg9yfcZNcIO0cXc5LkiPrZBCxMlwfKFiQi7vBNtDvmJBMyDXWmFVttYIT5qaLHibO4DkS2N11Cii7udO3ttfnqcqerYOkYDnVe2kpihcKZTRmWfqCtTeURs2xAivetF7AuvIbsVhLqGi31ZHbatNyOwwL1L0tUPfs94XPJ52wRl9sZQDU6UZgDBBWOsGtVzhc2Dzof6MoZPUulEXqfljDM9oiIxbSD5xZdANqH6jTiKHzmujCJdnXha6GDBvhRjqjEif7vwj26In5NCibiRWp7lycmE7hHsHX5BGqMFReUhbYpM1WBKgK7DmhE4cFMslLp6qCKWq2JmfNHp3YuyzszyQXgMTj2IbMWSEUrkhty4q4CupXqxKML6NIL4eFQv5qUrIH8Tdq')
	   ,(112,1,0,'Io95HZPwg','OE','OUGHTOUGHTOUGHT','iseeAWSWnOl8iP88','bg07DKvTL8Qp','3f4XaW6ADlj3TIqf','RQ',507111111,'2853265683298386','2006-01-02 15:04:05','GC',50000,0.2021,-10,10,1,0,'A4uwE6BgOhwMFSN7vJqV2TgbL59EAHFbrUVVlnhRA6p82nLzY88oaSuUkDq31drdrYFsWPKHPKTgUqQl2c5EySi1IxCxICEfbhrSvfGsDk34kje0ORy94VJHVDV8gzimLoZsG9WTDCgaApAgMFlUjkUz8ulRGUCU0ZxJayTJZl6eo1fbDD9DggVFJ2vFB9G7Tgv9GBq1c7z0gEnJQdZZGFpn1m9GewXJOwZ8vbGxo0n6x1ELLppyscrtYr7jiX7kxzVPmR0a47x1jk7TEqS1wDoeCqDRmutMnfmuc1N4RkZX1gQCugyoGg98W3vPTEFgSIwdp3mT7ox1hNSavBqqG12UbsJ6nsOP2v8rsA')
	   ,(113,1,0,'gftMgOcbCoA','OE','OUGHTOUGHTABLE','j6gRdLiVAM','xet2Ohx2OEeZ4Zc7wh','O8rPQJ0AOEcKRwlh','NW',470611111,'5575270845837696','2006-01-02 15:04:05','GC',50000,0.0557,-10,10,1,0,'MhkTRgFgTictaa7hMLxajMIqwoW2ZQhAufVnh7SFJYCqlU8fvfXAqcAeQeUJxe0131B8RJesIhMasZ1T99zf6FxuutFZddXapA4K5PFS33VEikaSpIQgV278Ok0pYG7K6YKTFjOB7gUWkIMeNTEJXLyBtTQTU1vQXqqQsh3OYTj95D2y6dN1m8ngeXbyCSPdJRhJQrUoaMMrLHNtHa9abxvaG2vzicfS7P5edllKFBL3mwkQFwVQUJxgMkzO3jMi4wtqwYJZ8VrYQy90NXppQtX4cN5L2TDQfQ0GnUYxG1lJItgoAUsktRA5qT0nQjchW2Xoin2XORDCikp45gPH7kabVPmwE2PkoA55aOJSs2UTQe0n9w5giLQuGxxaS7x7nr')
	   ,(114,1,0,'PzoV3akDMdD','OE','OUGHTOUGHTPRI','DsOethz4Xnjae','XGoWEyRibifEyOTLXRG7','kxVvKEkmwbwR','FD',884011111,'7091520748637297','2006-01-02 15:04:05','GC',50000,0.0341,-10,10,1,0,'jAGRIpxz70qEDgS6RcWRFjfqAJcrNexMz2apCMmMSLSMywL3sy5LGy8n6TabtsYd9mhQyFT9hVbzYLTLNFh9VouYrJ5T41u8CvYF2hFSUQDv9EFZmLWTKCwP19GiLPQCra7fxrq430fYuNBoUkY9mwA0pTJBbFI456fQKAVEuqRHcb1gqh9OJSOcQc0RhGeC61LIHHK2v96owMxz9bznBX5QWEM1A2dCvCKtCRFKahAFtZ3DoDpPsHcd82UlhYxtSKxuAjnSWtBhs7QAtpjr1fZAbOB1pfDkpBSDCuDVbRYSIS7cBj00j5BIHWnHXtKQ6fyrdndbXNZKPLcadIZzr4hcFfSbzk7SiD1HhYudS6B0Vf06A48OQlpmneAjgH2JzvayMSdDJnqWCAnDzcrVVWooEQm')
	   ,(115,1,0,'Ozuqvey17Yef7Td','OE','OUGHTOUGHTPRES','Wt7zUgNp3G86cJ','2RBSv8gjsQSW','IYnYuw3fKr','HD',645711111,'3877174807932019','2006-01-02 15:04:05','GC',50000,0.3623,-10,10,1,0,'F1fBUTlmu8GPSIucEN3El8KZowp38rpTmZFbhmQS25BMxmd7DAxvImEwjvX3vMYjZFyfLpU1ulbflq9g0gteFGKvv9EMVLKij7lx9eaQdIkeH2jyLKO8jBHpyMNteaVImfheOU6luRkdTKVmfcjQzUBjyp3W9W4RWGPAk8s7FXI70mImHcpDlPFSQoEUv9Gd05kqcYyu1MiUgMaNL58RWqmn7UVIpOywHuuHMtfRPKOwJSvPz4FWN3cQPt6JOBZo7bKTvJ7KAggrQYCLBaz7vPWZVST000o2dGC8Ut7oy59pJ0q61R4877')
	   ,(116,1,0,'qwDt7ufpsfWV','OE','OUGHTOUGHTESE','JLQW8oDO1oMJ','HYybYbqaRMftOEyX7Vr','UpO40IHLuWn6','QO',275711111,'4108054643118493','2006-01-02 15:04:05','BC',50000,0.0913,-10,10,1,0,'zdvSzKYpGrsdByNPk7fywBUV0lfIUyqqRArSayEgeuvnwK8RZHRKbg3jcqEUF39IwFpyQFCj13xFN5AM6lDbe8yP43iOmZ3ga6WscGCcm468sCswOh5W7gfXh9kQiVBAHibsMgPIB4aONuyv6lwKGFqlrtweWAfMnQ5cGeQU7ksQAeGfNUdfWDUkdWdFoX46i6dw7DyXQm34WwwewswYoRANgrIImg0VPzizwjfqpgSZX7J3T7ZXuGMS8zOSppnHXjd1VSOd5WoTRSf2tVPh3nfkugFa5VJPUukcFtFcXDPP5gxV1Aa3YjvOCayt5xigUwz20NOGhmP092CL2AGb81JKlDNsQ8LLNOUmohaFvrLHVIeU38RO9ZcgAuZo3Vn1sLsA8EjvpqMNxWa5k1gFvL0ZXDRx0B8HdbVvmiyl2eKGsSjYlDssycz0sICwxdOjAnBzSjun6PMdl3gXCWv2K1F9HbVYBlCDw')
	   ,(117,1,0,'WANSYJA8vx7ZSVO','OE','OUGHTOUGHTANTI','xpzNHoazMcmZIis5fR','oGQ4wy8DMPYT','hdcWBXkwYAENts6J3','TE',108811111,'8266304340700704','2006-01-02 15:04:05','GC',50000,0.2939,-10,10,1,0,'4G8vty0uPceNhFHXMbI5aJlOePl6JwHoTcs3Mlh6LWgg1DHcOrNUFRdfO830OAZu7bYTikPPdB5fAUHlYZDcx8mtEZLkJR0hGtuwAApX0WTYQk0urxe1RFdZsLXpc0QWviYkT6VvZZdL3nGUYPztps8vDY8zlfwVYLqM6Cd87nx52GQxjZGyMQLMsOX9i4CTTAGczPO5C5kISFQYRIpDqlwQHztpqSzIlZxSI3gwLCXDNPdgkUH0KMUXKid83Ipsxb3zOyo87KiuJG3msDwQrgjrZsU0dTmVc1Htcex7xTReSWdp5Z4KNrq8hP7zmtDzWm5veQTGzh0OBueRAsUOigdEYwfweXeCH67nzUXj6jtQwpR8804kU6CkLIv6')
	   ,(118,1,0,'AOs29SFzRp7teh','OE','OUGHTOUGHTCALLY','OHBsy3RhzHSjZ5E','SzyEbGQ3HS5YK','kQYTRFWGBAc9yJ6clw','YB',939811111,'9491679413919264','2006-01-02 15:04:05','GC',50000,0.1849,-10,10,1,0,'Q7b524Afl2c3vQPOPIc13No64VJypMYD5ROmeAtFwEErLo2aOzizq5MvZ2NokUOcaVBuKiwt8h6whbE8kj2zdWz2xhhpMDvdDlB0ikEodKCSFhmGnk7x0q5fsLFGGBkjPFnxEIVR1Q8oyNbdhBRG20kZMIQBf15UKw3Zfd4a2Cy0oZFtZYc7SPjzZud74LExGEtkXrTGw9CAhg91XfKjb4toB3mm8c2OfZkSzUS3O5n01j63NX8jtTTtu4jb24adAnswE51OQBU2ob7KnaTZ8AWMJ4PSpN48MbaZWsXrqMIl')
	   ,(119,1,0,'GqG1NZK8sZlS','OE','OUGHTOUGHTATION','sYaYIJoRWmob','YGIeUlirR5','wDh139TYBLLpOr2Rp','JP',203411111,'0594502936064465','2006-01-02 15:04:05','GC',50000,0.1582,-10,10,1,0,'pDPYr9ScA7jAvK7Fz7eIyNCYGZhpIc9yJc4inzq0rZb5cvLF0z17hHSJEhTNehwm2YDVIW6R2f1C2mho2pNkgc5BhBSeKewVjUod2p35px9fq3xhgiBMzoSubgf7HLZk3b21sYzEEmbbS9ja9HyzUOiMBLBEkfUgR4jui6jHSAuD09yPm7FzNFZL988J98ivPhoHn4yH3TtZQXFOMwxlIv8wK2oR6H7zrANYmRWbq4maRbJigrp6kuQH36m72XuFHyY74UbYseggMSBvzhBtozx98bs0s9Whpb9fsY9a53gcxDygzeaescXrVQRychKm9sBsfHE1T7tWnGX3tdvO6Ek69WkgcwTNObztOsAw04jISGrSwlVtEIVS3zlIO6uC694b8rcXx4h8y7JeawvRPtTfjMLYuU3cKA535cW72liYALEZmQR5AQfizcMWtliuReKJmYnn')
	   ,(120,1,0,'XLBl60CL83','OE','OUGHTOUGHTEING','jcPqKdNQZZw','MzWxZRzqBE4hOyiBmb','Y5GUCbDW3vTZV0nZ7ljA','FT',153111111,'9972007044994777','2006-01-02 15:04:05','GC',50000,0.3654,-10,10,1,0,'vnObI5kIl9FfEYbHrvrN8EiBfVTeTl49PPQr48rGGYW7srejFaEGg1ypBCgJ58Tdw83KLMFI1inDR03iKTIV6eh5jfjRIR61Vor7QTHbI4sl5nAr1V60GP1G5hGmcTiz4Bh4DLbLi8fDaB9SqnL05WB5yJnQ1XuNHzSJirLsEvh4ORtmDgP0V6inuzKulruX9uCoQSNF3rJUK1YY24cizFzhbR4P9ZUgaIHhU6ApkVDBSob7Vpw2tTWfwSnlyPUMfJJ2micAtdwIet1LIRwmwDoVAAOzMAoq75iEuTGxtsTBftt0o6I2JIDiBTTOUNWJIs6vrfNPuIIQqusikYqea0KCJhfDCCegkIW1J2fbVqfGj8Xt3')
	   ,(121,1,0,'H8D9rPpqstCA','OE','OUGHTABLEBAR','JrbaE0FHBo3SQ','MzCmfbwLE9vr','fXUeNRPFnssjzXFf7n','TO',024111111,'6174100500771271','2006-01-02 15:04:05','GC',50000,0.3234,-10,10,1,0,'YSDgzx8qTtvju0fHyWVhCd4EC2ypUV5ilAqthDuoCP6USB6WXcZpXFUMYGQ0tWJNE8gSjuNvtOmoaz3aoYr9vPdw4Oxo6CTcc4Ck5tVbbHOD8T5wZC7yUcJWXid8BSEcT8qVMkqdQLhLmLP2YPxmlYx4sGLuYXX9jFWzdmJsB0wABkTvSskVEuZQc6D7J0kFo9CeAlJKyL6veWfZ2n8aFWVPy37xsXQN2kteHVTeO3OvpAArzltC7nONi7q6epv5O2Hw62W8om97esLrdsLJKJxQzikWplDPG84Tk6etBkfKYUORJgYYGYugJemtCqJmvdGjuOiC5b9XOrIEWSrEtED5')
	   ,(122,1,0,'NHOmeINSQIGfp','OE','OUGHTABLEOUGHT','lQFpodZnCk','ieIqLWfGm9cDYW','KRZmUYVS2qtqbo','ER',446211111,'3602970625083679','2006-01-02 15:04:05','GC',50000,0.2857,-10,10,1,0,'T3erR0iok67lcBX9FS7oI4FBZdJBD6xh0ciwFyIS9vxmWf4mw7iopAdlCEHDGmcf7HbTMxWOmsSykbllgUblfg9svz7JGBwKzBX9lMkZ16HVMoimxJVo8dWKVJU1BsqnqZIx1qFfkCtkVpRYyd3Ugac6Z82t3AcAJq2E0NyBm1RkdeCfKgks2oGuloWQ2oTlman6GUSDtzZiKyIQQJ4qbjlaNRBaMxWQoQ1mO4Yp2KsvmKXJtCRlc14Ek2FBsKgYI0GPebdcKilugKhUtfgaRzaliNeYCWCYDjj28Bqt3VCsDdQlpb8Jd0AqPnf0ucgKVBHuZ3p31XQRp4fsMG1XFzu6h8RUKRrNjBiBwXAOJDjA2whxE1xu')
	   ,(123,1,0,'fzVt966kYF','OE','OUGHTABLEABLE','r4Yad3OXQJpMk9Pdlm2k','5vnBHZ4DtvPbwg','EsAgizYFGe','CM',081711111,'4890092182526982','2006-01-02 15:04:05','GC',50000,0.2111,-10,10,1,0,'HNXXxom5gtxVCf8AnAmg0bPR8ZWjQ9QYTeCF7Z5CuVz0qDleityTKavbRIrouDIvOmCvW4UDW3kEi1iKRUoPSxuzqyXvucGNAt0ViwSxm3awf3zqbwADqEmZB4frqXdmld7laU2743AgRxMPffYonzfGXlRXIVNME1diRlxOVtdPPu1pqRg6MtpBfzMl3SGbbTjDU0ZukB5FimbothtjdAOq3ld3Wn7hgN7ZPYfNyeRkBlfVLXcP7nZfBHkWvE3ju7YBX2i9vmbV9tvdpgvLmPEAl71UJTJbIQSw2nQBfaQFnRbMNQ7UxxZvbKK4832Lo9aJUPBzMVSKdI84E38gnbN6NUfd04IJ4gHvpg4aeZsbuEoHqfUavEhxK4Sl98OPgLMWQ2rTj3IruhRHxcHyDK1QC4wdkSzNqnr')
	   ,(124,1,0,'VH1Y4rQE8TWSg63','OE','OUGHTABLEPRI','ENjg5D4NgJjfaizw','8AQgwjo1gURnYg','UfAgM8nsbB2cgYa','BZ',723511111,'4806438075425374','2006-01-02 15:04:05','GC',50000,0.2526,-10,10,1,0,'FpJRuf8DHqvnJs6CTOJq3M5Rw6QOVMgZU0j9Z7NJlwMhCvgS4fL5o0nIouEchewWOS8BSWeVp3z7QZSCLsy437hOJFNgOjW7jGdFEpNuvy4UmaGGh8rWadkFv6X21280aat6MRKQYq7Bga18OibX9CsnV037pBe3U2BtSXyuqlbKILwF2rqevD40CLjVmLmKS9KkeS2cnpZYcgpGVbvIDFDw3IBzrUyL8pbrVMyYZNP0qEROI4RG82s0ADCsgIjJK0WaRrjaZT0Z01F3BvCYzzaHUWSALjBWknQJabgMRmqZdNfpcVmCfjeR0YKCsDo341kSHiW2wt93eWpY9SEJnTz98f62DYLb7EzRXPv3GQD9sQ4UnDKrKY2Y9XYfOdTdnR7ZCRAcScu23GulO0K6KJz7dLL7aR2XZFUqU80c1mwCS70izPU4kP3Jt4gPvMNwwz2hi4YhdwMoJFKGc5FZ')
	   ,(125,1,0,'SnGtNASSp','OE','OUGHTABLEPRES','EAQBjgshnipKq','bQRPXwstaZwQs','1NF1u8G6Sm7','KH',069611111,'3186555554146446','2006-01-02 15:04:05','GC',50000,0.0115,-10,10,1,0,'h89CHYFYvlSlxE6sNLr9wWWg3xWKwAa1yqCNoJ2jBNETBYssKayQAV1XkmA8SSp3SbIzWOkm7nsqEhxqsQEVstRwSYkupKzGNUxzynmxckI6oyTTMhZJlWOpMvncC4mr83MzrPMsOWBHsVRhDgmxLk2LGIDrbKF3hPAkpjX8n8nMtNZgUtMkZktxjaZnAdHCKFvYgEYEEEBTTyIN4vFlM4CucfdfF0sRTIi7DjONrbwr1RngwhLb3yJmyn5REZx75KdCYn7Mv9uq4k88DisG7dr3R4KfCh4Ho4KHU9kK6EaUZYfOzq78Ld12H5eW10931JWyyR6ZK4lp8HmftdDwXlNS6R4R2MHkBGxcTsxue3e8x3OMGz5YFHs2xYTamRMKMArHhsJFJ6regpg0nPujNbeJoRSqbgwS')
	   ,(126,1,0,'E2iqFUgmcCH','OE','OUGHTABLEESE','EogJoSSd9L4','Uqouw4uzpBKG','vflvRd6xQeIeoy','BL',991511111,'8643160102728523','2006-01-02 15:04:05','GC',50000,0.3067,-10,10,1,0,'B8zezqchibo7iibKAsJBU7QkUkRoVXgr8A30Ze4PM4hhMJT7JzjB2WSObtxdZhjtrhHo4O8Yb896JWhJAt68xX2PWaAKDmu7mmIviupD7ZMubUp8wptcpDait992L3pmX4lTRDRZFwnQYByvxTb29bjvuPTFZIlZF8nKafPr7dRR94aWwhZbWdgYXl0XH9kyzwPzHQUl1NmIpGpa7li7B0K45y0UB57P9e8aj0zmoWlQqmVIqluJP9w505GD8MwiyHUANSP5Wmy0MEDeouddFIEk6enkkTzPAHb1d3WZDfuRe9mkWy0ytNiwEgwLIvthCmrC6dzKetwCf3MjM72dvYhfCPm0rYLWWHObGxNnp9xNCjQitjfgzLgAxJiKPpNz5sTzsWybylP1zQtwdsheumlQ9erSOIqnB0hrX6NkmjmMHMylWvWU')
	   ,(127,1,0,'BvlWNI2nZ','OE','OUGHTABLEANTI','9zz1Eg1qBEeQRmXV6HW','UKAaGjPcNk2gPHRG1p','imWUXEAhLsIQxq','TX',053411111,'6253326870542643','2006-01-02 15:04:05','GC',50000,0.195,-10,10,1,0,'oMADhKO4F25AEqKccciHByNplrHiE8m3A9S0sZAeDDtyWlEaTagGDCIDI0VqlMCO4dQxXNOHR90BHBJ3JOefc0ouw6ofPKtfqzbcu9x9mHvy5lizJyMHkh3lrrTuEOVkxlkXcUKl0AztVoD9J9ICIF9SpamL702EVhQFx7CeMJzlMTOZ1IJvsR6cNYxiXePsgjmVpBPuFpjpytPH7wLOfoDLzA7wUXEDaGnYQyOH8LXX4pKwPqKJDhRC750SBzs87fMgErjb9MwF9wOiuK0VgEhvNOQGKdFfqd0uNxXM1fm5fnzfYe5W8t0yBPQdHBzQWrrr2aiGXC0OUPIc9aAngmrukJTjZHcdvM2wifFWYEYssZScF4YHeibqSc0BRKpfKSkaGIADrNTZOeleBEMxeFPLJJEQNluvLSJFrM2XYuB1Y7qOfG3okwDZQ1ZwGokXD01RyzgEjel6')
	   ,(128,1,0,'S55EbCENSAQ','OE','OUGHTABLECALLY','Nt4oTu1ry5VjGOUfwaMC','bUn8l78lIjiX79EXkTY','6unEc2sgVw9pEW','FM',550011111,'7417238500937453','2006-01-02 15:04:05','GC',50000,0.3132,-10,10,1,0,'OlZlNOQ8xTEajmde9p7O4P1kywABsKNBgxyxNxS8U8FQX25fhBIX72WOzDIXUfUlf0ozAIfgEjsSVuRduNtqCPg9m4gxElFT0yWKlK9fQoLBc00Db46zgTfgi3xaRavKIaagne28NXLFF6lSdzgyhcHdfJkkHsNvbmSFOOSkeGaYrIyT3wQJaGfeeremdWR1eYxIwnstxR5xU1WM8Pt8D7mhySsgyaVfGryONsNRshtNpqg2pB31brwN8nszhNpB0EAioj7mGUKT0ZOgP96MOgOkjlx3MPkM3UhmmqteV7ZHiVFHBXXWNItYI2AU1YZHCMy3BejxOiIc6fRz2LHq4EYidXXPFyjUnoTEOGBl401yv2Njxc7naZwou9dJHGXAAPMK8hRfHw9Krbi5uRYFipyEY07O9sRQ')
	   ,(129,1,0,'Q4eiFB3M','OE','OUGHTABLEATION','rkVaQuB4sURmAsebGpU','0DQaTVf9KPBuH1Pv','tUCH1RY0349H','ZJ',367211111,'6493778966811140','2006-01-02 15:04:05','GC',50000,0.3115,-10,10,1,0,'ZyVKI9GQSUAidkQoI5jYNzP4iHsUftvP2G8mIzERqLKSDNLxfOY1guftejYryXSM9x8fdRqdaiLVMmjWbhvT2ItTDcOcHtja3qap1jQWpMYtKPVk5HBj1HV51GDhak1KiCRZFtAj89tSGWxHJRUflZM1BBL537CJzUWeJfiRNalo3Hhodot1LgPQ8Nw0rYLiIjhSs7agCPFacebd4RV4P5DdGmm34wffC1meDcA0Fjn5psZNJPPBUCwEAzoQFkhJoSjhqiVw7lHOWLwApIJZhGWVpeErDSSRTO7ogRKQQYwwHBZR3i1v4DXmFanFvUJdsrVnJxAEr1ptJWCw5JfonwsGsga2mC')
	   ,(130,1,0,'bwSq5h9BuTENG8','OE','OUGHTABLEEING','xjZBF9A3NK2SBsFqxpH','ItmyyIaht3ivdYBHCV','2G4CEUFsJ0g3','PM',088411111,'2845022324052488','2006-01-02 15:04:05','GC',50000,0.2659,-10,10,1,0,'zbXAXaR1uuMxUlegyIm9rpP9saPhvt6eW3b0qa1n6gZQEaItNKGG5dni5TyzMQ9OeIMcX838hCzPfvWidTalclSRh0S2z6i522fQ4hiC823gdaEee68CBDXPmLOymyvcdPfa9NxOfQp19aUigsoW9Vjv9TYya9Q7AikxyrTOkLmRa9oSndb2RIalkwNK9e9MBI71Kp5ha05AhgpiHmf42vccyY2yFAHupGgJSrAWHKhcv2PEne6NeJgzKuYpsdEruCvOpShLV7H23tOKL0OHJSGi8l1ypKQFmxScJASuYYudrqroUrNwHZRKtf4AH4AkmDG7drW9mBWGhoQk15fRpZkkWTJeva8hiuHLsIuXyA7R63JL655hRUW5tKUtkInO56rdF3s8TOOexPIJ5r75OGtbgZjKvzz7ZLAwwkEtBm2KU6joKlTFWNYsCqg0BqcS0OTUIAm8xpbmrP4')
	   ,(131,1,0,'00WHBvSD','OE','OUGHTPRIBAR','CPNLhDN47O6kLXq6','q2VqXOZrG6L2Da','5xGN7OtyG7Vg6ZPF','KJ',947911111,'8259746974532522','2006-01-02 15:04:05','BC',50000,0.0468,-10,10,1,0,'D66XooPrvpWYIWXcDbgw2oUJfjKiST8pKkwLQn4nPD2klvmPSwXOZ2wxH3G3GgkMs2Q7NEEzetoAfuPurYZ6HLYdHqa26uC53lhI9S9jqmsSZ1oAzjgRbmRidpUwtMgGtR1FDBDFDm4fGBPXUCFc3LrhcpE3Z0eNZNM5gVhswi86sgL8glmtdzHbNlLLP70b86z5fAzQUzPvUlT1P3yeDHFdtloHRP4XkPYMpafky4wVwhPtpmCUpfjrfy1NnkH1P093HGE8iAFSISHhTso6ZMbGO1PsjYq2LmtEixXZMA0oK5X11MTAarGQC7FtBDYFs7c5oKV5nyjiCgdYj3KmcJOvXu264sSvvYoGpvzhfWGpePLfpx9YiqV')
	   ,(132,1,0,'XgT1brVxDB','OE','OUGHTPRIOUGHT','PlOBMjBzFhF','mIHaPoicKYn2kMUnw','bYZq0nY9gbCT29VHyxWy','PQ',153411111,'4574797142281840','2006-01-02 15:04:05','GC',50000,0.3928,-10,10,1,0,'m65LBQ7moGqdYdsHWjmbd3w3SvLeevVq1Ns7qK5RoliZiyBuVzbVKPSMukDvdT9ijEMkAtL9z8MVcKjkMBUPC1QeHlWFK5fXLHyFXi7gdQnge635qsUKSMoNW7DgHqsurmostIzruetQYuDC7EScNYyS10IHiARRvlxzkeU04EqO3n904mnL9PaaDAwA7sfdPafkfrScJksh07DNUYb3EjCiNntWmtmY6bam8JAMsEEXXbv5LRh1dvfVHJe5lF0WLCbfrdO75fyXtkTpeFLUe4hzPRTkhtC9vEMQc4CNaUKf65t15Qp4ZvtaaJugTADdpZJofOdpM7we7RO6khA0fEU9OiuqhXW')
	   ,(133,1,0,'rsjT6wlf','OE','OUGHTPRIABLE','v9dsI8KUklhbgcrCJ','2ycGnDEnv8Sx5','srTTAXk5jnlY','ER',919111111,'2026375320133357','2006-01-02 15:04:05','GC',50000,0.2545,-10,10,1,0,'p8H8wO3qzHZD4q3DiU1UW8a07EQ4vZUxT2J85gds2C5VIBpAcHA2QJcHShByGlReL5jrZmSk3ZTyrbcZR882fgPmK2NgxlOcFrF74C77pryEnXmXsoxqbgUSioznG5bs5mjSFOFZiz2XQPhxNkWNlMqMTN3Kc9rqKF5nzNQxBeyQ6nleq1YcR1PbQ7F6khrzuHclrt0gWeER5ysXHyK2PBWcRvmApS7tTV6npe1bIbIwluaS3IsWfKGUXCYYmHxFINysvnXOF7K88yj5bZD6CiWZxDldJ8jTvwbKuFKoMRAha1faS9dYonMDJkHnCwWR480i3r')
	   ,(134,1,0,'nHea7PKxT5e','OE','OUGHTPRIPRI','P8rAkVs9cko0XIhl','dUNkbq8EtBRfkp','h0TN7XBQMkUPAa','FO',330211111,'9976596152763184','2006-01-02 15:04:05','BC',50000,0.0166,-10,10,1,0,'5tKCRBqp35Ep6uvpk6FnBram64sRRtstiVBnw6svtAWVUVztOmfrAUwNcN3rnATWZfVZsLLzcYUFWecR3EG9iAu7JEBvR2qk94BfbNLaNQUkcoTCIjeuuhyH4RzdhhYLV450PR3KKMhJj4ZN6VLBujpH1FWOBrMFHRzUJEW14n0zcQuYLcaXSYOBNWfZTUZA01mX4Ztq2yjHoR7aolm4MvULs5hAaPh26JVmPjJNfkNirJf2I8iZLUk13R5jb7spj72PVXgofbuILdTjWmsNswzle69hzFl8vGhip7GEiZwmL5KQYtHLaOkNJvGED10ZHiffHu8DvFqxhvZIHGT1l5UvalAbfEP4q5AVnR4pk0gfQj5')
	   ,(135,1,0,'2eLisBAbuHz','OE','OUGHTPRIPRES','SjnvQruSNOzTj','xj5mtrOrnUkdD','pd2K2m0blBGUb4jUgI','SO',938711111,'4299823654376068','2006-01-02 15:04:05','GC',50000,0.3666,-10,10,1,0,'vtpzdT891CmPyv6ptS6NUmsG7wKkhbYhwK8RTkLyPPLwG86z2mviLzuvpJZTLfau5HCSk7U0qJmDungMBdFPiTU0K0VX0iXuFMd6ExF0TTKHz4XYGZtfWbnWeZIR0xACrpyPnaQZUQsLzEHXrsMijKm6BrIWU1i9BrzhLoFBuvzT1YVh3bm4XjAOVx0DbwrdAGyMT1WerjHaVyz7PfcoOcLBoX2ObxcDIXvIdpnSY6KHcEFhJ5f8DNjzpE5jncrWdbcyPTvgYT3nNtOf1CPWtShbf8oWzsp64aKFnws1OYkle1LqBDYFGp7nY5ph3nwO8t8mbWzSw55NtLdXbQm4w5cIQ0xMBlA2DpuVFG2qmp4TcjtO8YvPlxC69w2AB9O0EQx5j5NwbO4yp8DqebAnG44YYR7HUwwnIcwYll6UHDbSH5iToWOLmGnLZZFxcw44vZIO0KDNqP6Nqu43dfR')
	   ,(136,1,0,'evrIgguH','OE','OUGHTPRIESE','DFEaaDKkt8HINxmFg4N','FRJpPmWsJDX85ptrW','hCRnexyG9dg36uABA9','WD',881211111,'2779202396837222','2006-01-02 15:04:05','BC',50000,0.428,-10,10,1,0,'IAPbcb9AyjAF9kmnsf9Cx8nMVJ4Wly8JpUevu0Y9G41j0enejyeqgnLt4393OV9xyRKy3e7YbWLUA9T2qPo6Ee1UWgLJSvxxyDkYX7uDo471wDEnpwWEUIkq3jvbjqTY7YocSDG7iDSqq0YspMuqRj7RE5uzv5qZlARM3tOnxIBXx6bQVjiAT4a33e6YRGZcPUVehjeVgSpftvnvCGidIf6cO0ImczqJdUqDiKzGgDKbYA2pgFxr7ksmcD3XFME9qqbVQUfe45q8Oj4eFgn9KgrjhmdaabMPv1abx1Ixa0dY6yyouqTeT8SiduMQxNv2i02Uby7Ss91nPdblsrSj5dAoOPIV6CZtfQhbyarW9IbZQJIT0tXhbXSTjKWYCPeG0rd8yeBmWIjSMLBhQzdIWn9yktSLarih7kBP71HzLCqk1W26XOfcHblsIyktlK2D6ZlHOnW4RRV2uz1lomXf4tt8GLkoV1oucPsXOVMhLBXdIcUjBy')
	   ,(137,1,0,'iij4kugGPeTVi','OE','OUGHTPRIANTI','LetinLCJJ0fbG6BSoc','q5wbyD4v2P96iH','Ojt6v5DNCX8E6M','BZ',580911111,'9541258282876008','2006-01-02 15:04:05','GC',50000,0.0673,-10,10,1,0,'XDW59413A7XF3OkB79SReHM7rTj1EZyciG6fQgzbgE7LLbw2uknOgWCbOgeXVoNaSV4PTm6eHil0vtdVzVEI71ZpW7KSa0kVdgKSgxUjd6RULrFla8Gnhox8C302DAjCnGUOWh1Qu4gYCMacQ3idoU8CoDJAnBAo3znKjB8w786JBe072j7X35IgbAeUwGzJfBzOTE3pLs7rbfQxxrD6ekQkXtkra9fpCWmHkfEejgWIk745fDaQuapjgotSavRlgj1khG9CFHsVs7ySbqgo4ph3OgR3JTJBxLhKWJMY62Xy0K0')
	   ,(138,1,0,'jGIV07xdEtD0Lby','OE','OUGHTPRICALLY','oHK1BckgyeV7','GoRfzmH1jUZGHf','Or47cXFUpfGwO90','BY',425211111,'2288703392868659','2006-01-02 15:04:05','GC',50000,0.1902,-10,10,1,0,'6hIlcBPPIM06rQjXkWw4NQ6I0oiiTtBIj3ib99PHWxTiZ4QwQdMwb1YA9dEofP5YQPIMjGCpc34I2rcqiPsdWnGQkRUznQQsfswBqOMDoGJnCoLA3mSV1WZwPVyP7NGVYUwY7ouDJiQYPVlK4oOIFWQGxUG9QhsKkt9w32vJnJhdsRAUIQQjGJ0fstCBdD6DcQMbiu9KRHOwZ8g8e7MOsKE9QWdrPBaxgxSIRKcxUYdm2NU2bxjxqFDCWsPYoAenDAliufbG02ziQorLZyhnphny5mKRRiv20dZ2cWNr2c9G4n2yw0f46HRPG8gWnxLWSBnPrRLzQCoEjL20deL9Cgx6OmvgcL7Zw9tL53c9tjRzqtU4bdSL4ZcY4MUBMPWU3XHOQVvrzMQoW1BxNx1voa7P88SWofA6eWDnYBeQJbg3piduUZCTOdgvHrWKXDoIYaUZoqhjcjnI5cipBs1SnAa74xQUEOhoDbwijJwW5fE')
	   ,(139,1,0,'TO3KoouNwBJg','OE','OUGHTPRIATION','nzX7nEzNjend3Bb','SQ6U9YnAYnLNIa','bobwnYib2GlKr3hjz','IO',896811111,'4673541401633535','2006-01-02 15:04:05','GC',50000,0.3661,-10,10,1,0,'GCI5GLskOLWXTN7WryfmPh8E0ohOOXfJnMyiW7GNfNUmzeKS38r1ybkArBTR25VicPKIuZjMTHstaBYCjHDFmsu42V4ONibj893Po8H1FIcruIIv1qBshnvsBrI5ygLfpSwpGuZfEoWz5eVGTLKdNV4yYhjzV79WeV5G4KfIkvDPo0BiH24YdRFzeikYsXjGJhl77OhmtGglRTlfXPMmjkUEnUi1m0E1A6HHmoGWvd7Bl2cOEXVepJ4kQcPWCCpyQEmB5Mg6rVFtES6VVU6bvt4673bY0mci6LBhQP0wMOAy')
	   ,(140,1,0,'jmXDTMEOggvW','OE','OUGHTPRIEING','gbwizK3T6hnei7j','Jel2Zb9kqnZUqz73','eT3biypf41VGw9uVO','AU',310411111,'2900444272964847','2006-01-02 15:04:05','BC',50000,0.0221,-10,10,1,0,'zxbSPKqXAjrQiGzdy6HFh3HkUZqjAu4tcwJKJ9IWyrYitV2l2FWszz5KrV5wvs8CNRLrtrnjbMLonissC75Ug0uUFS4405Stjr7agXogNqsGMvIEfMnEZRcPP1yUNwgYiNwnnWuMdQBSt2N0b3uucQTW5wfY9WXDwdMK2M9NmLYtGn0gXQnv82nHptPxvUKRMdCug4f8TE6b8zLl0s9bEYtjQLxrJ1QZD0y2jSLEaDjymI5m1uTUEN8GToymEF2ieGnGPRGbNwxWoJDEzXMCzwAiHIm7pQ2onaMdVz297n0YN6FmN5ntCoJPXIoX999ZTQauuOTBhMMjYOyiY4g3XmQsSg2uKDNLgFygFG3xEaCDBbIDnGeekGGlcyvbBipgKOUJ9F4bgWxqNAAVtHRFxbchWKpG3qVsgllYBovA68hSAj66nY')
	   ,(141,1,0,'RSZ6fbbU2bxo','OE','OUGHTPRESBAR','IaosydeH2V','hz9iLWSEa3w','kilxnyump5KRHWr0','BV',012411111,'6366996907149248','2006-01-02 15:04:05','GC',50000,0.2141,-10,10,1,0,'jk8xl2ZyQ6025gaGojZaqUT2hc5oGiORGIRWc9BMaxIigD3P7DyH6MDIY7twdQWw8hpkOm3oZyOO1kH27KJGCrRx77Htnq4VDZMVBeaeCaV0Y0EbENaG0cOLnVk1VvuI1WfObRs6D2GMVrufLw7h92eiX0gTHJ4bB4X0UJjdSxYBPkHom95sWW72jZZhNjDdliYui7WBmwHLgxFDYYaZ4ucR37LxXnmfV706qvFxnKZEo2vtfJpMxoGjtkqu59aE0v8T21H3xtA17XAaouG5pyyePtruKEYKIPE73wPBwcd2LgtN8EHkIwihmwNf8VquNvtInFgCc7sF681NQR3RXJTMCnLikSsJwkfno8quTsTW3RUsGgpbi2mFLM69ONmRahRW610IWkJXUrI6DbgI3LueBRMDJUecTKPpWPqlTxg5')
	   ,(142,1,0,'RR6HVKly','OE','OUGHTPRESOUGHT','2hKPx3qUNTLfzXOh','ZJqrglNQIq','71OhYZFy8UuCffH','BB',873411111,'0991626961884040','2006-01-02 15:04:05','BC',50000,0.0829,-10,10,1,0,'hRT9izDpwQZROChB4PDY3msnROzOVL4xdq3u0bHoGZrqA44BUyRHLxxqkFYm3YHHypo7qJIHjd51ulHHCYPhRaRHJlC6sKHwPgXiFaYMebIK9xGBdFnjmFTmit68XyxOEp4HwqZo2s9HBl2jTazFsijyNLwxzYDfHKBHLDdSlSUTXjCfuDNeMbLtgiEDclirQuioPlEFtkMjDr23ICxxsmpSieAzqvkazcNBTBBN6mOgJKOuikocxdAaOtYA4jhJ8RURnzzyzi5fujNT3U1qmuxxFiDWGWq7GEY71evfHXKkd89')
	   ,(143,1,0,'iLyVWZHeA','OE','OUGHTPRESABLE','t8t1FYIK4Kuruk','1sKfywaNeJow','XiFrB8OuV2vVNJ','FK',252411111,'9839361907730521','2006-01-02 15:04:05','GC',50000,0.3581,-10,10,1,0,'NRXTL90TTkniaQKzkzyGguljuTxyAMfNNHCpQVQZ6BC72eDZzUVJAGskqk5y8sBuqvbnEk7O7jQGueVWUD3bJb2HmqUFAaXNc5W1kfHess7Dc4x9aGRVktjxlAFmWW9l9qpWR4segLgoz8fJMHoCtJBwg8i8IHU7CRqhV0I557DVtVJlwvGoKPzKwvGcxCQ82BvwQAdCZtkHykFzyH9QMTIzmURPgSpHEHDeUJn0PJBE8m55fdlAsiOZ160rOeXMM07me53mP7ZiVIZZJdBkBgHxf9vT6lHw5PXOUaLSR12Aso8qilar3WBQFBbLxWvcxWy1wIcq83n67IoZ1PwFALxzxiqaBp2hjz8g4fygvzhdB6B9FmNVoPwOmP6F')
	   ,(144,1,0,'rlBgbsnR','OE','OUGHTPRESPRI','Ny2Mn5GKgM','LTzWFrOyRmT10o','2oIJGpL2JotHS','KC',788411111,'5136071665876952','2006-01-02 15:04:05','GC',50000,0.4218,-10,10,1,0,'xYbapROEaqLlHDISVf417CuWePG7FOAr56XIiZIkbzloxm59WHubfI6STQiaHPnp9Rk6AW1YiTI06eWBOM0nbprdDgNc28SvR2NzjUIlVMTqQyTsp6XF0wPSmJK4avdFth9iPBMyG05r99SFRUJqxCu3l1ELz9DxtgKJoqOaM0GG5eyeoQZSXHPGJdG9zsG5DqgMMWKqlO4lNzYD1jYspIFwYYzNpd4uyzzs50qENURI4XIAbA1JcMruwb5HkwZajJdbffxGPazdi510xGpHk0BMN9F16mwZV3R8cvzxvOkSzWo1L016DRvsfEfBtgmD2OEYWwtE4fbPmisNXvbP8XpEW0k4uLC14E7kTxyVC3MLQBPpT4KufoBWTohnsy95r4Fp95IIKpKVstEPvja8ysFAah01Ec2ISvRwwh9hPT45U2faVzc4qhBmx')
	   ,(145,1,0,'siNf0V7GOgeUK','OE','OUGHTPRESPRES','RnGnpc7I9Rh','aG4tHzsc99ZUVb0okgU','OEUjOPoSUG','ZF',061511111,'2763975312111130','2006-01-02 15:04:05','GC',50000,0.0346,-10,10,1,0,'IszclOvm9H4BOShfAttKTMVy3SfEaQpagHt71fAqzkWmzMcGyp9tCAAIdnbxtv3YslWmCn9pHr7ftxpAUmciQAXuqvLqnTvjRx3uo6Us1MejAy7XiqeRKkGCSiOyDl3ADxnxTCK5IX3NpmSpgBGGAvEfBSnhNhIh34oBElQIRJMiCmsTS3DkU1wZmOXtlWgWtRZ6s5IAFaUW7kwIn1oVweFhvIZwd4lYxoUENKr0xAGdqINo3QKc0tsuV57s3ghtaPaZfLudLEqbfUXVJYXrDAU6XTfkMrKsQYgXy2yeMymAsYe4okyMvqCTizBpZhtIxkXxv3qaLQEuUtox8O9Uv0qasKPDUsWqGNd0FlPsFqXm4mpkf5SQeeMSvlaADeINm5E6')
	   ,(146,1,0,'bA5zD6D9osU','OE','OUGHTPRESESE','HPox7yR6fxGvmyBpUhuW','icm7R2NrABEtev9','Yqc8xItY5A','SX',169311111,'7339413542175483','2006-01-02 15:04:05','GC',50000,0.3674,-10,10,1,0,'yHqxlSi2Hu3fFdCFOepwk1cryetk9pNwCzOQ8BI9RTJVwwmJ2ugPyWACOS8K0i9kNoZWV8s8Z2U1Cf1kVq9DkR1vPX6Gj3wtXD5J6rYa3XNugWgaAMG2Ko6ikbS4RQmLgmlJHDUQwPixPmnHmbujZxlJ1nVLrJFbIUX3jktRw2ITDs04ivzcFI63il5tRgcN4XVotUFBEV93nMBQLawUTT7KCvyHgeMDODjBnU7VsXH8ep6zsmyTFg7iE1e4vejXHUHS2ZwNsdxmVe7HobWDezZHveVnHKEjQTQw4pT4F1ID2F6l9I319Rkyz2EzhlGZwkD2UkdfsJb1hxN03Aj8jtlimInWvHBx0hr1N0cfeh')
	   ,(147,1,0,'VjoW1YTDlkhFL','OE','OUGHTPRESANTI','V5LyFa6SDM83oL','8ermVqP1fq6v','cJCLiIfK87LxMLTgeaO','JG',317311111,'5357324237855516','2006-01-02 15:04:05','GC',50000,0.0261,-10,10,1,0,'xlFub7fNDGXHQSQhoFcDRSixmf6hm4aveSPF8qKLB2JkzH1oYxtRv1YE93MKUJhzSPAriqZKxhN3V9GpAmE8jJ5j7qSgiT2MLGfNAZcpAxFJPNQgyLtLrQmmJkFJOV8jthlP1ht5iQi1We544i3U0pTUaSio3UaEjVtQrjSAeNTZWadzmCzONqBjvqwRxFOmCER5RhYpkOhrY8ws0w2yxt3thFczk444P3b4NoZykCIO7STkJYWqJBRVJfUDvO0xjE9ZUcqpStrJsi10YnWTpkeftxmBUpX2qVbFnwcXFKUfsTgaOmGcQFeVucjhQAgJaPEEaanNcQv4ILPDXUj0m0xbERWS3w3r9PFoAJcPhYBtGYv10upZJbrxO6DKkhpkp1WcId90SHmUEdehF')
	   ,(148,1,0,'rJ0Kh90b','OE','OUGHTPRESCALLY','8JJMBJaRfNGVf','Enjy9crKYSDWCVmqjr','6RVgHtOvGVqQ','FB',190711111,'0126521854361283','2006-01-02 15:04:05','GC',50000,0.4662,-10,10,1,0,'ZgaABJAt9MAbiTsbLbtcTAxM6VfdrQ1BxBTtNDYkd24PhDWfMQg5pLjaN6NdqeIBzkN8YrciLJ7Qzf1hlhsGXWO4eQtdnuDMo6MePjQw7bEsLNbKKKG5CfmYZ3Fu65bMscl5qK1GqoDBJCeVbil8I7pVb6NnHiyHOZnfhKSpmg4ibwbFJHf3vH61sf8tKDVV7sSIxJ9mKZXjCX0Y6Fjo0UCbYrM7baKJxnoHFc2iR1Z47bguyjKvHRsylwTMISVsn7sEx9Pu3xGuNFlMOHepklKjCkDkcN3bsrrATkD0tTFghR3tZcf8TVXeHTpxtu6rVHxoTDUVg3RLxJc4tWF71ixIlaSZJrTThfSHtm8gCQKbWx3qxFDb')
	   ,(149,1,0,'TUgyuip7WGGxuGiS','OE','OUGHTPRESATION','EdwjOi4Rmwbqt0','5oTS8TmAS1WEvF','wCZDgovtHUKdPET','UH',485511111,'8731381439503898','2006-01-02 15:04:05','GC',50000,0.2847,-10,10,1,0,'8bqF57FR6zeN0tSicUVMui6SrP2rDgYyqZ83XkUnX2RXMq95gep7fBPOsFxXL72XOOoY5FEpcC6Mwdve2q6TMtMPV7qWLNu2jf3A19Ve4EuTiJbBX1WxMEyJQ8BMkxwYZEwmSiJCTxntDwLr24tZ8rm7FrztjLa1JZqvTBfC9PW41HU3QNXqQgWeFJKVL0dCJccxmNsIirRwxRxxRPugw08eL0CEVPwvXbI59f9hsPnFDWhBOVi7DPSj3Yn1SYMw6oi5sE60joWwBNEHB9LIGdYUsU6u2BSrKfFUUDgEPEy3letwFOwKQ9o1YDg23Dw9riKYBksblnwO2IozqzzlhMb2ZaPGBilWZZeZoTIMzm8dcmJt1SMnOeEoeVLaH7C8B4lkEnX5o0wN8484EJXKhbK7ByS')
	   ,(150,1,0,'8PoWaF9xbGxggy2','OE','OUGHTPRESEING','NWcBAZJL93ZLzOdMvOZ','I5MrTeGXsOhF1xw','FtX3IZ0b2aVme','WV',260711111,'3112853019507451','2006-01-02 15:04:05','GC',50000,0.0972,-10,10,1,0,'NsrnUQleW4XH0GB5REJKIuod61tPUPqLbdP6SZRIsNBgk9IXwXcl7MaXi3St6qpe9e1gt18xAL3noynkSxSSnBkNVIGL1I18pw9UWG1EflBH4rfLs6o2NNXkqnEKBx9kAXqrBzj2LZjVLxywssgUGlrE2IaQ1qJk1fAMV6HWQLHTyQdJqMxw9ceaaHEQUfpaRqru2aEWD7fu2PFuaUu5hXMLWAPu5FePsVEp3WD1MkZfQ04BaIxniEiYvfMpEG6dVAbKsPmCrbT2YCfdhJ8bgKm65CEIVPLo0kdiwisrDF3tc5sIjhtvrfFe78rWkF7D37VEPSswi44on3wojoRcLUKfXewakPe6rJj7G5oUaOUPGgcT8yIVgoksaGiceAu5tIPikDcEz2savk1cOdrPiMCDleCEvrwNnfWkLyPLCgPICsB3jbqWqNBTm6073EsdAdNa2AjpXZwHF9ZYKfCpy4mcgrMQIDSaJfD66PSe')
	   ,(151,1,0,'u1DzR3JHSY','OE','OUGHTESEBAR','XPAThTNPX9Evz5d','XzYcgggl2GAA2','zAUq8DTAIyzz63R0KeR','BI',574811111,'2108909210605050','2006-01-02 15:04:05','BC',50000,0.0158,-10,10,1,0,'DgGj9rhEiT13M1ybu3pd4rfgiSqSyM3lJ8EBCBSWWw8IJuGKKTyZhQoV6jKruFsXRdXLurLXVA4BMt0KnPQwUMTiqixfkmrIoXgDjJFe93edqO3HAc94V8KWczj0RFgtqOmiUdqefH4LB3n6dL7N4oMPIzBrAW5BinK2u5MFVW1ncod6Y0qNZahkjJLI1ifbIH68iEUvKuao2gtRwpgTW4erYAvFU1lB1nORmxA3HR2Ibpm5RAPlMfyOtPoAT53oQQynatCRPKYArPj9xBP4uhUqtCZDhhurefsqLj5qJAU9Kmf9mPHAAH04QRWAhtahXK5A9BYGayPDKvyLtZle7Kue1SwllUW6edKzYxtbDwcquveLVw0YGStfiU6zeBu5DRLi')
	   ,(152,1,0,'2YGad5MmLhc','OE','OUGHTESEOUGHT','dU2YqyvfbbMG','GYvhMUbPOj7oSqU7TDXq','EDEe951sQwEA','HZ',914611111,'0421876259492766','2006-01-02 15:04:05','GC',50000,0.444,-10,10,1,0,'fWoajRIKSVrRTMsnGJpQbqX9nyqh5IpL6L9EoHQ36pDhgHVgTYZJpkrKenlfbYhfixRGe5CiDQ2J6J6jcuG7Js81PC6WOc5jnchiE0KE3fwFaEm4RzgUXruxVFvvgprVkLIOMPqZQ10iuefBYRK0bHVgVteDR4ECSSfz2nHMIpVAnnDeAfsDTJ06OZHTZedU9frwy48rlYWx0BK0ZpELsLhEjER3mDk4XEcw6FrjEIoyz0GB5b9h8G35CF6Y9xjzi4HtUlMOjdZrl9ziR7vP4xNLjOxi7wrZL7ZOC4R6lXqL0Ed9xTX0SqDE3m3rhmQ3NsxPRul1haltIUkPyhMHMvKXvHAMB01RKwXrUTTVcuifhc6zYeJixR4q4Gq9LyhFI7XZMOF9diQwMG')
	   ,(153,1,0,'w7cZtWMLovSunes','OE','OUGHTESEABLE','2NiPfwX1mcHQNJ','b1DQPJ8jSr','Pl8ngjUkDAWRPWF','YZ',748411111,'0542597629889672','2006-01-02 15:04:05','BC',50000,0.0062,-10,10,1,0,'kyNwfEjIT3FpDDOqXTwS9ecFEuUhRdOlXyaGrAvKcOTgXmOykYiEwtMXmYRgCxVnS86kk2z8rohjJ9zVZOewzMjRANSdEHa3mcDumP9D3mORE8BDrJC9gYcGOk4PL5nBMhTmk5sGMpioUlvpYdYIIR5kszbNwOY0XJWOcKWFxNvEuCYuwFc94oKknmQlSKbrbCeSU3fsg4L2imtOyIQ8clDrhYkRGRue8JlOizaXOJQOkgWG15rRedyqZZtTAxTNPpjE7JjdgBlVaTtgqhxjimqAMnHxR4luvmLInKNsq5H6KVBSEqgIGmdkfUBVhIiUPGkcBLFzIIXiQBk9QsoHrPa3MhBLuDT3XkIHNGeR4ZaQJ6zaZs89mfhExQlcZ3ev0IrOIS8gt7w2')
	   ,(154,1,0,'LkVq9eYUe85UkCE','OE','OUGHTESEPRI','WrdCDSoB4SA4z','PucIWUpdfG','9vMVIklehPblujR','RE',401111111,'6394284054065198','2006-01-02 15:04:05','GC',50000,0.3229,-10,10,1,0,'WQBQtzPYnWzk7nK6GxNd61GyqGcLlcWNNSuNG7lbfE6lCZMuRQNEH5hIYFVyPocuL6n6mS87wq7rZhDTTMb0WGQ62S9AZTaSGMk4bYlrQJyL4dvgOUrGn6GPSYqoEeLghvwuOu0ylSUgx3e7TFLk4GMQbJbO7RTmfnzZYozFPiggU7mDoohicL8jEi59cWmY7siRAh6oPumb3Ty4sK5hZ2tvhruh72bPnmJofASearj0rnEKMtOi9phmo9RkkwtHXHoLUnuLcY2Y8bVbeIRqmZfK9FlzYrZz4r5yS8zt2LkyfFDL70muMyiVlsRMulxOKTjRmpWlZ3wvqVyv08B44n4ylCfh9vWWVQmkM55QRFsMf0XWpwFzIg3JFFzStrAdSt8BajCQTYpz9CV4mxAUBUEoZW7rCdWqrdSnQhgo9qBHIFBj3IEDvtzCOV1HZZEuo5VqkCnAIcQPgRmxWMIwmPW5gsCLNm')
	   ,(155,1,0,'WgTCNDxydahuYG','OE','OUGHTESEPRES','BQaFGkpFYG','8vbgeI4L26UH8ds','3e0ne6RJXnPG8w','SK',368511111,'5421601543780166','2006-01-02 15:04:05','GC',50000,0.382,-10,10,1,0,'6NakCVEXsno46oQHQM0vPTgCsCYzaMJuXzomfFSaXGJP55KjaBNSrG0hiJN0sJoqjzSi0irlnUgBOZLyEJ9cvSf8cBO1I6JiRvWCvgQolDl3MaXuYztNCyPZFWdTx6WF3qcCTwb675RrqIl8VcBDsHKd0cvSIj5hdDy9jRi6nYpjUQXsQh5nOme6N2wXEPj6OypM9qhz8cPbaHQ1Od1uTJUj6MBQpmKsX0T5HROAlkD3ZmbalXomuJUNQU7zVPu2eloYaqTyoJEty5hXcC1dLk1CeAmGfxYs1uS9KU7sz0N9TujvZa08zxeVYOtFQP7NSRPZHwf6Mu9zoi7fGeVAeyBjX')
	   ,(156,1,0,'k5iyIyR3','OE','OUGHTESEESE','yCCpnRd8b5Ik','Zb8QirGeFWzOa','VsqrlYuVZsI2ac','JN',518111111,'1054905953854390','2006-01-02 15:04:05','GC',50000,0.4473,-10,10,1,0,'iegHzUM00mNBa6xgKxZUAmnLGJ8QhZw7hy2PHjZ2q7y9uJ7RcFfOwKlGCRIzhHkLg0z3WirEUFWkGq3wUgmD1YIK0HWKUiA2fD24iuHM8We7XO2ryOVyv5sXjuXYH8bte7uYaTmNj5Q3SqCDGXnWrbNmYnY5MoMDvUMPtOK6ojnTT2MnWtwq0224Y81TGpnkIkSu0p0yINfjwaHsD4USbheJIovIfbgAygeaOkRunkuk68IQXB5y4FjHryZ63lxXNXqBFNjI1AZ9qqVRNdhHknsGHi47lBpDXWHq4tYmxZyv8SqTJ74NTqGSNWXOaXrrR9z9tNYUSyAKZLXRmi0BlCRAz3IqJDyKCPt0Q0Jovrm2F6UK59fQo7YnT3zAlyKgyVJXcx3YEa7YN1QKel37pR0wmAPQMMVih80yqiYXlWzMhZHlo7zvLSyonfoy8IZ16iH6TrpUaKatKKYw90piHGuxlZEzZuLroUqUt9ZyT2')
	   ,(157,1,0,'HczEFj3Rtj2vq1','OE','OUGHTESEANTI','aYVF5VTcmxtUrEv55R','nUnUKMGOIq8z','E1YPoj4rZIh','PX',760011111,'4160580368901380','2006-01-02 15:04:05','GC',50000,0.1413,-10,10,1,0,'FKEznAFSrNyj71dMh6uyyMFc11fiQPaVj5WqPxmVzPLcrHk6H96sxUl5LdYr6rRZ130VOMXbVdhg1U0fgNJCz6wm3vDcxewYBVIBbMdFjU28Ll5F8Dpuy1kYSYt8xhSHPKNdYGQitv0sbadXXoDQF4XafyeWzGqL21rsXo2ShCr3RlNqYhfzGGV5iNJGWOIjGSmfZlw2dfFBKMZA7slhaaenmInsfBh4BV64fIZGag5cGmEvfhg2Jr6MPWxxneSeHGBXrFlVOk0ZrcKjXtaxXg6PqGbodZ0AzTSJZ2qNLRr3e55WAV93VXLDp8XD8D8zyuUAHre0g83kL296GzZ2zv2rTxZ3TYGrwXM0ARHk7IR3LkN')
	   ,(158,1,0,'L5I3lcS5JKyojNq','OE','OUGHTESECALLY','AiJtfXUdceFZ1bDg','HSScACtboySlyL','k8cCGoPYJwbLaw6','WZ',306211111,'4298877485803082','2006-01-02 15:04:05','GC',50000,0.0675,-10,10,1,0,'SuA8i2CetSNKzy4UeqjR1FiPp3zTaTrr9ljmJN05pzSi0Kci4m6yAwEdLr2xL4qU0wOBRVhhGaOrEJNce3a1ZfVdcIaaJjAN5S27hUemjKBrjtGY4Heev6RH0IMoRu6YOtMWm6KeShKKBKJbYLe8Wm9DE0Ycr3di6gHTQWBLkPj9L5Uy90RxGM7Uco3I4d7CxFK34wekn7QEsE8xV1uaXEO2tevlvGdmGBhcgQqDpBNx7YSg0qKSe6XWPTvaTrGdJ5pCgzXT2NlB27KyxcqVeZzhM7WCgdHkgdIjQ2r72SJ5KXUxOb7GSXn2SGSGwZqjKicNJeMqlPRy5vYzvJ3yeo3LYRgCDYBOgKuLDNEStoO0aMwLmi98v')
	   ,(159,1,0,'F7eEHZ0ZH5XJKo','OE','OUGHTESEATION','pWOgzmFK6KIbP1','38fJk41fMtllWA','awtTUEwMhlGsP6uKGQl','PB',690911111,'0086392379285695','2006-01-02 15:04:05','GC',50000,0.4084,-10,10,1,0,'mxj9Hu1TBD8xoUvBeuLa42TAlrh6UFmYVJtt12K6LpZBbfC8OPQhm4RbpyHpolW8zMitBJTQYk9nFEPOLyBHdMeqDQjNCCGG2msBpufDjeZVaAZBG8bdGFdRyVTN7HpOvzBdJ6tAakoSBjJ4ETUG9qGsktCpG1vJF5npjeJrTJS6PRs5sVtvHKtg16PkBf2fRRYka5T22eGDadWv35lBxJ5aEgoCvzQNyekMoBUyvqVF7UHZB9GZAsK2sAXhwsuOy1hulP99w4imW7priEa6iD54CGdHYWuuoNzJXfYn4lehkLGsR3edaKrmeqyYa5X17OubOWXBdEofjrgmt4kakXL54VZBD8BcABRztIq7KHbt70GRaqrtG4IGRLPlPZj4zVGShzsZwfeUvlcsmoN3j9QdqNwNHyT6rG0FJXNL4ed6vKwGNGBx7bvDjChLGAzNItRYcvYiZvMjFrw0Q1UCScHabGU3vu1lPBXKD')
	   ,(160,1,0,'DXjmQ4iZuT7x','OE','OUGHTESEEING','dZ4BRH6UDJegcH7DjP4g','x3s7tgZIkAQ','06TLCtuSx1Y1ih2T','HV',965811111,'3049869126829675','2006-01-02 15:04:05','GC',50000,0.1707,-10,10,1,0,'whY8LlMIOTgH7CwM3zh4kEesejj6cL1AMVrKCZSuaHdxZgdlYxEx0AKpb7AmRBUWiVqquIfihYiVHaRqiUVHd4SLWBIm5ts5hjC4uHeC2MClnYZhmdtuQccfNefpbRGx1elQ57QP0DFI0mict17IIuntfck3J8IRT7Qxg4rvbDGlnmmc6ou3z7WY83KXpyRFmSsn6kFXNhNayDIzVDx2t1EcCUS0zGuyWyXHvuyLqZUWf5swDQTT0IEAN04fPcTrqdCGNNqkqFVXR2CzVlIVEBL6jDcFtdYM6EY80DPUobGcenwVN23TKy9jn58Hzv4ZDjMiVHnQbo1JqYLvtBBitvHQAu2wkt0Jb7Px3dHCyoFaA8AHasDvetSZeVEnaOsVJnBV0guIwKM')
	   ,(161,1,0,'uOD3FfC6nCHmFke','OE','OUGHTANTIBAR','k18jnnPfqCLK','wo7rsUScj6a9boacRkF2','IUYDZ13DJAJWPYi','KN',036211111,'7557625057341284','2006-01-02 15:04:05','BC',50000,0.4092,-10,10,1,0,'4X2LKeLIbKSTcLsWayhr1qE9pMmMvjGHbf2hXbqu41gIVx3xPddRlrSzu0E3leR7IoXnpxxL8fTVb8wQcMN6M9vw7RMCPb1Af8LmvJ6r5t58MMkEdtdHkmh7d9Dgxar74kAH1XZG510iYZ37tKMFkFJpwarr36kWMRvhhtsbZhibJzkKtvDUUud0eUmiVz0bwKwurPc5y9Q4VpJVZocgfoHnjy3k4BBdA32Dn57v75Jcij329hMFYYvsEGubwoVv7f9MNnoqauVU7IaUz7X0DhakoUEzhiDKhc3NAeBO0i1HFSfqw06B3bz8ZgIxWQHxirkOQKUoYeD3RF6j0b9GnM4Q8REFtxsVptCxQS1OTxvtjPJvYT6CpaKNJkwvC')
	   ,(162,1,0,'yOtZXuZdI','OE','OUGHTANTIOUGHT','Wja22z12aOAcjYL','1vWgXtFPGJg','mT6a03k9Br','JO',889611111,'0144418379794523','2006-01-02 15:04:05','GC',50000,0.1602,-10,10,1,0,'NtyjNpfVHSW5mOz8cEz5vqLseooBFgEk7VkQvD8Vh1ELvPlJpgGi0YinlztPpgdg8TvbzCszo2gLgP3vemiF0AbYAt0YinAzs47UMsffEYifHV64jnS8rxtLG1UGSePkfmPW1mB8M3qyzSZIUlMBBFoNY8YLGIXl4oOzHhJjqhxIsMkPgq5aPCPmLyQYQDFuEDORhRuL3CPb0ebowI1MvfIY9dNo2XySUR1KxTCBtiqCmc5JNLKzHkZZxfFa51GWHFTpZvsY0Rs3QDz8kMUCHOH0MPgrU0E611BITjgIMOYSJbQTG8bAHpOSxYjJQN8nwunIPy9QFE4ezHiajP7EVXCrUWmy1UPURa3bro8UMNwe3xTOJRWfMdTgFuOJqCSo1eWUezokJNApvEx6MMERhc7U2Q14aN5dJd8vx0F7bTSWeOI7ALTbFNIhrta09vcZPpezDxG7HIufPR2CkIabEWpSUjLuRHYpdB')
	   ,(163,1,0,'u1aQWachzEPcOAk','OE','OUGHTANTIABLE','2U0lRKKMg44Gqb7','JlTjOf07p6yiuS2dP','KQBsFfnGF8r','KP',203911111,'7040758863909937','2006-01-02 15:04:05','GC',50000,0.2041,-10,10,1,0,'go5rVSKUGXjz7FUNlyAkmpjcOG9Qasv51dlyzOWtHZIRYH9aERydpQQmf6ms4Fc9Ye3lwIQxRvgIHZ28978oC06tvlUGkLzK4rOtxJvP9N3kzCjM7p6G370pSH5UwW5qbH2aDdZ1uuvuLTc5ywFU3plavluVX9dVrfbiu18NZvYm6UZKbIaaEgHyO3AmICXNILIQmNGqsZPDB5qtfi2nqJ9Xk0LxnxtIu2QySq4K0x8UaQCherCjpU3EiHfY1jdzCZjsQKET4pI6f7SUCHz8jtwjv5tumW1XgltqR9XMW2wu9XsanTTPWwYwc0ummXhNeWzxFjHGOLMuzzgPVGN3ZdfLwg1tlqmNhGNLkNwqH22iD1g5C6PlBxTaKAyQKPk6YufQkLOagUWhRhyHNfzoRfLFKz7zbSmvG9L9DhfoWgpU06GPkFyGGoCJgEC2jOJFz3IL6IQKt0oyPDBEuuOmni')
	   ,(164,1,0,'9tvtbHkIrefmpe','OE','OUGHTANTIPRI','n1IC84j4Z1yHyqjBzK92','8SyMSx8a9C','rDX35XG6bBJpN6Qo52pg','HF',457011111,'9222324562175989','2006-01-02 15:04:05','GC',50000,0.2362,-10,10,1,0,'oPZtDBZHA4NmJEs6niSORgWrRoINV6JerhHmmPYofVaLm3PXxJyXKzGDVY62vcIOX2tQbXDHZzYT86Lre1nRpYiinlIPOMnwvv1XT4qaNNSdmLDoTVwBqZO9Cm3NjrEkb9zAAxHNzwxEx5YYVdZ4Bn0O1rsqFVvxzI3VwhoRJiKUDclvhoa8wAtO5aczG3XX8BzeIzltn563i53GZmlXoUxlVCXryiTvCgGYyi38yuIiCM9oArC6GUHQGmiIRzkr31XCSuQKR1t3TVCzfFGAq2zKHgtUrZ7Ai7bmVplIfZp9fJfZSZLpdBoUraDTeUpL8Q6IgERZPHNS7HtWwTM7A2hWELSX7ONodzMIUmjuvbtkRzaI1C3uzvEKydTxYRnucXY8VwQ7hQwGVN3fvPuN6soNgoDp7tJaojV1E2KPpFCofY1aI9m1')
	   ,(165,1,0,'5wzOEhvGAAblX','OE','OUGHTANTIPRES','FWvQPMnx7CDWqcF','YMM6thAKaNtwab','T4TlptzbsQoPv','BW',749411111,'7769721898447385','2006-01-02 15:04:05','GC',50000,0.0243,-10,10,1,0,'QduO1TEvH7bbspVNCZRiShqZOp570pThFnD5DOW7yKQsOGfduKSrxPZQTxbGhrFFLGOUchyHAMopMs7mZb7HF85IAkdgyowHNhXprnJ1xVbuOHCZJ4OfgpDpOyMcaoJKwwN5nCFK4yJwvsXBScISGrsP6NwuyzYcd48ylFLLYAZaNK9X1WjfYqljtwU7SgdG0hQPRXTilxKcviSY1GowMBNFSOB7btnaqOLWEZAdx14e63wbELQFdaMc4iUhlxd4ulKneOguvCCP8kA81QB1GMGAW5aHyBI8FxP1bJMebZbhOwzEHsE2al9pb4zfAGDBy4Ql2u4n6msFoO7cPTfujkzrvoe5J')
	   ,(166,1,0,'Nbi3Pt7v9b5Ir6V','OE','OUGHTANTIESE','4O63dcmNPXw7WJYeHOX','ROjcjcqOOCn72KXfPODe','rW7O2dfD8wSCoZeMzH','BJ',818111111,'4448811094770760','2006-01-02 15:04:05','GC',50000,0.2303,-10,10,1,0,'7ZEZXcJkPhpnRCKQCFw5fT69vwx0eZEnb00FF8jaqwmn6uoR5GgcXPmiOv5FGMF6XUu39i9e7IkdRCHZq7SCdhFh76zrZgHbdyr9wVCrIXk0v1f76QCt6LfOH3Z1KS5JLjgNEolW7GBVTesj6j9gDsUoTjusFMdav5bMG5lxoujt5tCSrLVHV0o5YFHP2sfgZuQZjaqCNKtN3yhxfnxBZWGdUV69TFwnHgWaaDdDcH7Vn5D44mzgvDUAL50FeWAz78fKLixcVNfPJDkBnde6cCzmnQuXWbZlLQ61D8iFuOMlyVZa6HppiyKFuNTan9mjedcPOLdcir71tuyOaM0G0rDTbPPoKOInC7SAKnEgkpehfP')
	   ,(167,1,0,'vOX4CeA4lHhy','OE','OUGHTANTIANTI','LERRyE3wsz0QDTmZf','JfNhRQyw54h2CM','n3x8WeBWSk3O','KU',063511111,'0127930924978973','2006-01-02 15:04:05','GC',50000,0.4923,-10,10,1,0,'kvMWXpcD17iyLI9XFPcl3NIcfUeppYG7IoPV7XJ0A29xnzGRrjqvQfyK1ZzmbvHkiVexxtSHPWxRRtC0jRowDtXtBhkorbjZkedeSd2N2oOuY42oj9pSSdjkPSPwhIDjvs3H5goBh2t1Q4bfZWqNbcLuc2hgDtBCzWGJAi1p4uOitbQXYXjElXCHog6H3MTvjt1LfOoySdRlrxnVArGHzcuJ0pWu9frxFdXNDtCejI6k2CXeyXm63Mgr6hYVtu3gY2uFGv9dIhJwuKX0Kd8RzviZtANa7mRcBa4o6Yh8VYWxyl6Eb0Ex9N83q4CbxIuHuVeBT5AI4Sh14q6tS2StqmXokEfCVELjWrRCZg2e5rFqKunEpaZKiDXXLLU9Iw0OpjBsmSKe74LhqbT2qRVM9Gao8jNToKYgNA4NNWOO8MssGOuDyBp7WNXlcNvEt6PKNsVraEAcTL3VGWh0KsSyfyr9w7a4vvAnyd2IJIrqzqIj79Pq5Fvq')
	   ,(168,1,0,'7hqTlCwu','OE','OUGHTANTICALLY','DE09O0c3kx9UzFP','O0q7SxS3gA','Rcbc0r17uuOgY2bjE8H','BY',300311111,'1100625311954827','2006-01-02 15:04:05','GC',50000,0.2144,-10,10,1,0,'ZspKElUGkzZevb2KMDlpjjW8BHdNuMGGsOOjt4OaRsfYYCJgxj8Y0GVcgrTQ4TWwQBmNK96yccqWDDfwKsA3vdMBVmZMvdh5PdO9uYtaQJ9X3Xu6zefViYIrOEFIzE98NrNzP9ivljZJ20BuhOgVw89sa7BpAYMFu1yZPVQUuAn9PO3Kj1ugGvbDxyfdyfTxlQ1A6ZEBbflrNEhCzIfpsEN0oai9Y30uLeJihR9znXyGuDUx0PVfWBCcZxt1HTYHLZdFtHSA0YEATwM1OgovznEexrnPzYImJObsYFExVyhgIabZOg6cWrZYPUSdSawwms6TLu668JtSlqQAtGPefJtDRKub16n9Xhkh0m83wf6RtMtrR2mjbVKYKYwpDvNAWD0vpyw3CiSDx9NATy2mhOEFuXw79klj4kK0uBlab2khbdWgDGXIBx0pJBcMZqrNoVY5zX3QY2UEss1xiuTDWxjAhMRBa')
	   ,(169,1,0,'L4CrQfdbzfl','OE','OUGHTANTIATION','yOm7yKKBGi9arXBm','x3bqMnecO7yyQfpXj','doCQfUQV4aWCsCzytPS','ES',671711111,'1590102910098352','2006-01-02 15:04:05','GC',50000,0.3813,-10,10,1,0,'cpcy4NfLEheHRZZMACYy7jjqlStKJGNbLjUAzWFQGhhnXeE2GPPbPDXB9wtqVkyyxSnrZjK7taDcQTrfQNiQdA9ISs73bABjlZLkhus140zkcdHFKKZAeEUBrc5d738kYBDlg7AihFgxVwOZFn6zjmRRsH7R6eGirzk0poBMVRyz4StH5jJABmHq0lMMet0tHB9moQqm8xfpXRvQbyZfqDih6uJxOxWWVgO14ho5shbJjn4g8R77EYSnW0UPRU6ih9dtLAXqMVjm5mTrSJKEioMUxK5E8SMn5q8FmCS9tPc2qaFCEa1gdnyK42GvSZBTfkcDH3cCBpjWSi4XECrmsMVQ2wVV1FzbXTtjcANrDB3pV5waA20Z2GLZD8c9BEUgZKySLcqZJzbh0XmaeFVgEyNrvpH26H8CjVRqaI8jOLVsdXFM1UUiLN9YE8Zb4wdg42iRMLgRY8FdVcv2EOA07vjX9FXApcoUSLngv4Q56ZRxnt')
	   ,(170,1,0,'Fox8QppI','OE','OUGHTANTIEING','T2yRHsFWzQ1wgPjDA','YRfZePFKqp89','Qg5gnmaiBj6DGx2Y','DZ',673611111,'9984343680183979','2006-01-02 15:04:05','GC',50000,0.2356,-10,10,1,0,'T8xoxQ5kee6rc11B9FVgQT4QE8m6smC5aG1iZwoqW54AJQiYCMioqjOX9UbCWz56ilsyMvmi5Ll1c4vpTDJemxCpl3PXpvjKRpXIpdu8qYpmKmMNfpBSO8hPAJEVTD6RAz43D2B2Y2f1eB3LPSpkmuaxTT0A5Rnjr6XE6xBrre2IX2ZU9L0xqaI8zZFMzSzBaPlKmi2bqajShaemPvXib3uPudchiRzvENPZ7Gk6RVwRYI40Homl6PFWgxV1lOghXfIr2VslEdLD8MuSOQ23dg3QiXUNb688y0ZvEBaECOjhibClCJa8wzhJZOyYim4afAfsP9i8C0JrAq1z79pvKb6f9z62OCLyzT8h6oX4DsPx6igNc9CeibRlsaIgR9mAvxzCgsnIgsYnQdEuIzpUyfO6XZ1FvXHQkcBZkDjKWTY7mhRpNacq3HNWG')
	   ,(171,1,0,'UeUu7DN195njl','OE','OUGHTCALLYBAR','liHy6sojVWWmi6u8ec','fih7dVcxRiKhL','fYjBG293NMierizFmJR','ER',262211111,'4684172970298340','2006-01-02 15:04:05','GC',50000,0.4032,-10,10,1,0,'fu54uAt0pggfuFn7TpthlJzTZ9NB9ofqDVQJvTxtgk4sydEvmo47aCriH3uSDiOPKvk8WTqGlmJutnN0Fj9k5Q8QF3J5jZL1X6s1Po7Iwdhk2epAwpDglAhWXWPaBMPMVdkrgTJLI0MDZW3eLlJH0P1zQaojUfNmMQtBAGdsvQzFzIptkC2JU8E8Rvxm4uF7b1HUb7ZarMTOdOoMOaTlc5fC6UUMr7nSQEr1IpaFvJVXWxH8KvGkoc8zHTqlLgJIJG8ofxkNGUroHTnuzeI4adGreCTA4BHdpxIxisRhglCogWiJzzDz4aF5kgPgFI7iY9uqP2M4oNQApV8z9UYmHoQ2232nljpR3ksV3FQ1OBqHUCuaYrw59Ux7aIFRiXBPG7fhC9HtOYZdgTrOSRFj2wiZB96JRiv20Pj9CZutafy9e')
	   ,(172,1,0,'1aEhMfWAky8Qv8B','OE','OUGHTCALLYOUGHT','obwV1h2ODifQV','wBVhK8YHpK4CMjh','YbtqHYI0Gp','EC',497411111,'9013848322025280','2006-01-02 15:04:05','GC',50000,0.0397,-10,10,1,0,'eIu1re9gSTDm2bIbUPTeiuMVf6V5bbs7wpRbL6bgPz9DFDrgNNx7xB5ChL6jt1R5n2FY9KDSGlSQBzdJGPmHf0M96iX0ZBDsaS3LgNYn7867kRxg76jpDusmGZr1nVZRVWFZPOveDf212SLiL2z3gIUgdm0FtjSx194Pq3g3tc9qnUsi1b8ZcNxFXlIaHYNNVTqsUZbuXmEhFAl1bECB5qnNznXehvr7kYr2bFg1j2dg92U3LC1cVm1652mSN2PtSuiQPGQVVUIfIrcuyFy4LoJ7VdfQglFEdJlBfdiKdQNz9Np5Iu73cF9EcxjLVzO8Wh4nDNzyXOxT4OyMCHwQdbH1ppz5pF8JibNzMBjpFQzcFJOYPeqVMhjgqwRwZLoBurEFxUaxtRpG885zgkq4SQhpnHV9uzOT7KjNG6iMvQ1xVcgiwIUgkEJ8Rk2mAxaa99rTSFumjmAdU8gOCbgjrFm6stPmosZ1x7JK54YHqmdYk')
	   ,(173,1,0,'4P1uBQxN','OE','OUGHTCALLYABLE','qfVMXenM1qKWDIEj5apj','C1FrkCUWz71kSA5','a7rtTqo0A2ae2S1pALqM','TW',145711111,'6810365874986416','2006-01-02 15:04:05','GC',50000,0.4161,-10,10,1,0,'Nq5N2FvXcw419eFJwZiG52AlXvNVcfpJRAr5YFSnZdUTnnnjqLycXF0bwe1POoYDpZlXeRZdSo37smf0oZJuM3foL8nDmyOz6mkyx9izjfpNj5eUgQNYmteYmk2T0SD6Lg05WC55inTb9S2azdLZIO1z71UVLJOakiH3ZBBQUXOgMa2RnPu91nB1HFKrWo85pw9wgldLYeKA2eptA0V9yeMmGDnIVSSaPY7sm29VWkjFncro161mHxWf3AFFeiV2gVUMaKo08j1PRCOQLw2WqjlN4QbzGTXpizyyIoq5KP53s72rOyHBzqGrzriV2Xp3XNJcrQer9nevOgqmLYJLCaC864PDwO9Uu7QlQptMIq9maaM3ZYCPv6fhaIQQUwzTjhjmB99jf7V02f8XK74KgnWmwNJDbw0Hq49JR6V0NZJA4ZUmTjC6HHva1wtJpotIGLAxd0')
	   ,(174,1,0,'LONdYT5hWBVtue','OE','OUGHTCALLYPRI','01FUgUbmCR','SSddHOrPSSVRb0QZ','pibm3tvfdsSsBqPdOXy','HO',468211111,'4138555921093338','2006-01-02 15:04:05','GC',50000,0.3627,-10,10,1,0,'vM5UitMTBtgMCzuv3GOczxv1IajWPvUt0SaDN9cN1iVzqyx2Z2PH4uPWYiyBeNdBE3BFCyeVWCtYUNFZosIBAKdhBe9mb0roKusgSojbi6kSq427GPvGj6QAatn4MxoQJkNWT9q7oEynIZOhtmuXnIzXiu71sTZFoQw5FY3N5Dgd0AEr9AmkALQiXUjCvNTrNFlVuHVn9pnJGTXJbJDqtqv2ADCbAuqxSlhmR1OIR5O6BPchAGhIL52npLf7K2gf4NmJbPa2lnHjiCBxgVMPRHwn76VrlmhawHvKsKmsJtjts3x0GXMYNiuWTSIr4vHjS4Mzi464UVpnfUeSgfp')
	   ,(175,1,0,'SInxQjtmrvtdwS5Z','OE','OUGHTCALLYPRES','Xsa0U2ds5hFmQYFhEQ','HkrEgPeYtr','IzJSQ7GspoBD2k','YQ',474711111,'4219142505139533','2006-01-02 15:04:05','GC',50000,0.3875,-10,10,1,0,'K4arUCxdEWIZT8V60XSSX2bgFvw3HNp2Q5AnQRfozfuATGyv2WbGdWfpCzZ0ke1escCrjbHCtIhmy0IIr8FO4vQ7hG7O2xoYnQ8zJ9kNd93wRhYcSso2BLXVpuwUI4ZoKD955iabuvXBLwl4OmvIIQBUANDzFxweaOjcr50tKlo7EuwtD3KnHzva1xL3A8HDsKSELm1RxhrjXtqkWxmXL4XEPuUIVtYa6BP39C7EZlnokx2fyL5tP7vsNzJz4Vjk5hPmSR3xdJJ0anXw5K4qFsUOilaRZ315OqOHOsFwNNobrI8BUrsO2p2R9jeJu9bXIAJPOx8zdO9pWtzV4JUrWxQ4uXw01sBNJ')
	   ,(176,1,0,'871U1eIhKVMh','OE','OUGHTCALLYESE','rHtRPmEwzxF','IQxcKn0bBDkqcGLQJ0u','p0WorKsfnckB0rrXZe','HP',173911111,'4030959447481014','2006-01-02 15:04:05','GC',50000,0.3525,-10,10,1,0,'YmVPH5ALs0IOi8NH58ex4sSDWYE9RjpQPCBvNzCcnHVfOAtrBesCcupYUlaOwH38fgi5jfrEt70WcLJ4giLqBqCgw0GWcLq2GwrakFn63Xx0EBCOsCxexbjgusMBJWpkcYU8EBS9FUhTyX2trbqmdvZmtdpBaey0IX6ka9P21ERizjP3Md6JlAAr8iX78nkByPmaCWqSSawSy3CSYnj7bpWLSdWGTNQ3FVUAcLtPo4oYaLOtpGzGXHStTVOu3scnOxQZHig99fmqYKhT5GuKQnmPCULwAwrapB15JCURxO8E0cDcfrZKmrMXLVlk1deWQ6zPFc7xZcd076Qe3JAFoM6ZP2wuU48kWGgiSS2jK8juBxQGbrQIj83xRCOlDiTmkbVjesBZ8qVH6z9BymmJb2aNpjOD0bnARlVzv9CDzcTgZbL80HaRG3GGiuWCWIuOHz6UCXPcZKAssI2zBPlhCkdGuKolzxcErh')
	   ,(177,1,0,'UHNwpuBT4VpNfpdi','OE','OUGHTCALLYANTI','6kd6bq5Qsy9sdX1jx','foNMzl8YiWx9XlWPAO','qPLm9LtCkRKrylO4','KK',413311111,'9049566558883484','2006-01-02 15:04:05','GC',50000,0.019,-10,10,1,0,'GeybtCipUwRjcKftOwfGKCK6mKlWEDrNn6xNmoZoDDpyB1i158yAlO8d42ERbw9CLt7L9g6DHrJWzloDCVCufk986ieVvbFFftOsuuIo4RdJ6tGtAkQXeXB4WVRf4htrGjmRYwrDup4okMf4DrTL50vAcafGT4mlNOIliHoArM0pPsEHunEHU8zCVzXsSYAbzdeXWbFMjOdwj0clN0pHORocJILtxmV9yAJzCyzauPHb5VYDzk6x1hpAy5vptwKznDUDDOziCnZoEskZIuLhsclXKLOSwR6VijDkDHGVmbirFx9Ml3jJsP7yL5ZEwOr7SDzggJr3I448SfLXbXIGyH3gMyuLHfgOj9WzP5lc8ZbT1nI59r5Ki0m3GOIaSPZKzi3F7NTKnohwL5xaYPgffFW5PXyNzvM2aIIlz2qg86xd6heySPDJWwO45pMmOxBp6Pl60h9hJKBxpStyQ55Lr4tqd5ky9')
	   ,(178,1,0,'76fU5KBs','OE','OUGHTCALLYCALLY','u1gCRY25eZKL7V8OmSa','Ek39JhRIRdBOirNK4yK','Fxw5FyPOdiGmnUv','RP',467111111,'6353975742464542','2006-01-02 15:04:05','GC',50000,0.0573,-10,10,1,0,'WAVnf4XZjrWhGT7HhikuStbETf4rZNanGiie7Jiii3zW39FXKDtFxkKzS0nDw1qZxKhMEniYW1v6hW8mQwUa0Y1EUIWHeh3QNCQ6hSHv9rUqcl98uloIXXNZdLB2NtxXkm0RTezKkSu8X3uIII6XFFlO2zJBoSAaV1oOuCJ9ZI4LS7JwNF5iv0dI2nNPN463sNxuZu48FIMaRgjmpok9N19QkjeNAEfGme4Jwwrx0OKij490XqeDPlo7854psdyHouVe4NKfBsfTuF0fBA7XgYSqoukh9N7v5bDALc2AQkXvUDaiNS6vIpR9QACGf4kq')
	   ,(179,1,0,'fa3cfwF1Cx53u00y','OE','OUGHTCALLYATION','yf2mSaM96lXRuVIl','xMfUnl74NMyVnEuLM','ttxW1oNKbdJQDYe23s8','LB',944011111,'7624655842972947','2006-01-02 15:04:05','GC',50000,0.2927,-10,10,1,0,'kh6FltWcaqdvZA0m09Lnn1HUFM0OmTD0z2GFRJ3JAUXeDiaXobBo6yrNickVcVafCZxM9slK1u0MHAmdVFqXeQQe5JkAcnpcQBBCuJk1aNXZUub1fzlVPDEjp2OOgCNsiyEspQO1taTZceLDlPctaMC5cHRYihMjWohHtINLygYRLH7ziRGZ0f83ZUL8b9hyVKWs3Rv9VTEMitYC625GygXXeDVGNX32uGrkUuzjUJQngxMG2m19OqJpiCSPVv383JP5PTxi5vSP08SJYleRvPbuZHtkoEdB34frSGXZCTPiLhbMm7NujFNEapygCqqmX5SDNGG89gYdfLrJapu1VNkJirHQbIldsxXTxbEm7Dpg8xZS7ZXyE4AwDQrPTMKNoynGMM7dUqIVVsQ1Cecto6ZY38Am5GpPxTazKwKhFlPJXh44on3rVnFIFMy2ggWoikJhzfFp8Hff7gAFuGOJ0h4Nj3RgJBnY5uD6wX')
	   ,(180,1,0,'LylmEWAKWOfE3Sb','OE','OUGHTCALLYEING','LpMJvwTVJ2c1e0Wz','GxK0VurPrPH','wS0eYGFuuRpV0IRX','WI',409811111,'2224225097426867','2006-01-02 15:04:05','GC',50000,0.0715,-10,10,1,0,'OQQfbaFkzAMZZW5M2boyECNe6f7IZiJfqOCO5VFkIPi1lNwhbUjb2KaK5T8Z4gCXAwoKBuD9wCYgrVUVTzYONMi35ofd60JGWwjPXObJAFPh3J8MhIbiOHddozCRPJPWahJtNNnY4bGkqAgkf8ZOu4oI5pVeMjjIHwG2WX4oL6nN2RRjl5xB5LBFZRXDsmssf7lX5lwcGsy5z8lhqUZBdQc7a2KisBwJfheZAHGmAuDSHANWiUNgND6WJ24O9mYRA1acaKoL9lMew3DKSkF240PUTPIeHCGpEW3Xcxgwzc9qqDiFWYANTMejm2EQjwZ2iDLafjtHvUlQCaSIXbJloVC7J2BdEwnab5lSxgoNz1nj38dkYuwxaOAMR0N5OHLGWl2z7v3eHAv57')
	   ,(181,1,0,'dLRcwfchgmpD','OE','OUGHTATIONBAR','B4jSUSllZYGZF6a0Lv6','mvWK5dbZdxYiebMmzYqh','tkYg8k9i38y','BJ',553611111,'2313472449584034','2006-01-02 15:04:05','GC',50000,0.372,-10,10,1,0,'AnWhnyvD8IBKD0ENeeCtxOaea5N8HGZQHHDrYlRJiaLOfFB6KXW3R5oXsESx2UeNEgydAfCqJTBhBJECcwgXWUXAn0JykyAiaxgnZd4WHNnbEwStz4x4bQiwoDeKkR43wkOoYy9caprNj9jkRuzdsBduXp8riEW0q11XF0OKp4lR986VXEoobxG7AYsEPG9EHbwT9jO2gHvWY9RzeK860k3QWtbKp4LCtTEOSbMRUy63MExzlJMZIYM9GUqUa29WKfncyvxfnShl4pS9vmsxpw54XCAmvxneJrQrbdXj8vwHJ');
		`,
		args:    []interface{}{},
		cleanup: "TRUNCATE TABLE customer",
	},
	{
		name:  "const-agg",
		query: `SELECT * FROM k GROUP BY id HAVING sum(a) > 100`,
		args:  []interface{}{},
	},
}

func init() {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)

	// Add a table with many columns and many indexes.
	var indexes strings.Builder
	for i := 0; i < 250; i++ {
		indexes.WriteString(fmt.Sprintf(
			",\nINDEX idx%d (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w)",
			i,
		))
	}
	tableK := fmt.Sprintf(`CREATE TABLE k (
		id INT PRIMARY KEY,
		a INT, b INT, c INT, d INT, e INT, f INT, g INT, h INT, i INT, j INT,
		k INT, l INT, m INT, n INT, o INT, p INT, q INT, r INT, s INT, t INT,
		u INT, v INT, w INT, x INT, y INT, z INT
		%s
	)`, indexes.String())
	schemas = append(schemas, tableK)
}

// BenchmarkPhases measures the time that each of the optimization phases takes
// to run. See the comments for the Phase enumeration for more details
// on what each phase includes.
func BenchmarkPhases(b *testing.B) {
	for _, query := range queriesToTest(b) {
		h := newHarness(b, query, schemas)
		b.Run(query.name, func(b *testing.B) {
			b.Run("Simple", func(b *testing.B) {
				for _, phase := range SimplePhases {
					b.Run(phase.String(), func(b *testing.B) {
						for i := 0; i < b.N; i++ {
							h.runSimple(b, query, phase)
						}
					})
				}
			})
			b.Run("Prepared", func(b *testing.B) {
				phases := PreparedPhases
				if h.prepMemo.IsOptimized() {
					// If the query has no placeholders or the placeholder fast path
					// succeeded, the only phase which does something is ExecBuild.
					phases = []Phase{ExecBuild}
				}
				for _, phase := range phases {
					b.Run(phase.String(), func(b *testing.B) {
						for i := 0; i < b.N; i++ {
							h.runPrepared(b, phase)
						}
					})
				}
			})
		})
	}
}

type harness struct {
	ctx       context.Context
	semaCtx   tree.SemaContext
	evalCtx   eval.Context
	prepMemo  *memo.Memo
	testCat   *testcat.Catalog
	optimizer xform.Optimizer
	gf        explain.PlanGistFactory
}

func newHarness(tb testing.TB, query benchQuery, schemas []string) *harness {
	h := &harness{
		ctx:     context.Background(),
		semaCtx: tree.MakeSemaContext(nil /* resolver */),
		evalCtx: eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings()),
	}

	// Set session settings to their global defaults.
	if err := sql.TestingResetSessionVariables(h.ctx, h.evalCtx); err != nil {
		panic(errors.Wrap(err, "could not reset session variables"))
	}

	// Set up the test catalog.
	h.testCat = testcat.New()
	for _, schema := range schemas {
		stmts, err := parser.Parse(schema)
		if err != nil {
			tb.Fatalf("%v", err)
		}
		for _, stmt := range stmts {
			_, err := h.testCat.ExecuteDDLStmt(stmt)
			if err != nil {
				tb.Fatalf("%v", err)
			}
		}
	}

	h.semaCtx.Placeholders.Init(len(query.args), nil /* typeHints */)
	// Run optbuilder to build the memo for Prepare. Even if we will not be using
	// the Prepare method, we still want to run the optbuilder to infer any
	// placeholder types.
	stmt, err := parser.ParseOne(query.query)
	if err != nil {
		tb.Fatalf("%v", err)
	}
	h.optimizer.Init(context.Background(), &h.evalCtx, h.testCat)
	bld := optbuilder.New(h.ctx, &h.semaCtx, &h.evalCtx, h.testCat, h.optimizer.Factory(), stmt.AST)
	bld.KeepPlaceholders = true
	if err := bld.Build(); err != nil {
		tb.Fatalf("%v", err)
	}

	// If there are no placeholders, we explore during PREPARE.
	if len(query.args) == 0 {
		if _, err := h.optimizer.Optimize(); err != nil {
			tb.Fatalf("%v", err)
		}
	} else {
		if _, err := h.optimizer.TryPlaceholderFastPath(); err != nil {
			tb.Fatalf("%v", err)
		}
	}
	h.prepMemo = h.optimizer.DetachMemo(context.Background())
	h.optimizer = xform.Optimizer{}

	// Construct placeholder values.
	h.semaCtx.Placeholders.Values = make(tree.QueryArguments, len(query.args))
	for i, arg := range query.args {
		var parg tree.Expr
		parg, err := parser.ParseExpr(fmt.Sprintf("%v", arg))
		if err != nil {
			tb.Fatalf("%v", err)
		}

		id := tree.PlaceholderIdx(i)
		typ, _ := h.semaCtx.Placeholders.ValueType(id)
		texpr, err := schemaexpr.SanitizeVarFreeExpr(
			context.Background(),
			parg,
			typ,
			"", /* context */
			&h.semaCtx,
			volatility.Volatile,
			false, /*allowAssignmentCast*/
		)
		if err != nil {
			tb.Fatalf("%v", err)
		}

		h.semaCtx.Placeholders.Values[i] = texpr
	}
	h.evalCtx.Placeholders = &h.semaCtx.Placeholders
	h.evalCtx.Annotations = &h.semaCtx.Annotations
	return h
}

// runSimple simulates running a query through the "simple protocol" (no prepare
// step). The placeholders are replaced with their values automatically when we
// build the memo.
func (h *harness) runSimple(tb testing.TB, query benchQuery, phase Phase) {
	stmt, err := parser.ParseOne(query.query)
	if err != nil {
		tb.Fatalf("%v", err)
	}

	if phase == Parse {
		return
	}

	h.optimizer.Init(context.Background(), &h.evalCtx, h.testCat)
	if phase == OptBuildNoNorm {
		h.optimizer.DisableOptimizations()
	}

	bld := optbuilder.New(h.ctx, &h.semaCtx, &h.evalCtx, h.testCat, h.optimizer.Factory(), stmt.AST)
	// Note that KeepPlaceholders is false and we have placeholder values in the
	// evalCtx, so the optbuilder will replace all placeholders with their values.
	if err = bld.Build(); err != nil {
		tb.Fatalf("%v", err)
	}

	if phase == OptBuildNoNorm || phase == OptBuildNorm {
		return
	}

	if _, err := h.optimizer.Optimize(); err != nil {
		panic(err)
	}
	execMemo := h.optimizer.Memo()

	if phase == Explore {
		return
	}

	if phase != ExecBuild {
		tb.Fatalf("invalid phase %s for Simple", phase)
	}

	h.gf.Init(exec.StubFactory{})
	root := execMemo.RootExpr()
	eb := execbuilder.New(
		context.Background(),
		&h.gf,
		&h.optimizer,
		execMemo,
		nil, /* catalog */
		root,
		&h.semaCtx,
		&h.evalCtx,
		true, /* allowAutoCommit */
		statements.IsANSIDML(stmt.AST),
	)
	if _, err = eb.Build(); err != nil {
		tb.Fatalf("%v", err)
	}
}

// runPrepared simulates running the query after it was prepared.
func (h *harness) runPrepared(tb testing.TB, phase Phase) {
	h.optimizer.Init(context.Background(), &h.evalCtx, h.testCat)

	if !h.prepMemo.IsOptimized() {
		if phase == AssignPlaceholdersNoNorm {
			h.optimizer.DisableOptimizations()
		}
		err := h.optimizer.Factory().AssignPlaceholders(h.prepMemo)
		if err != nil {
			tb.Fatalf("%v", err)
		}
	}

	if phase == AssignPlaceholdersNoNorm || phase == AssignPlaceholdersNorm {
		return
	}

	var execMemo *memo.Memo
	if h.prepMemo.IsOptimized() {
		// No placeholders or the placeholder fast path succeeded; we already did
		// the exploration at prepare time.
		execMemo = h.prepMemo
	} else {
		if _, err := h.optimizer.Optimize(); err != nil {
			tb.Fatalf("%v", err)
		}
		execMemo = h.optimizer.Memo()
	}

	if phase == Explore {
		return
	}

	if phase != ExecBuild {
		tb.Fatalf("invalid phase %s for Prepared", phase)
	}

	h.gf.Init(exec.StubFactory{})
	root := execMemo.RootExpr()
	eb := execbuilder.New(
		context.Background(),
		&h.gf,
		&h.optimizer,
		execMemo,
		nil, /* catalog */
		root,
		&h.semaCtx,
		&h.evalCtx,
		true,  /* allowAutoCommit */
		false, /* isANSIDML */
	)
	if _, err := eb.Build(); err != nil {
		tb.Fatalf("%v", err)
	}
}

func makeChain(size int) benchQuery {
	var buf bytes.Buffer
	buf.WriteString(`SELECT * FROM `)
	comma := ""
	for i := 0; i < size; i++ {
		buf.WriteString(comma)
		fmt.Fprintf(&buf, "j AS tab%d", i+1)
		comma = ", "
	}

	if size > 1 {
		buf.WriteString(" WHERE ")
	}

	comma = ""
	for i := 0; i < size-1; i++ {
		buf.WriteString(comma)
		fmt.Fprintf(&buf, "tab%d.a = tab%d.b", i+1, i+2)
		comma = " AND "
	}

	return benchQuery{
		name:  fmt.Sprintf("chain-%d", size),
		query: buf.String(),
	}
}

func makeQueryWithORs(size int) benchQuery {
	var buf bytes.Buffer
	buf.WriteString(`SELECT * FROM stock WHERE `)
	sep := ""
	for i := 0; i < size; i++ {
		buf.WriteString(sep)
		fmt.Fprintf(&buf, "s_w_id = %d AND s_order_cnt = %d", i, i)
		sep = " OR "
	}
	return benchQuery{
		name:  fmt.Sprintf("ored-preds-%d", size),
		query: buf.String(),
	}
}

func makeParameterizedQueryWithORs(size int) benchQuery {
	var buf bytes.Buffer
	buf.WriteString(`SELECT * FROM stock WHERE `)
	sep := ""
	numParams := size
	parameterValues := make([]interface{}, numParams)
	for i := 1; i <= numParams; i++ {
		parameterValues[i-1] = i
		buf.WriteString(sep)
		fmt.Fprintf(&buf, "s_w_id = $%d AND s_order_cnt = $%d", i, i)
		sep = " OR "
	}
	return benchQuery{
		name:  fmt.Sprintf("ored-preds-using-params-%d", size),
		query: buf.String(),
		args:  parameterValues,
	}
}

// makeOredPredsTests constructs a set of non-parameterized queries and
// parameterized queries with a certain number of ORed predicates as indicated
// in the testSizes array and test name suffix. The test names produced are:
// ored-preds-100
// ored-preds-using-params-100
func makeOredPredsTests(b *testing.B) []benchQuery {
	// Add more entries to this array to test with different numbers of ORed
	// predicates.
	testSizes := [...]int{100}
	benchQueries := make([]benchQuery, len(testSizes)*2)
	for i := 0; i < len(testSizes); i++ {
		benchQueries[i] = makeQueryWithORs(testSizes[i])
	}
	for i := len(testSizes); i < len(testSizes)*2; i++ {
		benchQueries[i] = makeParameterizedQueryWithORs(testSizes[i-len(testSizes)])
	}
	return benchQueries
}

func queriesToTest(b *testing.B) []benchQuery {
	allQueries := append(queries[:], makeOredPredsTests(b)...)
	return allQueries
}

// BenchmarkChain benchmarks the planning of a "chain" query, where
// some number of tables are joined together, with there being a
// predicate joining the first and second, second and third, third
// and fourth, etc.
//
// For example, a 5-chain looks like:
//
//	SELECT * FROM a, b, c, d, e
//	WHERE a.x = b.y
//	  AND b.x = c.y
//	  AND c.x = d.y
//	  AND d.x = e.y
func BenchmarkChain(b *testing.B) {
	for i := 1; i < 20; i++ {
		q := makeChain(i)
		h := newHarness(b, q, schemas)
		b.Run(q.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				h.runSimple(b, q, Explore)
			}
		})
	}
}

// BenchmarkEndToEnd measures the time to execute a query end-to-end (against a
// test server).
func BenchmarkEndToEnd(b *testing.B) {
	defer log.Scope(b).Close(b)

	// Set up database.
	srv, db, _ := serverutils.StartServer(b, base.TestServerArgs{UseDatabase: "bench"})
	defer srv.Stopper().Stop(context.Background())
	sr := sqlutils.MakeSQLRunner(db)
	sr.Exec(b, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`)
	sr.Exec(b, `SET CLUSTER SETTING sql.stats.flush.enabled = false`)
	sr.Exec(b, `SET CLUSTER SETTING sql.metrics.statement_details.enabled = false`)
	sr.Exec(b, `CREATE DATABASE bench`)
	for _, schema := range schemas {
		sr.Exec(b, schema)
	}

	for _, query := range queriesToTest(b) {
		args := trimSingleQuotes(query.args)
		b.Run(query.name, func(b *testing.B) {
			for _, vectorize := range []string{"on", "off"} {
				b.Run("vectorize="+vectorize, func(b *testing.B) {
					sr.Exec(b, "SET vectorize="+vectorize)
					b.Run("Simple", func(b *testing.B) {
						for i := 0; i < b.N; i++ {
							sr.Exec(b, query.query, args...)
							if query.cleanup != "" {
								sr.Exec(b, query.cleanup)
							}
						}
					})
					b.Run("Prepared", func(b *testing.B) {
						prepared, err := db.Prepare(query.query)
						if err != nil {
							b.Fatalf("%v", err)
						}
						for i := 0; i < b.N; i++ {
							res, err := prepared.Exec(args...)
							if err != nil {
								b.Fatalf("%v", err)
							}
							if query.cleanup != "" {
								sr.Exec(b, query.cleanup)
							}
							rows, err := res.RowsAffected()
							if err != nil {
								b.Fatalf("%v", err)
							}
							if rows > 0 {
								b.ReportMetric(float64(rows), "rows/op")
							}
						}
					})
				})
			}
		})
	}
}

func trimSingleQuotes(args []interface{}) []interface{} {
	res := make([]interface{}, len(args))
	for i, arg := range args {
		if s, ok := arg.(string); ok {
			res[i] = strings.Trim(s, "'")
		} else {
			res[i] = arg
		}
	}
	return res
}

var slowQueries = [...]benchQuery{
	// 1. The first long-running query taken from #64793.
	// 2. The most recent long-running query from #64793 (as of July 2022).
	// 3. A long-running query inspired by support issue #1710.
	{
		name: "slow-query-1",
		query: `
			WITH with_186941 (col_1103773, col_1103774) AS (
				SELECT
					*
				FROM
					(
						VALUES
							('clvl', 3 :: INT2),
							(
								'n',
								(
									SELECT
										tab_455284.col1_6 AS col_1103772
									FROM
										table64793_1@[0] AS tab_455284
									ORDER BY
										tab_455284.col1_2 DESC,
										tab_455284.col1_1 DESC
									LIMIT
										1 ::: INT8
								)
							),
							(NULL, 6736 ::: INT8)
					) AS tab_455285 (col_1103773, col_1103774)
			),
			with_186942 (col_1103775) AS (
				SELECT
					*
				FROM
					(
						VALUES
							('yk'),
							(NULL)
					) AS tab_455286 (col_1103775)
			)
			SELECT
				0 ::: OID AS col_1103776,
				(
					(-32244820164.24410487)::: DECIMAL :: DECIMAL + tab_455291.col1_10 :: INT8
				):: DECIMAL AS col_1103777,
				tab_455287._bool AS col_1103778
			FROM
				with_186942 AS cte_ref_54113,
				seed64793@[0] AS tab_455287
				JOIN seed64793 AS tab_455288
				JOIN seed64793 AS tab_455289 ON (tab_455288._int8) = (tab_455289._int8)
				AND (tab_455288._date) = (tab_455289._date)
				AND (tab_455288._float8) = (tab_455289._float8)
				JOIN table64793_1@[0] AS tab_455290
				JOIN table64793_1@primary AS tab_455291
				JOIN table64793_1@[0] AS tab_455295
				JOIN seed64793 AS tab_455296
				JOIN seed64793 AS tab_455297 ON (tab_455296._int8) = (tab_455297._int8)
				AND (tab_455296._date) = (tab_455297._date) ON (tab_455295.col1_5) = (tab_455297._float8)
				AND (tab_455295.col1_5) = (tab_455296._float8)
				AND (tab_455295.col1_5) = (tab_455297._float8)
				AND (tab_455295.col1_5) = (tab_455297._float8) ON (tab_455291.col1_2) = (tab_455295.tableoid)
				AND (tab_455291.col1_7) = (tab_455295.col1_1) ON (tab_455290.col1_2) = (tab_455291.col1_9)
				AND (tab_455290.col1_7) = (tab_455291.col1_7) ON (tab_455289._float8) = (tab_455296._float8) ON (tab_455287._float4) = (tab_455290.col1_5)
				AND (tab_455287.tableoid) = (tab_455295.col1_9)
				AND (tab_455287._bool) = (tab_455295.col1_7);
		`,
		args: []interface{}{},
	},
	{
		name: "slow-query-2",
		query: `
			WITH with_121707 (col_692430) AS (
				SELECT
					*
				FROM
					(
						VALUES
							(
								(-0.19099748134613037)::: FLOAT8
							),
							(0.9743397235870361 ::: FLOAT8),
							(
								(-1.6944892406463623)::: FLOAT8
							)
					) AS tab_297691 (col_692430)
			)
			SELECT
				'-35 years -11 mons -571 days -08:18:57.001029' ::: INTERVAL AS col_692441
			FROM
				table64793_2@table64793_2_col1_8_col1_11_col1_3_col1_7_col1_6_col1_4_col1_1_key AS tab_297692
				JOIN table64793_3@table64793_3_col2_0_col2_1_col2_2_key AS tab_297693
				JOIN table64793_2@[0] AS tab_297694
				JOIN seed64793@seed64793__int8__float8__date_idx AS tab_297695
				RIGHT JOIN table64793_3@[0] AS tab_297696
				JOIN table64793_4@table64793_4_col3_10_col3_3_col2_1_col3_9_key AS tab_297697 ON (tab_297696.col2_0) = (tab_297697.col3_3) CROSS
				JOIN table64793_4@[0] AS tab_297698
				JOIN table64793_3 AS tab_297699 ON (tab_297698.col2_0) = (tab_297699.col2_0) ON TRUE
				JOIN table64793_4@[0] AS tab_297700 ON (tab_297697.col3_12) = (tab_297700.col3_8) ON (tab_297694.tableoid) = (tab_297695.tableoid)
				AND (tab_297694.col1_5) = (tab_297698.col3_8)
				AND (tab_297694.tableoid) = (tab_297698.col3_2)
				AND (tab_297694.col1_5) = (tab_297697.col3_12) ON (tab_297693.col2_2) = (tab_297700.col3_3)
				AND (tab_297693.col2_1) = (tab_297698.col2_1)
				AND (tab_297693.tableoid) = (tab_297699.tableoid)
				AND (tab_297693.col2_1) = (tab_297697.col2_1)
				AND (tab_297693.tableoid) = (tab_297694.col1_1)
				AND (tab_297693.col2_2) = (tab_297695._string)
				AND (tab_297693.col2_2) = (tab_297696.col2_0)
				AND (tab_297693.col2_2) = (tab_297698.col3_3) ON (tab_297692.col1_11) = (tab_297694.col1_11)
			ORDER BY
				tab_297695._enum DESC
			LIMIT
				57 ::: INT8;
		`,
		args: []interface{}{},
	},
	{
		name: "slow-query-3",
		query: `
      SELECT
        *
      FROM
        tab1
        INNER JOIN tab2 ON
            tab1.col1 = tab2.col1 AND tab1.col2 = tab2.col2
      WHERE
        tab1.col1
        IN (
            10:::INT8,
            20:::INT8,
            30:::INT8,
            40:::INT8,
            50:::INT8,
            60:::INT8,
            70:::INT8,
            80:::INT8,
            90:::INT8,
            100:::INT8,
            110:::INT8,
            120:::INT8,
            130:::INT8,
            140:::INT8,
            150:::INT8,
            160:::INT8,
            170:::INT8,
            180:::INT8,
            190:::INT8,
            200:::INT8,
            210:::INT8,
            220:::INT8,
            230:::INT8,
            240:::INT8,
            250:::INT8,
            260:::INT8,
            270:::INT8,
            280:::INT8,
            290:::INT8,
            300:::INT8,
            310:::INT8,
            320:::INT8,
            330:::INT8,
            340:::INT8,
            350:::INT8,
            360:::INT8,
            370:::INT8,
            380:::INT8,
            390:::INT8,
            400:::INT8,
            410:::INT8,
            420:::INT8,
            430:::INT8,
            440:::INT8,
            450:::INT8,
            460:::INT8,
            470:::INT8,
            480:::INT8,
            490:::INT8,
            500:::INT8
          )
        AND tab1.col8 > 0
        AND tab1.col8 < 10
        AND tab1.col20 = 4500
        AND tab1.col40 = 10
      ORDER BY
        tab1.col8 DESC;
    `,
		args: []interface{}{},
	},
	{
		name: "slow-query-4",
		query: `
			SELECT * FROM multi_col_pk t1
			LEFT JOIN multi_col_pk t2 ON t1.region = t2.region AND t1.id = t2.id
			LEFT JOIN multi_col_pk t3 ON t1.region = t3.region AND t1.id = t3.id
			LEFT JOIN multi_col_pk t4 ON t1.region = t4.region AND t1.id = t4.id
			LEFT JOIN multi_col_pk t5 ON t1.region = t5.region AND t1.id = t5.id
			LEFT JOIN multi_col_pk t6 ON t1.region = t6.region AND t1.id = t6.id
			LEFT JOIN multi_col_pk t7 ON t1.region = t7.region AND t1.id = t7.id
			LEFT JOIN multi_col_pk t8 ON t1.region = t8.region AND t1.id = t8.id
			LEFT JOIN multi_col_pk t9 ON t1.region = t9.region AND t1.id = t9.id
			WHERE t1.c1 IN ($1, $2, $3) AND t1.c2 IN ($4, $5, $6) AND t1.c3 IN ($7, $8, $9)
    `,
		args: []interface{}{10, 20, 30, 40, 50, 60, 70, 80, 90},
	},
	{
		name: "slow-query-5",
		query: `
			SELECT *
			FROM multi_col_pk_no_indexes AS t1
			LEFT JOIN multi_col_pk_no_indexes AS t2 ON t2.region = $3 AND t1.c1 = t2.id
			LEFT JOIN multi_col_pk_no_indexes AS t3 ON t3.region = $3 AND t2.id = t3.c1 AND t2.region = t3.region
			LEFT JOIN multi_col_pk_no_indexes AS t4 ON t4.region = $3 AND t2.id = t4.c1 AND t2.region = t4.region
			LEFT JOIN multi_col_pk_no_indexes AS t5 ON t5.region = $3 AND t4.c1 = t5.id
			LEFT JOIN multi_col_pk_no_indexes AS t6 ON t6.region = $3 AND t2.id = t6.c1 AND t2.region = t6.region
			LEFT JOIN multi_col_pk_no_indexes AS t7 ON t7.region = $3 AND t1.c1 = t7.id
			LEFT JOIN multi_col_pk_no_indexes AS t8 ON t8.region = $3 AND t1.c1 = t8.id
			LEFT JOIN multi_col_pk_no_indexes AS t9 ON t9.region = $3 AND t1.c1 = t9.id
			LEFT JOIN multi_col_pk_no_indexes AS t10 ON t10.region = $3 AND t9.id = t10.c1 AND t9.region = t10.region
			LEFT JOIN multi_col_pk_no_indexes AS t11 ON t11.region = $3 AND t10.c1 = t11.id
			LEFT JOIN multi_col_pk_no_indexes AS t12 ON t12.region = $3 AND t1.id = t12.c1 AND t1.region = t12.region
			LEFT JOIN multi_col_pk_no_indexes AS t13 ON t13.region = $3 AND t12.c1 = t13.id
			LEFT JOIN multi_col_pk_no_indexes AS t14 ON t14.region = $3 AND t1.id = t14.c1 AND t1.region = t14.region
			LEFT JOIN multi_col_pk_no_indexes AS t15 ON t15.region = $3 AND t1.id = t15.c1 AND t1.region = t15.region
			LEFT JOIN multi_col_pk_no_indexes AS t16 ON t16.region = $3 AND t1.id = t16.c1 AND t1.region = t16.region
			LEFT JOIN multi_col_pk_no_indexes AS t17 ON t17.region = $3 AND t1.c1 = t17.id
			LEFT JOIN multi_col_pk_no_indexes AS t18 ON t18.region = $3 AND t1.c1 = t18.id
			LEFT JOIN multi_col_pk_no_indexes AS t19 ON t19.region = $3 AND t1.id = t19.c1 AND t1.region = t19.region
			LEFT JOIN multi_col_pk_no_indexes AS t20 ON t20.region = $3 AND t19.id = t20.c1 AND t19.region = t20.region 
			LEFT JOIN multi_col_pk_no_indexes AS t21 ON t21.region = $3 AND t20.id = t21.c1 AND t20.region = t21.region
			LEFT JOIN multi_col_pk_no_indexes AS t22 ON t22.region = $3 AND t21.c1 = t22.id
			LEFT JOIN multi_col_pk_no_indexes AS t23 ON t23.region = $3 AND t19.id = t23.c9 AND t19.region = t23.region
			LEFT JOIN multi_col_pk_no_indexes AS t24 ON t24.region = $3 AND t23.c1 = t24.id
			LEFT JOIN multi_col_pk_no_indexes AS t25 ON t25.region = $3 AND t23.c1 = t25.id
			LEFT JOIN multi_col_pk_no_indexes AS t26 ON t26.region = $3 AND t26.c1 = t23.id AND t26.region = t23.region
			LEFT JOIN multi_col_pk_no_indexes AS t27 ON t27.region = $3 AND t26.c1 = t27.id
			LEFT JOIN multi_col_pk_no_indexes AS t28 ON t28.region = $3 AND t28.c1 = t19.id AND t28.region = t19.region
			LEFT JOIN multi_col_pk_no_indexes AS t29 ON t29.region = $3 AND t28.c1 = t29.id
			LEFT JOIN multi_col_pk_no_indexes AS t30 ON t30.region = $3 AND t28.c1 = t30.id
			LEFT JOIN multi_col_pk_no_indexes AS t31 ON t31.region = $3 AND t31.c1 = t28.id AND t31.region = t28.region
			LEFT JOIN multi_col_pk_no_indexes AS t32 ON t32.region = $3 AND t31.c1 = t32.id
			LEFT JOIN multi_col_pk_no_indexes AS t33 ON t33.region = $3 AND t33.c1 = t19.id AND t33.region = t19.region
			LEFT JOIN multi_col_pk_no_indexes AS t34 ON t34.region = $3 AND t33.c1 = t34.id
			LEFT JOIN multi_col_pk_no_indexes AS t35 ON t35.region = $3 AND t35.c1 = t33.id AND t35.region = t33.region
			LEFT JOIN multi_col_pk_no_indexes AS t36 ON t36.region = $3 AND t35.c1 = t36.id
			LEFT JOIN multi_col_pk_no_indexes AS t37 ON t37.region = $3 AND t37.c1 = t19.id AND t37.region = t19.region
			LEFT JOIN multi_col_pk_no_indexes AS t38 ON t38.region = $3 AND t37.c1 = t38.id
			LEFT JOIN multi_col_pk_no_indexes AS t39 ON t39.region = $3 AND t37.c1 = t39.id
			LEFT JOIN multi_col_pk_no_indexes AS t40 ON t40.region = $3 AND t37.c1 = t40.id
			LEFT JOIN multi_col_pk_no_indexes AS t41 ON t41.region = $3 AND t41.c1 = t37.id AND t41.region = t37.region
			LEFT JOIN multi_col_pk_no_indexes AS t42 ON t42.region = $3 AND t41.c1 = t42.id
			WHERE t1.id = $1 AND t1.c2 = $2 AND t1.region = $3;
    `,
		args: []interface{}{10001, 5, "'east'"},
	},
	{
		name: "slow-query-6",
		query: `
			SELECT t1.*, t2.*, t4.*, t17.*, t8.*, t9.*, t3.*, t11.*
			FROM sq6a AS t1
			JOIN sq6b AS t2 ON
				t2.c1 = t1.c1
				AND t2.c2 = t1.c2
				AND t2.c3 = t1.c3
				AND t2.c4 = t1.c4
				AND t2.c44 = t1.c100
			JOIN sq6c AS t3 ON
				t3.c1 = t2.c1
				AND t3.c2 = t2.c6
				AND t3.c34 = t2.c44
			JOIN sq6d AS t4 ON
				t4.c1 = t2.c1
				AND t4.c2 = t2.c41
				AND t4.c3 = $1
				AND t4.c28 = t2.c44
			JOIN sq6e AS t5 ON
				t5.c1 = t1.c81
				AND t5.c23 = t1.c100
			JOIN sq6f AS t6 ON
				t6.c12 = t1.c100
				AND t6.c1 = t1.c1
				AND t6.c4 = t1.c2
				AND t6.c5 = t1.c3
				AND t6.c6 = t1.c4
				AND t6.c2 IN ($1, $2)
			JOIN sq6g AS t7 ON
				t7.c24 = t6.c12
				AND t7.c1 = t6.c1
				AND t7.c3 = t6.c3
				AND t7.c2 = t6.c2
			LEFT JOIN sq6h AS t8 ON
				t8.c2 = t2.c37
				AND t8.c1 = t2.c1
				AND t8.c3 = $3
				AND t8.c13 = t2.c44
			LEFT JOIN sq6h AS t9 ON
				t9.c2 = t2.c37
				AND t9.c1 = t2.c1
				AND t9.c3 = $4
				AND t9.c13 = t2.c44
			LEFT JOIN sq6i AS t10 ON
				t10.c1 = t1.c1
				AND t10.c2 = t1.c2
				AND t10.c3 = t1.c3
				AND t10.c4 = t1.c4
				AND t10.c6 = 1
				AND t10.c7 = t1.c100
			LEFT JOIN sq6j AS t11 ON
				t11.c1 = t1.c1
				AND t11.c2 = t1.c2
				AND t11.c3 = t1.c3
				AND t11.c4 = t1.c4
				AND t11.c36 = t1.c100
			LEFT JOIN sq6k AS t12 ON
				t12.c1 = t1.c1
				AND t12.c2 = t1.c2
				AND t12.c3 = t1.c3
				AND t12.c4 = t1.c4
				AND t12.c6 = $5
				AND t12.c5 = t1.c100
			LEFT JOIN sq6k AS t13 ON
				t13.c1 = t1.c1
				AND t13.c2 = t1.c2
				AND t13.c3 = t1.c3
				AND t13.c4 = t1.c4
				AND t13.c6 = $6
				AND t13.c5 = t1.c100
			LEFT JOIN sq6k AS t15 ON
				t15.c1 = t1.c1
				AND t15.c2 = t1.c2
				AND t15.c3 = t1.c3
				AND t15.c4 = t1.c4
				AND t15.c6 = $7
				AND t15.c5 = t1.c100
			LEFT JOIN sq6k AS t16 ON
				t16.c1 = t1.c1
				AND t16.c2 = t1.c2
				AND t16.c3 = t1.c3
				AND t16.c4 = t1.c4
				AND t16.c6 = $8
				AND t16.c5 = t1.c100
			LEFT JOIN sq6l AS t17 ON
				t17.c1 = t1.c1
				AND t17.c2 = t1.c2
				AND t17.c3 = t1.c3
				AND t17.c4 = t1.c4
				AND t17.c5 = $9
				AND t17.c28 = t1.c100
			LEFT JOIN sq6m AS t18 ON
				t18.c4 = t1.c2
				AND t18.c1 = t1.c3
				AND t18.c2 = t1.c4
				AND t18.c36 = t1.c100
			WHERE
				t1.c1 IN ($10, $11, $12)
				AND t1.c2 = $13
				AND t1.c3 = $14
				AND t1.c4 = $15
				AND t1.c100 = $16
    `,
		args: []interface{}{
			"'WX'", "'ZG'", "'TTT'", "'FFF'", "'JQYJSUWE'", "'RDDQXFRN'", "'HCLDUJOA'", "'GESIUACE'",
			"'EJ'", "'LQIZHIJX'", "'NNSMWHCO'", "'UDJG'", "'82894'", "'AAYUMINX'", "'JGJYKJZR'", 0,
		},
	},
	{
		name: "slow-query-7",
		query: `
			WITH t1 AS MATERIALIZED (
				SELECT tmp1.*, tmp2.*
				FROM sq6a AS tmp1
				JOIN sq6a AS tmp2 ON tmp1.c1 = tmp2.c1 AND tmp1.c2 = tmp2.c2
			)
			SELECT t1.*, t2.*, t4.*, t17.*, t8.*, t9.*, t3.*, t11.*
			FROM t1
			JOIN sq6b AS t2 ON
				t2.c1 = t1.c1
				AND t2.c2 = t1.c2
				AND t2.c3 = t1.c3
				AND t2.c4 = t1.c4
				AND t2.c44 = t1.c100
			JOIN sq6c AS t3 ON
				t3.c1 = t2.c1
				AND t3.c2 = t2.c6
				AND t3.c34 = t2.c44
			JOIN sq6d AS t4 ON
				t4.c1 = t2.c1
				AND t4.c2 = t2.c41
				AND t4.c3 = $1
				AND t4.c28 = t2.c44
			JOIN sq6e AS t5 ON
				t5.c1 = t1.c81
				AND t5.c23 = t1.c100
			JOIN sq6f AS t6 ON
				t6.c12 = t1.c100
				AND t6.c1 = t1.c1
				AND t6.c4 = t1.c2
				AND t6.c5 = t1.c3
				AND t6.c6 = t1.c4
				AND t6.c2 IN ($1, $2)
			JOIN sq6g AS t7 ON
				t7.c24 = t6.c12
				AND t7.c1 = t6.c1
				AND t7.c3 = t6.c3
				AND t7.c2 = t6.c2
			LEFT JOIN sq6h AS t8 ON
				t8.c2 = t2.c37
				AND t8.c1 = t2.c1
				AND t8.c3 = $3
				AND t8.c13 = t2.c44
			LEFT JOIN sq6h AS t9 ON
				t9.c2 = t2.c37
				AND t9.c1 = t2.c1
				AND t9.c3 = $4
				AND t9.c13 = t2.c44
			LEFT JOIN sq6i AS t10 ON
				t10.c1 = t1.c1
				AND t10.c2 = t1.c2
				AND t10.c3 = t1.c3
				AND t10.c4 = t1.c4
				AND t10.c6 = 1
				AND t10.c7 = t1.c100
			LEFT JOIN sq6j AS t11 ON
				t11.c1 = t1.c1
				AND t11.c2 = t1.c2
				AND t11.c3 = t1.c3
				AND t11.c4 = t1.c4
				AND t11.c36 = t1.c100
			LEFT JOIN sq6k AS t12 ON
				t12.c1 = t1.c1
				AND t12.c2 = t1.c2
				AND t12.c3 = t1.c3
				AND t12.c4 = t1.c4
				AND t12.c6 = $5
				AND t12.c5 = t1.c100
			LEFT JOIN sq6k AS t13 ON
				t13.c1 = t1.c1
				AND t13.c2 = t1.c2
				AND t13.c3 = t1.c3
				AND t13.c4 = t1.c4
				AND t13.c6 = $6
				AND t13.c5 = t1.c100
			LEFT JOIN sq6k AS t15 ON
				t15.c1 = t1.c1
				AND t15.c2 = t1.c2
				AND t15.c3 = t1.c3
				AND t15.c4 = t1.c4
				AND t15.c6 = $7
				AND t15.c5 = t1.c100
			LEFT JOIN sq6k AS t16 ON
				t16.c1 = t1.c1
				AND t16.c2 = t1.c2
				AND t16.c3 = t1.c3
				AND t16.c4 = t1.c4
				AND t16.c6 = $8
				AND t16.c5 = t1.c100
			LEFT JOIN sq6l AS t17 ON
				t17.c1 = t1.c1
				AND t17.c2 = t1.c2
				AND t17.c3 = t1.c3
				AND t17.c4 = t1.c4
				AND t17.c5 = $9
				AND t17.c28 = t1.c100
			LEFT JOIN sq6m AS t18 ON
				t18.c4 = t1.c2
				AND t18.c1 = t1.c3
				AND t18.c2 = t1.c4
				AND t18.c36 = t1.c100
			WHERE
				t1.c1 IN ($10, $11, $12)
				AND t1.c2 = $13
				AND t1.c3 = $14
				AND t1.c4 = $15
				AND t1.c100 = $16
    `,
		args: []interface{}{
			"'WX'", "'ZG'", "'TTT'", "'FFF'", "'JQYJSUWE'", "'RDDQXFRN'", "'HCLDUJOA'", "'GESIUACE'",
			"'EJ'", "'LQIZHIJX'", "'NNSMWHCO'", "'UDJG'", "'82894'", "'AAYUMINX'", "'JGJYKJZR'", 0,
		},
	},
}

func BenchmarkSlowQueries(b *testing.B) {
	p := datapathutils.TestDataPath(b, "slow-schemas.sql")
	slowSchemas, err := os.ReadFile(p)
	if err != nil {
		b.Fatalf("%v", err)
	}
	for _, query := range slowQueries {
		b.Run(query.name, func(b *testing.B) {
			for _, reorderJoinLimit := range []int64{0, 8} {
				b.Run(fmt.Sprintf("reorder-join-%d", reorderJoinLimit), func(b *testing.B) {
					h := newHarness(b, query, []string{string(slowSchemas)})
					h.evalCtx.SessionData().ReorderJoinsLimit = reorderJoinLimit
					for i := 0; i < b.N; i++ {
						h.runSimple(b, query, Explore)
					}
				})
			}
		})
	}
}

// BenchmarkSysbenchDistinctRange measures the time to optimize the
// distinct-range query from sysbench oltp_read_only (and oltp_read_write).
func BenchmarkSysbenchDistinctRange(b *testing.B) {
	const schema = `
  CREATE TABLE public.sbtest (
    id INT8 NOT NULL,
    k INT8 NOT NULL DEFAULT 0:::INT8,
    c CHAR(120) NOT NULL DEFAULT '':::STRING,
    pad CHAR(60) NOT NULL DEFAULT '':::STRING,
    CONSTRAINT sbtest_pkey PRIMARY KEY (id ASC),
    INDEX k_idx (k ASC)
  );
  `
	benchQueries := []benchQuery{
		{
			name:  "sysbench-distinct-range",
			query: "SELECT DISTINCT c FROM sbtest WHERE id BETWEEN 1 AND 16 ORDER BY c",
			args:  []interface{}{},
		},
	}
	for _, query := range benchQueries {
		b.Run(query.name, func(b *testing.B) {
			h := newHarness(b, query, []string{schema})
			for i := 0; i < b.N; i++ {
				h.runSimple(b, query, Explore)
			}
		})
	}
}

// BenchmarkExecBuild measures the time that the execbuilder phase takes. It
// does not include any other phases.
func BenchmarkExecBuild(b *testing.B) {
	type testCase struct {
		query  benchQuery
		schema []string
	}
	var testCases []testCase

	// Add the basic queries.
	for _, query := range queriesToTest(b) {
		testCases = append(testCases, testCase{query, schemas})
	}

	// Add the slow queries.
	p := datapathutils.TestDataPath(b, "slow-schemas.sql")
	slowSchemas, err := os.ReadFile(p)
	if err != nil {
		b.Fatalf("%v", err)
	}
	for _, query := range slowQueries {
		testCases = append(testCases, testCase{query, []string{string(slowSchemas)}})
	}

	for _, tc := range testCases {
		h := newHarness(b, tc.query, tc.schema)

		stmt, err := parser.ParseOne(tc.query.query)
		if err != nil {
			b.Fatalf("%v", err)
		}

		h.optimizer.Init(context.Background(), &h.evalCtx, h.testCat)
		bld := optbuilder.New(h.ctx, &h.semaCtx, &h.evalCtx, h.testCat, h.optimizer.Factory(), stmt.AST)
		if err = bld.Build(); err != nil {
			b.Fatalf("%v", err)
		}

		if _, err := h.optimizer.Optimize(); err != nil {
			panic(err)
		}

		execMemo := h.optimizer.Memo()
		root := execMemo.RootExpr()

		var gf explain.PlanGistFactory
		b.Run(tc.query.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				gf.Init(exec.StubFactory{})
				eb := execbuilder.New(
					context.Background(),
					&gf,
					&h.optimizer,
					execMemo,
					nil, /* catalog */
					root,
					&h.semaCtx,
					&h.evalCtx,
					true,  /* allowAutoCommit */
					false, /* isANSIDML */
				)
				if _, err := eb.Build(); err != nil {
					b.Fatalf("%v", err)
				}
			}
		})
	}
}
