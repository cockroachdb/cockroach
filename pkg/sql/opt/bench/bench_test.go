// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bench

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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
//  - AssignPlaceholdersNoNorm
//  - AssignPlaceholdersNorm
//  - Explore
//  - ExecBuild
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
	name  string
	query string
	args  []interface{}
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
		INDEX (b)
	)
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

	// 1. Table with more than 15 columns (triggers slow path for FastIntMap).
	// 2. Query with over 1000 ORed predicates involving the first primary key column.
	{
		name: "many-ored-preds",
		query: `
			SELECT * FROM stock WHERE s_w_id = $1 AND s_order_cnt = $2 OR  s_w_id = $3 AND s_order_cnt = $4 OR (s_w_id = 0 AND s_order_cnt = 0 OR s_w_id = 1 AND s_order_cnt = 1 OR s_w_id = 2 AND s_order_cnt = 4 OR s_w_id = 3 AND s_order_cnt = 9 OR s_w_id = 4 AND s_order_cnt = 16 OR s_w_id = 5 AND s_order_cnt = 25 OR s_w_id = 6 AND s_order_cnt = 36 OR s_w_id = 7 AND s_order_cnt = 49 OR s_w_id = 8 AND s_order_cnt = 64 OR s_w_id = 9 AND s_order_cnt = 81 OR s_w_id = 11 AND s_order_cnt = 100 OR s_w_id = 11 AND s_order_cnt = 121 OR s_w_id = 12 AND s_order_cnt = 144 OR s_w_id = 13 AND s_order_cnt = 169 OR s_w_id = 14 AND s_order_cnt = 196 OR s_w_id = 15 AND s_order_cnt = 225 OR s_w_id = 16 AND s_order_cnt = 256 OR s_w_id = 17 AND s_order_cnt = 289 OR s_w_id = 18 AND s_order_cnt = 324 OR s_w_id = 19 AND s_order_cnt = 361 OR s_w_id = 20 AND s_order_cnt = 400 OR s_w_id = 21 AND s_order_cnt = 441 OR s_w_id = 22 AND s_order_cnt = 484 OR s_w_id = 23 AND s_order_cnt = 529 OR s_w_id = 24 AND s_order_cnt = 576 OR s_w_id = 25 AND s_order_cnt = 625 OR s_w_id = 26 AND s_order_cnt = 676 OR s_w_id = 27 AND s_order_cnt = 729 OR s_w_id = 28 AND s_order_cnt = 784 OR s_w_id = 29 AND s_order_cnt = 841 OR s_w_id = 30 AND s_order_cnt = 900 OR s_w_id = 31 AND s_order_cnt = 961 OR s_w_id = 32 AND s_order_cnt = 1024 OR s_w_id = 33 AND s_order_cnt = 1089 OR s_w_id = 34 AND s_order_cnt = 1156 OR s_w_id = 35 AND s_order_cnt = 1225 OR s_w_id = 36 AND s_order_cnt = 1296 OR s_w_id = 37 AND s_order_cnt = 1369 OR s_w_id = 38 AND s_order_cnt = 1444 OR s_w_id = 39 AND s_order_cnt = 1521 OR s_w_id = 40 AND s_order_cnt = 1600 OR s_w_id = 41 AND s_order_cnt = 1681 OR s_w_id = 42 AND s_order_cnt = 1764 OR s_w_id = 43 AND s_order_cnt = 1849 OR s_w_id = 44 AND s_order_cnt = 1936 OR s_w_id = 45 AND s_order_cnt = 2025 OR s_w_id = 46 AND s_order_cnt = 2116 OR s_w_id = 47 AND s_order_cnt = 2209 OR s_w_id = 48 AND s_order_cnt = 2304 OR s_w_id = 49 AND s_order_cnt = 2401 OR s_w_id = 50 AND s_order_cnt = 2500 OR s_w_id = 51 AND s_order_cnt = 2601 OR s_w_id = 52 AND s_order_cnt = 2704 OR s_w_id = 53 AND s_order_cnt = 2809 OR s_w_id = 54 AND s_order_cnt = 2916 OR s_w_id = 55 AND s_order_cnt = 3025 OR s_w_id = 56 AND s_order_cnt = 3136 OR s_w_id = 57 AND s_order_cnt = 3249 OR s_w_id = 58 AND s_order_cnt = 3364 OR s_w_id = 59 AND s_order_cnt = 3481 OR s_w_id = 60 AND s_order_cnt = 3600 OR s_w_id = 61 AND s_order_cnt = 3721 OR s_w_id = 62 AND s_order_cnt = 3844 OR s_w_id = 63 AND s_order_cnt = 3969 OR s_w_id = 64 AND s_order_cnt = 4096 OR s_w_id = 65 AND s_order_cnt = 4225 OR s_w_id = 66 AND s_order_cnt = 4356 OR s_w_id = 67 AND s_order_cnt = 4489 OR s_w_id = 68 AND s_order_cnt = 4624 OR s_w_id = 69 AND s_order_cnt = 4761 OR s_w_id = 70 AND s_order_cnt = 4900 OR s_w_id = 71 AND s_order_cnt = 5041 OR s_w_id = 72 AND s_order_cnt = 5184 OR s_w_id = 73 AND s_order_cnt = 5329 OR s_w_id = 74 AND s_order_cnt = 5476 OR s_w_id = 75 AND s_order_cnt = 5625 OR s_w_id = 76 AND s_order_cnt = 5776 OR s_w_id = 77 AND s_order_cnt = 5929 OR s_w_id = 78 AND s_order_cnt = 6084 OR s_w_id = 79 AND s_order_cnt = 6241 OR s_w_id = 80 AND s_order_cnt = 6400 OR s_w_id = 81 AND s_order_cnt = 6561 OR s_w_id = 82 AND s_order_cnt = 6724 OR s_w_id = 83 AND s_order_cnt = 6889 OR s_w_id = 84 AND s_order_cnt = 7056 OR s_w_id = 85 AND s_order_cnt = 7225 OR s_w_id = 86 AND s_order_cnt = 7396 OR s_w_id = 87 AND s_order_cnt = 7569 OR s_w_id = 88 AND s_order_cnt = 7744 OR s_w_id = 89 AND s_order_cnt = 7921 OR s_w_id = 90 AND s_order_cnt = 8100 OR s_w_id = 91 AND s_order_cnt = 8281 OR s_w_id = 92 AND s_order_cnt = 8464 OR s_w_id = 93 AND s_order_cnt = 8649 OR s_w_id = 94 AND s_order_cnt = 8836 OR s_w_id = 95 AND s_order_cnt = 9025 OR s_w_id = 96 AND s_order_cnt = 9216 OR s_w_id = 97 AND s_order_cnt = 9409 OR s_w_id = 98 AND s_order_cnt = 9604 OR s_w_id = 99 AND s_order_cnt = 9801 OR s_w_id = 100 AND s_order_cnt = 10000 OR s_w_id = 101 AND s_order_cnt = 10201 OR s_w_id = 102 AND s_order_cnt = 10404 OR s_w_id = 103 AND s_order_cnt = 10609 OR s_w_id = 104 AND s_order_cnt = 10816 OR s_w_id = 105 AND s_order_cnt = 11025 OR s_w_id = 106 AND s_order_cnt = 11236 OR s_w_id = 107 AND s_order_cnt = 11449 OR s_w_id = 108 AND s_order_cnt = 11664 OR s_w_id = 109 AND s_order_cnt = 11881 OR s_w_id = 110 AND s_order_cnt = 12100 OR s_w_id = 111 AND s_order_cnt = 12321 OR s_w_id = 112 AND s_order_cnt = 12544 OR s_w_id = 113 AND s_order_cnt = 12769 OR s_w_id = 114 AND s_order_cnt = 12996 OR s_w_id = 115 AND s_order_cnt = 13225 OR s_w_id = 116 AND s_order_cnt = 13456 OR s_w_id = 117 AND s_order_cnt = 13689 OR s_w_id = 118 AND s_order_cnt = 13924 OR s_w_id = 119 AND s_order_cnt = 14161 OR s_w_id = 120 AND s_order_cnt = 14400 OR s_w_id = 121 AND s_order_cnt = 14641 OR s_w_id = 122 AND s_order_cnt = 14884 OR s_w_id = 123 AND s_order_cnt = 15129 OR s_w_id = 124 AND s_order_cnt = 15376 OR s_w_id = 125 AND s_order_cnt = 15625 OR s_w_id = 126 AND s_order_cnt = 15876 OR s_w_id = 127 AND s_order_cnt = 16129 OR s_w_id = 128 AND s_order_cnt = 16384 OR s_w_id = 129 AND s_order_cnt = 16641 OR s_w_id = 130 AND s_order_cnt = 16900 OR s_w_id = 131 AND s_order_cnt = 17161 OR s_w_id = 132 AND s_order_cnt = 17424 OR s_w_id = 133 AND s_order_cnt = 17689 OR s_w_id = 134 AND s_order_cnt = 17956 OR s_w_id = 135 AND s_order_cnt = 18225 OR s_w_id = 136 AND s_order_cnt = 18496 OR s_w_id = 137 AND s_order_cnt = 18769 OR s_w_id = 138 AND s_order_cnt = 19044 OR s_w_id = 139 AND s_order_cnt = 19321 OR s_w_id = 140 AND s_order_cnt = 19600 OR s_w_id = 141 AND s_order_cnt = 19881 OR s_w_id = 142 AND s_order_cnt = 20164 OR s_w_id = 143 AND s_order_cnt = 20449 OR s_w_id = 144 AND s_order_cnt = 20736 OR s_w_id = 145 AND s_order_cnt = 21025 OR s_w_id = 146 AND s_order_cnt = 21316 OR s_w_id = 147 AND s_order_cnt = 21609 OR s_w_id = 148 AND s_order_cnt = 21904 OR s_w_id = 149 AND s_order_cnt = 22201 OR s_w_id = 150 AND s_order_cnt = 22500 OR s_w_id = 151 AND s_order_cnt = 22801 OR s_w_id = 152 AND s_order_cnt = 23104 OR s_w_id = 153 AND s_order_cnt = 23409 OR s_w_id = 154 AND s_order_cnt = 23716 OR s_w_id = 155 AND s_order_cnt = 24025 OR s_w_id = 156 AND s_order_cnt = 24336 OR s_w_id = 157 AND s_order_cnt = 24649 OR s_w_id = 158 AND s_order_cnt = 24964 OR s_w_id = 159 AND s_order_cnt = 25281 OR s_w_id = 160 AND s_order_cnt = 25600 OR s_w_id = 161 AND s_order_cnt = 25921 OR s_w_id = 162 AND s_order_cnt = 26244 OR s_w_id = 163 AND s_order_cnt = 26569 OR s_w_id = 164 AND s_order_cnt = 26896 OR s_w_id = 165 AND s_order_cnt = 27225 OR s_w_id = 166 AND s_order_cnt = 27556 OR s_w_id = 167 AND s_order_cnt = 27889 OR s_w_id = 168 AND s_order_cnt = 28224 OR s_w_id = 169 AND s_order_cnt = 28561 OR s_w_id = 170 AND s_order_cnt = 28900 OR s_w_id = 171 AND s_order_cnt = 29241 OR s_w_id = 172 AND s_order_cnt = 29584 OR s_w_id = 173 AND s_order_cnt = 29929 OR s_w_id = 174 AND s_order_cnt = 30276 OR s_w_id = 175 AND s_order_cnt = 30625 OR s_w_id = 176 AND s_order_cnt = 30976 OR s_w_id = 177 AND s_order_cnt = 31329 OR s_w_id = 178 AND s_order_cnt = 31684 OR s_w_id = 179 AND s_order_cnt = 32041 OR s_w_id = 180 AND s_order_cnt = 32400 OR s_w_id = 181 AND s_order_cnt = 32761 OR s_w_id = 182 AND s_order_cnt = 33124 OR s_w_id = 183 AND s_order_cnt = 33489 OR s_w_id = 184 AND s_order_cnt = 33856 OR s_w_id = 185 AND s_order_cnt = 34225 OR s_w_id = 186 AND s_order_cnt = 34596 OR s_w_id = 187 AND s_order_cnt = 34969 OR s_w_id = 188 AND s_order_cnt = 35344 OR s_w_id = 189 AND s_order_cnt = 35721 OR s_w_id = 190 AND s_order_cnt = 36100 OR s_w_id = 191 AND s_order_cnt = 36481 OR s_w_id = 192 AND s_order_cnt = 36864 OR s_w_id = 193 AND s_order_cnt = 37249 OR s_w_id = 194 AND s_order_cnt = 37636 OR s_w_id = 195 AND s_order_cnt = 38025 OR s_w_id = 196 AND s_order_cnt = 38416 OR s_w_id = 197 AND s_order_cnt = 38809 OR s_w_id = 198 AND s_order_cnt = 39204 OR s_w_id = 199 AND s_order_cnt = 39601 OR s_w_id = 200 AND s_order_cnt = 40000 OR s_w_id = 201 AND s_order_cnt = 40401 OR s_w_id = 202 AND s_order_cnt = 40804 OR s_w_id = 203 AND s_order_cnt = 41209 OR s_w_id = 204 AND s_order_cnt = 41616 OR s_w_id = 205 AND s_order_cnt = 42025 OR s_w_id = 206 AND s_order_cnt = 42436 OR s_w_id = 207 AND s_order_cnt = 42849 OR s_w_id = 208 AND s_order_cnt = 43264 OR s_w_id = 209 AND s_order_cnt = 43681 OR s_w_id = 210 AND s_order_cnt = 44100 OR s_w_id = 211 AND s_order_cnt = 44521 OR s_w_id = 212 AND s_order_cnt = 44944 OR s_w_id = 213 AND s_order_cnt = 45369 OR s_w_id = 214 AND s_order_cnt = 45796 OR s_w_id = 215 AND s_order_cnt = 46225 OR s_w_id = 216 AND s_order_cnt = 46656 OR s_w_id = 217 AND s_order_cnt = 47089 OR s_w_id = 218 AND s_order_cnt = 47524 OR s_w_id = 219 AND s_order_cnt = 47961 OR s_w_id = 220 AND s_order_cnt = 48400 OR s_w_id = 221 AND s_order_cnt = 48841 OR s_w_id = 222 AND s_order_cnt = 49284 OR s_w_id = 223 AND s_order_cnt = 49729 OR s_w_id = 224 AND s_order_cnt = 50176 OR s_w_id = 225 AND s_order_cnt = 50625 OR s_w_id = 226 AND s_order_cnt = 51076 OR s_w_id = 227 AND s_order_cnt = 51529 OR s_w_id = 228 AND s_order_cnt = 51984 OR s_w_id = 229 AND s_order_cnt = 52441 OR s_w_id = 230 AND s_order_cnt = 52900 OR s_w_id = 231 AND s_order_cnt = 53361 OR s_w_id = 232 AND s_order_cnt = 53824 OR s_w_id = 233 AND s_order_cnt = 54289 OR s_w_id = 234 AND s_order_cnt = 54756 OR s_w_id = 235 AND s_order_cnt = 55225 OR s_w_id = 236 AND s_order_cnt = 55696 OR s_w_id = 237 AND s_order_cnt = 56169 OR s_w_id = 238 AND s_order_cnt = 56644 OR s_w_id = 239 AND s_order_cnt = 57121 OR s_w_id = 240 AND s_order_cnt = 57600 OR s_w_id = 241 AND s_order_cnt = 58081 OR s_w_id = 242 AND s_order_cnt = 58564 OR s_w_id = 243 AND s_order_cnt = 59049 OR s_w_id = 244 AND s_order_cnt = 59536 OR s_w_id = 245 AND s_order_cnt = 60025 OR s_w_id = 246 AND s_order_cnt = 60516 OR s_w_id = 247 AND s_order_cnt = 61009 OR s_w_id = 248 AND s_order_cnt = 61504 OR s_w_id = 249 AND s_order_cnt = 62001 OR s_w_id = 250 AND s_order_cnt = 62500 OR s_w_id = 251 AND s_order_cnt = 63001 OR s_w_id = 252 AND s_order_cnt = 63504 OR s_w_id = 253 AND s_order_cnt = 64009 OR s_w_id = 254 AND s_order_cnt = 64516 OR s_w_id = 255 AND s_order_cnt = 65025 OR s_w_id = 256 AND s_order_cnt = 65536 OR s_w_id = 257 AND s_order_cnt = 66049 OR s_w_id = 258 AND s_order_cnt = 66564 OR s_w_id = 259 AND s_order_cnt = 67081 OR s_w_id = 260 AND s_order_cnt = 67600 OR s_w_id = 261 AND s_order_cnt = 68121 OR s_w_id = 262 AND s_order_cnt = 68644 OR s_w_id = 263 AND s_order_cnt = 69169 OR s_w_id = 264 AND s_order_cnt = 69696 OR s_w_id = 265 AND s_order_cnt = 70225 OR s_w_id = 266 AND s_order_cnt = 70756 OR s_w_id = 267 AND s_order_cnt = 71289 OR s_w_id = 268 AND s_order_cnt = 71824 OR s_w_id = 269 AND s_order_cnt = 72361 OR s_w_id = 270 AND s_order_cnt = 72900 OR s_w_id = 271 AND s_order_cnt = 73441 OR s_w_id = 272 AND s_order_cnt = 73984 OR s_w_id = 273 AND s_order_cnt = 74529 OR s_w_id = 274 AND s_order_cnt = 75076 OR s_w_id = 275 AND s_order_cnt = 75625 OR s_w_id = 276 AND s_order_cnt = 76176 OR s_w_id = 277 AND s_order_cnt = 76729 OR s_w_id = 278 AND s_order_cnt = 77284 OR s_w_id = 279 AND s_order_cnt = 77841 OR s_w_id = 280 AND s_order_cnt = 78400 OR s_w_id = 281 AND s_order_cnt = 78961 OR s_w_id = 282 AND s_order_cnt = 79524 OR s_w_id = 283 AND s_order_cnt = 80089 OR s_w_id = 284 AND s_order_cnt = 80656 OR s_w_id = 285 AND s_order_cnt = 81225 OR s_w_id = 286 AND s_order_cnt = 81796 OR s_w_id = 287 AND s_order_cnt = 82369 OR s_w_id = 288 AND s_order_cnt = 82944 OR s_w_id = 289 AND s_order_cnt = 83521 OR s_w_id = 290 AND s_order_cnt = 84100 OR s_w_id = 291 AND s_order_cnt = 84681 OR s_w_id = 292 AND s_order_cnt = 85264 OR s_w_id = 293 AND s_order_cnt = 85849 OR s_w_id = 294 AND s_order_cnt = 86436 OR s_w_id = 295 AND s_order_cnt = 87025 OR s_w_id = 296 AND s_order_cnt = 87616 OR s_w_id = 297 AND s_order_cnt = 88209 OR s_w_id = 298 AND s_order_cnt = 88804 OR s_w_id = 299 AND s_order_cnt = 89401 OR s_w_id = 300 AND s_order_cnt = 90000 OR s_w_id = 301 AND s_order_cnt = 90601 OR s_w_id = 302 AND s_order_cnt = 91204 OR s_w_id = 303 AND s_order_cnt = 91809 OR s_w_id = 304 AND s_order_cnt = 92416 OR s_w_id = 305 AND s_order_cnt = 93025 OR s_w_id = 306 AND s_order_cnt = 93636 OR s_w_id = 307 AND s_order_cnt = 94249 OR s_w_id = 308 AND s_order_cnt = 94864 OR s_w_id = 309 AND s_order_cnt = 95481 OR s_w_id = 310 AND s_order_cnt = 96100 OR s_w_id = 311 AND s_order_cnt = 96721 OR s_w_id = 312 AND s_order_cnt = 97344 OR s_w_id = 313 AND s_order_cnt = 97969 OR s_w_id = 314 AND s_order_cnt = 98596 OR s_w_id = 315 AND s_order_cnt = 99225 OR s_w_id = 316 AND s_order_cnt = 99856 OR s_w_id = 317 AND s_order_cnt = 100489 OR s_w_id = 318 AND s_order_cnt = 101124 OR s_w_id = 319 AND s_order_cnt = 101761 OR s_w_id = 320 AND s_order_cnt = 102400 OR s_w_id = 321 AND s_order_cnt = 103041 OR s_w_id = 322 AND s_order_cnt = 103684 OR s_w_id = 323 AND s_order_cnt = 104329 OR s_w_id = 324 AND s_order_cnt = 104976 OR s_w_id = 325 AND s_order_cnt = 105625 OR s_w_id = 326 AND s_order_cnt = 106276 OR s_w_id = 327 AND s_order_cnt = 106929 OR s_w_id = 328 AND s_order_cnt = 107584 OR s_w_id = 329 AND s_order_cnt = 108241 OR s_w_id = 330 AND s_order_cnt = 108900 OR s_w_id = 331 AND s_order_cnt = 109561 OR s_w_id = 332 AND s_order_cnt = 110224 OR s_w_id = 333 AND s_order_cnt = 110889 OR s_w_id = 334 AND s_order_cnt = 111556 OR s_w_id = 335 AND s_order_cnt = 112225 OR s_w_id = 336 AND s_order_cnt = 112896 OR s_w_id = 337 AND s_order_cnt = 113569 OR s_w_id = 338 AND s_order_cnt = 114244 OR s_w_id = 339 AND s_order_cnt = 114921 OR s_w_id = 340 AND s_order_cnt = 115600 OR s_w_id = 341 AND s_order_cnt = 116281 OR s_w_id = 342 AND s_order_cnt = 116964 OR s_w_id = 343 AND s_order_cnt = 117649 OR s_w_id = 344 AND s_order_cnt = 118336 OR s_w_id = 345 AND s_order_cnt = 119025 OR s_w_id = 346 AND s_order_cnt = 119716 OR s_w_id = 347 AND s_order_cnt = 120409 OR s_w_id = 348 AND s_order_cnt = 121104 OR s_w_id = 349 AND s_order_cnt = 121801 OR s_w_id = 350 AND s_order_cnt = 122500 OR s_w_id = 351 AND s_order_cnt = 123201 OR s_w_id = 352 AND s_order_cnt = 123904 OR s_w_id = 353 AND s_order_cnt = 124609 OR s_w_id = 354 AND s_order_cnt = 125316 OR s_w_id = 355 AND s_order_cnt = 126025 OR s_w_id = 356 AND s_order_cnt = 126736 OR s_w_id = 357 AND s_order_cnt = 127449 OR s_w_id = 358 AND s_order_cnt = 128164 OR s_w_id = 359 AND s_order_cnt = 128881 OR s_w_id = 360 AND s_order_cnt = 129600 OR s_w_id = 361 AND s_order_cnt = 130321 OR s_w_id = 362 AND s_order_cnt = 131044 OR s_w_id = 363 AND s_order_cnt = 131769 OR s_w_id = 364 AND s_order_cnt = 132496 OR s_w_id = 365 AND s_order_cnt = 133225 OR s_w_id = 366 AND s_order_cnt = 133956 OR s_w_id = 367 AND s_order_cnt = 134689 OR s_w_id = 368 AND s_order_cnt = 135424 OR s_w_id = 369 AND s_order_cnt = 136161 OR s_w_id = 370 AND s_order_cnt = 136900 OR s_w_id = 371 AND s_order_cnt = 137641 OR s_w_id = 372 AND s_order_cnt = 138384 OR s_w_id = 373 AND s_order_cnt = 139129 OR s_w_id = 374 AND s_order_cnt = 139876 OR s_w_id = 375 AND s_order_cnt = 140625 OR s_w_id = 376 AND s_order_cnt = 141376 OR s_w_id = 377 AND s_order_cnt = 142129 OR s_w_id = 378 AND s_order_cnt = 142884 OR s_w_id = 379 AND s_order_cnt = 143641 OR s_w_id = 380 AND s_order_cnt = 144400 OR s_w_id = 381 AND s_order_cnt = 145161 OR s_w_id = 382 AND s_order_cnt = 145924 OR s_w_id = 383 AND s_order_cnt = 146689 OR s_w_id = 384 AND s_order_cnt = 147456 OR s_w_id = 385 AND s_order_cnt = 148225 OR s_w_id = 386 AND s_order_cnt = 148996 OR s_w_id = 387 AND s_order_cnt = 149769 OR s_w_id = 388 AND s_order_cnt = 150544 OR s_w_id = 389 AND s_order_cnt = 151321 OR s_w_id = 390 AND s_order_cnt = 152100 OR s_w_id = 391 AND s_order_cnt = 152881 OR s_w_id = 392 AND s_order_cnt = 153664 OR s_w_id = 393 AND s_order_cnt = 154449 OR s_w_id = 394 AND s_order_cnt = 155236 OR s_w_id = 395 AND s_order_cnt = 156025 OR s_w_id = 396 AND s_order_cnt = 156816 OR s_w_id = 397 AND s_order_cnt = 157609 OR s_w_id = 398 AND s_order_cnt = 158404 OR s_w_id = 399 AND s_order_cnt = 159201 OR s_w_id = 400 AND s_order_cnt = 160000 OR s_w_id = 401 AND s_order_cnt = 160801 OR s_w_id = 402 AND s_order_cnt = 161604 OR s_w_id = 403 AND s_order_cnt = 162409 OR s_w_id = 404 AND s_order_cnt = 163216 OR s_w_id = 405 AND s_order_cnt = 164025 OR s_w_id = 406 AND s_order_cnt = 164836 OR s_w_id = 407 AND s_order_cnt = 165649 OR s_w_id = 408 AND s_order_cnt = 166464 OR s_w_id = 409 AND s_order_cnt = 167281 OR s_w_id = 410 AND s_order_cnt = 168100 OR s_w_id = 411 AND s_order_cnt = 168921 OR s_w_id = 412 AND s_order_cnt = 169744 OR s_w_id = 413 AND s_order_cnt = 170569 OR s_w_id = 414 AND s_order_cnt = 171396 OR s_w_id = 415 AND s_order_cnt = 172225 OR s_w_id = 416 AND s_order_cnt = 173056 OR s_w_id = 417 AND s_order_cnt = 173889 OR s_w_id = 418 AND s_order_cnt = 174724 OR s_w_id = 419 AND s_order_cnt = 175561 OR s_w_id = 420 AND s_order_cnt = 176400 OR s_w_id = 421 AND s_order_cnt = 177241 OR s_w_id = 422 AND s_order_cnt = 178084 OR s_w_id = 423 AND s_order_cnt = 178929 OR s_w_id = 424 AND s_order_cnt = 179776 OR s_w_id = 425 AND s_order_cnt = 180625 OR s_w_id = 426 AND s_order_cnt = 181476 OR s_w_id = 427 AND s_order_cnt = 182329 OR s_w_id = 428 AND s_order_cnt = 183184 OR s_w_id = 429 AND s_order_cnt = 184041 OR s_w_id = 430 AND s_order_cnt = 184900 OR s_w_id = 431 AND s_order_cnt = 185761 OR s_w_id = 432 AND s_order_cnt = 186624 OR s_w_id = 433 AND s_order_cnt = 187489 OR s_w_id = 434 AND s_order_cnt = 188356 OR s_w_id = 435 AND s_order_cnt = 189225 OR s_w_id = 436 AND s_order_cnt = 190096 OR s_w_id = 437 AND s_order_cnt = 190969 OR s_w_id = 438 AND s_order_cnt = 191844 OR s_w_id = 439 AND s_order_cnt = 192721 OR s_w_id = 440 AND s_order_cnt = 193600 OR s_w_id = 441 AND s_order_cnt = 194481 OR s_w_id = 442 AND s_order_cnt = 195364 OR s_w_id = 443 AND s_order_cnt = 196249 OR s_w_id = 444 AND s_order_cnt = 197136 OR s_w_id = 445 AND s_order_cnt = 198025 OR s_w_id = 446 AND s_order_cnt = 198916 OR s_w_id = 447 AND s_order_cnt = 199809 OR s_w_id = 448 AND s_order_cnt = 200704 OR s_w_id = 449 AND s_order_cnt = 201601 OR s_w_id = 450 AND s_order_cnt = 202500 OR s_w_id = 451 AND s_order_cnt = 203401 OR s_w_id = 452 AND s_order_cnt = 204304 OR s_w_id = 453 AND s_order_cnt = 205209 OR s_w_id = 454 AND s_order_cnt = 206116 OR s_w_id = 455 AND s_order_cnt = 207025 OR s_w_id = 456 AND s_order_cnt = 207936 OR s_w_id = 457 AND s_order_cnt = 208849 OR s_w_id = 458 AND s_order_cnt = 209764 OR s_w_id = 459 AND s_order_cnt = 210681 OR s_w_id = 460 AND s_order_cnt = 211600 OR s_w_id = 461 AND s_order_cnt = 212521 OR s_w_id = 462 AND s_order_cnt = 213444 OR s_w_id = 463 AND s_order_cnt = 214369 OR s_w_id = 464 AND s_order_cnt = 215296 OR s_w_id = 465 AND s_order_cnt = 216225 OR s_w_id = 466 AND s_order_cnt = 217156 OR s_w_id = 467 AND s_order_cnt = 218089 OR s_w_id = 468 AND s_order_cnt = 219024 OR s_w_id = 469 AND s_order_cnt = 219961 OR s_w_id = 470 AND s_order_cnt = 220900 OR s_w_id = 471 AND s_order_cnt = 221841 OR s_w_id = 472 AND s_order_cnt = 222784 OR s_w_id = 473 AND s_order_cnt = 223729 OR s_w_id = 474 AND s_order_cnt = 224676 OR s_w_id = 475 AND s_order_cnt = 225625 OR s_w_id = 476 AND s_order_cnt = 226576 OR s_w_id = 477 AND s_order_cnt = 227529 OR s_w_id = 478 AND s_order_cnt = 228484 OR s_w_id = 479 AND s_order_cnt = 229441 OR s_w_id = 480 AND s_order_cnt = 230400 OR s_w_id = 481 AND s_order_cnt = 231361 OR s_w_id = 482 AND s_order_cnt = 232324 OR s_w_id = 483 AND s_order_cnt = 233289 OR s_w_id = 484 AND s_order_cnt = 234256 OR s_w_id = 485 AND s_order_cnt = 235225 OR s_w_id = 486 AND s_order_cnt = 236196 OR s_w_id = 487 AND s_order_cnt = 237169 OR s_w_id = 488 AND s_order_cnt = 238144 OR s_w_id = 489 AND s_order_cnt = 239121 OR s_w_id = 490 AND s_order_cnt = 240100 OR s_w_id = 491 AND s_order_cnt = 241081 OR s_w_id = 492 AND s_order_cnt = 242064 OR s_w_id = 493 AND s_order_cnt = 243049 OR s_w_id = 494 AND s_order_cnt = 244036 OR s_w_id = 495 AND s_order_cnt = 245025 OR s_w_id = 496 AND s_order_cnt = 246016 OR s_w_id = 497 AND s_order_cnt = 247009 OR s_w_id = 498 AND s_order_cnt = 248004 OR s_w_id = 499 AND s_order_cnt = 249001 OR s_w_id = 500 AND s_order_cnt = 250000 OR s_w_id = 501 AND s_order_cnt = 251001 OR s_w_id = 502 AND s_order_cnt = 252004 OR s_w_id = 503 AND s_order_cnt = 253009 OR s_w_id = 504 AND s_order_cnt = 254016 OR s_w_id = 505 AND s_order_cnt = 255025 OR s_w_id = 506 AND s_order_cnt = 256036 OR s_w_id = 507 AND s_order_cnt = 257049 OR s_w_id = 508 AND s_order_cnt = 258064 OR s_w_id = 509 AND s_order_cnt = 259081 OR s_w_id = 510 AND s_order_cnt = 260100 OR s_w_id = 511 AND s_order_cnt = 261121 OR s_w_id = 512 AND s_order_cnt = 262144 OR s_w_id = 513 AND s_order_cnt = 263169 OR s_w_id = 514 AND s_order_cnt = 264196 OR s_w_id = 515 AND s_order_cnt = 265225 OR s_w_id = 516 AND s_order_cnt = 266256 OR s_w_id = 517 AND s_order_cnt = 267289 OR s_w_id = 518 AND s_order_cnt = 268324 OR s_w_id = 519 AND s_order_cnt = 269361 OR s_w_id = 520 AND s_order_cnt = 270400 OR s_w_id = 521 AND s_order_cnt = 271441 OR s_w_id = 522 AND s_order_cnt = 272484 OR s_w_id = 523 AND s_order_cnt = 273529 OR s_w_id = 524 AND s_order_cnt = 274576 OR s_w_id = 525 AND s_order_cnt = 275625 OR s_w_id = 526 AND s_order_cnt = 276676 OR s_w_id = 527 AND s_order_cnt = 277729 OR s_w_id = 528 AND s_order_cnt = 278784 OR s_w_id = 529 AND s_order_cnt = 279841 OR s_w_id = 530 AND s_order_cnt = 280900 OR s_w_id = 531 AND s_order_cnt = 281961 OR s_w_id = 532 AND s_order_cnt = 283024 OR s_w_id = 533 AND s_order_cnt = 284089 OR s_w_id = 534 AND s_order_cnt = 285156 OR s_w_id = 535 AND s_order_cnt = 286225 OR s_w_id = 536 AND s_order_cnt = 287296 OR s_w_id = 537 AND s_order_cnt = 288369 OR s_w_id = 538 AND s_order_cnt = 289444 OR s_w_id = 539 AND s_order_cnt = 290521 OR s_w_id = 540 AND s_order_cnt = 291600 OR s_w_id = 541 AND s_order_cnt = 292681 OR s_w_id = 542 AND s_order_cnt = 293764 OR s_w_id = 543 AND s_order_cnt = 294849 OR s_w_id = 544 AND s_order_cnt = 295936 OR s_w_id = 545 AND s_order_cnt = 297025 OR s_w_id = 546 AND s_order_cnt = 298116 OR s_w_id = 547 AND s_order_cnt = 299209 OR s_w_id = 548 AND s_order_cnt = 300304 OR s_w_id = 549 AND s_order_cnt = 301401 OR s_w_id = 550 AND s_order_cnt = 302500 OR s_w_id = 551 AND s_order_cnt = 303601 OR s_w_id = 552 AND s_order_cnt = 304704 OR s_w_id = 553 AND s_order_cnt = 305809 OR s_w_id = 554 AND s_order_cnt = 306916 OR s_w_id = 555 AND s_order_cnt = 308025 OR s_w_id = 556 AND s_order_cnt = 309136 OR s_w_id = 557 AND s_order_cnt = 310249 OR s_w_id = 558 AND s_order_cnt = 311364 OR s_w_id = 559 AND s_order_cnt = 312481 OR s_w_id = 560 AND s_order_cnt = 313600 OR s_w_id = 561 AND s_order_cnt = 314721 OR s_w_id = 562 AND s_order_cnt = 315844 OR s_w_id = 563 AND s_order_cnt = 316969 OR s_w_id = 564 AND s_order_cnt = 318096 OR s_w_id = 565 AND s_order_cnt = 319225 OR s_w_id = 566 AND s_order_cnt = 320356 OR s_w_id = 567 AND s_order_cnt = 321489 OR s_w_id = 568 AND s_order_cnt = 322624 OR s_w_id = 569 AND s_order_cnt = 323761 OR s_w_id = 570 AND s_order_cnt = 324900 OR s_w_id = 571 AND s_order_cnt = 326041 OR s_w_id = 572 AND s_order_cnt = 327184 OR s_w_id = 573 AND s_order_cnt = 328329 OR s_w_id = 574 AND s_order_cnt = 329476 OR s_w_id = 575 AND s_order_cnt = 330625 OR s_w_id = 576 AND s_order_cnt = 331776 OR s_w_id = 577 AND s_order_cnt = 332929 OR s_w_id = 578 AND s_order_cnt = 334084 OR s_w_id = 579 AND s_order_cnt = 335241 OR s_w_id = 580 AND s_order_cnt = 336400 OR s_w_id = 581 AND s_order_cnt = 337561 OR s_w_id = 582 AND s_order_cnt = 338724 OR s_w_id = 583 AND s_order_cnt = 339889 OR s_w_id = 584 AND s_order_cnt = 341056 OR s_w_id = 585 AND s_order_cnt = 342225 OR s_w_id = 586 AND s_order_cnt = 343396 OR s_w_id = 587 AND s_order_cnt = 344569 OR s_w_id = 588 AND s_order_cnt = 345744 OR s_w_id = 589 AND s_order_cnt = 346921 OR s_w_id = 590 AND s_order_cnt = 348100 OR s_w_id = 591 AND s_order_cnt = 349281 OR s_w_id = 592 AND s_order_cnt = 350464 OR s_w_id = 593 AND s_order_cnt = 351649 OR s_w_id = 594 AND s_order_cnt = 352836 OR s_w_id = 595 AND s_order_cnt = 354025 OR s_w_id = 596 AND s_order_cnt = 355216 OR s_w_id = 597 AND s_order_cnt = 356409 OR s_w_id = 598 AND s_order_cnt = 357604 OR s_w_id = 599 AND s_order_cnt = 358801 OR s_w_id = 600 AND s_order_cnt = 360000 OR s_w_id = 601 AND s_order_cnt = 361201 OR s_w_id = 602 AND s_order_cnt = 362404 OR s_w_id = 603 AND s_order_cnt = 363609 OR s_w_id = 604 AND s_order_cnt = 364816 OR s_w_id = 605 AND s_order_cnt = 366025 OR s_w_id = 606 AND s_order_cnt = 367236 OR s_w_id = 607 AND s_order_cnt = 368449 OR s_w_id = 608 AND s_order_cnt = 369664 OR s_w_id = 609 AND s_order_cnt = 370881 OR s_w_id = 610 AND s_order_cnt = 372100 OR s_w_id = 611 AND s_order_cnt = 373321 OR s_w_id = 612 AND s_order_cnt = 374544 OR s_w_id = 613 AND s_order_cnt = 375769 OR s_w_id = 614 AND s_order_cnt = 376996 OR s_w_id = 615 AND s_order_cnt = 378225 OR s_w_id = 616 AND s_order_cnt = 379456 OR s_w_id = 617 AND s_order_cnt = 380689 OR s_w_id = 618 AND s_order_cnt = 381924 OR s_w_id = 619 AND s_order_cnt = 383161 OR s_w_id = 620 AND s_order_cnt = 384400 OR s_w_id = 621 AND s_order_cnt = 385641 OR s_w_id = 622 AND s_order_cnt = 386884 OR s_w_id = 623 AND s_order_cnt = 388129 OR s_w_id = 624 AND s_order_cnt = 389376 OR s_w_id = 625 AND s_order_cnt = 390625 OR s_w_id = 626 AND s_order_cnt = 391876 OR s_w_id = 627 AND s_order_cnt = 393129 OR s_w_id = 628 AND s_order_cnt = 394384 OR s_w_id = 629 AND s_order_cnt = 395641 OR s_w_id = 630 AND s_order_cnt = 396900 OR s_w_id = 631 AND s_order_cnt = 398161 OR s_w_id = 632 AND s_order_cnt = 399424 OR s_w_id = 633 AND s_order_cnt = 400689 OR s_w_id = 634 AND s_order_cnt = 401956 OR s_w_id = 635 AND s_order_cnt = 403225 OR s_w_id = 636 AND s_order_cnt = 404496 OR s_w_id = 637 AND s_order_cnt = 405769 OR s_w_id = 638 AND s_order_cnt = 407044 OR s_w_id = 639 AND s_order_cnt = 408321 OR s_w_id = 640 AND s_order_cnt = 409600 OR s_w_id = 641 AND s_order_cnt = 410881 OR s_w_id = 642 AND s_order_cnt = 412164 OR s_w_id = 643 AND s_order_cnt = 413449 OR s_w_id = 644 AND s_order_cnt = 414736 OR s_w_id = 645 AND s_order_cnt = 416025 OR s_w_id = 646 AND s_order_cnt = 417316 OR s_w_id = 647 AND s_order_cnt = 418609 OR s_w_id = 648 AND s_order_cnt = 419904 OR s_w_id = 649 AND s_order_cnt = 421201 OR s_w_id = 650 AND s_order_cnt = 422500 OR s_w_id = 651 AND s_order_cnt = 423801 OR s_w_id = 652 AND s_order_cnt = 425104 OR s_w_id = 653 AND s_order_cnt = 426409 OR s_w_id = 654 AND s_order_cnt = 427716 OR s_w_id = 655 AND s_order_cnt = 429025 OR s_w_id = 656 AND s_order_cnt = 430336 OR s_w_id = 657 AND s_order_cnt = 431649 OR s_w_id = 658 AND s_order_cnt = 432964 OR s_w_id = 659 AND s_order_cnt = 434281 OR s_w_id = 660 AND s_order_cnt = 435600 OR s_w_id = 661 AND s_order_cnt = 436921 OR s_w_id = 662 AND s_order_cnt = 438244 OR s_w_id = 663 AND s_order_cnt = 439569 OR s_w_id = 664 AND s_order_cnt = 440896 OR s_w_id = 665 AND s_order_cnt = 442225 OR s_w_id = 666 AND s_order_cnt = 443556 OR s_w_id = 667 AND s_order_cnt = 444889 OR s_w_id = 668 AND s_order_cnt = 446224 OR s_w_id = 669 AND s_order_cnt = 447561 OR s_w_id = 670 AND s_order_cnt = 448900 OR s_w_id = 671 AND s_order_cnt = 450241 OR s_w_id = 672 AND s_order_cnt = 451584 OR s_w_id = 673 AND s_order_cnt = 452929 OR s_w_id = 674 AND s_order_cnt = 454276 OR s_w_id = 675 AND s_order_cnt = 455625 OR s_w_id = 676 AND s_order_cnt = 456976 OR s_w_id = 677 AND s_order_cnt = 458329 OR s_w_id = 678 AND s_order_cnt = 459684 OR s_w_id = 679 AND s_order_cnt = 461041 OR s_w_id = 680 AND s_order_cnt = 462400 OR s_w_id = 681 AND s_order_cnt = 463761 OR s_w_id = 682 AND s_order_cnt = 465124 OR s_w_id = 683 AND s_order_cnt = 466489 OR s_w_id = 684 AND s_order_cnt = 467856 OR s_w_id = 685 AND s_order_cnt = 469225 OR s_w_id = 686 AND s_order_cnt = 470596 OR s_w_id = 687 AND s_order_cnt = 471969 OR s_w_id = 688 AND s_order_cnt = 473344 OR s_w_id = 689 AND s_order_cnt = 474721 OR s_w_id = 690 AND s_order_cnt = 476100 OR s_w_id = 691 AND s_order_cnt = 477481 OR s_w_id = 692 AND s_order_cnt = 478864 OR s_w_id = 693 AND s_order_cnt = 480249 OR s_w_id = 694 AND s_order_cnt = 481636 OR s_w_id = 695 AND s_order_cnt = 483025 OR s_w_id = 696 AND s_order_cnt = 484416 OR s_w_id = 697 AND s_order_cnt = 485809 OR s_w_id = 698 AND s_order_cnt = 487204 OR s_w_id = 699 AND s_order_cnt = 488601 OR s_w_id = 700 AND s_order_cnt = 490000 OR s_w_id = 701 AND s_order_cnt = 491401 OR s_w_id = 702 AND s_order_cnt = 492804 OR s_w_id = 703 AND s_order_cnt = 494209 OR s_w_id = 704 AND s_order_cnt = 495616 OR s_w_id = 705 AND s_order_cnt = 497025 OR s_w_id = 706 AND s_order_cnt = 498436 OR s_w_id = 707 AND s_order_cnt = 499849 OR s_w_id = 708 AND s_order_cnt = 501264 OR s_w_id = 709 AND s_order_cnt = 502681 OR s_w_id = 710 AND s_order_cnt = 504100 OR s_w_id = 711 AND s_order_cnt = 505521 OR s_w_id = 712 AND s_order_cnt = 506944 OR s_w_id = 713 AND s_order_cnt = 508369 OR s_w_id = 714 AND s_order_cnt = 509796 OR s_w_id = 715 AND s_order_cnt = 511225 OR s_w_id = 716 AND s_order_cnt = 512656 OR s_w_id = 717 AND s_order_cnt = 514089 OR s_w_id = 718 AND s_order_cnt = 515524 OR s_w_id = 719 AND s_order_cnt = 516961 OR s_w_id = 720 AND s_order_cnt = 518400 OR s_w_id = 721 AND s_order_cnt = 519841 OR s_w_id = 722 AND s_order_cnt = 521284 OR s_w_id = 723 AND s_order_cnt = 522729 OR s_w_id = 724 AND s_order_cnt = 524176 OR s_w_id = 725 AND s_order_cnt = 525625 OR s_w_id = 726 AND s_order_cnt = 527076 OR s_w_id = 727 AND s_order_cnt = 528529 OR s_w_id = 728 AND s_order_cnt = 529984 OR s_w_id = 729 AND s_order_cnt = 531441 OR s_w_id = 730 AND s_order_cnt = 532900 OR s_w_id = 731 AND s_order_cnt = 534361 OR s_w_id = 732 AND s_order_cnt = 535824 OR s_w_id = 733 AND s_order_cnt = 537289 OR s_w_id = 734 AND s_order_cnt = 538756 OR s_w_id = 735 AND s_order_cnt = 540225 OR s_w_id = 736 AND s_order_cnt = 541696 OR s_w_id = 737 AND s_order_cnt = 543169 OR s_w_id = 738 AND s_order_cnt = 544644 OR s_w_id = 739 AND s_order_cnt = 546121 OR s_w_id = 740 AND s_order_cnt = 547600 OR s_w_id = 741 AND s_order_cnt = 549081 OR s_w_id = 742 AND s_order_cnt = 550564 OR s_w_id = 743 AND s_order_cnt = 552049 OR s_w_id = 744 AND s_order_cnt = 553536 OR s_w_id = 745 AND s_order_cnt = 555025 OR s_w_id = 746 AND s_order_cnt = 556516 OR s_w_id = 747 AND s_order_cnt = 558009 OR s_w_id = 748 AND s_order_cnt = 559504 OR s_w_id = 749 AND s_order_cnt = 561001 OR s_w_id = 750 AND s_order_cnt = 562500 OR s_w_id = 751 AND s_order_cnt = 564001 OR s_w_id = 752 AND s_order_cnt = 565504 OR s_w_id = 753 AND s_order_cnt = 567009 OR s_w_id = 754 AND s_order_cnt = 568516 OR s_w_id = 755 AND s_order_cnt = 570025 OR s_w_id = 756 AND s_order_cnt = 571536 OR s_w_id = 757 AND s_order_cnt = 573049 OR s_w_id = 758 AND s_order_cnt = 574564 OR s_w_id = 759 AND s_order_cnt = 576081 OR s_w_id = 760 AND s_order_cnt = 577600 OR s_w_id = 761 AND s_order_cnt = 579121 OR s_w_id = 762 AND s_order_cnt = 580644 OR s_w_id = 763 AND s_order_cnt = 582169 OR s_w_id = 764 AND s_order_cnt = 583696 OR s_w_id = 765 AND s_order_cnt = 585225 OR s_w_id = 766 AND s_order_cnt = 586756 OR s_w_id = 767 AND s_order_cnt = 588289 OR s_w_id = 768 AND s_order_cnt = 589824 OR s_w_id = 769 AND s_order_cnt = 591361 OR s_w_id = 770 AND s_order_cnt = 592900 OR s_w_id = 771 AND s_order_cnt = 594441 OR s_w_id = 772 AND s_order_cnt = 595984 OR s_w_id = 773 AND s_order_cnt = 597529 OR s_w_id = 774 AND s_order_cnt = 599076 OR s_w_id = 775 AND s_order_cnt = 600625 OR s_w_id = 776 AND s_order_cnt = 602176 OR s_w_id = 777 AND s_order_cnt = 603729 OR s_w_id = 778 AND s_order_cnt = 605284 OR s_w_id = 779 AND s_order_cnt = 606841 OR s_w_id = 780 AND s_order_cnt = 608400 OR s_w_id = 781 AND s_order_cnt = 609961 OR s_w_id = 782 AND s_order_cnt = 611524 OR s_w_id = 783 AND s_order_cnt = 613089 OR s_w_id = 784 AND s_order_cnt = 614656 OR s_w_id = 785 AND s_order_cnt = 616225 OR s_w_id = 786 AND s_order_cnt = 617796 OR s_w_id = 787 AND s_order_cnt = 619369 OR s_w_id = 788 AND s_order_cnt = 620944 OR s_w_id = 789 AND s_order_cnt = 622521 OR s_w_id = 790 AND s_order_cnt = 624100 OR s_w_id = 791 AND s_order_cnt = 625681 OR s_w_id = 792 AND s_order_cnt = 627264 OR s_w_id = 793 AND s_order_cnt = 628849 OR s_w_id = 794 AND s_order_cnt = 630436 OR s_w_id = 795 AND s_order_cnt = 632025 OR s_w_id = 796 AND s_order_cnt = 633616 OR s_w_id = 797 AND s_order_cnt = 635209 OR s_w_id = 798 AND s_order_cnt = 636804 OR s_w_id = 799 AND s_order_cnt = 638401 OR s_w_id = 800 AND s_order_cnt = 640000 OR s_w_id = 801 AND s_order_cnt = 641601 OR s_w_id = 802 AND s_order_cnt = 643204 OR s_w_id = 803 AND s_order_cnt = 644809 OR s_w_id = 804 AND s_order_cnt = 646416 OR s_w_id = 805 AND s_order_cnt = 648025 OR s_w_id = 806 AND s_order_cnt = 649636 OR s_w_id = 807 AND s_order_cnt = 651249 OR s_w_id = 808 AND s_order_cnt = 652864 OR s_w_id = 809 AND s_order_cnt = 654481 OR s_w_id = 810 AND s_order_cnt = 656100 OR s_w_id = 811 AND s_order_cnt = 657721 OR s_w_id = 812 AND s_order_cnt = 659344 OR s_w_id = 813 AND s_order_cnt = 660969 OR s_w_id = 814 AND s_order_cnt = 662596 OR s_w_id = 815 AND s_order_cnt = 664225 OR s_w_id = 816 AND s_order_cnt = 665856 OR s_w_id = 817 AND s_order_cnt = 667489 OR s_w_id = 818 AND s_order_cnt = 669124 OR s_w_id = 819 AND s_order_cnt = 670761 OR s_w_id = 820 AND s_order_cnt = 672400 OR s_w_id = 821 AND s_order_cnt = 674041 OR s_w_id = 822 AND s_order_cnt = 675684 OR s_w_id = 823 AND s_order_cnt = 677329 OR s_w_id = 824 AND s_order_cnt = 678976 OR s_w_id = 825 AND s_order_cnt = 680625 OR s_w_id = 826 AND s_order_cnt = 682276 OR s_w_id = 827 AND s_order_cnt = 683929 OR s_w_id = 828 AND s_order_cnt = 685584 OR s_w_id = 829 AND s_order_cnt = 687241 OR s_w_id = 830 AND s_order_cnt = 688900 OR s_w_id = 831 AND s_order_cnt = 690561 OR s_w_id = 832 AND s_order_cnt = 692224 OR s_w_id = 833 AND s_order_cnt = 693889 OR s_w_id = 834 AND s_order_cnt = 695556 OR s_w_id = 835 AND s_order_cnt = 697225 OR s_w_id = 836 AND s_order_cnt = 698896 OR s_w_id = 837 AND s_order_cnt = 700569 OR s_w_id = 838 AND s_order_cnt = 702244 OR s_w_id = 839 AND s_order_cnt = 703921 OR s_w_id = 840 AND s_order_cnt = 705600 OR s_w_id = 841 AND s_order_cnt = 707281 OR s_w_id = 842 AND s_order_cnt = 708964 OR s_w_id = 843 AND s_order_cnt = 710649 OR s_w_id = 844 AND s_order_cnt = 712336 OR s_w_id = 845 AND s_order_cnt = 714025 OR s_w_id = 846 AND s_order_cnt = 715716 OR s_w_id = 847 AND s_order_cnt = 717409 OR s_w_id = 848 AND s_order_cnt = 719104 OR s_w_id = 849 AND s_order_cnt = 720801 OR s_w_id = 850 AND s_order_cnt = 722500 OR s_w_id = 851 AND s_order_cnt = 724201 OR s_w_id = 852 AND s_order_cnt = 725904 OR s_w_id = 853 AND s_order_cnt = 727609 OR s_w_id = 854 AND s_order_cnt = 729316 OR s_w_id = 855 AND s_order_cnt = 731025 OR s_w_id = 856 AND s_order_cnt = 732736 OR s_w_id = 857 AND s_order_cnt = 734449 OR s_w_id = 858 AND s_order_cnt = 736164 OR s_w_id = 859 AND s_order_cnt = 737881 OR s_w_id = 860 AND s_order_cnt = 739600 OR s_w_id = 861 AND s_order_cnt = 741321 OR s_w_id = 862 AND s_order_cnt = 743044 OR s_w_id = 863 AND s_order_cnt = 744769 OR s_w_id = 864 AND s_order_cnt = 746496 OR s_w_id = 865 AND s_order_cnt = 748225 OR s_w_id = 866 AND s_order_cnt = 749956 OR s_w_id = 867 AND s_order_cnt = 751689 OR s_w_id = 868 AND s_order_cnt = 753424 OR s_w_id = 869 AND s_order_cnt = 755161 OR s_w_id = 870 AND s_order_cnt = 756900 OR s_w_id = 871 AND s_order_cnt = 758641 OR s_w_id = 872 AND s_order_cnt = 760384 OR s_w_id = 873 AND s_order_cnt = 762129 OR s_w_id = 874 AND s_order_cnt = 763876 OR s_w_id = 875 AND s_order_cnt = 765625 OR s_w_id = 876 AND s_order_cnt = 767376 OR s_w_id = 877 AND s_order_cnt = 769129 OR s_w_id = 878 AND s_order_cnt = 770884 OR s_w_id = 879 AND s_order_cnt = 772641 OR s_w_id = 880 AND s_order_cnt = 774400 OR s_w_id = 881 AND s_order_cnt = 776161 OR s_w_id = 882 AND s_order_cnt = 777924 OR s_w_id = 883 AND s_order_cnt = 779689 OR s_w_id = 884 AND s_order_cnt = 781456 OR s_w_id = 885 AND s_order_cnt = 783225 OR s_w_id = 886 AND s_order_cnt = 784996 OR s_w_id = 887 AND s_order_cnt = 786769 OR s_w_id = 888 AND s_order_cnt = 788544 OR s_w_id = 889 AND s_order_cnt = 790321 OR s_w_id = 890 AND s_order_cnt = 792100 OR s_w_id = 891 AND s_order_cnt = 793881 OR s_w_id = 892 AND s_order_cnt = 795664 OR s_w_id = 893 AND s_order_cnt = 797449 OR s_w_id = 894 AND s_order_cnt = 799236 OR s_w_id = 895 AND s_order_cnt = 801025 OR s_w_id = 896 AND s_order_cnt = 802816 OR s_w_id = 897 AND s_order_cnt = 804609 OR s_w_id = 898 AND s_order_cnt = 806404 OR s_w_id = 899 AND s_order_cnt = 808201 OR s_w_id = 900 AND s_order_cnt = 810000 OR s_w_id = 901 AND s_order_cnt = 811801 OR s_w_id = 902 AND s_order_cnt = 813604 OR s_w_id = 903 AND s_order_cnt = 815409 OR s_w_id = 904 AND s_order_cnt = 817216 OR s_w_id = 905 AND s_order_cnt = 819025 OR s_w_id = 906 AND s_order_cnt = 820836 OR s_w_id = 907 AND s_order_cnt = 822649 OR s_w_id = 908 AND s_order_cnt = 824464 OR s_w_id = 909 AND s_order_cnt = 826281 OR s_w_id = 910 AND s_order_cnt = 828100 OR s_w_id = 911 AND s_order_cnt = 829921 OR s_w_id = 912 AND s_order_cnt = 831744 OR s_w_id = 913 AND s_order_cnt = 833569 OR s_w_id = 914 AND s_order_cnt = 835396 OR s_w_id = 915 AND s_order_cnt = 837225 OR s_w_id = 916 AND s_order_cnt = 839056 OR s_w_id = 917 AND s_order_cnt = 840889 OR s_w_id = 918 AND s_order_cnt = 842724 OR s_w_id = 919 AND s_order_cnt = 844561 OR s_w_id = 920 AND s_order_cnt = 846400 OR s_w_id = 921 AND s_order_cnt = 848241 OR s_w_id = 922 AND s_order_cnt = 850084 OR s_w_id = 923 AND s_order_cnt = 851929 OR s_w_id = 924 AND s_order_cnt = 853776 OR s_w_id = 925 AND s_order_cnt = 855625 OR s_w_id = 926 AND s_order_cnt = 857476 OR s_w_id = 927 AND s_order_cnt = 859329 OR s_w_id = 928 AND s_order_cnt = 861184 OR s_w_id = 929 AND s_order_cnt = 863041 OR s_w_id = 930 AND s_order_cnt = 864900 OR s_w_id = 931 AND s_order_cnt = 866761 OR s_w_id = 932 AND s_order_cnt = 868624 OR s_w_id = 933 AND s_order_cnt = 870489 OR s_w_id = 934 AND s_order_cnt = 872356 OR s_w_id = 935 AND s_order_cnt = 874225 OR s_w_id = 936 AND s_order_cnt = 876096 OR s_w_id = 937 AND s_order_cnt = 877969 OR s_w_id = 938 AND s_order_cnt = 879844 OR s_w_id = 939 AND s_order_cnt = 881721 OR s_w_id = 940 AND s_order_cnt = 883600 OR s_w_id = 941 AND s_order_cnt = 885481 OR s_w_id = 942 AND s_order_cnt = 887364 OR s_w_id = 943 AND s_order_cnt = 889249 OR s_w_id = 944 AND s_order_cnt = 891136 OR s_w_id = 945 AND s_order_cnt = 893025 OR s_w_id = 946 AND s_order_cnt = 894916 OR s_w_id = 947 AND s_order_cnt = 896809 OR s_w_id = 948 AND s_order_cnt = 898704 OR s_w_id = 949 AND s_order_cnt = 900601 OR s_w_id = 950 AND s_order_cnt = 902500 OR s_w_id = 951 AND s_order_cnt = 904401 OR s_w_id = 952 AND s_order_cnt = 906304 OR s_w_id = 953 AND s_order_cnt = 908209 OR s_w_id = 954 AND s_order_cnt = 910116 OR s_w_id = 955 AND s_order_cnt = 912025 OR s_w_id = 956 AND s_order_cnt = 913936 OR s_w_id = 957 AND s_order_cnt = 915849 OR s_w_id = 958 AND s_order_cnt = 917764 OR s_w_id = 959 AND s_order_cnt = 919681 OR s_w_id = 960 AND s_order_cnt = 921600 OR s_w_id = 961 AND s_order_cnt = 923521 OR s_w_id = 962 AND s_order_cnt = 925444 OR s_w_id = 963 AND s_order_cnt = 927369 OR s_w_id = 964 AND s_order_cnt = 929296 OR s_w_id = 965 AND s_order_cnt = 931225 OR s_w_id = 966 AND s_order_cnt = 933156 OR s_w_id = 967 AND s_order_cnt = 935089 OR s_w_id = 968 AND s_order_cnt = 937024 OR s_w_id = 969 AND s_order_cnt = 938961 OR s_w_id = 970 AND s_order_cnt = 940900 OR s_w_id = 971 AND s_order_cnt = 942841 OR s_w_id = 972 AND s_order_cnt = 944784 OR s_w_id = 973 AND s_order_cnt = 946729 OR s_w_id = 974 AND s_order_cnt = 948676 OR s_w_id = 975 AND s_order_cnt = 950625 OR s_w_id = 976 AND s_order_cnt = 952576 OR s_w_id = 977 AND s_order_cnt = 954529 OR s_w_id = 978 AND s_order_cnt = 956484 OR s_w_id = 979 AND s_order_cnt = 958441 OR s_w_id = 980 AND s_order_cnt = 960400 OR s_w_id = 981 AND s_order_cnt = 962361 OR s_w_id = 982 AND s_order_cnt = 964324 OR s_w_id = 983 AND s_order_cnt = 966289 OR s_w_id = 984 AND s_order_cnt = 968256 OR s_w_id = 985 AND s_order_cnt = 970225 OR s_w_id = 986 AND s_order_cnt = 972196 OR s_w_id = 987 AND s_order_cnt = 974169 OR s_w_id = 988 AND s_order_cnt = 976144 OR s_w_id = 989 AND s_order_cnt = 978121 OR s_w_id = 990 AND s_order_cnt = 980100 OR s_w_id = 991 AND s_order_cnt = 982081 OR s_w_id = 992 AND s_order_cnt = 984064 OR s_w_id = 993 AND s_order_cnt = 986049 OR s_w_id = 994 AND s_order_cnt = 988036 OR s_w_id = 995 AND s_order_cnt = 990025 OR s_w_id = 996 AND s_order_cnt = 992016 OR s_w_id = 997 AND s_order_cnt = 994009 OR s_w_id = 998 AND s_order_cnt = 996004 OR s_w_id = 999 AND s_order_cnt = 998001 OR s_w_id = 1000 AND s_order_cnt = 1000000)
		`,
		args: []interface{}{1, 2, 3, 4},
	},
}

func init() {
	security.SetAssetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)

	// Add a table with many columns and many indexes.
	var indexes strings.Builder
	for i := 0; i < 250; i++ {
		indexes.WriteString(",\nINDEX (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w)")
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
	for _, query := range queries {
		h := newHarness(b, query)
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
	evalCtx   tree.EvalContext
	prepMemo  *memo.Memo
	testCat   *testcat.Catalog
	optimizer xform.Optimizer
}

func newHarness(tb testing.TB, query benchQuery) *harness {
	h := &harness{
		ctx:     context.Background(),
		semaCtx: tree.MakeSemaContext(),
		evalCtx: tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings()),
	}

	// Set up the test catalog.
	h.testCat = testcat.New()
	for _, schema := range schemas {
		_, err := h.testCat.ExecuteDDL(schema)
		if err != nil {
			tb.Fatalf("%v", err)
		}
	}

	if err := h.semaCtx.Placeholders.Init(len(query.args), nil /* typeHints */); err != nil {
		tb.Fatal(err)
	}
	// Run optbuilder to build the memo for Prepare. Even if we will not be using
	// the Prepare method, we still want to run the optbuilder to infer any
	// placeholder types.
	stmt, err := parser.ParseOne(query.query)
	if err != nil {
		tb.Fatalf("%v", err)
	}
	h.optimizer.Init(&h.evalCtx, h.testCat)
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
		if _, _, err := h.optimizer.TryPlaceholderFastPath(); err != nil {
			tb.Fatalf("%v", err)
		}
	}
	h.prepMemo = h.optimizer.DetachMemo()
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
			tree.VolatilityVolatile,
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

	h.optimizer.Init(&h.evalCtx, h.testCat)
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

	root := execMemo.RootExpr()
	eb := execbuilder.New(
		exec.StubFactory{},
		&h.optimizer,
		execMemo,
		nil, /* catalog */
		root,
		&h.evalCtx,
		true, /* allowAutoCommit */
	)
	if _, err = eb.Build(); err != nil {
		tb.Fatalf("%v", err)
	}
}

// runPrepared simulates running the query after it was prepared.
func (h *harness) runPrepared(tb testing.TB, phase Phase) {
	h.optimizer.Init(&h.evalCtx, h.testCat)

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

	root := execMemo.RootExpr()
	eb := execbuilder.New(
		exec.StubFactory{},
		&h.optimizer,
		execMemo,
		nil, /* catalog */
		root,
		&h.evalCtx,
		true, /* allowAutoCommit */
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

// BenchmarkChain benchmarks the planning of a "chain" query, where
// some number of tables are joined together, with there being a
// predicate joining the first and second, second and third, third
// and fourth, etc.
//
// For example, a 5-chain looks like:
//
//   SELECT * FROM a, b, c, d, e
//   WHERE a.x = b.y
//     AND b.x = c.y
//     AND c.x = d.y
//     AND d.x = e.y
//
func BenchmarkChain(b *testing.B) {
	for i := 1; i < 20; i++ {
		q := makeChain(i)
		h := newHarness(b, q)
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
	sr.Exec(b, `CREATE DATABASE bench`)
	for _, schema := range schemas {
		sr.Exec(b, schema)
	}

	for _, query := range queries {
		b.Run(query.name, func(b *testing.B) {
			b.Run("Simple", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					sr.Exec(b, query.query, query.args...)
				}
			})
			b.Run("Prepared", func(b *testing.B) {
				prepared, err := db.Prepare(query.query)
				if err != nil {
					b.Fatalf("%v", err)
				}
				for i := 0; i < b.N; i++ {
					if _, err = prepared.Exec(query.args...); err != nil {
						b.Fatalf("%v", err)
					}
				}
			})
		})
	}
}
