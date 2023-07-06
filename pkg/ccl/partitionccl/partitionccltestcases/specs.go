// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package partitionccltestcases

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// PartitioningTestSpec represents a single test case used in the various
// partitioning-related tests.
type PartitioningTestSpec struct {
	// name is a name for the test, suitable for use as the subtest name.
	Name string

	// Schema is a full CREATE TABLE statement with a literal `%s` where the
	// table name should be.
	Schema string

	// configs are each a shorthand for a zone config, formatted as
	// `@index_name` or `.partition_name`. Optionally a suffix of a colon and a
	// comma-separated list of constraints may be included (`@index_name:+dc1`).
	// These will be Parsed into `Parsed.subzones`.
	Configs []string

	// GeneratedSpans is 1:1 to the output of GenerateSubzoneSpans, each
	// formatted as `{subzone} {start}-{end}` (e.g. `@primary /1-/2`), where
	// {subzone} is formatted identically to the test shorthand above, and
	// {start} and {end} are formatted using our key pretty printer, but with
	// the table removed. The end key is always specified in here (though
	// GenerateSubzoneSpans omits it under certain conditions to save space).
	GeneratedSpans []string

	// Scans are each a shorthand for an assertion of where data should live.
	// The map key is the used for the `WHERE` clause of a `SELECT *` and the
	// value is a comma separated allowlist of nodes that are allowed to serve
	// this query. Example: `map[string]string{`b = 1`: `n2`}` means that
	// `SELECT * FROM t WHERE b = 1` is required to be served entirely by node2.
	//
	// TODO(dan): These should be based on replication zone attributes instead
	// of node IDs.
	Scans map[string]string
}

func (ps PartitioningTestSpec) HasScans() bool {
	return len(ps.Scans) > 0
}

func (ps PartitioningTestSpec) HasGeneratedSpans() bool {
	return len(ps.GeneratedSpans) > 0
}

func DeterministicPartitioningTestSpecs() []PartitioningTestSpec {
	return []PartitioningTestSpec{
		{
			Name:   `unpartitioned`,
			Schema: `CREATE TABLE %s (a INT PRIMARY KEY)`,
		},

		{
			Name:           `all_indexes`,
			Schema:         `CREATE TABLE %s (a INT PRIMARY KEY, b INT, c INT, INDEX idx1 (b), INDEX idx2 (c))`,
			Configs:        []string{`@primary`, `@idx1:+n2`, `@idx2:+n3`},
			GeneratedSpans: []string{`@primary /1-/2`, `@idx1 /2-/3`, `@idx2 /3-/4`},
			Scans:          map[string]string{`b = 1`: `n2`, `c = 1`: `n3`},
		},
		{
			Name:           `all_indexes_shuffled`,
			Schema:         `CREATE TABLE %s (a INT PRIMARY KEY, b INT, c INT, INDEX idx1 (b), INDEX idx2 (c))`,
			Configs:        []string{`@idx2:+n2`, `@primary`, `@idx1:+n3`},
			GeneratedSpans: []string{`@primary /1-/2`, `@idx1 /2-/3`, `@idx2 /3-/4`},
			Scans:          map[string]string{`b = 1`: `n3`, `c = 1`: `n2`},
		},
		{
			Name:           `some_indexes`,
			Schema:         `CREATE TABLE %s (a INT PRIMARY KEY, b INT, c INT, INDEX idx1 (b), INDEX idx2 (c))`,
			Configs:        []string{`@primary`, `@idx2:+n2`},
			GeneratedSpans: []string{`@primary /1-/2`, `@idx2 /3-/4`},
			Scans:          map[string]string{`c = 1`: `n2`},
		},

		{
			Name: `single_col_list_partitioning`,
			Schema: `CREATE TABLE %s (a INT PRIMARY KEY) PARTITION BY LIST (a) (
				PARTITION p3 VALUES IN (3),
				PARTITION p4 VALUES IN (4)
			)`,
			Configs: []string{`@primary:+n1`, `.p3:+n2`, `.p4:+n3`},
			GeneratedSpans: []string{
				`@primary /1-/1/3`,
				`     .p3 /1/3-/1/4`,
				`     .p4 /1/4-/1/5`,
				`@primary /1/5-/2`,
			},
			Scans: map[string]string{`a < 3`: `n1`, `a = 3`: `n2`, `a = 4`: `n3`, `a > 4`: `n1`},
		},
		{
			// Intentionally a little different than `single col list
			// partitioning` for the repartitioning tests.
			Name: `single_col_list_partitioning_default`,
			Schema: `CREATE TABLE %s (a INT PRIMARY KEY) PARTITION BY LIST (a) (
				PARTITION p4 VALUES IN (4),
				PARTITION p5 VALUES IN (5),
				PARTITION pd VALUES IN (DEFAULT)
			)`,
			Configs: []string{`@primary`, `.p4:+n2`, `.p5:+n3`, `.pd:+n1`},
			GeneratedSpans: []string{
				`.pd /1-/1/4`,
				`.p4 /1/4-/1/5`,
				`.p5 /1/5-/1/6`,
				`.pd /1/6-/2`,
			},
			Scans: map[string]string{`a < 4`: `n1`, `a = 4`: `n2`, `a = 5`: `n3`, `a > 5`: `n1`},
		},
		{
			Name: `multi_col_list_partitioning`,
			Schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a, b) (
				PARTITION p34 VALUES IN ((3, 4)),
				PARTITION p56 VALUES IN ((5, 6)),
				PARTITION p57 VALUES IN ((5, 7))
			)`,
			Configs: []string{`@primary:+n1`, `.p34:+n2`, `.p56:+n3`, `.p57:+n1`},
			GeneratedSpans: []string{
				`@primary /1-/1/3/4`,
				`    .p34 /1/3/4-/1/3/5`,
				`@primary /1/3/5-/1/5/6`,
				`    .p56 /1/5/6-/1/5/7`,
				`    .p57 /1/5/7-/1/5/8`,
				`@primary /1/5/8-/2`,
			},
			Scans: map[string]string{
				`(a, b) < (3, 4)`:                     `n1`,
				`a = 3 AND b = 4`:                     `n2`,
				`(a, b) > (3, 4) AND (a, b) < (5, 6)`: `n1`,
				`a = 5 AND b = 6`:                     `n3`,
				`a = 5 AND b = 7`:                     `n1`,
				`(a, b) > (5, 7)`:                     `n1`,
			},
		},
		{
			// Intentionally a little different than `multi col list
			// partitioning` for the repartitioning tests.
			Name: `multi_col_list_partitioning_default`,
			Schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a, b) (
				PARTITION p34 VALUES IN ((3, 4)),
				PARTITION p57 VALUES IN ((5, 7)),
				PARTITION p58 VALUES IN ((5, 8)),
				PARTITION p5d VALUES IN ((5, DEFAULT))
			)`,
			Configs: []string{`@primary:+n1`, `.p34:+n2`, `.p57:+n3`, `.p58:+n1`, `.p5d:+n2`},
			GeneratedSpans: []string{
				`@primary /1-/1/3/4`,
				`    .p34 /1/3/4-/1/3/5`,
				`@primary /1/3/5-/1/5`,
				`    .p5d /1/5-/1/5/7`,
				`    .p57 /1/5/7-/1/5/8`,
				`    .p58 /1/5/8-/1/5/9`,
				`    .p5d /1/5/9-/1/6`,
				`@primary /1/6-/2`,
			},
			Scans: map[string]string{
				`(a, b) < (3, 4)`:           `n1`,
				`a = 3 AND b = 4`:           `n2`,
				`(a, b) > (3, 4) AND a < 5`: `n1`,
				`a = 5 AND b < 7`:           `n2`,
				`a = 5 AND b = 7`:           `n3`,
				`a = 5 AND b = 8`:           `n1`,
				`a = 5 AND b > 8`:           `n2`,
				`a > 5`:                     `n1`,
			},
		},
		{
			Name: `multi_col_list_partitioning_default_default`,
			Schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a, b) (
				PARTITION p34 VALUES IN ((3, 4)),
				PARTITION p57 VALUES IN ((5, 7)),
				PARTITION p58 VALUES IN ((5, 8)),
				PARTITION p5d VALUES IN ((5, DEFAULT)),
				PARTITION pd VALUES IN ((DEFAULT, DEFAULT))
			)`,
			Configs: []string{`@primary`, `.p34:+n1`, `.p57:+n2`, `.p58:+n3`, `.p5d:+n1`, `.pd:+n2`},
			GeneratedSpans: []string{
				` .pd /1-/1/3/4`,
				`.p34 /1/3/4-/1/3/5`,
				` .pd /1/3/5-/1/5`,
				`.p5d /1/5-/1/5/7`,
				`.p57 /1/5/7-/1/5/8`,
				`.p58 /1/5/8-/1/5/9`,
				`.p5d /1/5/9-/1/6`,
				` .pd /1/6-/2`,
			},
			Scans: map[string]string{
				`(a, b) < (3, 4)`:           `n2`,
				`a = 3 AND b = 4`:           `n1`,
				`(a, b) > (3, 4) AND a < 5`: `n2`,
				`a = 5 AND b < 7`:           `n1`,
				`a = 5 AND b = 7`:           `n2`,
				`a = 5 AND b = 8`:           `n3`,
				`a = 5 AND b > 8`:           `n1`,
				`a > 5`:                     `n2`,
			},
		},
		{
			// Similar to `multi_col_list_partitioning_default_default` but
			// via subpartitioning instead of multi col.
			Name: `multi_col_list_partitioning_default_default_subpartitioned`,
			Schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a) (
				PARTITION p3 VALUES IN (3) PARTITION BY LIST (b) (
					PARTITION p34 VALUES IN (4)
				),
				PARTITION p5 VALUES IN (5) PARTITION BY LIST (b) (
					PARTITION p57 VALUES IN (7),
					PARTITION p58 VALUES IN (8),
					PARTITION p5d VALUES IN (DEFAULT)
				),
				PARTITION pd VALUES IN (DEFAULT)
			)`,
			Configs: []string{`@primary`, `.p34:+n1`, `.p57:+n2`, `.p58:+n3`, `.p5d:+n1`, `.pd:+n2`},
			GeneratedSpans: []string{
				` .pd /1-/1/3/4`,
				`.p34 /1/3/4-/1/3/5`,
				` .pd /1/3/5-/1/5`,
				`.p5d /1/5-/1/5/7`,
				`.p57 /1/5/7-/1/5/8`,
				`.p58 /1/5/8-/1/5/9`,
				`.p5d /1/5/9-/1/6`,
				` .pd /1/6-/2`,
			},
			Scans: map[string]string{
				`(a, b) < (3, 4)`:           `n2`,
				`a = 3 AND b = 4`:           `n1`,
				`(a, b) > (3, 4) AND a < 5`: `n2`,
				`a = 5 AND b < 7`:           `n1`,
				`a = 5 AND b = 7`:           `n2`,
				`a = 5 AND b = 8`:           `n3`,
				`a = 5 AND b > 8`:           `n1`,
				`a > 5`:                     `n2`,
			},
		},

		{
			Name: `single_col_range_partitioning`,
			Schema: `CREATE TABLE %s (a INT PRIMARY KEY) PARTITION BY RANGE (a) (
				PARTITION p3 VALUES FROM (MINVALUE) TO (3),
				PARTITION p4 VALUES FROM (3) TO (4)
			)`,
			Configs: []string{`@primary:+n1`, `.p3:+n2`, `.p4:+n3`},
			GeneratedSpans: []string{
				`     .p3 /1-/1/3`,
				`     .p4 /1/3-/1/4`,
				`@primary /1/4-/2`,
			},
			Scans: map[string]string{
				`a < 3`:            `n2`,
				`a >= 3 AND a < 4`: `n3`,
				`a >= 4`:           `n1`,
			},
		},
		{
			// If this test seems confusing, see the note on the multi-col equivalent.
			Name: `single_col_range_partitioning_descending`,
			Schema: `CREATE TABLE %s (a INT, PRIMARY KEY (a DESC)) PARTITION BY RANGE (a) (
				PARTITION p4 VALUES FROM (MINVALUE) TO (4),
				PARTITION p3 VALUES FROM (4) TO (3),
				PARTITION px VALUES FROM (3) TO (MAXVALUE)
			)`,
			Configs: []string{`.p4:+n1`, `.p3:+n2`, `.px:+n3`},
			GeneratedSpans: []string{
				`.p4 /1-/1/4`,
				`.p3 /1/4-/1/3`,
				`.px /1/3-/2`,
			},
			Scans: map[string]string{
				`a > 4`:            `n1`,
				`a <= 4 AND a > 3`: `n2`,
				`a <= 3`:           `n3`,
			},
		},
		{
			Name: `sparse_single_col_range_partitioning`,
			Schema: `CREATE TABLE %s (a INT PRIMARY KEY) PARTITION BY RANGE (a) (
				PARTITION p1 VALUES FROM (1) TO (2),
				PARTITION p3 VALUES FROM (3) TO (4)
			)`,
			Configs: []string{`@primary:+n1`, `.p1:+n2`, `.p3:+n3`},
			GeneratedSpans: []string{
				`@primary /1-/1/1`,
				`     .p1 /1/1-/1/2`,
				`@primary /1/2-/1/3`,
				`     .p3 /1/3-/1/4`,
				`@primary /1/4-/2`,
			},
			Scans: map[string]string{
				`a < 1`:            `n1`,
				`a >= 1 AND a < 2`: `n2`,
				`a >= 2 AND a < 3`: `n1`,
				`a >= 3 AND a < 4`: `n3`,
				`a > 4`:            `n1`,
			},
		},
		{
			// Intentionally a little different than `single col range
			// partitioning` for the repartitioning tests.
			Name: `single_col_range_partitioning_maxvalue`,
			Schema: `CREATE TABLE %s (a INT PRIMARY KEY) PARTITION BY RANGE (a) (
				PARTITION p4 VALUES FROM (MINVALUE) TO (4),
				PARTITION p5 VALUES FROM (4) TO (5),
				PARTITION px VALUES FROM (5) TO (MAXVALUE)
			)`,
			Configs: []string{`@primary`, `.p4:+n1`, `.p5:+n2`, `.px:+n3`},
			GeneratedSpans: []string{
				`.p4 /1-/1/4`,
				`.p5 /1/4-/1/5`,
				`.px /1/5-/2`,
			},
			Scans: map[string]string{
				`a < 4`:            `n1`,
				`a >= 4 AND a < 5`: `n2`,
				`a > 5`:            `n3`,
			},
		},
		{
			Name: `multi_col_range_partitioning`,
			Schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY RANGE (a, b) (
				PARTITION p34 VALUES FROM (MINVALUE, MINVALUE) TO (3, 4),
				PARTITION p56 VALUES FROM (3, 4) TO (5, 6),
				PARTITION p57 VALUES FROM (5, 6) TO (5, 7)
			)`,
			Configs: []string{`@primary:+n1`, `.p34:+n2`, `.p56:+n3`, `.p57:+n1`},
			GeneratedSpans: []string{
				`    .p34 /1-/1/3/4`,
				`    .p56 /1/3/4-/1/5/6`,
				`    .p57 /1/5/6-/1/5/7`,
				`@primary /1/5/7-/2`,
			},
			Scans: map[string]string{
				`(a, b) < (3, 4)`:                      `n2`,
				`(a, b) >= (3, 4) AND (a, b) < (5, 6)`: `n3`,
				`(a, b) >= (5, 6) AND (a, b) < (5, 7)`: `n1`,
				`(a, b) >= (5, 7)`:                     `n1`,
			},
		},
		{
			// MINVALUE and MAXVALUE are brutally confusing when used with a column
			// stored in descending order. MINVALUE means "the value that sorts before
			// the earliest value in the Index", and so in the case of a descending
			// INT column, represents a large *positive* integer, i.e., one greater
			// than the maximum representable integer. Similarly, MAXVALUE represents
			// a large *negative* integer.
			//
			// It's not clear that anything can be done. Switching the meaning of
			// MINVALUE/MAXVALUE for descending columns would be quite confusing in
			// the multi-col case. For example, in the table below, the minimum
			// possible tuple would be (MINVALUE, MAXVALUE, MINVALUE) and the maximum
			// possible would be (MAXVALUE, MINVALUE, MAXVALUE). Neither is exactly
			// intuitive. Consider also that (6, MINVALUE, MINVALUE) would be invalid,
			// as a descending MINVALUE is not equivalent to an ascending MINVALUE.
			// How would we even describe these requirements?
			//
			// Better to let the meaning of MINVALUE/MAXVALUE be consistent
			// everywhere, and document the gotcha thoroughly.
			Name: `multi_col_range_partitioning_descending`,
			Schema: `CREATE TABLE %s (a INT, b INT, c INT, PRIMARY KEY (a, b DESC, c)) PARTITION BY RANGE (a, b, c) (
				PARTITION p6xx VALUES FROM (MINVALUE, MINVALUE, MINVALUE) TO (6, MAXVALUE, MAXVALUE),
				PARTITION p75n VALUES FROM (7, MINVALUE, MINVALUE) TO (7, 5, MINVALUE),
				PARTITION pxxx VALUES FROM (7, 5, MINVALUE) TO (MAXVALUE, MAXVALUE, MAXVALUE)
			)`,
			Configs: []string{`.p6xx:+n1`, `.p75n:+n2`, `.pxxx:+n3`},
			GeneratedSpans: []string{
				`.p6xx /1-/1/7`,
				`.p75n /1/7-/1/7/5`,
				`.pxxx /1/7/5-/2`,
			},
			Scans: map[string]string{
				`a < 7`:                       `n1`,
				`a = 7 AND b > 5`:             `n2`,
				`a > 7 OR (a = 7 AND b <= 5)`: `n3`,
			},
		},
		{
			Name: `sparse_multi_col_range_partitioning`,
			Schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY RANGE (a, b) (
				PARTITION p34  VALUES FROM (1, 2) TO (3, 4),
				PARTITION p78 VALUES FROM (5, 6) TO (7, 8)
			)`,
			Configs: []string{`@primary:+n1`, `.p34:+n2`, `.p78:+n3`},
			GeneratedSpans: []string{
				`@primary /1-/1/1/2`,
				`    .p34 /1/1/2-/1/3/4`,
				`@primary /1/3/4-/1/5/6`,
				`    .p78 /1/5/6-/1/7/8`,
				`@primary /1/7/8-/2`,
			},
			Scans: map[string]string{
				`(a, b) < (1, 2)`:                      `n1`,
				`(a, b) >= (1, 2) AND (a, b) < (3, 4)`: `n2`,
				`(a, b) >= (3, 4) AND (a, b) < (5, 6)`: `n1`,
				`(a, b) >= (5, 6) AND (a, b) < (7, 8)`: `n3`,
				`(a, b) >= (7, 8)`:                     `n1`,
			},
		},
		{
			// Intentionally a little different than `multi col range
			// partitioning` for the repartitioning tests.
			Name: `multi_col_range_partitioning_maxvalue`,
			Schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY RANGE (a, b) (
				PARTITION p3n VALUES FROM (MINVALUE, MINVALUE) TO (3, MINVALUE),
				PARTITION p3x VALUES FROM (3, MINVALUE) TO (3, MAXVALUE),
				PARTITION p56 VALUES FROM (3, MAXVALUE) TO (5, 6),
				PARTITION p57 VALUES FROM (5, 6) TO (5, 7)
			)`,
			Configs: []string{`@primary:+n1`, `.p3n:+n2`, `.p3x:+n3`, `.p56:+n1`, `.p57:+n2`},
			GeneratedSpans: []string{
				`    .p3n /1-/1/3`,
				`    .p3x /1/3-/1/4`,
				`    .p56 /1/4-/1/5/6`,
				`    .p57 /1/5/6-/1/5/7`,
				`@primary /1/5/7-/2`,
			},
			Scans: map[string]string{
				`a < 3`:                                `n2`,
				`a >= 3 AND a < 4`:                     `n3`,
				`a >= 4 AND (a, b) < (5, 6)`:           `n1`,
				`(a, b) >= (5, 6) AND (a, b) < (5, 7)`: `n2`,
				`(a, b) >= (5, 7)`:                     `n1`,
			},
		},
		{
			Name: `multi_col_range_partitioning_maxvalue_maxvalue`,
			Schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY RANGE (a, b) (
				PARTITION p34 VALUES FROM (MINVALUE, MINVALUE) TO (3, 4),
				PARTITION p3x VALUES FROM (3, 4) TO (3, MAXVALUE),
				PARTITION p56 VALUES FROM (3, MAXVALUE) TO (5, 6),
				PARTITION p57 VALUES FROM (5, 6) TO (5, 7),
				PARTITION pxx VALUES FROM (5, 7) TO (MAXVALUE, MAXVALUE)
			)`,
			Configs: []string{`@primary`, `.p34:+n1`, `.p3x:+n2`, `.p56:+n3`, `.p57:+n1`, `.pxx:+n2`},
			GeneratedSpans: []string{
				`.p34 /1-/1/3/4`,
				`.p3x /1/3/4-/1/4`,
				`.p56 /1/4-/1/5/6`,
				`.p57 /1/5/6-/1/5/7`,
				`.pxx /1/5/7-/2`,
			},
			Scans: map[string]string{
				`(a, b) < (3, 4)`:                      `n1`,
				`(a, b) >= (3, 4) AND a < 4`:           `n2`,
				`a >= 4 AND (a, b) < (5, 6)`:           `n3`,
				`(a, b) >= (5, 6) AND (a, b) < (5, 7)`: `n1`,
				`(a, b) >= (5, 7)`:                     `n2`,
			},
		},

		{
			Name: `list_list_partitioning`,
			Schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a) (
				PARTITION p3 VALUES IN (3) PARTITION BY LIST (b) (
					PARTITION p34 VALUES IN (4)
				),
				PARTITION p5 VALUES IN (5) PARTITION BY LIST (b) (
					PARTITION p56 VALUES IN (6),
					PARTITION p5d VALUES IN (DEFAULT)
				),
				PARTITION pd VALUES IN (DEFAULT)
			)`,
			Configs: []string{`@primary:+n1`, `.p3:+n2`, `.p34:+n3`, `.p5:+n1`, `.p56:+n2`, `.p5d:+n3`, `.pd:+n1`},
			GeneratedSpans: []string{
				` .pd /1-/1/3`,
				` .p3 /1/3-/1/3/4`,
				`.p34 /1/3/4-/1/3/5`,
				` .p3 /1/3/5-/1/4`,
				` .pd /1/4-/1/5`,
				`.p5d /1/5-/1/5/6`,
				`.p56 /1/5/6-/1/5/7`,
				`.p5d /1/5/7-/1/6`,
				` .pd /1/6-/2`,
			},
			Scans: map[string]string{
				`a < 3`:           `n1`,
				`a = 3 AND b < 4`: `n2`,
				`a = 3 AND b = 4`: `n3`,
				`a = 3 AND b > 4`: `n2`,
				`a > 3 AND a < 5`: `n1`,
				`a = 5 AND b < 6`: `n3`,
				`a = 5 AND b = 6`: `n2`,
				`a = 5 AND b > 6`: `n3`,
				`a > 5`:           `n1`,
			},
		},
		{
			Name: `list_range_partitioning`,
			Schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a) (
				PARTITION p3 VALUES IN (3) PARTITION BY RANGE (b) (
					PARTITION p34 VALUES FROM (MINVALUE) TO (4)
				),
				PARTITION p5 VALUES IN (5) PARTITION BY RANGE (b) (
					PARTITION p56 VALUES FROM (MINVALUE) TO (6),
					PARTITION p5d VALUES FROM (6) TO (MAXVALUE)
				),
				PARTITION pd VALUES IN (DEFAULT)
			)`,
			Configs: []string{`@primary:+n1`, `.p3:+n2`, `.p34:+n3`, `.p5:+n1`, `.p56:+n2`, `.p5d:+n3`, `.pd:+n1`},
			GeneratedSpans: []string{
				` .pd /1-/1/3`,
				`.p34 /1/3-/1/3/4`,
				` .p3 /1/3/4-/1/4`,
				` .pd /1/4-/1/5`,
				`.p56 /1/5-/1/5/6`,
				`.p5d /1/5/6-/1/6`,
				` .pd /1/6-/2`,
			},
			Scans: map[string]string{
				`a < 3`:            `n1`,
				`a = 3 AND b < 4`:  `n3`,
				`a = 3 AND b >= 4`: `n2`,
				`a > 3 AND a < 5`:  `n1`,
				`a = 5 AND b < 6`:  `n2`,
				`a = 5 AND b >= 6`: `n3`,
				`a > 5`:            `n1`,
			},
		},

		{
			Name: `inheritance_index`,
			Schema: `CREATE TABLE %s (a INT PRIMARY KEY) PARTITION BY LIST (a) (
				PARTITION pd VALUES IN (DEFAULT)
			)`,
			Configs:        []string{`@primary`},
			GeneratedSpans: []string{`@primary /1-/2`},
		},
		{
			Name: `inheritance_single_col_default`,
			Schema: `CREATE TABLE %s (a INT PRIMARY KEY) PARTITION BY LIST (a) (
				PARTITION p3 VALUES IN (3),
				PARTITION pd VALUES IN (DEFAULT)
			)`,
			Configs:        []string{`@primary`, `.pd`},
			GeneratedSpans: []string{`.pd /1-/2`},
		},
		{
			Name: `inheritance_multi_col_default`,
			Schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a, b) (
				PARTITION p34 VALUES IN ((3, 4)),
				PARTITION p3d VALUES IN ((3, DEFAULT)),
				PARTITION p56 VALUES IN ((5, 6)),
				PARTITION p5d VALUES IN ((5, DEFAULT))
			)`,
			Configs: []string{`@primary`, `.p3d`, `.p56`},
			GeneratedSpans: []string{
				`@primary /1-/1/3`,
				`    .p3d /1/3-/1/4`,
				`@primary /1/4-/1/5/6`,
				`    .p56 /1/5/6-/1/5/7`,
				`@primary /1/5/7-/2`,
			},
		},
		{
			Name: `inheritance_subpartitioning`,
			Schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a) (
				PARTITION p3 VALUES IN (3) PARTITION BY LIST (b) (
					PARTITION p34 VALUES IN (4),
					PARTITION p3d VALUES IN (DEFAULT)
				),
				PARTITION p5 VALUES IN (5) PARTITION BY LIST (b) (
					PARTITION p56 VALUES IN (6),
					PARTITION p5d VALUES IN (DEFAULT)
				),
				PARTITION p7 VALUES IN (7) PARTITION BY LIST (b) (
					PARTITION p78 VALUES IN (8),
					PARTITION p7d VALUES IN (DEFAULT)
				)
			)`,
			Configs: []string{`@primary`, `.p3d`, `.p56`, `.p7`},
			GeneratedSpans: []string{
				`@primary /1-/1/3`,
				`    .p3d /1/3-/1/4`,
				`@primary /1/4-/1/5/6`,
				`    .p56 /1/5/6-/1/5/7`,
				`@primary /1/5/7-/1/7`,
				`     .p7 /1/7-/1/8`,
				`@primary /1/8-/2`,
			},
		},

		{
			Name:   `secondary_index_unpartitioned`,
			Schema: `CREATE TABLE %s (a INT PRIMARY KEY, b INT, INDEX b_idx (b))`,
		},
		{
			Name: `secondary_index_list_partitioning`,
			Schema: `CREATE TABLE %s (a INT PRIMARY KEY, b INT, INDEX b_idx (b) PARTITION BY LIST (b) (
				PARTITION p3 VALUES IN (3),
				PARTITION p4 VALUES IN (4)
			))`,
			Configs: []string{`@b_idx:+n1`, `@b_idx.p3:+n2`, `@b_idx.p4:+n3`},
			GeneratedSpans: []string{
				`@b_idx /2-/2/3`,
				`   .p3 /2/3-/2/4`,
				`   .p4 /2/4-/2/5`,
				`@b_idx /2/5-/3`,
			},
			Scans: map[string]string{`b < 3`: `n1`, `b = 3`: `n2`, `b = 4`: `n3`, `b > 4`: `n1`},
		},
		{
			// Intentionally a little different than `single col list
			// partitioning` for the repartitioning tests.
			Name: `secondary_index_list_partitioning_default`,
			Schema: `CREATE TABLE %s (a INT PRIMARY KEY, b INT, INDEX b_idx (b) PARTITION BY LIST (b) (
				PARTITION p4 VALUES IN (4),
				PARTITION p5 VALUES IN (5),
				PARTITION pd VALUES IN (DEFAULT)
			))`,
			Configs: []string{`@b_idx`, `@b_idx.p4:+n2`, `@b_idx.p5:+n3`, `@b_idx.pd:+n1`},
			GeneratedSpans: []string{
				`.pd /2-/2/4`,
				`.p4 /2/4-/2/5`,
				`.p5 /2/5-/2/6`,
				`.pd /2/6-/3`,
			},
			Scans: map[string]string{`b < 4`: `n1`, `b = 4`: `n2`, `b = 5`: `n3`, `b > 5`: `n1`},
		},
		{
			Name: `secondary_index_null`,
			Schema: `CREATE TABLE %s (a INT PRIMARY KEY, b INT, INDEX b_idx (b) PARTITION BY LIST (b) (
				PARTITION pl1 VALUES IN (NULL, 1),
				PARTITION p3  VALUES IN (3)
			))`,
			Configs: []string{`@b_idx:+n1`, `@b_idx.pl1:+n2`, `@b_idx.p3:+n3`},
			GeneratedSpans: []string{
				`@b_idx /2-/2/NULL`,
				`  .pl1 /2/NULL-/2/!NULL`,
				`@b_idx /2/!NULL-/2/1`,
				`  .pl1 /2/1-/2/2`,
				`@b_idx /2/2-/2/3`,
				`   .p3 /2/3-/2/4`,
				`@b_idx /2/4-/3`,
			},
			Scans: map[string]string{
				`b = -1`:             `n1`,
				`b IS NULL`:          `n2`,
				`b IS NULL OR b = 1`: `n2`,
				`b = 3`:              `n3`,
			},
		},

		{
			Name: `scans`,
			Schema: `CREATE TABLE %s (a INT PRIMARY KEY, b INT) PARTITION BY LIST (a) (
				PARTITION p3p5 VALUES IN ((3), (5)),
				PARTITION p4 VALUES IN (4),
				PARTITION pd VALUES IN (DEFAULT)
			)`,
			Configs: []string{`@primary:+n1`, `.p3p5:+n2`, `.p4:+n3`, `.pd:+n1`},
			GeneratedSpans: []string{
				`  .pd /1-/1/3`,
				`.p3p5 /1/3-/1/4`,
				`  .p4 /1/4-/1/5`,
				`.p3p5 /1/5-/1/6`,
				`  .pd /1/6-/2`,
			},
			Scans: map[string]string{
				`a < 3`: `n1`,
				`a = 3`: `n2`,
				`a = 4`: `n3`,
				`a = 5`: `n2`,
				`a > 5`: `n1`,

				`a = 3 OR a = 5`:     `n2`,
				`a IN ((3), (5))`:    `n2`,
				`(a, b) IN ((3, 7))`: `n2`,
				`a IN (3) AND a > 2`: `n2`,
				`a IN (3) AND a < 2`: `n2`,
			},
		},
	}
}

func RandomizedPartitioningTestSpecs(rng *rand.Rand) (tests []PartitioningTestSpec) {
	const schemaFmt = `CREATE TABLE %%s (a %s PRIMARY KEY) PARTITION BY LIST (a) (PARTITION p VALUES IN (%s))`
	for _, typ := range append(types.Scalar, types.AnyCollatedString) {
		switch typ.Family() {
		case types.JsonFamily, types.GeographyFamily, types.GeometryFamily:
			// Not indexable.
			continue
		case types.CollatedStringFamily:
			typ = types.MakeCollatedString(types.String, *randgen.RandCollationLocale(rng))
		}
		datum := randgen.RandDatum(rng, typ, false /* nullOk */)
		if datum == tree.DNull {
			// DNull is returned by RandDatum for types.UNKNOWN or if the
			// column type is unimplemented in RandDatum. In either case, the
			// correct thing to do is skip this one.
			continue
		}
		serializedDatum := tree.Serialize(datum)
		// name can be "char" (with quotes), so needs to be escaped.
		escapedName := fmt.Sprintf("%s_table", typ.String())
		for _, c := range []string{"\"", "{", "}"} {
			escapedName = strings.Replace(escapedName, c, "_", -1)
		}
		// schema is used in a fmt.Sprintf to fill in the table name, so we have
		// to escape any stray %s.
		escapedDatum := strings.Replace(serializedDatum, `%`, `%%`, -1)
		test := PartitioningTestSpec{
			Name:    escapedName,
			Schema:  fmt.Sprintf(schemaFmt, typ.SQLString(), escapedDatum),
			Configs: []string{`@primary:+n1`, `.p:+n2`},
			Scans: map[string]string{
				fmt.Sprintf(`a < %s`, serializedDatum):    `n1`,
				fmt.Sprintf(`a = %s`, serializedDatum):    `n2`,
				fmt.Sprintf(`a IN (%s)`, serializedDatum): `n2`,
				fmt.Sprintf(`a > %s`, serializedDatum):    `n1`,
			},
		}
		tests = append(tests, test)
	}
	return tests
}

type RepartitioningTestSpec struct {
	Name     string
	Index    string
	Old, New PartitioningTestSpec
}

func AllRepartitioningTestSpecs() []RepartitioningTestSpec {
	partitioningTestSpecs := DeterministicPartitioningTestSpecs()
	tests := []RepartitioningTestSpec{
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `unpartitioned`},
			New:   PartitioningTestSpec{Name: `unpartitioned`},
		},
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `unpartitioned`},
			New:   PartitioningTestSpec{Name: `single_col_list_partitioning`},
		},
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `unpartitioned`},
			New:   PartitioningTestSpec{Name: `single_col_list_partitioning_default`},
		},
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `unpartitioned`},
			New:   PartitioningTestSpec{Name: `single_col_range_partitioning`},
		},
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `unpartitioned`},
			New:   PartitioningTestSpec{Name: `single_col_range_partitioning_maxvalue`},
		},

		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `single_col_list_partitioning`},
			New:   PartitioningTestSpec{Name: `single_col_list_partitioning_default`},
		},
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `single_col_list_partitioning_default`},
			New:   PartitioningTestSpec{Name: `single_col_list_partitioning`},
		},
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `multi_col_list_partitioning`},
			New:   PartitioningTestSpec{Name: `multi_col_list_partitioning_default`},
		},
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `multi_col_list_partitioning_default`},
			New:   PartitioningTestSpec{Name: `multi_col_list_partitioning`},
		},
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `multi_col_list_partitioning_default`},
			New:   PartitioningTestSpec{Name: `multi_col_list_partitioning_default_default`},
		},
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `multi_col_list_partitioning_default_default`},
			New:   PartitioningTestSpec{Name: `multi_col_list_partitioning_default`},
		},

		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `single_col_range_partitioning`},
			New:   PartitioningTestSpec{Name: `single_col_range_partitioning_maxvalue`},
		},
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `single_col_range_partitioning_maxvalue`},
			New:   PartitioningTestSpec{Name: `single_col_range_partitioning`},
		},
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `multi_col_range_partitioning`},
			New:   PartitioningTestSpec{Name: `multi_col_range_partitioning_maxvalue`},
		},
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `multi_col_range_partitioning_maxvalue`},
			New:   PartitioningTestSpec{Name: `multi_col_range_partitioning`},
		},
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `multi_col_range_partitioning_maxvalue`},
			New:   PartitioningTestSpec{Name: `multi_col_range_partitioning_maxvalue_maxvalue`},
		},
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `multi_col_range_partitioning_maxvalue_maxvalue`},
			New:   PartitioningTestSpec{Name: `multi_col_range_partitioning_maxvalue`},
		},

		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `single_col_list_partitioning`},
			New:   PartitioningTestSpec{Name: `single_col_range_partitioning`},
		},
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `single_col_range_partitioning`},
			New:   PartitioningTestSpec{Name: `single_col_list_partitioning`},
		},

		// TODO(dan): One repartitioning is fully implemented, these tests also
		// need to pass with no ccl code.
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `single_col_list_partitioning`},
			New:   PartitioningTestSpec{Name: `unpartitioned`},
		},
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `single_col_list_partitioning_default`},
			New:   PartitioningTestSpec{Name: `unpartitioned`},
		},
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `single_col_range_partitioning`},
			New:   PartitioningTestSpec{Name: `unpartitioned`},
		},
		{
			Index: `primary`,
			Old:   PartitioningTestSpec{Name: `single_col_range_partitioning_maxvalue`},
			New:   PartitioningTestSpec{Name: `unpartitioned`},
		},

		{
			Index: `b_idx`,
			Old:   PartitioningTestSpec{Name: `secondary_index_unpartitioned`},
			New:   PartitioningTestSpec{Name: `secondary_index_list_partitioning`},
		},
		{
			Index: `b_idx`,
			Old:   PartitioningTestSpec{Name: `secondary_index_list_partitioning`},
			New:   PartitioningTestSpec{Name: `secondary_index_unpartitioned`},
		},
		{
			Index: `b_idx`,
			Old:   PartitioningTestSpec{Name: `secondary_index_list_partitioning`},
			New:   PartitioningTestSpec{Name: `secondary_index_list_partitioning_default`},
		},
		{
			Index: `b_idx`,
			Old:   PartitioningTestSpec{Name: `secondary_index_list_partitioning_default`},
			New:   PartitioningTestSpec{Name: `secondary_index_list_partitioning`},
		},
	}

	partitioningTestsByName := make(map[string]PartitioningTestSpec)
	for _, partitioningTest := range partitioningTestSpecs {
		partitioningTestsByName[partitioningTest.Name] = partitioningTest
	}
	for i := range tests {
		t, ok := partitioningTestsByName[tests[i].Old.Name]
		if !ok {
			panic(errors.Errorf("unknown partitioning test: %s", tests[i].Old.Name))
		}
		tests[i].Old = t
		t, ok = partitioningTestsByName[tests[i].New.Name]
		if !ok {
			panic(errors.Errorf("unknown partitioning test: %s", tests[i].New.Name))
		}
		tests[i].New = t
		tests[i].Name = fmt.Sprintf("%s_to_%s", tests[i].Old.Name, tests[i].New.Name)
	}

	return tests
}
