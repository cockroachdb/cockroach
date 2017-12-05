// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlccl

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// partitioningTest represents a single test case used in the various
// partitioning-related tests.
type partitioningTest struct {
	// name is a name for the test, suitable for use as the subtest name.
	name string

	// schema is a full CREATE TABLE statement with a literal `%s` where the
	// table name should be.
	schema string

	// configs are each a shorthand for a zone config, formatted as
	// `@index_name` or `.partition_name`. Optionally a suffix of a colon and a
	// comma-separated list of constraints may be included (`@index_name:+dc1`).
	// These will be parsed into `parsed.subzones`.
	configs []string

	// generatedSpans is 1:1 to the output of GenerateSubzoneSpans, each
	// formatted as `{subzone} {start}-{end}` (e.g. `@primary /1-/2`), where
	// {subzone} is formatted identically to the test shorthand above, and
	// {start} and {end} are formatted using our key pretty printer, but with
	// the table removed. The end key is always specified in here (though
	// GenerateSubzoneSpans omits it under certain conditions to save space).
	generatedSpans []string

	// scans are each a shorthand for an assertion of where data should live.
	// The map key is the used for the `WHERE` clause of a `SELECT *` and the
	// value is a comma separated whitelist of nodes that are allowed to serve
	// this query. Example: `map[string]string{`b = 1`: `n2`}` means that
	// `SELECT * FROM t WHERE b = 1` is required to be served entirely by node2.
	//
	// TODO(dan): These should be based on replication zone attributes instead
	// of node IDs.
	scans map[string]string

	// The following are all filled in by `parse()`.
	parsed struct {
		// tableName is `name` but escaped for use in SQL.
		tableName string

		// createStmt is `schema` with a table name of `escapedName`
		createStmt string

		// tableDesc is the TableDescriptor created by `createStmt`.
		tableDesc *sqlbase.TableDescriptor

		// zoneConfigStmt constains SQL that effects the zone configs described by
		// `configs`.
		zoneConfigStmts string

		// subzones are the `configs` shorthand parsed into Subzones.
		subzones []config.Subzone
	}
}

// parse fills in the various fields of `partitioningTest.parsed`.
func (t *partitioningTest) parse() error {
	t.parsed.tableName = tree.Name(t.name).String()
	t.parsed.createStmt = fmt.Sprintf(t.schema, t.parsed.tableName)

	{
		ctx := context.Background()
		stmt, err := parser.ParseOne(t.parsed.createStmt)
		if err != nil {
			return errors.Wrapf(err, `parsing %s`, t.parsed.createStmt)
		}
		createTable, ok := stmt.(*tree.CreateTable)
		if !ok {
			return errors.Errorf("expected *tree.CreateTable got %T", stmt)
		}
		const parentID, tableID = keys.MaxReservedDescID + 1, keys.MaxReservedDescID + 2
		t.parsed.tableDesc, err = makeCSVTableDescriptor(
			ctx, createTable, parentID, tableID, hlc.UnixNano())
		if err != nil {
			return err
		}
		if err := t.parsed.tableDesc.ValidateTable(); err != nil {
			return err
		}
	}

	var zoneConfigStmts bytes.Buffer
	// TODO(dan): Can we run all the zoneConfigStmts in a txn?
	for _, c := range t.configs {
		var subzoneShort, constraints string
		configParts := strings.Split(c, `:`)
		switch len(configParts) {
		case 1:
			subzoneShort = configParts[0]
		case 2:
			subzoneShort, constraints = configParts[0], configParts[1]
		default:
			panic(errors.Errorf("unsupported config: %s", c))
		}

		var subzone config.Subzone
		if strings.HasPrefix(subzoneShort, "@") {
			idxDesc, _, err := t.parsed.tableDesc.FindIndexByName(subzoneShort[1:])
			if err != nil {
				return errors.Wrapf(err, "could not find index %s", subzoneShort)
			}
			subzone.IndexID = uint32(idxDesc.ID)
			// TODO(dan): This `data.` is hardcoded because of a bug in
			// ALTER INDEX/EXPERIMENTAL CONFIGURE ZONE.
			fmt.Fprintf(&zoneConfigStmts,
				`ALTER INDEX data.%s@%s EXPERIMENTAL CONFIGURE ZONE 'constraints: [%s]';`,
				tree.Name(t.name), idxDesc.Name, constraints,
			)
		} else if strings.HasPrefix(subzoneShort, ".") {
			// TODO(dan): decide if config.Subzone needs to have IndexID
			// set when PartitionName is non-empty. The proto comment
			// doesn't specify.
			subzone.PartitionName = subzoneShort[1:]
			// TODO(dan): This `data.` is hardcoded because of a bug in
			// ALTER TABLE/PARTITION/EXPERIMENTAL CONFIGURE ZONE.
			fmt.Fprintf(&zoneConfigStmts,
				`ALTER TABLE data.%s PARTITION %s EXPERIMENTAL CONFIGURE ZONE 'constraints: [%s]';`,
				tree.Name(t.name), subzone.PartitionName, constraints,
			)
		}

		for _, constraintStr := range strings.Split(constraints, `,`) {
			if constraintStr == "" {
				continue
			}
			var c config.Constraint
			if err := c.FromString(constraintStr); err != nil {
				return errors.Wrapf(err, "parsing constraint: %s", constraintStr)
			}
			subzone.Config.Constraints.Constraints = append(subzone.Config.Constraints.Constraints, c)
		}

		t.parsed.subzones = append(t.parsed.subzones, subzone)
	}
	t.parsed.zoneConfigStmts = zoneConfigStmts.String()

	return nil
}

// verifyScansFn returns a closure that runs the test's `scans` and returns a
// descriptive error if any of them fail. It is not required for `parse` to have
// been called.
func (t *partitioningTest) verifyScansFn(ctx context.Context, db *gosql.DB) func() error {
	return func() error {
		for where, expectedNodes := range t.scans {
			query := fmt.Sprintf(`SELECT * FROM %s WHERE %s`, tree.Name(t.name), where)
			log.Infof(ctx, "query: %s", query)
			if err := verifyScansOnNode(db, query, expectedNodes); err != nil {
				if log.V(1) {
					log.Errorf(ctx, "scan verification failed: %s", err)
				}
				return err
			}
		}
		return nil
	}
}

// allPartitioningTests returns the standard set of `partitioningTest`s used in
// the various partitioning tests. Most of them are curated, but the ones that
// make sure each column type is tested are randomized.
//
// TODO(dan): It already seems odd to only have one of these sets. The
// repartitioning & fast path tests only use a subset and a few entries are only
// present because they're interesting for the before after of a partitioning
// change. Revisit.
func allPartitioningTests(rng *rand.Rand) []partitioningTest {
	tests := []partitioningTest{
		{
			name:   `unpartitioned`,
			schema: `CREATE TABLE %s (a INT PRIMARY KEY)`,
		},

		{
			name:           `all indexes`,
			schema:         `CREATE TABLE %s (a INT PRIMARY KEY, b INT, c INT, INDEX idx1 (b), INDEX idx2 (c))`,
			configs:        []string{`@primary`, `@idx1:+n2`, `@idx2:+n3`},
			generatedSpans: []string{`@primary /1-/2`, `@idx1 /2-/3`, `@idx2 /3-/4`},
			scans:          map[string]string{`b = 1`: `n2`, `c = 1`: `n3`},
		},
		{
			name:           `all indexes - shuffled`,
			schema:         `CREATE TABLE %s (a INT PRIMARY KEY, b INT, c INT, INDEX idx1 (b), INDEX idx2 (c))`,
			configs:        []string{`@idx2:+n2`, `@primary`, `@idx1:+n3`},
			generatedSpans: []string{`@primary /1-/2`, `@idx1 /2-/3`, `@idx2 /3-/4`},
			scans:          map[string]string{`b = 1`: `n3`, `c = 1`: `n2`},
		},
		{
			name:           `some indexes`,
			schema:         `CREATE TABLE %s (a INT PRIMARY KEY, b INT, c INT, INDEX idx1 (b), INDEX idx2 (c))`,
			configs:        []string{`@primary`, `@idx2:+n2`},
			generatedSpans: []string{`@primary /1-/2`, `@idx2 /3-/4`},
			scans:          map[string]string{`c = 1`: `n2`},
		},

		{
			name: `single col list partitioning`,
			schema: `CREATE TABLE %s (a INT PRIMARY KEY) PARTITION BY LIST (a) (
				PARTITION p3 VALUES IN (3),
				PARTITION p4 VALUES IN (4)
			)`,
			configs: []string{`@primary:+n1`, `.p3:+n2`, `.p4:+n3`},
			generatedSpans: []string{
				`@primary /1-/1/3`,
				`     .p3 /1/3-/1/4`,
				`     .p4 /1/4-/1/5`,
				`@primary /1/5-/2`,
			},
			scans: map[string]string{`a < 3`: `n1`, `a = 3`: `n2`, `a = 4`: `n3`, `a > 4`: `n1`},
		},
		{
			// Intentionally a little different than `single col list
			// partitioning` for the repartitioning tests.
			name: `single col list partitioning - DEFAULT`,
			schema: `CREATE TABLE %s (a INT PRIMARY KEY) PARTITION BY LIST (a) (
				PARTITION p4 VALUES IN (4),
				PARTITION p5 VALUES IN (5),
				PARTITION pd VALUES IN (DEFAULT)
			)`,
			configs: []string{`@primary`, `.p4:+n2`, `.p5:+n3`, `.pd:+n1`},
			generatedSpans: []string{
				`.pd /1-/1/4`,
				`.p4 /1/4-/1/5`,
				`.p5 /1/5-/1/6`,
				`.pd /1/6-/2`,
			},
			scans: map[string]string{`a < 4`: `n1`, `a = 4`: `n2`, `a = 5`: `n3`, `a > 5`: `n1`},
		},
		{
			name: `multi col list partitioning`,
			schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a, b) (
				PARTITION p34 VALUES IN ((3, 4)),
				PARTITION p56 VALUES IN ((5, 6)),
				PARTITION p57 VALUES IN ((5, 7))
			)`,
			configs: []string{`@primary:+n1`, `.p34:+n2`, `.p56:+n3`, `.p57:+n1`},
			generatedSpans: []string{
				`@primary /1-/1/3/4`,
				`    .p34 /1/3/4-/1/3/5`,
				`@primary /1/3/5-/1/5/6`,
				`    .p56 /1/5/6-/1/5/7`,
				`    .p57 /1/5/7-/1/5/8`,
				`@primary /1/5/8-/2`,
			},
			scans: map[string]string{
				`(a, b) < (3, 4)`: `n1`,
				`a = 3 AND b = 4`: `n2`,
				// TODO(dan): Uncomment when #20504 is fixed.
				// `(a, b) > (3, 4) AND (a, b) < (5, 6)`: `n1`,
				`a = 5 AND b = 6`: `n3`,
				`a = 5 AND b = 7`: `n1`,
				`(a, b) > (5, 7)`: `n1`,
			},
		},
		{
			// Intentionally a little different than `multi col list
			// partitioning` for the repartitioning tests.
			name: `multi col list partitioning - DEFAULT`,
			schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a, b) (
				PARTITION p34 VALUES IN ((3, 4)),
				PARTITION p57 VALUES IN ((5, 7)),
				PARTITION p58 VALUES IN ((5, 8)),
				PARTITION p5d VALUES IN ((5, DEFAULT))
			)`,
			configs: []string{`@primary:+n1`, `.p34:+n2`, `.p57:+n3`, `.p58:+n1`, `.p5d:+n2`},
			generatedSpans: []string{
				`@primary /1-/1/3/4`,
				`    .p34 /1/3/4-/1/3/5`,
				`@primary /1/3/5-/1/5`,
				`    .p5d /1/5-/1/5/7`,
				`    .p57 /1/5/7-/1/5/8`,
				`    .p58 /1/5/8-/1/5/9`,
				`    .p5d /1/5/9-/1/6`,
				`@primary /1/6-/2`,
			},
			scans: map[string]string{
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
			name: `multi col list partitioning - DEFAULT DEFAULT`,
			schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a, b) (
				PARTITION p34 VALUES IN ((3, 4)),
				PARTITION p57 VALUES IN ((5, 7)),
				PARTITION p58 VALUES IN ((5, 8)),
				PARTITION p5d VALUES IN ((5, DEFAULT)),
				PARTITION pd VALUES IN ((DEFAULT, DEFAULT))
			)`,
			configs: []string{`@primary`, `.p34:+n1`, `.p57:+n2`, `.p58:+n3`, `.p5d:+n1`, `.pd:+n2`},
			generatedSpans: []string{
				` .pd /1-/1/3/4`,
				`.p34 /1/3/4-/1/3/5`,
				` .pd /1/3/5-/1/5`,
				`.p5d /1/5-/1/5/7`,
				`.p57 /1/5/7-/1/5/8`,
				`.p58 /1/5/8-/1/5/9`,
				`.p5d /1/5/9-/1/6`,
				` .pd /1/6-/2`,
			},
			scans: map[string]string{
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
			// Similar to `multi col list partitioning - DEFAULT DEFAULT` but
			// via subpartitioning instead of multi col.
			name: `multi col list partitioning - DEFAULT DEFAULT subpartitioned`,
			schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a) (
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
			configs: []string{`@primary`, `.p34:+n1`, `.p57:+n2`, `.p58:+n3`, `.p5d:+n1`, `.pd:+n2`},
			generatedSpans: []string{
				` .pd /1-/1/3/4`,
				`.p34 /1/3/4-/1/3/5`,
				` .pd /1/3/5-/1/5`,
				`.p5d /1/5-/1/5/7`,
				`.p57 /1/5/7-/1/5/8`,
				`.p58 /1/5/8-/1/5/9`,
				`.p5d /1/5/9-/1/6`,
				` .pd /1/6-/2`,
			},
			scans: map[string]string{
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
			name: `single col range partitioning`,
			schema: `CREATE TABLE %s (a INT PRIMARY KEY) PARTITION BY RANGE (a) (
				PARTITION p3 VALUES < 3,
				PARTITION p4 VALUES < 4
			)`,
			configs: []string{`@primary:+n1`, `.p3:+n2`, `.p4:+n3`},
			generatedSpans: []string{
				`     .p3 /1-/1/3`,
				`     .p4 /1/3-/1/4`,
				`@primary /1/4-/2`,
			},
			scans: map[string]string{
				`a < 3`:            `n2`,
				`a >= 3 AND a < 4`: `n3`,
				`a > 4`:            `n1`,
			},
		},
		{
			// Intentionally a little different than `single col range
			// partitioning` for the repartitioning tests.
			name: `single col range partitioning - MAXVALUE`,
			schema: `CREATE TABLE %s (a INT PRIMARY KEY) PARTITION BY RANGE (a) (
				PARTITION p4 VALUES < 4,
				PARTITION p5 VALUES < 5,
				PARTITION pm VALUES < MAXVALUE
			)`,
			configs: []string{`@primary`, `.p4:+n1`, `.p5:+n2`, `.pm:+n3`},
			generatedSpans: []string{
				`.p4 /1-/1/4`,
				`.p5 /1/4-/1/5`,
				`.pm /1/5-/2`,
			},
			scans: map[string]string{
				`a < 4`:            `n1`,
				`a >= 4 AND a < 5`: `n2`,
				`a > 5`:            `n3`,
			},
		},
		{
			name: `multi col range partitioning`,
			schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY RANGE (a, b) (
				PARTITION p34 VALUES < (3, 4),
				PARTITION p56 VALUES < (5, 6),
				PARTITION p57 VALUES < (5, 7)
			)`,
			configs: []string{`@primary:+n1`, `.p34:+n2`, `.p56:+n3`, `.p57:+n1`},
			generatedSpans: []string{
				`    .p34 /1-/1/3/4`,
				`    .p56 /1/3/4-/1/5/6`,
				`    .p57 /1/5/6-/1/5/7`,
				`@primary /1/5/7-/2`,
			},
			scans: map[string]string{
				`(a, b) < (3, 4)`: `n2`,
				// TODO(dan): Uncomment when #20504 is fixed.
				// `(a, b) >= (3, 4) AND (a, b) < (5, 6)`: `n3`,
				// `(a, b) >= (5, 6) AND (a, b) < (5, 7)`: `n1`,
				`(a, b) >= (5, 7)`: `n1`,
			},
		},
		{
			// Intentionally a little different than `multi col range
			// partitioning` for the repartitioning tests.
			name: `multi col range partitioning - MAXVALUE`,
			schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY RANGE (a, b) (
				PARTITION p34 VALUES < (3, 4),
				PARTITION p3m VALUES < (3, MAXVALUE),
				PARTITION p56 VALUES < (5, 6),
				PARTITION p57 VALUES < (5, 7)
			)`,
			configs: []string{`@primary:+n1`, `.p34:+n2`, `.p3m:+n3`, `.p56:+n1`, `.p57:+n2`},
			generatedSpans: []string{
				`    .p34 /1-/1/3/4`,
				`    .p3m /1/3/4-/1/4`,
				`    .p56 /1/4-/1/5/6`,
				`    .p57 /1/5/6-/1/5/7`,
				`@primary /1/5/7-/2`,
			},
			scans: map[string]string{
				`(a, b) < (3, 4)`:            `n2`,
				`(a, b) >= (3, 4) AND a < 4`: `n3`,
				`a >= 4 AND (a, b) < (5, 6)`: `n1`,
				// TODO(dan): Uncomment when #20504 is fixed.
				// `(a, b) >= (5, 6) AND (a, b) < (5, 7)`: `n2`,
				`(a, b) >= (5, 7)`: `n1`,
			},
		},
		{
			name: `multi col range partitioning - MAXVALUE MAXVALUE`,
			schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY RANGE (a, b) (
				PARTITION p34 VALUES < (3, 4),
				PARTITION p3m VALUES < (3, MAXVALUE),
				PARTITION p56 VALUES < (5, 6),
				PARTITION p57 VALUES < (5, 7),
				PARTITION pm VALUES < (MAXVALUE, MAXVALUE)
			)`,
			configs: []string{`@primary`, `.p34:+n1`, `.p3m:+n2`, `.p56:+n3`, `.p57:+n1`, `.pm:+n2`},
			generatedSpans: []string{
				`.p34 /1-/1/3/4`,
				`.p3m /1/3/4-/1/4`,
				`.p56 /1/4-/1/5/6`,
				`.p57 /1/5/6-/1/5/7`,
				` .pm /1/5/7-/2`,
			},
			scans: map[string]string{
				`(a, b) < (3, 4)`:            `n1`,
				`(a, b) >= (3, 4) AND a < 4`: `n2`,
				`a >= 4 AND (a, b) < (5, 6)`: `n3`,
				// TODO(dan): Uncomment when #20504 is fixed.
				// `(a, b) >= (5, 6) AND (a, b) < (5, 7)`: `n1`,
				`(a, b) >= (5, 7)`: `n2`,
			},
		},

		{
			name: `list-list partitioning`,
			schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a) (
				PARTITION p3 VALUES IN (3) PARTITION BY LIST (b) (
					PARTITION p34 VALUES IN (4)
				),
				PARTITION p5 VALUES IN (5) PARTITION BY LIST (b) (
					PARTITION p56 VALUES IN (6),
					PARTITION p5d VALUES IN (DEFAULT)
				),
				PARTITION pd VALUES IN (DEFAULT)
			)`,
			configs: []string{`@primary:+n1`, `.p3:+n2`, `.p34:+n3`, `.p5:+n1`, `.p56:+n2`, `.p5d:+n3`, `.pd:+n1`},
			generatedSpans: []string{
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
			scans: map[string]string{
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
			name: `list-range partitioning`,
			schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a) (
				PARTITION p3 VALUES IN (3) PARTITION BY RANGE (b) (
					PARTITION p34 VALUES < 4
				),
				PARTITION p5 VALUES IN (5) PARTITION BY RANGE (b) (
					PARTITION p56 VALUES < 6,
					PARTITION p5d VALUES < MAXVALUE
				),
				PARTITION pd VALUES IN (DEFAULT)
			)`,
			configs: []string{`@primary:+n1`, `.p3:+n2`, `.p34:+n3`, `.p5:+n1`, `.p56:+n2`, `.p5d:+n3`, `.pd:+n1`},
			generatedSpans: []string{
				` .pd /1-/1/3`,
				`.p34 /1/3-/1/3/4`,
				` .p3 /1/3/4-/1/4`,
				` .pd /1/4-/1/5`,
				`.p56 /1/5-/1/5/6`,
				`.p5d /1/5/6-/1/6`,
				` .pd /1/6-/2`,
			},
			scans: map[string]string{
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
			name: `inheritance - index`,
			schema: `CREATE TABLE %s (a INT PRIMARY KEY) PARTITION BY LIST (a) (
				PARTITION pd VALUES IN (DEFAULT)
			)`,
			configs:        []string{`@primary`},
			generatedSpans: []string{`@primary /1-/2`},
		},
		{
			name: `inheritance - single col default`,
			schema: `CREATE TABLE %s (a INT PRIMARY KEY) PARTITION BY LIST (a) (
				PARTITION p3 VALUES IN (3),
				PARTITION pd VALUES IN (DEFAULT)
			)`,
			configs:        []string{`@primary`, `.pd`},
			generatedSpans: []string{`.pd /1-/2`},
		},
		{
			name: `inheritance - multi col default`,
			schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a, b) (
				PARTITION p34 VALUES IN ((3, 4)),
				PARTITION p3d VALUES IN ((3, DEFAULT)),
				PARTITION p56 VALUES IN ((5, 6)),
				PARTITION p5d VALUES IN ((5, DEFAULT))
			)`,
			configs: []string{`@primary`, `.p3d`, `.p56`},
			generatedSpans: []string{
				`@primary /1-/1/3`,
				`    .p3d /1/3-/1/4`,
				`@primary /1/4-/1/5/6`,
				`    .p56 /1/5/6-/1/5/7`,
				`@primary /1/5/7-/2`,
			},
		},
		{
			name: `inheritance - subpartitioning`,
			schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a) (
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
			configs: []string{`@primary`, `.p3d`, `.p56`, `.p7`},
			generatedSpans: []string{
				`@primary /1-/1/3`,
				`    .p3d /1/3-/1/4`,
				`@primary /1/4-/1/5/6`,
				`    .p56 /1/5/6-/1/5/7`,
				`@primary /1/5/7-/1/7`,
				`     .p7 /1/7-/1/8`,
				`@primary /1/8-/2`,
			},
		},
	}

	const schemaFmt = `CREATE TABLE %%s (a %s PRIMARY KEY) PARTITION BY LIST (a) (PARTITION p VALUES IN (%s))`
	for semTypeID, semTypeName := range sqlbase.ColumnType_SemanticType_name {
		typ := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_SemanticType(semTypeID)}
		switch typ.SemanticType {
		case sqlbase.ColumnType_STRING, sqlbase.ColumnType_OID, sqlbase.ColumnType_NAME,
			sqlbase.ColumnType_BYTES, sqlbase.ColumnType_UUID, sqlbase.ColumnType_INET:
			// TODO(dan): Flaky. The regex to extract the context in SHOW TRACE
			// FOR breaks on some of these. There's probably something else
			// going on here, too, but this is the obvious first thing to fix.
			continue
		case sqlbase.ColumnType_COLLATEDSTRING:
			typ.Locale = sqlbase.RandCollationLocale(rng)
			// TODO(dan): Get this to work.
			continue
		case sqlbase.ColumnType_JSON:
			// Not indexable.
			continue
		}
		datum := sqlbase.RandDatum(rng, typ, false /* nullOk */)
		if datum == tree.DNull {
			// DNull is returned by RandDatum for ColumnType_NULL or if the
			// column type is unimplemented in RandDatum. In either case, the
			// correct thing to do is skip this one.
			continue
		}
		test := partitioningTest{
			name:    semTypeName,
			schema:  fmt.Sprintf(schemaFmt, semTypeName, tree.Serialize(datum)),
			configs: []string{`@primary:+n1`, `.p:+n2`},
			scans: map[string]string{
				fmt.Sprintf(`a < %s:::%s`, datum, semTypeName): `n1`,
				fmt.Sprintf(`a = %s:::%s`, datum, semTypeName): `n2`,
				fmt.Sprintf(`a > %s:::%s`, datum, semTypeName): `n1`,
			},
		}
		tests = append(tests, test)
	}
	return tests
}

func verifyScansOnNode(db *gosql.DB, query string, node string) error {
	rows, err := db.Query(
		fmt.Sprintf(`SELECT tag, message FROM [SHOW TRACE FOR %s]`, query),
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	var scansWrongNode []string
	var traceLines []string
	var ctx, message gosql.NullString
	for rows.Next() {
		if err := rows.Scan(&ctx, &message); err != nil {
			return err
		}
		traceLine := fmt.Sprintf("%s %s", ctx.String, message.String)
		traceLines = append(traceLines, traceLine)
		if strings.Contains(message.String, "read completed") && !strings.Contains(ctx.String, node) {
			scansWrongNode = append(scansWrongNode, traceLine)
		}
	}
	if len(scansWrongNode) > 0 {
		var err bytes.Buffer
		fmt.Fprintf(&err, "expected to scan on %s: %s\n%s\nfull trace:",
			node, query, strings.Join(scansWrongNode, "\n"))
		for _, traceLine := range traceLines {
			err.WriteString("\n  ")
			err.WriteString(traceLine)
		}
		return errors.New(err.String())
	}
	return nil
}

func TestInitialPartitioning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rng, _ := randutil.NewPseudoRand()

	cfg := config.DefaultZoneConfig()
	cfg.NumReplicas = 1
	defer config.TestingSetDefaultZoneConfig(cfg)()

	ctx := context.Background()
	tsArgs := func(attr string) base.TestServerArgs {
		return base.TestServerArgs{
			ScanInterval: time.Second,
			StoreSpecs: []base.StoreSpec{
				{InMemory: true, Attributes: roachpb.Attributes{Attrs: []string{attr}}},
			},
		}
	}
	tcArgs := base.TestClusterArgs{ServerArgsPerNode: map[int]base.TestServerArgs{
		0: tsArgs("n1"),
		1: tsArgs("n2"),
		2: tsArgs("n3"),
	}}
	tc := testcluster.StartTestCluster(t, 3, tcArgs)
	defer tc.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE DATABASE data`)
	sqlDB.Exec(t, `USE data`)

	testCases := allPartitioningTests(rng)
	for _, test := range testCases {
		if len(test.scans) == 0 {
			continue
		}
		t.Run(test.name, func(t *testing.T) {
			if err := test.parse(); err != nil {
				t.Fatalf("%+v", err)
			}
			sqlDB.Exec(t, test.parsed.createStmt)
			sqlDB.Exec(t, test.parsed.zoneConfigStmts)

			testutils.SucceedsSoon(t, test.verifyScansFn(ctx, sqlDB.DB))
		})
	}
}
