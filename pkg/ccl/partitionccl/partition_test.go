// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package partitionccl

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
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
	// value is a comma separated allowlist of nodes that are allowed to serve
	// this query. Example: `map[string]string{`b = 1`: `n2`}` means that
	// `SELECT * FROM t WHERE b = 1` is required to be served entirely by node2.
	//
	// TODO(dan): These should be based on replication zone attributes instead
	// of node IDs.
	scans map[string]string

	// The following are all filled in by `parse()`.
	parsed struct {
		parsed bool

		// tableName is `name` but escaped for use in SQL.
		tableName string

		// createStmt is `schema` with a table name of `tableName`
		createStmt string

		// tableDesc is the TableDescriptor created by `createStmt`.
		tableDesc *tabledesc.Mutable

		// zoneConfigStmt contains SQL that effects the zone configs described
		// by `configs`.
		zoneConfigStmts string

		// subzones are the `configs` shorthand parsed into Subzones.
		subzones []zonepb.Subzone

		// generatedSpans are the `generatedSpans` with @primary replaced with
		// the actual primary key name.
		generatedSpans []string

		// scans are the `scans` with @primary replaced with
		// the actual primary key name.
		scans map[string]string
	}
}

type repartitioningTest struct {
	index    string
	old, new partitioningTest
}

// parse fills in the various fields of `partitioningTest.parsed`.
func (pt *partitioningTest) parse() error {
	if pt.parsed.parsed {
		return nil
	}

	pt.parsed.tableName = tree.NameStringP(&pt.name)
	pt.parsed.createStmt = fmt.Sprintf(pt.schema, pt.parsed.tableName)

	{
		ctx := context.Background()
		semaCtx := tree.MakeSemaContext()
		stmt, err := parser.ParseOne(pt.parsed.createStmt)
		if err != nil {
			return errors.Wrapf(err, `parsing %s`, pt.parsed.createStmt)
		}
		createTable, ok := stmt.AST.(*tree.CreateTable)
		if !ok {
			return errors.Errorf("expected *tree.CreateTable got %T", stmt)
		}
		st := cluster.MakeTestingClusterSettings()
		parentID, tableID := descpb.ID(bootstrap.TestingUserDescID(0)), descpb.ID(bootstrap.TestingUserDescID(1))
		mutDesc, err := importer.MakeTestingSimpleTableDescriptor(
			ctx, &semaCtx, st, createTable, parentID, keys.PublicSchemaID, tableID, importer.NoFKs, hlc.UnixNano())
		if err != nil {
			return err
		}
		pt.parsed.tableDesc = mutDesc
		if err := descbuilder.ValidateSelf(pt.parsed.tableDesc, clusterversion.TestingClusterVersion); err != nil {
			return err
		}
	}

	replPK := func(s string) string {
		return strings.ReplaceAll(s, "@primary", fmt.Sprintf("@%s_pkey", pt.parsed.tableDesc.Name))
	}
	pt.parsed.generatedSpans = make([]string, len(pt.generatedSpans))
	for i, gs := range pt.generatedSpans {
		pt.parsed.generatedSpans[i] = replPK(gs)
	}
	pt.parsed.scans = make(map[string]string, len(pt.scans))
	for k, v := range pt.scans {
		pt.parsed.scans[replPK(k)] = replPK(v)
	}

	var zoneConfigStmts bytes.Buffer
	// TODO(dan): Can we run all the zoneConfigStmts in a txn?
	for _, c := range pt.configs {
		c = replPK(c)
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

		var indexName string
		var subzone zonepb.Subzone
		subzoneParts := strings.Split(subzoneShort, ".")
		switch len(subzoneParts) {
		case 1:
			indexName = subzoneParts[0]
		case 2:
			if subzoneParts[0] == "" {
				indexName = fmt.Sprintf("@%s", pt.parsed.tableDesc.Name+"_pkey")
			} else {
				indexName = subzoneParts[0]
			}
			subzone.PartitionName = subzoneParts[1]
		default:
			panic(errors.Errorf("unsupported config: %s", c))
		}
		if !strings.HasPrefix(indexName, "@") {
			panic(errors.Errorf("unsupported config: %s", c))
		}
		idx, err := pt.parsed.tableDesc.FindIndexWithName(indexName[1:])
		if err != nil {
			return errors.Wrapf(err, "could not find index %s", indexName)
		}
		subzone.IndexID = uint32(idx.GetID())
		if len(constraints) > 0 {
			if subzone.PartitionName == "" {
				fmt.Fprintf(&zoneConfigStmts,
					`ALTER INDEX %s@%s CONFIGURE ZONE USING constraints = '[%s]';`,
					pt.parsed.tableName, idx.GetName(), constraints,
				)
			} else {
				fmt.Fprintf(&zoneConfigStmts,
					`ALTER PARTITION %s OF INDEX %s@%s CONFIGURE ZONE USING constraints = '[%s]';`,
					subzone.PartitionName, pt.parsed.tableName, idx.GetName(), constraints,
				)
			}
		}

		var parsedConstraints zonepb.ConstraintsList
		if err := yaml.UnmarshalStrict([]byte("["+constraints+"]"), &parsedConstraints); err != nil {
			return errors.Wrapf(err, "parsing constraints: %s", constraints)
		}
		subzone.Config.Constraints = parsedConstraints.Constraints
		subzone.Config.InheritedConstraints = parsedConstraints.Inherited

		pt.parsed.subzones = append(pt.parsed.subzones, subzone)
	}
	pt.parsed.zoneConfigStmts = zoneConfigStmts.String()
	pt.parsed.parsed = true

	return nil
}

// verifyScansFn returns a closure that runs the test's `scans` and returns a
// descriptive error if any of them fail. It is not required for `parse` to have
// been called.
func (pt *partitioningTest) verifyScansFn(
	ctx context.Context, t *testing.T, db *gosql.DB,
) func() error {
	return func() error {
		for where, expectedNodes := range pt.parsed.scans {
			query := fmt.Sprintf(`SELECT count(*) FROM %s WHERE %s`, tree.NameStringP(&pt.name), where)
			log.Infof(ctx, "query: %s", query)
			if err := verifyScansOnNode(ctx, t, db, query, expectedNodes); err != nil {
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
// repartitioning tests only use a subset and a few entries are only present
// because they're interesting for the before after of a partitioning change.
// Revisit.
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
				PARTITION p3 VALUES FROM (MINVALUE) TO (3),
				PARTITION p4 VALUES FROM (3) TO (4)
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
				`a >= 4`:           `n1`,
			},
		},
		{
			// If this test seems confusing, see the note on the multi-col equivalent.
			name: `single col range partitioning - descending`,
			schema: `CREATE TABLE %s (a INT, PRIMARY KEY (a DESC)) PARTITION BY RANGE (a) (
				PARTITION p4 VALUES FROM (MINVALUE) TO (4),
				PARTITION p3 VALUES FROM (4) TO (3),
				PARTITION px VALUES FROM (3) TO (MAXVALUE)
			)`,
			configs: []string{`.p4:+n1`, `.p3:+n2`, `.px:+n3`},
			generatedSpans: []string{
				`.p4 /1-/1/4`,
				`.p3 /1/4-/1/3`,
				`.px /1/3-/2`,
			},
			scans: map[string]string{
				`a > 4`:            `n1`,
				`a <= 4 AND a > 3`: `n2`,
				`a <= 3`:           `n3`,
			},
		},
		{
			name: `sparse single col range partitioning`,
			schema: `CREATE TABLE %s (a INT PRIMARY KEY) PARTITION BY RANGE (a) (
				PARTITION p1 VALUES FROM (1) TO (2),
				PARTITION p3 VALUES FROM (3) TO (4)
			)`,
			configs: []string{`@primary:+n1`, `.p1:+n2`, `.p3:+n3`},
			generatedSpans: []string{
				`@primary /1-/1/1`,
				`     .p1 /1/1-/1/2`,
				`@primary /1/2-/1/3`,
				`     .p3 /1/3-/1/4`,
				`@primary /1/4-/2`,
			},
			scans: map[string]string{
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
			name: `single col range partitioning - MAXVALUE`,
			schema: `CREATE TABLE %s (a INT PRIMARY KEY) PARTITION BY RANGE (a) (
				PARTITION p4 VALUES FROM (MINVALUE) TO (4),
				PARTITION p5 VALUES FROM (4) TO (5),
				PARTITION px VALUES FROM (5) TO (MAXVALUE)
			)`,
			configs: []string{`@primary`, `.p4:+n1`, `.p5:+n2`, `.px:+n3`},
			generatedSpans: []string{
				`.p4 /1-/1/4`,
				`.p5 /1/4-/1/5`,
				`.px /1/5-/2`,
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
				PARTITION p34 VALUES FROM (MINVALUE, MINVALUE) TO (3, 4),
				PARTITION p56 VALUES FROM (3, 4) TO (5, 6),
				PARTITION p57 VALUES FROM (5, 6) TO (5, 7)
			)`,
			configs: []string{`@primary:+n1`, `.p34:+n2`, `.p56:+n3`, `.p57:+n1`},
			generatedSpans: []string{
				`    .p34 /1-/1/3/4`,
				`    .p56 /1/3/4-/1/5/6`,
				`    .p57 /1/5/6-/1/5/7`,
				`@primary /1/5/7-/2`,
			},
			scans: map[string]string{
				`(a, b) < (3, 4)`:                      `n2`,
				`(a, b) >= (3, 4) AND (a, b) < (5, 6)`: `n3`,
				`(a, b) >= (5, 6) AND (a, b) < (5, 7)`: `n1`,
				`(a, b) >= (5, 7)`:                     `n1`,
			},
		},
		{
			// MINVALUE and MAXVALUE are brutally confusing when used with a column
			// stored in descending order. MINVALUE means "the value that sorts before
			// the earliest value in the index", and so in the case of a descending
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
			name: `multi col range partitioning - descending`,
			schema: `CREATE TABLE %s (a INT, b INT, c INT, PRIMARY KEY (a, b DESC, c)) PARTITION BY RANGE (a, b, c) (
				PARTITION p6xx VALUES FROM (MINVALUE, MINVALUE, MINVALUE) TO (6, MAXVALUE, MAXVALUE),
				PARTITION p75n VALUES FROM (7, MINVALUE, MINVALUE) TO (7, 5, MINVALUE),
				PARTITION pxxx VALUES FROM (7, 5, MINVALUE) TO (MAXVALUE, MAXVALUE, MAXVALUE)
			)`,
			configs: []string{`.p6xx:+n1`, `.p75n:+n2`, `.pxxx:+n3`},
			generatedSpans: []string{
				`.p6xx /1-/1/7`,
				`.p75n /1/7-/1/7/5`,
				`.pxxx /1/7/5-/2`,
			},
			scans: map[string]string{
				`a < 7`:                       `n1`,
				`a = 7 AND b > 5`:             `n2`,
				`a > 7 OR (a = 7 AND b <= 5)`: `n3`,
			},
		},
		{
			name: `sparse multi col range partitioning`,
			schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY RANGE (a, b) (
				PARTITION p34  VALUES FROM (1, 2) TO (3, 4),
				PARTITION p78 VALUES FROM (5, 6) TO (7, 8)
			)`,
			configs: []string{`@primary:+n1`, `.p34:+n2`, `.p78:+n3`},
			generatedSpans: []string{
				`@primary /1-/1/1/2`,
				`    .p34 /1/1/2-/1/3/4`,
				`@primary /1/3/4-/1/5/6`,
				`    .p78 /1/5/6-/1/7/8`,
				`@primary /1/7/8-/2`,
			},
			scans: map[string]string{
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
			name: `multi col range partitioning - MAXVALUE`,
			schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY RANGE (a, b) (
				PARTITION p3n VALUES FROM (MINVALUE, MINVALUE) TO (3, MINVALUE),
				PARTITION p3x VALUES FROM (3, MINVALUE) TO (3, MAXVALUE),
				PARTITION p56 VALUES FROM (3, MAXVALUE) TO (5, 6),
				PARTITION p57 VALUES FROM (5, 6) TO (5, 7)
			)`,
			configs: []string{`@primary:+n1`, `.p3n:+n2`, `.p3x:+n3`, `.p56:+n1`, `.p57:+n2`},
			generatedSpans: []string{
				`    .p3n /1-/1/3`,
				`    .p3x /1/3-/1/4`,
				`    .p56 /1/4-/1/5/6`,
				`    .p57 /1/5/6-/1/5/7`,
				`@primary /1/5/7-/2`,
			},
			scans: map[string]string{
				`a < 3`:                                `n2`,
				`a >= 3 AND a < 4`:                     `n3`,
				`a >= 4 AND (a, b) < (5, 6)`:           `n1`,
				`(a, b) >= (5, 6) AND (a, b) < (5, 7)`: `n2`,
				`(a, b) >= (5, 7)`:                     `n1`,
			},
		},
		{
			name: `multi col range partitioning - MAXVALUE MAXVALUE`,
			schema: `CREATE TABLE %s (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY RANGE (a, b) (
				PARTITION p34 VALUES FROM (MINVALUE, MINVALUE) TO (3, 4),
				PARTITION p3x VALUES FROM (3, 4) TO (3, MAXVALUE),
				PARTITION p56 VALUES FROM (3, MAXVALUE) TO (5, 6),
				PARTITION p57 VALUES FROM (5, 6) TO (5, 7),
				PARTITION pxx VALUES FROM (5, 7) TO (MAXVALUE, MAXVALUE)
			)`,
			configs: []string{`@primary`, `.p34:+n1`, `.p3x:+n2`, `.p56:+n3`, `.p57:+n1`, `.pxx:+n2`},
			generatedSpans: []string{
				`.p34 /1-/1/3/4`,
				`.p3x /1/3/4-/1/4`,
				`.p56 /1/4-/1/5/6`,
				`.p57 /1/5/6-/1/5/7`,
				`.pxx /1/5/7-/2`,
			},
			scans: map[string]string{
				`(a, b) < (3, 4)`:                      `n1`,
				`(a, b) >= (3, 4) AND a < 4`:           `n2`,
				`a >= 4 AND (a, b) < (5, 6)`:           `n3`,
				`(a, b) >= (5, 6) AND (a, b) < (5, 7)`: `n1`,
				`(a, b) >= (5, 7)`:                     `n2`,
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
					PARTITION p34 VALUES FROM (MINVALUE) TO (4)
				),
				PARTITION p5 VALUES IN (5) PARTITION BY RANGE (b) (
					PARTITION p56 VALUES FROM (MINVALUE) TO (6),
					PARTITION p5d VALUES FROM (6) TO (MAXVALUE)
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

		{
			name:   `secondary index - unpartitioned`,
			schema: `CREATE TABLE %s (a INT PRIMARY KEY, b INT, INDEX b_idx (b))`,
		},
		{
			name: `secondary index - list partitioning`,
			schema: `CREATE TABLE %s (a INT PRIMARY KEY, b INT, INDEX b_idx (b) PARTITION BY LIST (b) (
				PARTITION p3 VALUES IN (3),
				PARTITION p4 VALUES IN (4)
			))`,
			configs: []string{`@b_idx:+n1`, `@b_idx.p3:+n2`, `@b_idx.p4:+n3`},
			generatedSpans: []string{
				`@b_idx /2-/2/3`,
				`   .p3 /2/3-/2/4`,
				`   .p4 /2/4-/2/5`,
				`@b_idx /2/5-/3`,
			},
			scans: map[string]string{`b < 3`: `n1`, `b = 3`: `n2`, `b = 4`: `n3`, `b > 4`: `n1`},
		},
		{
			// Intentionally a little different than `single col list
			// partitioning` for the repartitioning tests.
			name: `secondary index - list partitioning - DEFAULT`,
			schema: `CREATE TABLE %s (a INT PRIMARY KEY, b INT, INDEX b_idx (b) PARTITION BY LIST (b) (
				PARTITION p4 VALUES IN (4),
				PARTITION p5 VALUES IN (5),
				PARTITION pd VALUES IN (DEFAULT)
			))`,
			configs: []string{`@b_idx`, `@b_idx.p4:+n2`, `@b_idx.p5:+n3`, `@b_idx.pd:+n1`},
			generatedSpans: []string{
				`.pd /2-/2/4`,
				`.p4 /2/4-/2/5`,
				`.p5 /2/5-/2/6`,
				`.pd /2/6-/3`,
			},
			scans: map[string]string{`b < 4`: `n1`, `b = 4`: `n2`, `b = 5`: `n3`, `b > 5`: `n1`},
		},
		{
			name: `secondary index - NULL`,
			schema: `CREATE TABLE %s (a INT PRIMARY KEY, b INT, INDEX b_idx (b) PARTITION BY LIST (b) (
				PARTITION pl1 VALUES IN (NULL, 1),
				PARTITION p3  VALUES IN (3)
			))`,
			configs: []string{`@b_idx:+n1`, `@b_idx.pl1:+n2`, `@b_idx.p3:+n3`},
			generatedSpans: []string{
				`@b_idx /2-/2/NULL`,
				`  .pl1 /2/NULL-/2/!NULL`,
				`@b_idx /2/!NULL-/2/1`,
				`  .pl1 /2/1-/2/2`,
				`@b_idx /2/2-/2/3`,
				`   .p3 /2/3-/2/4`,
				`@b_idx /2/4-/3`,
			},
			scans: map[string]string{
				`b = -1`:             `n1`,
				`b IS NULL`:          `n2`,
				`b IS NULL OR b = 1`: `n2`,
				`b = 3`:              `n3`,
			},
		},

		{
			name: `scans`,
			schema: `CREATE TABLE %s (a INT PRIMARY KEY, b INT) PARTITION BY LIST (a) (
				PARTITION p3p5 VALUES IN ((3), (5)),
				PARTITION p4 VALUES IN (4),
				PARTITION pd VALUES IN (DEFAULT)
			)`,
			configs: []string{`@primary:+n1`, `.p3p5:+n2`, `.p4:+n3`, `.pd:+n1`},
			generatedSpans: []string{
				`  .pd /1-/1/3`,
				`.p3p5 /1/3-/1/4`,
				`  .p4 /1/4-/1/5`,
				`.p3p5 /1/5-/1/6`,
				`  .pd /1/6-/2`,
			},
			scans: map[string]string{
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
		escapedName := fmt.Sprintf("%s_table", strings.Replace(typ.String(), "\"", "", -1))
		// schema is used in a fmt.Sprintf to fill in the table name, so we have
		// to escape any stray %s.
		escapedDatum := strings.Replace(serializedDatum, `%`, `%%`, -1)
		test := partitioningTest{
			name:    escapedName,
			schema:  fmt.Sprintf(schemaFmt, typ.SQLString(), escapedDatum),
			configs: []string{`@primary:+n1`, `.p:+n2`},
			scans: map[string]string{
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

func allRepartitioningTests(partitioningTests []partitioningTest) ([]repartitioningTest, error) {
	tests := []repartitioningTest{
		{
			index: `primary`,
			old:   partitioningTest{name: `unpartitioned`},
			new:   partitioningTest{name: `unpartitioned`},
		},
		{
			index: `primary`,
			old:   partitioningTest{name: `unpartitioned`},
			new:   partitioningTest{name: `single col list partitioning`},
		},
		{
			index: `primary`,
			old:   partitioningTest{name: `unpartitioned`},
			new:   partitioningTest{name: `single col list partitioning - DEFAULT`},
		},
		{
			index: `primary`,
			old:   partitioningTest{name: `unpartitioned`},
			new:   partitioningTest{name: `single col range partitioning`},
		},
		{
			index: `primary`,
			old:   partitioningTest{name: `unpartitioned`},
			new:   partitioningTest{name: `single col range partitioning - MAXVALUE`},
		},

		{
			index: `primary`,
			old:   partitioningTest{name: `single col list partitioning`},
			new:   partitioningTest{name: `single col list partitioning - DEFAULT`},
		},
		{
			index: `primary`,
			old:   partitioningTest{name: `single col list partitioning - DEFAULT`},
			new:   partitioningTest{name: `single col list partitioning`},
		},
		{
			index: `primary`,
			old:   partitioningTest{name: `multi col list partitioning`},
			new:   partitioningTest{name: `multi col list partitioning - DEFAULT`},
		},
		{
			index: `primary`,
			old:   partitioningTest{name: `multi col list partitioning - DEFAULT`},
			new:   partitioningTest{name: `multi col list partitioning`},
		},
		{
			index: `primary`,
			old:   partitioningTest{name: `multi col list partitioning - DEFAULT`},
			new:   partitioningTest{name: `multi col list partitioning - DEFAULT DEFAULT`},
		},
		{
			index: `primary`,
			old:   partitioningTest{name: `multi col list partitioning - DEFAULT DEFAULT`},
			new:   partitioningTest{name: `multi col list partitioning - DEFAULT`},
		},

		{
			index: `primary`,
			old:   partitioningTest{name: `single col range partitioning`},
			new:   partitioningTest{name: `single col range partitioning - MAXVALUE`},
		},
		{
			index: `primary`,
			old:   partitioningTest{name: `single col range partitioning - MAXVALUE`},
			new:   partitioningTest{name: `single col range partitioning`},
		},
		{
			index: `primary`,
			old:   partitioningTest{name: `multi col range partitioning`},
			new:   partitioningTest{name: `multi col range partitioning - MAXVALUE`},
		},
		{
			index: `primary`,
			old:   partitioningTest{name: `multi col range partitioning - MAXVALUE`},
			new:   partitioningTest{name: `multi col range partitioning`},
		},
		{
			index: `primary`,
			old:   partitioningTest{name: `multi col range partitioning - MAXVALUE`},
			new:   partitioningTest{name: `multi col range partitioning - MAXVALUE MAXVALUE`},
		},
		{
			index: `primary`,
			old:   partitioningTest{name: `multi col range partitioning - MAXVALUE MAXVALUE`},
			new:   partitioningTest{name: `multi col range partitioning - MAXVALUE`},
		},

		{
			index: `primary`,
			old:   partitioningTest{name: `single col list partitioning`},
			new:   partitioningTest{name: `single col range partitioning`},
		},
		{
			index: `primary`,
			old:   partitioningTest{name: `single col range partitioning`},
			new:   partitioningTest{name: `single col list partitioning`},
		},

		// TODO(dan): One repartitioning is fully implemented, these tests also
		// need to pass with no ccl code.
		{
			index: `primary`,
			old:   partitioningTest{name: `single col list partitioning`},
			new:   partitioningTest{name: `unpartitioned`},
		},
		{
			index: `primary`,
			old:   partitioningTest{name: `single col list partitioning - DEFAULT`},
			new:   partitioningTest{name: `unpartitioned`},
		},
		{
			index: `primary`,
			old:   partitioningTest{name: `single col range partitioning`},
			new:   partitioningTest{name: `unpartitioned`},
		},
		{
			index: `primary`,
			old:   partitioningTest{name: `single col range partitioning - MAXVALUE`},
			new:   partitioningTest{name: `unpartitioned`},
		},

		{
			index: `b_idx`,
			old:   partitioningTest{name: `secondary index - unpartitioned`},
			new:   partitioningTest{name: `secondary index - list partitioning`},
		},
		{
			index: `b_idx`,
			old:   partitioningTest{name: `secondary index - list partitioning`},
			new:   partitioningTest{name: `secondary index - unpartitioned`},
		},
		{
			index: `b_idx`,
			old:   partitioningTest{name: `secondary index - list partitioning`},
			new:   partitioningTest{name: `secondary index - list partitioning - DEFAULT`},
		},
		{
			index: `b_idx`,
			old:   partitioningTest{name: `secondary index - list partitioning - DEFAULT`},
			new:   partitioningTest{name: `secondary index - list partitioning`},
		},
	}

	partitioningTestsByName := make(map[string]partitioningTest, len(partitioningTests))
	for _, partitioningTest := range partitioningTests {
		partitioningTestsByName[partitioningTest.name] = partitioningTest
	}
	for i := range tests {
		t, ok := partitioningTestsByName[tests[i].old.name]
		if !ok {
			return nil, errors.Errorf("unknown partitioning test: %s", tests[i].old.name)
		}
		tests[i].old = t
		if err := tests[i].old.parse(); err != nil {
			return nil, err
		}

		t, ok = partitioningTestsByName[tests[i].new.name]
		if !ok {
			return nil, errors.Errorf("unknown partitioning test: %s", tests[i].new.name)
		}
		tests[i].new = t
		if err := tests[i].new.parse(); err != nil {
			return nil, err
		}
	}

	return tests, nil
}

func verifyScansOnNode(
	ctx context.Context, t *testing.T, db *gosql.DB, query string, node string,
) error {
	// TODO(dan): This is a stopgap. At some point we should have a syntax for
	// doing this directly (running a query and getting back the nodes it ran on
	// and attributes/localities of those nodes). Users will also want this to
	// be sure their partitioning is working.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to create conn: %v", err)
	}
	sqlDB := sqlutils.MakeSQLRunner(conn)
	defer func() { _ = conn.Close() }()
	sqlDB.Exec(t, fmt.Sprintf(`SET tracing = on; %s; SET tracing = off`, query))
	rows := sqlDB.Query(t, `SELECT concat(tag, ' ', message) FROM [SHOW TRACE FOR SESSION]`)
	defer rows.Close()
	var scansWrongNode []string
	var traceLines []string
	var traceLine gosql.NullString
	for rows.Next() {
		if err := rows.Scan(&traceLine); err != nil {
			t.Fatal(err)
		}
		traceLines = append(traceLines, traceLine.String)
		if strings.Contains(traceLine.String, "read completed") {
			if strings.Contains(traceLine.String, "SystemCon") || strings.Contains(traceLine.String, "NamespaceTab") {
				// Ignore trace lines for the system config range (abbreviated as
				// "SystemCon" in pretty printing of the range descriptor). A read might
				// be performed to the system config range to update the table lease.
				//
				// Also ignore trace lines for the system.namespace table, which is a
				// system table that resides outside the system config range. (abbreviated
				// as "NamespaceTab" in pretty printing of the range descriptor).
				continue
			}
			if !strings.Contains(traceLine.String, node) {
				scansWrongNode = append(scansWrongNode, traceLine.String)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "unexpected error querying traces")
	}
	if len(scansWrongNode) > 0 {
		err := errors.Newf("expected to scan on %s: %s", node, query)
		err = errors.WithDetailf(err, "scans:\n%s", strings.Join(scansWrongNode, "\n"))
		var trace strings.Builder
		for _, traceLine := range traceLines {
			trace.WriteString("\n  ")
			trace.WriteString(traceLine)
		}
		err = errors.WithDetailf(err, "trace:%s", trace.String())
		return err
	}
	return nil
}

func setupPartitioningTestCluster(
	ctx context.Context, t testing.TB,
) (*gosql.DB, *sqlutils.SQLRunner, func()) {
	cfg := zonepb.DefaultZoneConfig()
	cfg.NumReplicas = proto.Int32(1)

	tsArgs := func(attr string) base.TestServerArgs {
		return base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					// Disable LBS because when the scan is happening at the rate it's happening
					// below, it's possible that one of the system ranges trigger a split.
					DisableLoadBasedSplitting: true,
				},
				Server: &server.TestingKnobs{
					DefaultZoneConfigOverride: &cfg,
				},
			},
			ScanInterval: 100 * time.Millisecond,
			StoreSpecs: []base.StoreSpec{
				{InMemory: true, Attributes: roachpb.Attributes{Attrs: []string{attr}}},
			},
			UseDatabase: "data",
		}
	}
	tcArgs := base.TestClusterArgs{ServerArgsPerNode: map[int]base.TestServerArgs{
		0: tsArgs("n1"),
		1: tsArgs("n2"),
		2: tsArgs("n3"),
	}}
	tc := testcluster.StartTestCluster(t, 3, tcArgs)

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE DATABASE data`)

	// Disabling store throttling vastly speeds up rebalancing.
	sqlDB.Exec(t, `SET CLUSTER SETTING server.declined_reservation_timeout = '0s'`)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.failed_reservation_timeout = '0s'`)

	return tc.Conns[0], sqlDB, func() {
		tc.Stopper().Stop(context.Background())
	}
}

func TestInitialPartitioning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Skipping as part of test-infra-team flaky test cleanup.
	skip.WithIssue(t, 49909)

	// This test configures many sub-tests and is too slow to run under nightly
	// race stress.
	skip.UnderStressRace(t)
	skip.UnderShort(t)

	rng, _ := randutil.NewTestRand()
	testCases := allPartitioningTests(rng)

	ctx := context.Background()
	db, sqlDB, cleanup := setupPartitioningTestCluster(ctx, t)
	defer cleanup()

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

			testutils.SucceedsSoon(t, test.verifyScansFn(ctx, t, db))
		})
	}
}

func TestSelectPartitionExprs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(dan): PartitionExprs for range partitions is waiting on the new
	// range partitioning syntax.
	testData := partitioningTest{
		name: `partition exprs`,
		schema: `CREATE TABLE %s (
			a INT, b INT, c INT, PRIMARY KEY (a, b, c)
		) PARTITION BY LIST (a, b) (
			PARTITION p33p44 VALUES IN ((3, 3), (4, 4)) PARTITION BY LIST (c) (
				PARTITION p335p445 VALUES IN (5),
				PARTITION p33dp44d VALUES IN (DEFAULT)
			),
			PARTITION p6d VALUES IN ((6, DEFAULT)),
			PARTITION pdd VALUES IN ((DEFAULT, DEFAULT))
		)`,
	}
	if err := testData.parse(); err != nil {
		t.Fatalf("%+v", err)
	}

	tests := []struct {
		// partitions is a comma-separated list of input partitions
		partitions string
		// expr is the expected output
		expr string
	}{
		{`p33p44`, `((a, b) = (3, 3)) OR ((a, b) = (4, 4))`},
		{`p335p445`, `((a, b, c) = (3, 3, 5)) OR ((a, b, c) = (4, 4, 5))`},
		{`p33dp44d`, `(((a, b) = (3, 3)) AND (NOT ((a, b, c) = (3, 3, 5)))) OR (((a, b) = (4, 4)) AND (NOT ((a, b, c) = (4, 4, 5))))`},
		// NB See the TODO in the impl for why this next case has some clearly
		// unrelated `!=`s.
		{`p6d`, `((a,) = (6,)) AND (NOT (((a, b) = (3, 3)) OR ((a, b) = (4, 4))))`},
		{`pdd`, `NOT ((((a, b) = (3, 3)) OR ((a, b) = (4, 4))) OR ((a,) = (6,)))`},

		{`p335p445,p6d`, `(((a, b, c) = (3, 3, 5)) OR ((a, b, c) = (4, 4, 5))) OR (((a,) = (6,)) AND (NOT (((a, b) = (3, 3)) OR ((a, b) = (4, 4)))))`},

		// TODO(dan): The expression simplification in this method is all done
		// by our normal SQL expression simplification code. Seems like it could
		// use some targeted work to clean these up. Ideally the following would
		// all simplyify to  `(a, b) IN ((3, 3), (4, 4))`. Some of them work
		// because for every requested partition, all descendent partitions are
		// omitted, which is an optimization to save a little work with the side
		// benefit of making more of these what we want.
		{`p335p445,p33dp44d`, `(((a, b, c) = (3, 3, 5)) OR ((a, b, c) = (4, 4, 5))) OR ((((a, b) = (3, 3)) AND (NOT ((a, b, c) = (3, 3, 5)))) OR (((a, b) = (4, 4)) AND (NOT ((a, b, c) = (4, 4, 5)))))`},
		{`p33p44,p335p445`, `((a, b) = (3, 3)) OR ((a, b) = (4, 4))`},
		{`p33p44,p335p445,p33dp44d`, `((a, b) = (3, 3)) OR ((a, b) = (4, 4))`},
	}

	evalCtx := &tree.EvalContext{
		Codec:    keys.SystemSQLCodec,
		Settings: cluster.MakeTestingClusterSettings(),
	}
	for _, test := range tests {
		t.Run(test.partitions, func(t *testing.T) {
			var partNames tree.NameList
			for _, p := range strings.Split(test.partitions, `,`) {
				partNames = append(partNames, tree.Name(p))
			}
			expr, err := selectPartitionExprs(evalCtx, testData.parsed.tableDesc, partNames)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			if exprStr := expr.String(); exprStr != test.expr {
				t.Errorf("got\n%s\nexpected\n%s", exprStr, test.expr)
			}
		})
	}
	t.Run("error", func(t *testing.T) {
		partNames := tree.NameList{`p33p44`, `nope`}
		_, err := selectPartitionExprs(evalCtx, testData.parsed.tableDesc, partNames)
		if !testutils.IsError(err, `unknown partition`) {
			t.Errorf(`expected "unknown partition" error got: %+v`, err)
		}
	})
}

func TestRepartitioning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Skipping as part of test-infra-team flaky test cleanup.
	skip.WithIssue(t, 49112)

	// This test configures many sub-tests and is too slow to run under nightly
	// race stress.
	skip.UnderStressRace(t)

	rng, _ := randutil.NewTestRand()
	testCases, err := allRepartitioningTests(allPartitioningTests(rng))
	if err != nil {
		t.Fatalf("%+v", err)
	}

	ctx := context.Background()
	db, sqlDB, cleanup := setupPartitioningTestCluster(ctx, t)
	defer cleanup()

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%s/%s", test.old.name, test.new.name), func(t *testing.T) {
			sqlDB.Exec(t, `DROP DATABASE IF EXISTS data`)
			sqlDB.Exec(t, `CREATE DATABASE data`)

			{
				if err := test.old.parse(); err != nil {
					t.Fatalf("%+v", err)
				}
				sqlDB.Exec(t, test.old.parsed.createStmt)
				sqlDB.Exec(t, test.old.parsed.zoneConfigStmts)

				testutils.SucceedsSoon(t, test.old.verifyScansFn(ctx, t, db))
			}

			{
				if err := test.new.parse(); err != nil {
					t.Fatalf("%+v", err)
				}
				sqlDB.Exec(t, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", test.old.parsed.tableName, test.new.parsed.tableName))

				testIndex, err := test.new.parsed.tableDesc.FindIndexWithName(test.index)
				if err != nil {
					t.Fatalf("%+v", err)
				}

				var repartition bytes.Buffer
				if testIndex.GetID() == test.new.parsed.tableDesc.GetPrimaryIndexID() {
					fmt.Fprintf(&repartition, `ALTER TABLE %s `, test.new.parsed.tableName)
				} else {
					fmt.Fprintf(&repartition, `ALTER INDEX %s@%s `, test.new.parsed.tableName, testIndex.GetName())
				}
				if testIndex.GetPartitioning().NumColumns() == 0 {
					repartition.WriteString(`PARTITION BY NOTHING`)
				} else {
					if err := sql.ShowCreatePartitioning(
						&tree.DatumAlloc{}, keys.SystemSQLCodec, test.new.parsed.tableDesc, testIndex,
						testIndex.GetPartitioning(), &repartition, 0 /* indent */, 0, /* colOffset */
					); err != nil {
						t.Fatalf("%+v", err)
					}
				}
				sqlDB.Exec(t, repartition.String())

				// Verify that repartitioning removes zone configs for partitions that
				// have been removed.
				newPartitionNames := map[string]struct{}{}
				for _, index := range test.new.parsed.tableDesc.NonDropIndexes() {
					_ = index.GetPartitioning().ForEachPartitionName(func(name string) error {
						newPartitionNames[name] = struct{}{}
						return nil
					})
				}
				for _, row := range sqlDB.QueryStr(
					t, "SELECT partition_name FROM crdb_internal.zones WHERE partition_name IS NOT NULL") {
					partitionName := row[0]
					if _, ok := newPartitionNames[partitionName]; !ok {
						t.Errorf("zone config for removed partition %q exists after repartitioning", partitionName)
					}
				}

				// NB: Not all old zone configurations are removed. This statement will
				// overwrite any with the same name and the repartitioning removes any
				// for partitions that no longer exist, but there could still be some
				// sitting around (e.g., when a repartitioning preserves a partition but
				// does not apply a new zone config). This is fine.
				sqlDB.Exec(t, test.new.parsed.zoneConfigStmts)
				testutils.SucceedsSoon(t, test.new.verifyScansFn(ctx, t, db))
			}
		})
	}
}

func TestPrimaryKeyChangeZoneConfigs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Write a table with some partitions into the database,
	// and change its primary key.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
USE t;
CREATE TABLE t (
  x INT PRIMARY KEY,
  y INT NOT NULL,
  z INT,
  w INT,
  INDEX i1 (z),
  INDEX i2 (w),
  FAMILY (x, y, z, w)
);
ALTER INDEX t@i1 PARTITION BY LIST (z) (
  PARTITION p1 VALUES IN (1)
);
ALTER INDEX t@i2 PARTITION BY LIST (w) (
  PARTITION p2 VALUES IN (3)
);
ALTER PARTITION p1 OF INDEX t@i1 CONFIGURE ZONE USING gc.ttlseconds = 15210;
ALTER PARTITION p2 OF INDEX t@i2 CONFIGURE ZONE USING gc.ttlseconds = 15213;
ALTER TABLE t ALTER PRIMARY KEY USING COLUMNS (y)
`); err != nil {
		t.Fatal(err)
	}

	// Get the zone config corresponding to the table.
	table := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "t")
	kv, err := kvDB.Get(ctx, config.MakeZoneKey(keys.SystemSQLCodec, table.GetID()))
	if err != nil {
		t.Fatal(err)
	}
	var zone zonepb.ZoneConfig
	if err := kv.ValueProto(&zone); err != nil {
		t.Fatal(err)
	}

	// Our subzones should be spans prefixed with dropped copy of i1,
	// dropped copy of i2, new copy of i1, and new copy of i2.
	// These have ID's 2, 3, 8 and 10 respectively.
	expectedSpans := []roachpb.Key{
		table.IndexSpan(keys.SystemSQLCodec, 2 /* indexID */).Key,
		table.IndexSpan(keys.SystemSQLCodec, 3 /* indexID */).Key,
		table.IndexSpan(keys.SystemSQLCodec, 8 /* indexID */).Key,
		table.IndexSpan(keys.SystemSQLCodec, 10 /* indexID */).Key,
	}
	if len(zone.SubzoneSpans) != len(expectedSpans) {
		t.Fatalf("expected subzones to have length %d", len(expectedSpans))
	}

	// Subzone spans have the table prefix omitted.
	prefix := keys.SystemSQLCodec.TablePrefix(uint32(table.GetID()))
	for i := range expectedSpans {
		// Subzone spans have the table prefix omitted.
		expected := bytes.TrimPrefix(expectedSpans[i], prefix)
		if !bytes.HasPrefix(zone.SubzoneSpans[i].Key, expected) {
			t.Fatalf(
				"expected span to have prefix %s but found %s",
				expected,
				zone.SubzoneSpans[i].Key,
			)
		}
	}
}

func TestRemovePartitioningExpiredLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer utilccl.TestingEnableEnterprise()()

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "d",
	})
	defer s.Stopper().Stop(ctx)

	// Create a partitioned table and index.
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE t (a INT PRIMARY KEY) PARTITION BY LIST (a) (
		PARTITION	p1 VALUES IN (1)
	)`)
	sqlDB.Exec(t, `CREATE INDEX i ON t (a) PARTITION BY RANGE (a) (
		PARTITION p34 VALUES FROM (3) TO (4)
	)`)
	sqlDB.Exec(t, `ALTER PARTITION p1 OF TABLE t CONFIGURE ZONE USING DEFAULT`)
	sqlDB.Exec(t, `ALTER PARTITION p34 OF INDEX t@i CONFIGURE ZONE USING DEFAULT`)
	sqlDB.Exec(t, `ALTER INDEX t@t_pkey CONFIGURE ZONE USING DEFAULT`)
	sqlDB.Exec(t, `ALTER INDEX t@i CONFIGURE ZONE USING DEFAULT`)

	// Remove the enterprise license.
	defer utilccl.TestingDisableEnterprise()()

	const partitionErr = "use of partitions requires an enterprise license"
	const zoneErr = "use of replication zones on indexes or partitions requires an enterprise license"
	expectErr := func(q string, expErr string) {
		t.Helper()
		sqlDB.ExpectErr(t, expErr, q)
	}

	// Partitions and zone configs cannot be modified without a valid license.
	expectErr(`ALTER TABLE t PARTITION BY LIST (a) (PARTITION p2 VALUES IN (2))`, partitionErr)
	expectErr(`ALTER INDEX t@i PARTITION BY RANGE (a) (PARTITION p45 VALUES FROM (4) TO (5))`, partitionErr)
	expectErr(`ALTER PARTITION p1 OF TABLE t CONFIGURE ZONE USING DEFAULT`, zoneErr)
	expectErr(`ALTER PARTITION p34 OF INDEX t@i CONFIGURE ZONE USING DEFAULT`, zoneErr)
	expectErr(`ALTER INDEX t@t_pkey CONFIGURE ZONE USING DEFAULT`, zoneErr)
	expectErr(`ALTER INDEX t@i CONFIGURE ZONE USING DEFAULT`, zoneErr)

	// But they can be removed.
	sqlDB.Exec(t, `ALTER TABLE t PARTITION BY NOTHING`)
	sqlDB.Exec(t, `ALTER INDEX t@i PARTITION BY NOTHING`)
	sqlDB.Exec(t, `ALTER INDEX t@t_pkey CONFIGURE ZONE DISCARD`)
	sqlDB.Exec(t, `ALTER INDEX t@i CONFIGURE ZONE DISCARD`)

	// Once removed, they cannot be added back.
	expectErr(`ALTER TABLE t PARTITION BY LIST (a) (PARTITION p2 VALUES IN (2))`, partitionErr)
	expectErr(`ALTER INDEX t@i PARTITION BY RANGE (a) (PARTITION p45 VALUES FROM (4) TO (5))`, partitionErr)
	expectErr(`ALTER INDEX t@t_pkey CONFIGURE ZONE USING DEFAULT`, zoneErr)
	expectErr(`ALTER INDEX t@i CONFIGURE ZONE USING DEFAULT`, zoneErr)
}

// Test that dropping an enum value fails if there's a concurrent index drop
// for an index partitioned by that enum value. The reason is that it
// would be bad if we rolled back the dropping of the index.
func TestDropEnumValueWithConcurrentPartitionedIndexDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var s serverutils.TestServerInterface
	var sqlDB *gosql.DB

	// Use the dropCh to block any DROP INDEX job until the channel is closed.
	dropCh := make(chan chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	s, sqlDB, _ = serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				RunBeforeResume: func(jobID jobspb.JobID) error {
					var isDropJob bool
					if err := sqlDB.QueryRow(`
SELECT count(*) > 0
  FROM [SHOW JOB $1]
 WHERE description LIKE 'DROP INDEX %'
`, jobID).Scan(&isDropJob); err != nil {
						return err
					}
					if !isDropJob {
						return nil
					}
					ch := make(chan struct{})
					select {
					case dropCh <- ch:
					case <-ctx.Done():
						return ctx.Err()
					}
					select {
					case <-ch:
					case <-ctx.Done():
						return ctx.Err()
					}
					return nil
				},
			},
		},
	})
	defer s.Stopper().Stop(context.Background())
	defer cancel()
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	// Set up the table to have an index which is partitioned by the enum value
	// we're going to drop.
	for _, stmt := range []string{
		`CREATE TYPE t AS ENUM ('a', 'b', 'c')`,
		`CREATE TABLE tbl (
    i INT8, k t,
    PRIMARY KEY (i, k),
    INDEX idx (k)
        PARTITION BY RANGE (k)
            (PARTITION a VALUES FROM ('a') TO ('b'))
)`,
	} {
		tdb.Exec(t, stmt)
	}
	// Run a transaction to drop the index and the enum value.
	errCh := make(chan error)
	go func() {
		errCh <- crdb.ExecuteTx(ctx, sqlDB, nil, func(tx *gosql.Tx) error {
			if _, err := tx.Exec("drop index tbl@idx;"); err != nil {
				return err
			}
			_, err := tx.Exec("alter type t drop value 'a';")
			return err
		})
	}()
	// Wait until the dropping of the enum value has finished.
	ch := <-dropCh
	testutils.SucceedsSoon(t, func() error {
		var done bool
		tdb.QueryRow(t, `
SELECT bool_and(done)
  FROM (
        SELECT status NOT IN `+jobs.NonTerminalStatusTupleString+` AS done
          FROM [SHOW JOBS]
         WHERE job_type = 'TYPEDESC SCHEMA CHANGE'
       );`).
			Scan(&done)
		if done {
			return nil
		}
		return errors.Errorf("not done")
	})
	// Allow the dropping of the index to proceed.
	close(ch)
	// Ensure we got the right error.
	require.Regexp(t,
		`could not remove enum value "a" as it is being used in the partitioning of index tbl@idx`,
		<-errCh)
}
