// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package partitionccl

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestValidIndexPartitionSetShowZones(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TODOTestTenantDisabled,
	})
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `
		CREATE DATABASE d;
		USE d;
		CREATE TABLE t (c STRING PRIMARY KEY) PARTITION BY LIST (c) (
			PARTITION p0 VALUES IN ('a'),
			PARTITION p1 VALUES IN (DEFAULT)
		)`)

	gcDefault := fmt.Sprintf("gc.ttlseconds = %d", s.DefaultZoneConfig().GC.TTLSeconds)
	gcOverride := "gc.ttlseconds = 42"
	zoneOverride := s.DefaultZoneConfig()
	zoneOverride.GC = &zonepb.GCPolicy{TTLSeconds: 42}
	partialZoneOverride := *zonepb.NewZoneConfig()
	partialZoneOverride.GC = &zonepb.GCPolicy{TTLSeconds: 42}

	dbID := sqlutils.QueryDatabaseID(t, db, "d")
	tableID := sqlutils.QueryTableID(t, db, "d", "public", "t")

	defaultRow := sqlutils.ZoneRow{
		ID:     keys.RootNamespaceID,
		Config: s.DefaultZoneConfig(),
	}
	defaultOverrideRow := sqlutils.ZoneRow{
		ID:     keys.RootNamespaceID,
		Config: zoneOverride,
	}
	dbRow := sqlutils.ZoneRow{
		ID:     dbID,
		Config: zoneOverride,
	}
	tableRow := sqlutils.ZoneRow{
		ID:     tableID,
		Config: zoneOverride,
	}
	primaryRow := sqlutils.ZoneRow{
		ID:     tableID,
		Config: zoneOverride,
	}
	p0Row := sqlutils.ZoneRow{
		ID:     tableID,
		Config: zoneOverride,
	}
	p1Row := sqlutils.ZoneRow{
		ID:     tableID,
		Config: zoneOverride,
	}

	// Partially filled config rows
	partialDbRow := sqlutils.ZoneRow{
		ID:     dbID,
		Config: partialZoneOverride,
	}
	partialTableRow := sqlutils.ZoneRow{
		ID:     tableID,
		Config: partialZoneOverride,
	}
	partialPrimaryRow := sqlutils.ZoneRow{
		ID:     tableID,
		Config: partialZoneOverride,
	}
	partialP0Row := sqlutils.ZoneRow{
		ID:     tableID,
		Config: partialZoneOverride,
	}
	partialP1Row := sqlutils.ZoneRow{
		ID:     tableID,
		Config: partialZoneOverride,
	}

	// Remove stock zone configs installed at cluster bootstrap. Otherwise this
	// test breaks whenever these stock zone configs are adjusted.
	sqlutils.RemoveAllZoneConfigs(t, sqlDB)

	// Ensure the default is reported for all zones at first.
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE default", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@t_pkey", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", defaultRow)

	// Ensure a database zone config applies to that database, its tables, and its
	// tables' indices and partitions.
	sqlutils.SetZoneConfig(t, sqlDB, "DATABASE d", gcOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialDbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@t_pkey", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", dbRow)

	// Ensure a table zone config applies to that table and its indices and
	// partitions, but no other zones.
	sqlutils.SetZoneConfig(t, sqlDB, "TABLE d.t", gcOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialDbRow, partialTableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@t_pkey", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", tableRow)

	// Ensure an index zone config applies to that index and its partitions, but
	// no other zones.
	sqlutils.SetZoneConfig(t, sqlDB, "INDEX d.t@t_pkey", gcOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialDbRow, partialTableRow, partialPrimaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@t_pkey", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", primaryRow)

	// Ensure a partition zone config applies to that partition, but no other
	// zones.
	sqlutils.SetZoneConfig(t, sqlDB, "PARTITION p0 OF TABLE d.t", gcOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialDbRow, partialTableRow, partialPrimaryRow, partialP0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@t_pkey", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", primaryRow)

	// Ensure updating the default zone propagates to zones without an override,
	// but not to those with overrides.
	sqlutils.SetZoneConfig(t, sqlDB, "RANGE default", gcOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, partialDbRow, partialTableRow, partialPrimaryRow, partialP0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@t_pkey", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", primaryRow)

	// Ensure deleting a database zone leaves child overrides in place.
	sqlutils.DeleteZoneConfig(t, sqlDB, "DATABASE d")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, partialTableRow, partialPrimaryRow, partialP0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@t_pkey", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", primaryRow)

	// Ensure deleting a table zone leaves child overrides in place.
	sqlutils.DeleteZoneConfig(t, sqlDB, "TABLE d.t")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, partialPrimaryRow, partialP0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@t_pkey", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", primaryRow)

	// Ensure deleting an index zone leaves child overrides in place.
	sqlutils.DeleteZoneConfig(t, sqlDB, "INDEX d.t@t_pkey")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, partialP0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@t_pkey", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", p0Row)

	// Ensure deleting a partition zone works.
	sqlutils.DeleteZoneConfig(t, sqlDB, "PARTITION p0 OF TABLE d.t")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", defaultOverrideRow)

	// Ensure deleting non-overridden zones is not an error.
	sqlutils.DeleteZoneConfig(t, sqlDB, "DATABASE d")
	sqlutils.DeleteZoneConfig(t, sqlDB, "TABLE d.t")
	sqlutils.DeleteZoneConfig(t, sqlDB, "PARTITION p1 OF TABLE d.t")

	// Ensure updating the default zone config applies to zones that have had
	// overrides added and removed.
	sqlutils.SetZoneConfig(t, sqlDB, "RANGE default", gcDefault)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE default", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@t_pkey", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", defaultRow)

	// Ensure subzones can be created even when no table zone exists.
	sqlutils.SetZoneConfig(t, sqlDB, "PARTITION p0 OF TABLE d.t", gcOverride)
	sqlutils.SetZoneConfig(t, sqlDB, "PARTITION p1 OF TABLE d.t", gcOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialP0Row, partialP1Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", p1Row)

	// Ensure the shorthand index syntax works.
	sqlutils.SetZoneConfig(t, sqlDB, `INDEX "t_pkey"`, gcOverride)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, `INDEX "t_pkey"`, primaryRow)

	// Ensure the session database is respected.
	sqlutils.SetZoneConfig(t, sqlDB, "PARTITION p0 OF TABLE t", gcOverride)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE t", p0Row)
}

func TestInvalidIndexPartitionSetShowZones(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TODOTestTenantDisabled,
	})
	defer s.Stopper().Stop(context.Background())

	for i, tc := range []struct {
		query string
		err   string
	}{
		{
			"ALTER INDEX foo CONFIGURE ZONE USING DEFAULT",
			`index "foo" does not exist`,
		},
		{
			"SHOW ZONE CONFIGURATION FOR INDEX foo",
			`index "foo" does not exist`,
		},
		// N.B. The following will always fallback to the legacy schema changer
		// because multi-statement txns are not yet supported by our declarative
		// schema changer.
		{
			"USE system; ALTER INDEX foo CONFIGURE ZONE USING DEFAULT",
			`index "foo" does not exist`,
		},
		{
			"USE system; SHOW ZONE CONFIGURATION FOR INDEX foo",
			`index "foo" does not exist`,
		},
		{
			"ALTER PARTITION p0 OF TABLE system.jobs CONFIGURE ZONE = 'foo'",
			`partition "p0" does not exist`,
		},
		{
			"SHOW ZONE CONFIGURATION FOR PARTITION p0 OF TABLE system.jobs",
			`partition "p0" does not exist`,
		},
	} {
		if _, err := db.Exec(tc.query); !testutils.IsError(err, tc.err) {
			t.Errorf("#%d: expected error matching %q, but got %v", i, tc.err, err)
		}
	}
}

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
	// These will be parsedPartitioningTest into `parsedPartitioningTest.subzones`.
	configs []string

	// generatedSpans is 1:1 to the output of GenerateSubzoneSpans, each
	// formatted as `{subzone} {start}-{end}` (e.g. `@primary /1-/2`), where
	// {subzone} is formatted identically to the test shorthand above, and
	// {start} and {end} are formatted using our key pretty printer, but with
	// the table removed. The end key is always specified in here (though
	// GenerateSubzoneSpans omits it under certain conditions to save space).
	generatedSpans []string
}

type parsedPartitioningTest struct {
	// tableName is `name` but escaped for use in SQL.
	tableName string

	// createStmt is `schema` with a table name of `tableName`
	createStmt string

	// tableDesc is the TableDescriptor created by `createStmt`.
	tableDesc *tabledesc.Mutable

	// zoneConfigStmt contains SQL that effects the zone configs described
	// by `configs`.
	zoneConfigStmts string

	// subzones are the `configs` shorthand parsedPartitioningTest into Subzones.
	subzones []zonepb.Subzone

	// generatedSpans are the `generatedSpans` with @primary replaced with
	// the actual primary key name.
	generatedSpans []string
}

func (pt partitioningTest) parse() (parsed parsedPartitioningTest, _ error) {

	parsed.tableName = tree.NameStringP(&pt.name)
	parsed.createStmt = fmt.Sprintf(pt.schema, parsed.tableName)

	{
		ctx := context.Background()
		semaCtx := tree.MakeSemaContext(nil /* resolver */)
		stmt, err := parser.ParseOne(parsed.createStmt)
		if err != nil {
			return parsed, errors.Wrapf(err, `parsing %s`, parsed.createStmt)
		}
		createTable, ok := stmt.AST.(*tree.CreateTable)
		if !ok {
			return parsed, errors.Errorf("expected *tree.CreateTable got %T", stmt)
		}
		st := cluster.MakeTestingClusterSettings()
		parentID, tableID := descpb.ID(bootstrap.TestingUserDescID(0)), descpb.ID(bootstrap.TestingUserDescID(1))
		mutDesc, err := importer.MakeTestingSimpleTableDescriptor(
			ctx, &semaCtx, st, createTable, parentID, keys.PublicSchemaID, tableID, importer.NoFKs, timeutil.Now().UnixNano())
		if err != nil {
			return parsed, err
		}
		parsed.tableDesc = mutDesc
		if err := desctestutils.TestingValidateSelf(parsed.tableDesc); err != nil {
			return parsed, err
		}
	}

	replPK := func(s string) string {
		return strings.ReplaceAll(s, "@primary", fmt.Sprintf("@%s_pkey", parsed.tableDesc.Name))
	}
	parsed.generatedSpans = make([]string, len(pt.generatedSpans))
	for i, gs := range pt.generatedSpans {
		parsed.generatedSpans[i] = replPK(gs)
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
				indexName = fmt.Sprintf("@%s", parsed.tableDesc.Name+"_pkey")
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
		idx, err := catalog.MustFindIndexByName(parsed.tableDesc, indexName[1:])
		if err != nil {
			return parsed, errors.Wrapf(err, "could not find index %s", indexName)
		}
		subzone.IndexID = uint32(idx.GetID())
		if len(constraints) > 0 {
			if subzone.PartitionName == "" {
				fmt.Fprintf(&zoneConfigStmts,
					`ALTER INDEX %s@%s CONFIGURE ZONE USING constraints = '[%s]';`,
					parsed.tableName, idx.GetName(), constraints,
				)
			} else {
				fmt.Fprintf(&zoneConfigStmts,
					`ALTER PARTITION %s OF INDEX %s@%s CONFIGURE ZONE USING constraints = '[%s]';`,
					subzone.PartitionName, parsed.tableName, idx.GetName(), constraints,
				)
			}
		}

		var parsedConstraints zonepb.ConstraintsList
		if err := yaml.UnmarshalStrict([]byte("["+constraints+"]"), &parsedConstraints); err != nil {
			return parsed, errors.Wrapf(err, "parsing constraints: %s", constraints)
		}
		subzone.Config.Constraints = parsedConstraints.Constraints
		subzone.Config.InheritedConstraints = parsedConstraints.Inherited

		parsed.subzones = append(parsed.subzones, subzone)
	}
	parsed.zoneConfigStmts = zoneConfigStmts.String()

	return parsed, nil
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
		},
		{
			name:           `all indexes - shuffled`,
			schema:         `CREATE TABLE %s (a INT PRIMARY KEY, b INT, c INT, INDEX idx1 (b), INDEX idx2 (c))`,
			configs:        []string{`@idx2:+n2`, `@primary`, `@idx1:+n3`},
			generatedSpans: []string{`@primary /1-/2`, `@idx1 /2-/3`, `@idx2 /3-/4`},
		},
		{
			name:           `some indexes`,
			schema:         `CREATE TABLE %s (a INT PRIMARY KEY, b INT, c INT, INDEX idx1 (b), INDEX idx2 (c))`,
			configs:        []string{`@primary`, `@idx2:+n2`},
			generatedSpans: []string{`@primary /1-/2`, `@idx2 /3-/4`},
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
		}
		tests = append(tests, test)
	}
	return tests
}

func TestPrimaryKeyChangeZoneConfigs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	codec := s.ApplicationLayer().Codec()

	// Write a table with some partitions into the database,
	// and change its primary key.
	for _, stmt := range []string{
		`CREATE DATABASE t`,
		`USE t`,
		`CREATE TABLE t (
  x INT PRIMARY KEY,
  y INT NOT NULL,
  z INT,
  w INT,
  INDEX i1 (z),
  INDEX i2 (w),
  FAMILY (x, y, z, w)
)`,
		`ALTER INDEX t@i1 PARTITION BY LIST (z) (
  PARTITION p1 VALUES IN (1)
)`,
		`ALTER INDEX t@i2 PARTITION BY LIST (w) (
  PARTITION p2 VALUES IN (3)
)`,
		`ALTER PARTITION p1 OF INDEX t@i1 CONFIGURE ZONE USING gc.ttlseconds = 15210`,
		`ALTER PARTITION p2 OF INDEX t@i2 CONFIGURE ZONE USING gc.ttlseconds = 15213`,
		`ALTER TABLE t ALTER PRIMARY KEY USING COLUMNS (y)`,
	} {
		if _, err := sqlDB.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}

	// Get the zone config corresponding to the table.
	table := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "t", "t")
	kv, err := kvDB.Get(ctx, config.MakeZoneKey(codec, table.GetID()))
	if err != nil {
		t.Fatal(err)
	}
	var zone zonepb.ZoneConfig
	if err := kv.ValueProto(&zone); err != nil {
		t.Fatal(err)
	}

	// Our subzones should be spans prefixed with dropped copy of i1,
	// dropped copy of i2, new copy of i1, and new copy of i2.
	// These have ID's 2, 3, 6 and 8 respectively.
	expectedSpans := []roachpb.Key{
		table.IndexSpan(codec, 2 /* indexID */).Key,
		table.IndexSpan(codec, 3 /* indexID */).Key,
		table.IndexSpan(codec, 6 /* indexID */).Key,
		table.IndexSpan(codec, 8 /* indexID */).Key,
	}
	if len(zone.SubzoneSpans) != len(expectedSpans) {
		t.Fatalf("expected subzones to have length %d", len(expectedSpans))
	}

	// Subzone spans have the table prefix omitted.
	prefix := codec.TablePrefix(uint32(table.GetID()))
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

func TestGenerateSubzoneSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rng, _ := randutil.NewTestRand()

	partitioningTests := allPartitioningTests(rng)
	for _, test := range partitioningTests {
		if test.generatedSpans == nil {
			// The randomized partition tests don't have generatedSpans, and
			// wouldn't be very interesting to test.
			continue
		}
		t.Run(test.name, func(t *testing.T) {
			parse, err := test.parse()
			if err != nil {
				t.Fatalf("%+v", err)
			}
			spans, err := sql.GenerateSubzoneSpans(keys.SystemSQLCodec, parse.tableDesc, parse.subzones)
			if err != nil {
				t.Fatalf("generating subzone spans: %+v", err)
			}

			var actual []string
			for _, span := range spans {
				subzone := parse.subzones[span.SubzoneIndex]
				idx, err := catalog.MustFindIndexByID(parse.tableDesc, descpb.IndexID(subzone.IndexID))
				if err != nil {
					t.Fatalf("could not find index with ID %d: %+v", subzone.IndexID, err)
				}

				directions := []encoding.Direction{encoding.Ascending /* index ID */}
				for i := 0; i < idx.NumKeyColumns(); i++ {
					cd := idx.GetKeyColumnDirection(i)
					ed, err := catalogkeys.IndexColumnEncodingDirection(cd)
					if err != nil {
						t.Fatal(err)
					}
					directions = append(directions, ed)
				}

				var subzoneShort string
				if len(subzone.PartitionName) > 0 {
					subzoneShort = "." + subzone.PartitionName
				} else {
					subzoneShort = "@" + idx.GetName()
				}

				// Verify that we're always doing the space savings when we can.
				var buf redact.StringBuilder
				var buf2 redact.StringBuilder
				if span.Key.PrefixEnd().Equal(span.EndKey) {
					encoding.PrettyPrintValue(&buf, directions, span.Key, "/")
					encoding.PrettyPrintValue(&buf2, directions, span.EndKey, "/")
					t.Errorf("endKey should be omitted when equal to key.PrefixEnd [%s, %s)",
						buf.String(),
						buf2.String())
				}
				if len(span.EndKey) == 0 {
					span.EndKey = span.Key.PrefixEnd()
				}

				// TODO(dan): Check that spans are sorted.
				encoding.PrettyPrintValue(&buf, directions, span.Key, "/")
				encoding.PrettyPrintValue(&buf2, directions, span.EndKey, "/")

				actual = append(actual, fmt.Sprintf("%s %s-%s", subzoneShort,
					buf.String(),
					buf2.String()))
			}

			if len(actual) != len(parse.generatedSpans) {
				t.Fatalf("got \n    %v\n expected \n    %v", actual, test.generatedSpans)
			}
			for i := range actual {
				if expected := strings.TrimSpace(parse.generatedSpans[i]); actual[i] != expected {
					t.Errorf("%d: got [%s] expected [%s]", i, actual[i], expected)
				}
			}
		})
	}
}

func TestZoneConfigAppliesToTemporaryIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	yamlOverride := "gc: {ttlseconds: 42}"

	errCh := make(chan error)
	startIndexMerge := make(chan interface{})
	atIndexMerge := make(chan interface{})
	var params base.TestServerArgs
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeTempIndexMerge: func() {
				atIndexMerge <- struct{}{}
				<-startIndexMerge
			},
		},
	}

	srv, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(context.Background())

	s := srv.ApplicationLayer()

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	codec := s.Codec()

	if _, err := sqlDB.Exec(`
SET use_declarative_schema_changer='off';
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);`); err != nil {
		t.Fatal(err)
	}

	defaultRow := sqlutils.ZoneRow{
		ID:     keys.RootNamespaceID,
		Config: s.DefaultZoneConfig(),
	}

	tableID := sqlutils.QueryTableID(t, sqlDB, "t", "public", "test")
	zoneOverride := s.DefaultZoneConfig()
	zoneOverride.GC = &zonepb.GCPolicy{TTLSeconds: 42}

	overrideRow := sqlutils.ZoneRow{
		ID:     tableID,
		Config: zoneOverride,
	}

	sqlutils.RemoveAllZoneConfigs(t, tdb)

	// Ensure the default is reported for all zones at first.
	sqlutils.VerifyAllZoneConfigs(t, tdb, defaultRow)

	go func() {
		_, err := sqlDB.Exec(`
CREATE INDEX idx ON t.test (v) PARTITION BY LIST (v) (
PARTITION p0 VALUES IN (1),
PARTITION p1 VALUES IN (DEFAULT));`)
		errCh <- err
	}()

	<-atIndexMerge

	// Find the temporary index corresponding to the new index.
	tbl := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "t", "test")
	newIndex, err := catalog.MustFindIndexByName(tbl, "idx")
	if err != nil {
		t.Fatal(err)
	}
	tempIndex := catalog.FindCorrespondingTemporaryIndexByID(tbl, newIndex.GetID())
	require.NotNil(t, tempIndex)

	// Test that partition targeting changes the zone config for both the new
	// index and temp index.
	tdb.Exec(t, fmt.Sprintf("ALTER %s CONFIGURE ZONE = %s", "PARTITION p0 OF TABLE t.test", lexbase.EscapeSQLString(yamlOverride)))

	sqlutils.VerifyZoneConfigForTarget(t, tdb, "PARTITION p0 OF INDEX t.test@idx", overrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, tdb, fmt.Sprintf("PARTITION p0 OF INDEX t.test@%s", tempIndex.GetName()), overrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, tdb, "PARTITION p1 OF INDEX t.test@idx", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, tdb, fmt.Sprintf("PARTITION p1 OF INDEX t.test@%s", tempIndex.GetName()), defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, tdb, "TABLE t.test", defaultRow)

	// Test that index targeting changes the zone config for both the new index
	// and the temp index.
	tdb.Exec(t, fmt.Sprintf("ALTER %s CONFIGURE ZONE = %s", "INDEX t.test@idx", lexbase.EscapeSQLString(yamlOverride)))

	sqlutils.VerifyZoneConfigForTarget(t, tdb, "PARTITION p0 OF INDEX t.test@idx", overrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, tdb, fmt.Sprintf("PARTITION p0 OF INDEX t.test@%s", tempIndex.GetName()), overrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, tdb, "PARTITION p1 OF INDEX t.test@idx", overrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, tdb, fmt.Sprintf("PARTITION p1 OF INDEX t.test@%s", tempIndex.GetName()), overrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, tdb, "TABLE t.test", defaultRow)

	startIndexMerge <- struct{}{}

	err = <-errCh
	if err != nil {
		t.Fatal(err)
	}
}
