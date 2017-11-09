// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package sqlccl

import (
	"fmt"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestValidIndexPartitionSetShowZones(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	sqlDB := sqlutils.MakeSQLRunner(t, db)
	sqlDB.Exec(`
		CREATE DATABASE d;
		USE d;
		CREATE TABLE t (c STRING PRIMARY KEY) PARTITION BY LIST (c) (
			PARTITION p0 VALUES IN ('a'),
			PARTITION p1 VALUES IN (DEFAULT)
		)`)

	yamlDefault := fmt.Sprintf("gc: {ttlseconds: %d}", config.DefaultZoneConfig().GC.TTLSeconds)
	yamlOverride := "gc: {ttlseconds: 42}"
	zoneOverride := config.DefaultZoneConfig()
	zoneOverride.GC.TTLSeconds = 42

	defaultRow := sqlutils.ZoneRow{
		ID:           keys.RootNamespaceID,
		CLISpecifier: ".default",
		Config:       config.DefaultZoneConfig(),
	}
	defaultOverrideRow := sqlutils.ZoneRow{
		ID:           keys.RootNamespaceID,
		CLISpecifier: ".default",
		Config:       zoneOverride,
	}
	dbRow := sqlutils.ZoneRow{
		ID:           keys.MaxReservedDescID + 1,
		CLISpecifier: "d",
		Config:       zoneOverride,
	}
	tableRow := sqlutils.ZoneRow{
		ID:           keys.MaxReservedDescID + 2,
		CLISpecifier: "d.t",
		Config:       zoneOverride,
	}
	primaryRow := sqlutils.ZoneRow{
		ID:           keys.MaxReservedDescID + 2,
		CLISpecifier: "d.t@primary",
		Config:       zoneOverride,
	}
	p0Row := sqlutils.ZoneRow{
		ID:           keys.MaxReservedDescID + 2,
		CLISpecifier: "d.t.p0",
		Config:       zoneOverride,
	}

	// Ensure the default is reported for all zones at first.
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "RANGE default", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE d", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "INDEX d.t@primary", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p0", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p1", defaultRow)

	// Ensure a database zone config applies to that database, its tables, and its
	// tables' indices and partitions.
	sqlutils.SetZoneConfig(sqlDB, "DATABASE d", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultRow, dbRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t", dbRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "INDEX d.t@primary", dbRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p0", dbRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p1", dbRow)

	// Ensure a table zone config applies to that table and its indices and
	// partitions, but no other zones.
	sqlutils.SetZoneConfig(sqlDB, "TABLE d.t", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultRow, dbRow, tableRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "INDEX d.t@primary", tableRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p0", tableRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p1", tableRow)

	// Ensure an index zone config applies to that index and its partitions, but
	// no other zones.
	sqlutils.SetZoneConfig(sqlDB, "INDEX d.t@primary", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultRow, dbRow, tableRow, primaryRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p0", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p1", primaryRow)

	// Ensure a partition zone config applies to that partition, but no other
	// zones.
	sqlutils.SetZoneConfig(sqlDB, "TABLE d.t PARTITION p0", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultRow, dbRow, tableRow, primaryRow, p0Row)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p0", p0Row)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p1", primaryRow)

	// Ensure updating the default zone propagates to zones without an override,
	// but not to those with overrides.
	sqlutils.SetZoneConfig(sqlDB, "RANGE default", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultOverrideRow, dbRow, tableRow, primaryRow, p0Row)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p0", p0Row)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p1", primaryRow)

	// Ensure deleting a database zone leaves child overrides in place.
	sqlutils.DeleteZoneConfig(sqlDB, "DATABASE d")
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultOverrideRow, tableRow, primaryRow, p0Row)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE d", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p0", p0Row)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p1", primaryRow)

	// Ensure deleting a table zone leaves child overrides in place.
	sqlutils.DeleteZoneConfig(sqlDB, "TABLE d.t")
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultOverrideRow, primaryRow, p0Row)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p0", p0Row)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p1", primaryRow)

	// Ensure deleting an index zone leaves child overrides in place.
	sqlutils.DeleteZoneConfig(sqlDB, "INDEX d.t@primary")
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultOverrideRow, p0Row)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "INDEX d.t@primary", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p0", p0Row)

	// Ensure deleting a partition zone works.
	sqlutils.DeleteZoneConfig(sqlDB, "TABLE d.t PARTITION p0")
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p0", defaultOverrideRow)

	// Ensure deleting non-overridden zones is not an error.
	sqlutils.DeleteZoneConfig(sqlDB, "DATABASE d")
	sqlutils.DeleteZoneConfig(sqlDB, "TABLE d.t")
	sqlutils.DeleteZoneConfig(sqlDB, "TABLE d.t PARTITION p1")

	// Ensure updating the default zone config applies to zones that have had
	// overrides added and removed.
	sqlutils.SetZoneConfig(sqlDB, "RANGE default", yamlDefault)
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "RANGE default", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE d", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "INDEX d.t@primary", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p0", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t PARTITION p1", defaultRow)

	// Ensure the shorthand index syntax works.
	sqlutils.SetZoneConfig(sqlDB, `INDEX "primary"`, yamlOverride)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, `INDEX "primary"`, primaryRow)
}

func TestInvalidIndexPartitionSetShowZones(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	for i, tc := range []struct {
		query string
		err   string
	}{
		{
			"ALTER INDEX foo EXPERIMENTAL CONFIGURE ZONE ''",
			`no database specified: "foo"`,
		},
		{
			"EXPERIMENTAL SHOW ZONE CONFIGURATION FOR INDEX foo",
			`no database specified: "foo"`,
		},
		{
			"USE system; ALTER INDEX foo EXPERIMENTAL CONFIGURE ZONE ''",
			`index "foo" not in any of the tables`,
		},
		{
			"USE system; EXPERIMENTAL SHOW ZONE CONFIGURATION FOR INDEX foo",
			`index "foo" not in any of the tables`,
		},
		{
			"ALTER TABLE system.jobs PARTITION p0 EXPERIMENTAL CONFIGURE ZONE 'foo'",
			`partition "p0" does not exist`,
		},
		{
			"EXPERIMENTAL SHOW ZONE CONFIGURATION FOR TABLE system.jobs PARTITION p0",
			`partition "p0" does not exist`,
		},
	} {
		if _, err := db.Exec(tc.query); !testutils.IsError(err, tc.err) {
			t.Errorf("#%d: expected error matching %q, but got %v", i, tc.err, err)
		}
	}
}

func TestGenerateSubzoneSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	prettyTrimmedKey := func(key []byte) string {
		untrimmedKey := append(keys.MakeTablePrefix(0), key...)
		return strings.TrimPrefix(roachpb.Key(untrimmedKey).String(), "/Table/0")
	}

	tests := []struct {
		name   string
		schema string

		// subzones are 1:1 to the `subzones` input of GenerateSubzoneSpans,
		// each formatted as `@index_name` or `.partition_name`
		subzones []string

		// expected are 1:1 to the output of GenerateSubzoneSpans, each
		// formatted as `{subzone} {start}-{end}` (e.g. `@primary /1-/2`), where
		// {subzone} is formatted identically to the test shorthand above, and
		// {start} and {end} are formatted using our key pretty printer, but
		// with the table removed. The end key is always specified in tests
		// (though GenerateSubzoneSpans may omit it to save space).
		expected []string
	}{
		{
			name:   `no subzones`,
			schema: `CREATE TABLE t (a INT PRIMARY KEY)`,
		},

		{
			name:     `all indexes`,
			schema:   `CREATE TABLE t (a INT PRIMARY KEY, b INT, INDEX idx1 (b), INDEX idx2 (b))`,
			subzones: []string{`@primary`, `@idx1`, `@idx2`},
			expected: []string{`@primary /1-/2`, `@idx1 /2-/3`, `@idx2 /3-/4`},
		},
		{
			name:     `all indexes - shuffled`,
			schema:   `CREATE TABLE t (a INT PRIMARY KEY, b INT, INDEX idx1 (b), INDEX idx2 (b))`,
			subzones: []string{`@idx2`, `@primary`, `@idx1`},
			expected: []string{`@primary /1-/2`, `@idx1 /2-/3`, `@idx2 /3-/4`},
		},
		{
			name:     `some indexes`,
			schema:   `CREATE TABLE t (a INT PRIMARY KEY, b INT, INDEX idx1 (b), INDEX idx2 (b))`,
			subzones: []string{`@primary`, `@idx2`},
			expected: []string{`@primary /1-/2`, `@idx2 /3-/4`},
		},

		{
			name: `single col list partitioning`,
			schema: `CREATE TABLE t (a INT PRIMARY KEY) PARTITION BY LIST (a) (
				PARTITION p3 VALUES IN (3),
				PARTITION p4 VALUES IN (4)
			)`,
			subzones: []string{`@primary`, `.p3`, `.p4`},
			expected: []string{
				`@primary /1-/1/3`,
				`     .p3 /1/3-/1/4`,
				`     .p4 /1/4-/1/5`,
				`@primary /1/5-/2`,
			},
		},
		{
			name: `single col list partitioning - DEFAULT`,
			schema: `CREATE TABLE t (a INT PRIMARY KEY) PARTITION BY LIST (a) (
				PARTITION p3 VALUES IN (3),
				PARTITION p4 VALUES IN (4),
				PARTITION pd VALUES IN (DEFAULT)
			)`,
			subzones: []string{`@primary`, `.p3`, `.p4`, `.pd`},
			expected: []string{
				`.pd /1-/1/3`,
				`.p3 /1/3-/1/4`,
				`.p4 /1/4-/1/5`,
				`.pd /1/5-/2`,
			},
		},
		{
			name: `multi col list partitioning`,
			schema: `CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a, b) (
				PARTITION p34 VALUES IN ((3, 4)),
				PARTITION p56 VALUES IN ((5, 6)),
				PARTITION p57 VALUES IN ((5, 7))
			)`,
			subzones: []string{`@primary`, `.p34`, `.p56`, `.p57`},
			expected: []string{
				`@primary /1-/1/3/4`,
				`    .p34 /1/3/4-/1/3/5`,
				`@primary /1/3/5-/1/5/6`,
				`    .p56 /1/5/6-/1/5/7`,
				`    .p57 /1/5/7-/1/5/8`,
				`@primary /1/5/8-/2`,
			},
		},
		{
			name: `multi col list partitioning - DEFAULT`,
			schema: `CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a, b) (
				PARTITION p34 VALUES IN ((3, 4)),
				PARTITION p56 VALUES IN ((5, 6)),
				PARTITION p57 VALUES IN ((5, 7)),
				PARTITION p5d VALUES IN ((5, DEFAULT)),
				PARTITION pd VALUES IN ((DEFAULT, DEFAULT))
			)`,
			subzones: []string{`@primary`, `.p34`, `.p56`, `.p57`, `.p5d`, `.pd`},
			expected: []string{
				` .pd /1-/1/3/4`,
				`.p34 /1/3/4-/1/3/5`,
				` .pd /1/3/5-/1/5`,
				`.p5d /1/5-/1/5/6`,
				`.p56 /1/5/6-/1/5/7`,
				`.p57 /1/5/7-/1/5/8`,
				`.p5d /1/5/8-/1/6`,
				` .pd /1/6-/2`,
			},
		},

		{
			name: `single col range partitioning`,
			schema: `CREATE TABLE t (a INT PRIMARY KEY) PARTITION BY RANGE (a) (
				PARTITION p3 VALUES < 3,
				PARTITION p4 VALUES < 4
			)`,
			subzones: []string{`@primary`, `.p3`, `.p4`},
			expected: []string{
				`     .p3 /1-/1/3`,
				`     .p4 /1/3-/1/4`,
				`@primary /1/4-/2`,
			},
		},
		{
			name: `single col range partitioning - MAXVALUE`,
			schema: `CREATE TABLE t (a INT PRIMARY KEY) PARTITION BY RANGE (a) (
				PARTITION p3 VALUES < 3,
				PARTITION p4 VALUES < 4,
				PARTITION pm VALUES < MAXVALUE
			)`,
			subzones: []string{`@primary`, `.p3`, `.p4`, `.pm`},
			expected: []string{
				`.p3 /1-/1/3`,
				`.p4 /1/3-/1/4`,
				`.pm /1/4-/2`,
			},
		},
		{
			name: `multi col range partitioning`,
			schema: `CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY RANGE (a, b) (
				PARTITION p34 VALUES < (3, 4),
				PARTITION p56 VALUES < (5, 6),
				PARTITION p57 VALUES < (5, 7)
			)`,
			subzones: []string{`@primary`, `.p34`, `.p56`, `.p57`},
			expected: []string{
				`    .p34 /1-/1/3/4`,
				`    .p56 /1/3/4-/1/5/6`,
				`    .p57 /1/5/6-/1/5/7`,
				`@primary /1/5/7-/2`,
			},
		},
		{
			name: `multi col range partitioning - MAXVALUE`,
			schema: `CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY RANGE (a, b) (
				PARTITION p34 VALUES < (3, 4),
				PARTITION p3m VALUES < (3, MAXVALUE),
				PARTITION p56 VALUES < (5, 6),
				PARTITION p57 VALUES < (5, 7),
				PARTITION pm VALUES < (MAXVALUE, MAXVALUE)
			)`,
			subzones: []string{`@primary`, `.p34`, `.p3m`, `.p56`, `.p57`, `.pm`},
			expected: []string{
				`.p34 /1-/1/3/4`,
				`.p3m /1/3/4-/1/4`,
				`.p56 /1/4-/1/5/6`,
				`.p57 /1/5/6-/1/5/7`,
				` .pm /1/5/7-/2`,
			},
		},

		{
			name: `list-list partitioning`,
			schema: `CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a) (
				PARTITION p3 VALUES IN (3) PARTITION BY LIST (b) (
					PARTITION p34 VALUES IN (4)
				),
				PARTITION p5 VALUES IN (5) PARTITION BY LIST (b) (
					PARTITION p56 VALUES IN (6),
					PARTITION p5d VALUES IN (DEFAULT)
				),
				PARTITION pd VALUES IN (DEFAULT)
			)`,
			subzones: []string{`@primary`, `.p3`, `.p34`, `.p5`, `.p56`, `.p5d`, `.pd`},
			expected: []string{
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
			schema: `CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a) (
				PARTITION p3 VALUES IN (3) PARTITION BY RANGE (b) (
					PARTITION p34 VALUES < 4
				),
				PARTITION p5 VALUES IN (5) PARTITION BY RANGE (b) (
					PARTITION p56 VALUES < 6,
					PARTITION p5d VALUES < MAXVALUE
				),
				PARTITION pd VALUES IN (DEFAULT)
			)`,
			subzones: []string{`@primary`, `.p3`, `.p34`, `.p5`, `.p56`, `.p5d`, `.pd`},
			expected: []string{
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
			schema: `CREATE TABLE t (a INT PRIMARY KEY) PARTITION BY LIST (a) (
				PARTITION pd VALUES IN (DEFAULT)
			)`,
			subzones: []string{`@primary`},
			expected: []string{`@primary /1-/2`},
		},
		{
			name: `inheritance - single col default`,
			schema: `CREATE TABLE t (a INT PRIMARY KEY) PARTITION BY LIST (a) (
				PARTITION p3 VALUES IN (3),
				PARTITION pd VALUES IN (DEFAULT)
			)`,
			subzones: []string{`@primary`, `.pd`},
			expected: []string{`.pd /1-/2`},
		},
		{
			name: `inheritance - multi col default`,
			schema: `CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a, b) (
				PARTITION p34 VALUES IN ((3, 4)),
				PARTITION p3d VALUES IN ((3, DEFAULT)),
				PARTITION p56 VALUES IN ((5, 6)),
				PARTITION p5d VALUES IN ((5, DEFAULT))
			)`,
			subzones: []string{`@primary`, `.p3d`, `.p56`},
			expected: []string{
				`@primary /1-/1/3`,
				`    .p3d /1/3-/1/4`,
				`@primary /1/4-/1/5/6`,
				`    .p56 /1/5/6-/1/5/7`,
				`@primary /1/5/7-/2`,
			},
		},
		{
			name: `inheritance - subpartitioning`,
			schema: `CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b)) PARTITION BY LIST (a) (
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
			subzones: []string{`@primary`, `.p3d`, `.p56`, `.p7`},
			expected: []string{
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

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()

			var tableDesc *sqlbase.TableDescriptor
			{
				stmt, err := parser.ParseOne(test.schema)
				if err != nil {
					t.Fatalf("%+v", err)
				}
				createTable, ok := stmt.(*parser.CreateTable)
				if !ok {
					t.Fatalf("expected *parser.CreateTable got %T", stmt)
				}
				const parentID, tableID = keys.MaxReservedDescID + 1, keys.MaxReservedDescID + 2
				tableDesc, err = makeCSVTableDescriptor(
					ctx, createTable, parentID, tableID, hlc.UnixNano())
				if err != nil {
					t.Fatalf("%+v", err)
				}
				if err := tableDesc.ValidateTable(); err != nil {
					t.Fatalf("%+v", err)
				}
			}

			var subzones []config.Subzone
			for _, subzoneShort := range test.subzones {
				if strings.HasPrefix(subzoneShort, "@") {
					idxDesc, _, err := tableDesc.FindIndexByName(subzoneShort[1:])
					if err != nil {
						log.Info(ctx, tableDesc)
						t.Fatalf("could not find index %s: %+v", subzoneShort, err)
					}
					subzones = append(subzones, config.Subzone{IndexID: uint32(idxDesc.ID)})
				} else if strings.HasPrefix(subzoneShort, ".") {
					// TODO(dan): decide if config.Subzone needs to have IndexID
					// set when PartitionName is non-empty. The proto comment
					// doesn't specify.
					subzones = append(subzones, config.Subzone{PartitionName: subzoneShort[1:]})
				}
			}
			spans, err := GenerateSubzoneSpans(tableDesc, subzones)
			if err != nil {
				t.Fatalf("generating subzone spans: %+v", err)
			}

			var actual []string
			for _, span := range spans {
				// Verify that we're always doing the space savings when we can.
				if span.Key.PrefixEnd().Equal(span.EndKey) {
					t.Errorf("endKey should be omitted when equal to key.PrefixEnd [%s, %s)",
						prettyTrimmedKey(span.Key), prettyTrimmedKey(span.EndKey))
				}
				if len(span.EndKey) == 0 {
					span.EndKey = span.Key.PrefixEnd()
				}

				// TODO(dan): Check that spans are sorted.

				var subzoneShort string
				if subzone := subzones[span.SubzoneIndex]; len(subzone.PartitionName) > 0 {
					subzoneShort = "." + subzone.PartitionName
				} else {
					idxDesc, err := tableDesc.FindIndexByID(sqlbase.IndexID(subzone.IndexID))
					if err != nil {
						t.Fatalf("could not find index with ID %d: %+v", subzone.IndexID, err)
					}
					subzoneShort = "@" + idxDesc.Name
				}
				actual = append(actual, fmt.Sprintf("%s %s-%s", subzoneShort,
					prettyTrimmedKey(span.Key), prettyTrimmedKey(span.EndKey),
				))
			}

			if len(actual) != len(test.expected) {
				t.Fatalf("got \n    %v\n expected \n    %v", actual, test.expected)
			}
			for i := range actual {
				if expected := strings.TrimSpace(test.expected[i]); actual[i] != expected {
					t.Errorf("%d: got [%s] expected [%s]", i, actual[i], expected)
				}
			}
		})
	}
}
