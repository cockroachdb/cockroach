// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestValidIndexPartitionSetShowZones(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `
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
	p1Row := sqlutils.ZoneRow{
		ID:           keys.MaxReservedDescID + 2,
		CLISpecifier: "d.t.p1",
		Config:       zoneOverride,
	}

	// Ensure the default is reported for all zones at first.
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE default", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p0", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p1", defaultRow)

	// Ensure a database zone config applies to that database, its tables, and its
	// tables' indices and partitions.
	sqlutils.SetZoneConfig(t, sqlDB, "DATABASE d", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p0", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p1", dbRow)

	// Ensure a table zone config applies to that table and its indices and
	// partitions, but no other zones.
	sqlutils.SetZoneConfig(t, sqlDB, "TABLE d.t", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, dbRow, tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p0", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p1", tableRow)

	// Ensure an index zone config applies to that index and its partitions, but
	// no other zones.
	sqlutils.SetZoneConfig(t, sqlDB, "INDEX d.t@primary", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, dbRow, tableRow, primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p0", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p1", primaryRow)

	// Ensure a partition zone config applies to that partition, but no other
	// zones.
	sqlutils.SetZoneConfig(t, sqlDB, "TABLE d.t PARTITION p0", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, dbRow, tableRow, primaryRow, p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p0", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p1", primaryRow)

	// Ensure updating the default zone propagates to zones without an override,
	// but not to those with overrides.
	sqlutils.SetZoneConfig(t, sqlDB, "RANGE default", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, dbRow, tableRow, primaryRow, p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p0", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p1", primaryRow)

	// Ensure deleting a database zone leaves child overrides in place.
	sqlutils.DeleteZoneConfig(t, sqlDB, "DATABASE d")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, tableRow, primaryRow, p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p0", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p1", primaryRow)

	// Ensure deleting a table zone leaves child overrides in place.
	sqlutils.DeleteZoneConfig(t, sqlDB, "TABLE d.t")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, primaryRow, p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p0", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p1", primaryRow)

	// Ensure deleting an index zone leaves child overrides in place.
	sqlutils.DeleteZoneConfig(t, sqlDB, "INDEX d.t@primary")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p0", p0Row)

	// Ensure deleting a partition zone works.
	sqlutils.DeleteZoneConfig(t, sqlDB, "TABLE d.t PARTITION p0")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p0", defaultOverrideRow)

	// Ensure deleting non-overridden zones is not an error.
	sqlutils.DeleteZoneConfig(t, sqlDB, "DATABASE d")
	sqlutils.DeleteZoneConfig(t, sqlDB, "TABLE d.t")
	sqlutils.DeleteZoneConfig(t, sqlDB, "TABLE d.t PARTITION p1")

	// Ensure updating the default zone config applies to zones that have had
	// overrides added and removed.
	sqlutils.SetZoneConfig(t, sqlDB, "RANGE default", yamlDefault)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE default", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p0", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p1", defaultRow)

	// Ensure subzones can be created even when no table zone exists.
	sqlutils.SetZoneConfig(t, sqlDB, "TABLE d.t PARTITION p0", yamlOverride)
	sqlutils.SetZoneConfig(t, sqlDB, "TABLE d.t PARTITION p1", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, p0Row, p1Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p0", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t PARTITION p1", p1Row)

	// Ensure the shorthand index syntax works.
	sqlutils.SetZoneConfig(t, sqlDB, `INDEX "primary"`, yamlOverride)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, `INDEX "primary"`, primaryRow)
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
	rng, _ := randutil.NewPseudoRand()

	prettyTrimmedKey := func(key []byte) string {
		untrimmedKey := append(keys.MakeTablePrefix(0), key...)
		return strings.TrimPrefix(roachpb.Key(untrimmedKey).String(), "/Table/0")
	}

	partitioningTests := allPartitioningTests(rng)
	for _, test := range partitioningTests {
		if test.generatedSpans == nil {
			// The randomized partition tests don't have generatedSpans, and
			// wouldn't be very interesting to test.
			continue
		}
		t.Run(test.name, func(t *testing.T) {
			if err := test.parse(); err != nil {
				t.Fatalf("%+v", err)
			}
			spans, err := GenerateSubzoneSpans(test.parsed.tableDesc, test.parsed.subzones)
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
				if subzone := test.parsed.subzones[span.SubzoneIndex]; len(subzone.PartitionName) > 0 {
					subzoneShort = "." + subzone.PartitionName
				} else {
					idxDesc, err := test.parsed.tableDesc.FindIndexByID(sqlbase.IndexID(subzone.IndexID))
					if err != nil {
						t.Fatalf("could not find index with ID %d: %+v", subzone.IndexID, err)
					}
					subzoneShort = "@" + idxDesc.Name
				}
				actual = append(actual, fmt.Sprintf("%s %s-%s", subzoneShort,
					prettyTrimmedKey(span.Key), prettyTrimmedKey(span.EndKey),
				))
			}

			if len(actual) != len(test.generatedSpans) {
				t.Fatalf("got \n    %v\n expected \n    %v", actual, test.generatedSpans)
			}
			for i := range actual {
				if expected := strings.TrimSpace(test.generatedSpans[i]); actual[i] != expected {
					t.Errorf("%d: got [%s] expected [%s]", i, actual[i], expected)
				}
			}
		})
	}
}
