// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package partitionccl

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestValidIndexPartitionSetShowZones(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `
		CREATE DATABASE d;
		USE d;
		CREATE TABLE t (c STRING PRIMARY KEY) PARTITION BY LIST (c) (
			PARTITION p0 VALUES IN ('a'),
			PARTITION p1 VALUES IN (DEFAULT)
		)`)

	yamlDefault := fmt.Sprintf("gc: {ttlseconds: %d}", s.(*server.TestServer).Cfg.DefaultZoneConfig.GC.TTLSeconds)
	yamlOverride := "gc: {ttlseconds: 42}"
	zoneOverride := s.(*server.TestServer).Cfg.DefaultZoneConfig
	zoneOverride.GC = &zonepb.GCPolicy{TTLSeconds: 42}
	partialZoneOverride := *zonepb.NewZoneConfig()
	partialZoneOverride.GC = &zonepb.GCPolicy{TTLSeconds: 42}

	dbID := sqlutils.QueryDatabaseID(t, db, "d")
	tableID := sqlutils.QueryTableID(t, db, "d", "public", "t")

	defaultRow := sqlutils.ZoneRow{
		ID:     keys.RootNamespaceID,
		Config: s.(*server.TestServer).Cfg.DefaultZoneConfig,
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
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", defaultRow)

	// Ensure a database zone config applies to that database, its tables, and its
	// tables' indices and partitions.
	sqlutils.SetZoneConfig(t, sqlDB, "DATABASE d", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialDbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", dbRow)

	// Ensure a table zone config applies to that table and its indices and
	// partitions, but no other zones.
	sqlutils.SetZoneConfig(t, sqlDB, "TABLE d.t", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialDbRow, partialTableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", tableRow)

	// Ensure an index zone config applies to that index and its partitions, but
	// no other zones.
	sqlutils.SetZoneConfig(t, sqlDB, "INDEX d.t@primary", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialDbRow, partialTableRow, partialPrimaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", primaryRow)

	// Ensure a partition zone config applies to that partition, but no other
	// zones.
	sqlutils.SetZoneConfig(t, sqlDB, "PARTITION p0 OF TABLE d.t", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialDbRow, partialTableRow, partialPrimaryRow, partialP0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", primaryRow)

	// Ensure updating the default zone propagates to zones without an override,
	// but not to those with overrides.
	sqlutils.SetZoneConfig(t, sqlDB, "RANGE default", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, partialDbRow, partialTableRow, partialPrimaryRow, partialP0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", primaryRow)

	// Ensure deleting a database zone leaves child overrides in place.
	sqlutils.DeleteZoneConfig(t, sqlDB, "DATABASE d")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, partialTableRow, partialPrimaryRow, partialP0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", primaryRow)

	// Ensure deleting a table zone leaves child overrides in place.
	sqlutils.DeleteZoneConfig(t, sqlDB, "TABLE d.t")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, partialPrimaryRow, partialP0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", primaryRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", primaryRow)

	// Ensure deleting an index zone leaves child overrides in place.
	sqlutils.DeleteZoneConfig(t, sqlDB, "INDEX d.t@primary")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, partialP0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", defaultOverrideRow)
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
	sqlutils.SetZoneConfig(t, sqlDB, "RANGE default", yamlDefault)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE default", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "INDEX d.t@primary", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", defaultRow)

	// Ensure subzones can be created even when no table zone exists.
	sqlutils.SetZoneConfig(t, sqlDB, "PARTITION p0 OF TABLE d.t", yamlOverride)
	sqlutils.SetZoneConfig(t, sqlDB, "PARTITION p1 OF TABLE d.t", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialP0Row, partialP1Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE d.t", p0Row)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p1 OF TABLE d.t", p1Row)

	// Ensure the shorthand index syntax works.
	sqlutils.SetZoneConfig(t, sqlDB, `INDEX "primary"`, yamlOverride)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, `INDEX "primary"`, primaryRow)

	// Ensure the session database is respected.
	sqlutils.SetZoneConfig(t, sqlDB, "PARTITION p0 OF TABLE t", yamlOverride)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "PARTITION p0 OF TABLE t", p0Row)
}

func TestInvalidIndexPartitionSetShowZones(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
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

func TestGenerateSubzoneSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rng, _ := randutil.NewPseudoRand()

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
			clusterID := uuid.MakeV4()
			hasNewSubzones := false
			spans, err := sql.GenerateSubzoneSpans(
				cluster.NoSettings, clusterID, keys.SystemSQLCodec, test.parsed.tableDesc, test.parsed.subzones, hasNewSubzones)
			if err != nil {
				t.Fatalf("generating subzone spans: %+v", err)
			}

			var actual []string
			for _, span := range spans {
				subzone := test.parsed.subzones[span.SubzoneIndex]
				idx, err := test.parsed.tableDesc.FindIndexWithID(descpb.IndexID(subzone.IndexID))
				if err != nil {
					t.Fatalf("could not find index with ID %d: %+v", subzone.IndexID, err)
				}

				directions := []encoding.Direction{encoding.Ascending /* index ID */}
				for i := 0; i < idx.NumKeyColumns(); i++ {
					cd := idx.GetKeyColumnDirection(i)
					ed, err := cd.ToEncodingDirection()
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
				if span.Key.PrefixEnd().Equal(span.EndKey) {
					t.Errorf("endKey should be omitted when equal to key.PrefixEnd [%s, %s)",
						encoding.PrettyPrintValue(directions, span.Key, "/"),
						encoding.PrettyPrintValue(directions, span.EndKey, "/"))
				}
				if len(span.EndKey) == 0 {
					span.EndKey = span.Key.PrefixEnd()
				}

				// TODO(dan): Check that spans are sorted.

				actual = append(actual, fmt.Sprintf("%s %s-%s", subzoneShort,
					encoding.PrettyPrintValue(directions, span.Key, "/"),
					encoding.PrettyPrintValue(directions, span.EndKey, "/")))
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

// TestGetHydratedZoneConfigForTable ensures that GetHydratedZoneConfigForTable
// correctly hydrates zone/subzone configurations. It constructs an inheritance
// hierarchy that includes secondary indexes/partitions and alters zone
// configurations of different objects in the inheritance hierarchy before
// making assertions.
//
// It's worth calling out that this test doesn't test SubzoneSpans -- it only
// ensures that Subzones are hydrated correctly. This is because the
// SubzoneSpans <-> Subzone mapping is already tested above and it's cumbersome
// to hardcode the span keys in this test.
func TestGetHydratedZoneConfigForTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numServers = 3
	serverArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		serverArgs[i] = base.TestServerArgs{
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "region", Value: fmt.Sprintf("region_%d", i+1)}},
			},
		}
	}
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgsPerNode: serverArgs,
	})

	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)
	sqlDB := tc.ServerConn(0)

	// Setup the test with:
	//      db (ID 52)
	//    / 	 \
	//   t1 (ID 53)
	//    |      \
	//   idx     t2 (ID 54) ------ partition
	//           |
	//          idx2
	// 	         |
	// 		    partition
	_, err := sqlDB.Exec("CREATE DATABASE db")
	require.NoError(t, err)
	_, err = sqlDB.Exec("CREATE TABLE db.t1(i INT)")
	require.NoError(t, err)
	_, err = sqlDB.Exec("CREATE INDEX idx1 ON db.t1(i)")
	require.NoError(t, err)
	_, err = sqlDB.Exec(`CREATE TABLE db.t2(i INT PRIMARY KEY, j INT) PARTITION BY LIST (i) (
PARTITION pi0 VALUES IN (1), 
PARTITION pi1 VALUES IN (DEFAULT))`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`CREATE INDEX idx2 ON db.t2(j) PARTITION BY LIST (j) (
PARTITION pj0 VALUES IN (10),
PARTITION pj1 VALUES IN (DEFAULT))`)
	require.NoError(t, err)

	type expectation struct {
		id   descpb.ID
		zone zonepb.ZoneConfig
	}

	defaultZoneConfig := zonepb.DefaultZoneConfig()
	// We don't test subzone spans in the expectations here.
	testCases := []struct {
		setup        []string
		expectations []expectation
	}{
		{
			// Setting the zone configuration on the database should cascade down
			// to both the tables.
			setup: []string{
				"ALTER DATABASE db CONFIGURE ZONE USING num_replicas = 10",
			},
			expectations: []expectation{
				{
					id: 53,
					zone: zonepb.ZoneConfig{
						RangeMinBytes:               defaultZoneConfig.RangeMinBytes,
						RangeMaxBytes:               defaultZoneConfig.RangeMaxBytes,
						NumReplicas:                 proto.Int32(10),
						GC:                          defaultZoneConfig.GC,
						NullVoterConstraintsIsEmpty: defaultZoneConfig.NullVoterConstraintsIsEmpty,
					},
				},
				{
					id: 54,
					zone: zonepb.ZoneConfig{
						RangeMinBytes:               defaultZoneConfig.RangeMinBytes,
						RangeMaxBytes:               defaultZoneConfig.RangeMaxBytes,
						NumReplicas:                 proto.Int32(10),
						GC:                          defaultZoneConfig.GC,
						NullVoterConstraintsIsEmpty: defaultZoneConfig.NullVoterConstraintsIsEmpty,
					},
				},
			},
		},
		{
			// Next, set the zone configuration on the index of t1 such that t1 acts
			// as a placeholder.
			setup: []string{
				"ALTER INDEX db.t1@idx1 CONFIGURE ZONE USING gc.ttlseconds=200",
			},
			expectations: []expectation{
				{
					id: 53,
					zone: zonepb.ZoneConfig{
						RangeMinBytes:               defaultZoneConfig.RangeMinBytes,
						RangeMaxBytes:               defaultZoneConfig.RangeMaxBytes,
						NumReplicas:                 proto.Int32(10),
						GC:                          defaultZoneConfig.GC,
						NullVoterConstraintsIsEmpty: defaultZoneConfig.NullVoterConstraintsIsEmpty,
						Subzones: []zonepb.Subzone{
							{
								IndexID: 2,
								Config: zonepb.ZoneConfig{
									RangeMinBytes: defaultZoneConfig.RangeMinBytes,
									RangeMaxBytes: defaultZoneConfig.RangeMaxBytes,
									NumReplicas:   proto.Int32(10),
									GC: &zonepb.GCPolicy{
										TTLSeconds: 200,
									},
									NullVoterConstraintsIsEmpty: defaultZoneConfig.NullVoterConstraintsIsEmpty,
								},
							},
						},
					},
				},
				{
					id: 54,
					zone: zonepb.ZoneConfig{
						RangeMinBytes:               defaultZoneConfig.RangeMinBytes,
						RangeMaxBytes:               defaultZoneConfig.RangeMaxBytes,
						NumReplicas:                 proto.Int32(10),
						GC:                          defaultZoneConfig.GC,
						NullVoterConstraintsIsEmpty: defaultZoneConfig.NullVoterConstraintsIsEmpty,
					},
				},
			},
		},
		{
			// Next, set the zone configuration on t2, t2@idx2, and both the
			// partitions of t2 and t2@idx2.
			setup: []string{
				"ALTER TABLE db.t2 CONFIGURE ZONE USING num_replicas = 12",
				"ALTER PARTITION pi0 OF TABLE db.t2 CONFIGURE ZONE USING num_voters= 6",
				"ALTER PARTITION pi1 OF TABLE db.t2 CONFIGURE ZONE USING num_voters=9",
				"ALTER INDEX db.t2@idx2 CONFIGURE ZONE USING gc.ttlseconds=4200",
				"ALTER PARTITION pj0 OF INDEX db.t2@idx2 CONFIGURE ZONE USING CONSTRAINTS='[+region=region_1]'",
				"ALTER PARTITION pj1 OF INDEX db.t2@idx2 CONFIGURE ZONE USING CONSTRAINTS='[+region=region_2]'",
			},
			expectations: []expectation{
				{
					id: 54,
					zone: zonepb.ZoneConfig{
						RangeMinBytes:               defaultZoneConfig.RangeMinBytes,
						RangeMaxBytes:               defaultZoneConfig.RangeMaxBytes,
						NumReplicas:                 proto.Int32(12),
						GC:                          defaultZoneConfig.GC,
						NullVoterConstraintsIsEmpty: defaultZoneConfig.NullVoterConstraintsIsEmpty,
						Subzones: []zonepb.Subzone{
							{
								IndexID:       1,
								PartitionName: "pi0",
								Config: zonepb.ZoneConfig{
									RangeMinBytes:               defaultZoneConfig.RangeMinBytes,
									RangeMaxBytes:               defaultZoneConfig.RangeMaxBytes,
									NumReplicas:                 proto.Int32(12),
									NumVoters:                   proto.Int32(6),
									GC:                          defaultZoneConfig.GC,
									NullVoterConstraintsIsEmpty: defaultZoneConfig.NullVoterConstraintsIsEmpty,
								},
							},
							{
								IndexID:       1,
								PartitionName: "pi1",
								Config: zonepb.ZoneConfig{
									RangeMinBytes:               defaultZoneConfig.RangeMinBytes,
									RangeMaxBytes:               defaultZoneConfig.RangeMaxBytes,
									NumReplicas:                 proto.Int32(12),
									NumVoters:                   proto.Int32(9),
									GC:                          defaultZoneConfig.GC,
									NullVoterConstraintsIsEmpty: defaultZoneConfig.NullVoterConstraintsIsEmpty,
								},
							},
							{
								IndexID:       2,
								PartitionName: "", // On the entire secondary index.
								Config: zonepb.ZoneConfig{
									RangeMinBytes:               defaultZoneConfig.RangeMinBytes,
									RangeMaxBytes:               defaultZoneConfig.RangeMaxBytes,
									NumReplicas:                 proto.Int32(12),
									GC:                          &zonepb.GCPolicy{TTLSeconds: 4200},
									NullVoterConstraintsIsEmpty: defaultZoneConfig.NullVoterConstraintsIsEmpty,
								},
							},
							{
								IndexID:       2,
								PartitionName: "pj0",
								Config: zonepb.ZoneConfig{
									RangeMinBytes:               defaultZoneConfig.RangeMinBytes,
									RangeMaxBytes:               defaultZoneConfig.RangeMaxBytes,
									NumReplicas:                 proto.Int32(12),
									GC:                          &zonepb.GCPolicy{TTLSeconds: 4200},
									NullVoterConstraintsIsEmpty: defaultZoneConfig.NullVoterConstraintsIsEmpty,
									Constraints: []zonepb.ConstraintsConjunction{
										{
											Constraints: []zonepb.Constraint{
												{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_1"},
											},
										},
									},
								},
							},
							{
								IndexID:       2,
								PartitionName: "pj1",
								Config: zonepb.ZoneConfig{
									RangeMinBytes:               defaultZoneConfig.RangeMinBytes,
									RangeMaxBytes:               defaultZoneConfig.RangeMaxBytes,
									NumReplicas:                 proto.Int32(12),
									GC:                          &zonepb.GCPolicy{TTLSeconds: 4200},
									NullVoterConstraintsIsEmpty: defaultZoneConfig.NullVoterConstraintsIsEmpty,
									Constraints: []zonepb.ConstraintsConjunction{
										{
											Constraints: []zonepb.Constraint{
												{Type: zonepb.Constraint_REQUIRED, Key: "region", Value: "region_2"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		for _, stmt := range testCase.setup {
			_, err := sqlDB.Exec(stmt)
			require.NoError(t, err)
		}

		for _, expectation := range testCase.expectations {
			err := tc.Server(0).DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				zone, err := sql.GetHydratedZoneConfigForTable(ctx, txn, keys.SystemSQLCodec, expectation.id)
				if err != nil {
					return err
				}
				if err := zone.EnsureFullyHydrated(); err != nil {
					return err
				}
				// This test doesn't test subzone spans.
				zone.SubzoneSpans = nil
				if !zone.Equal(expectation.zone) {
					return errors.Newf(
						"expected %v zone config for id %d, but found %v",
						expectation.zone, expectation.id, zone,
					)
				}
				return nil
			})
			require.NoError(t, err)
		}
	}
}
