// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigsqlwatcher_test

import (
	"context"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqlwatcher"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestSQLWatcherReactsToUpdates verifies that the SQLWatcher emits the correct
// updates following changes made to system.descriptor or system.zones.
func TestSQLWatcherReactsToUpdates(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		setup       string
		stmt        string
		expectedIDs descpb.IDs
	}{
		{
			stmt:        "CREATE TABLE t()",
			expectedIDs: descpb.IDs{52},
		},
		{
			setup:       "CREATE TABLE t2()",
			stmt:        "ALTER TABLE t2 CONFIGURE ZONE USING num_replicas = 3",
			expectedIDs: descpb.IDs{53},
		},
		{
			setup:       "CREATE DATABASE d; CREATE TABLE d.t1(); CREATE TABLE d.t2()",
			stmt:        "ALTER DATABASE d CONFIGURE ZONE USING num_replicas=5",
			expectedIDs: descpb.IDs{54},
		},
		{
			setup:       "CREATE TABLE t3(); CREATE TABLE t4()",
			stmt:        "ALTER TABLE t3 CONFIGURE ZONE USING num_replicas=5; CREATE TABLE t5(); DROP TABLE t4;",
			expectedIDs: descpb.IDs{57, 58, 59},
		},
		// Named zone tests.
		{
			stmt:        "ALTER RANGE DEFAULT CONFIGURE ZONE USING num_replicas = 7",
			expectedIDs: descpb.IDs{keys.RootNamespaceID},
		},
		{
			stmt:        "ALTER RANGE liveness CONFIGURE ZONE USING num_replicas = 7",
			expectedIDs: descpb.IDs{keys.LivenessRangesID},
		},
		{
			stmt:        "ALTER RANGE meta CONFIGURE ZONE USING num_replicas = 7",
			expectedIDs: descpb.IDs{keys.MetaRangesID},
		},
		{
			stmt:        "ALTER RANGE system CONFIGURE ZONE USING num_replicas = 7",
			expectedIDs: descpb.IDs{keys.SystemRangesID},
		},
		{
			stmt:        "ALTER RANGE timeseries CONFIGURE ZONE USING num_replicas = 7",
			expectedIDs: descpb.IDs{keys.TimeseriesRangesID},
		},
		// Test that events on types/schemas are also captured.
		{
			setup: "CREATE DATABASE db",
			stmt:  "CREATE SCHEMA db.sc",
			// one ID each for the parent database and the schema.
			expectedIDs: descpb.IDs{60, 61},
		},
		{
			stmt: "CREATE TYPE typ AS ENUM()",
			// One ID each for the enum and the array type.
			expectedIDs: descpb.IDs{62, 63},
		},
	}

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true, // disable the automatic job creation.
				},
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	})
	defer tc.Stopper().Stop(context.Background())

	ts := tc.Server(0 /* idx */)

	sqlDB := tc.ServerConn(0 /* idx */)
	for _, tc := range testCases {
		sqlWatcher := spanconfigsqlwatcher.New(
			keys.SystemSQLCodec,
			ts.ClusterSettings(),
			ts.RangeFeedFactory().(*rangefeed.Factory),
			ts.Stopper(),
		)

		_, err := sqlDB.Exec(tc.setup)
		require.NoError(t, err)

		prevCheckpointTS := ts.Clock().Now()

		done := make(chan struct{})

		var receivedIds descpb.IDs

		go func() {
			err = sqlWatcher.WatchForSQLUpdates(context.Background(),
				prevCheckpointTS,
				func(events []spanconfig.SQLWatcherEvent, checkpointTS hlc.Timestamp) error {
					require.True(t, prevCheckpointTS.Less(checkpointTS))
					for _, event := range events {
						receivedIds = append(receivedIds, event.ID)
					}
					if len(receivedIds) == len(tc.expectedIDs) {
						close(done)
					}
					prevCheckpointTS = checkpointTS
					return nil
				})
			require.NoError(t, err)
		}()

		_, err = sqlDB.Exec(tc.stmt)
		require.NoError(t, err)

		<-done

		// Rangefeed events aren't guaranteed to be in any particular order for
		// different keys.
		sort.Sort(receivedIds)
		require.Equal(t, receivedIds, tc.expectedIDs)

		sqlWatcher.Close()
	}
}
