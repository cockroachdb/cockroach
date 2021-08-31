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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqlwatcher"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSQLWatcherReactsToUpdates verifies that the SQLWatcher emits the correct
// updates following changes made to system.descriptor or system.zones.
func TestSQLWatcherReactsToUpdates(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type expectedUpdate struct {
		expectedSpan       roachpb.Span
		expectedSpanConfig roachpb.SpanConfig
		expectedDeleted    bool
	}

	defaultSpanConfig := zonepb.DefaultZoneConfig().AsSpanConfig()

	defaultSpanConfigWith := func(f func(roachpb.SpanConfig) roachpb.SpanConfig) roachpb.SpanConfig {
		defaultSpanConfig := zonepb.DefaultZoneConfig().AsSpanConfig()
		return f(defaultSpanConfig)
	}

	testCases := []struct {
		setup           string
		stmt            string
		expectedUpdates []expectedUpdate
	}{
		{
			// Ensure creating a table results in a span config update being generated
			// for it.
			stmt: "CREATE TABLE t()",
			expectedUpdates: []expectedUpdate{
				{
					expectedSpan: roachpb.Span{
						Key:    keys.SystemSQLCodec.TablePrefix(52),
						EndKey: keys.SystemSQLCodec.TablePrefix(52).PrefixEnd(),
					},
					expectedSpanConfig: defaultSpanConfig,
					expectedDeleted:    false,
				},
			},
		},
		{
			// Ensure altering an existing table's zone config results in the correct
			// span config update being generated for it.
			setup: "CREATE table t2()",
			stmt:  "ALTER TABLE t2 CONFIGURE ZONE USING num_replicas=5",
			expectedUpdates: []expectedUpdate{
				{
					expectedSpan: roachpb.Span{
						Key:    keys.SystemSQLCodec.TablePrefix(53),
						EndKey: keys.SystemSQLCodec.TablePrefix(53).PrefixEnd(),
					},
					expectedSpanConfig: defaultSpanConfigWith(func(config roachpb.SpanConfig) roachpb.SpanConfig {
						config.NumReplicas = 5
						return config
					}),
					expectedDeleted: false,
				},
			},
		},
		{
			// Ensure dropping a table results in the span config entry being deleted
			// (eventually). Note that the first update below is for the table
			// descriptor being marked DROPPED and the second one is for the
			// descriptor being actually deleted.
			setup: "CREATE TABLE t3(); ALTER TABLE t3 CONFIGURE ZONE USING gc.ttlseconds = 1;",
			stmt:  "DROP TABLE t3",
			expectedUpdates: []expectedUpdate{
				{
					expectedSpan: roachpb.Span{
						Key:    keys.SystemSQLCodec.TablePrefix(54),
						EndKey: keys.SystemSQLCodec.TablePrefix(54).PrefixEnd(),
					},
					expectedSpanConfig: defaultSpanConfigWith(func(config roachpb.SpanConfig) roachpb.SpanConfig {
						config.GCPolicy.TTLSeconds = 1
						return config
					}),
					expectedDeleted: false,
				},
				{
					expectedSpan: roachpb.Span{
						Key:    keys.SystemSQLCodec.TablePrefix(54),
						EndKey: keys.SystemSQLCodec.TablePrefix(54).PrefixEnd(),
					},
					expectedDeleted: true,
				},
			},
		},
		{
			// Ensure that changing the zone configuration for the database results in
			// a span config update for each of the (2 in this case) table's inside
			// the database.
			setup: "CREATE DATABASE d; CREATE TABLE d.t1(); CREATE TABLE d.t2();",
			stmt:  "ALTER DATABASE d CONFIGURE ZONE USING num_replicas=5",
			expectedUpdates: []expectedUpdate{
				{
					expectedSpan: roachpb.Span{
						Key:    keys.SystemSQLCodec.TablePrefix(56),
						EndKey: keys.SystemSQLCodec.TablePrefix(56).PrefixEnd(),
					},
					expectedSpanConfig: defaultSpanConfigWith(func(config roachpb.SpanConfig) roachpb.SpanConfig {
						config.NumReplicas = 5
						return config
					}),
					expectedDeleted: false,
				},
				{
					expectedSpan: roachpb.Span{
						Key:    keys.SystemSQLCodec.TablePrefix(57),
						EndKey: keys.SystemSQLCodec.TablePrefix(57).PrefixEnd(),
					},
					expectedSpanConfig: defaultSpanConfigWith(func(config roachpb.SpanConfig) roachpb.SpanConfig {
						config.NumReplicas = 5
						return config
					}),
					expectedDeleted: false,
				},
			},
		},
		{
			// Ensure that changing the zone configuration for a named zone results
			// in the correct span config update.
			stmt: "ALTER RANGE timeseries CONFIGURE ZONE USING num_replicas=5",
			expectedUpdates: []expectedUpdate{
				{
					expectedSpan: keys.TimeseriesSpan,
					expectedSpanConfig: defaultSpanConfigWith(func(config roachpb.SpanConfig) roachpb.SpanConfig {
						config.NumReplicas = 5
						return config
					}),
				},
			},
		},
		{
			// Ensure that discarding the zone configuration for a named zone results
			// in a span config update. The span config update should correspond to
			// RANGE DEFAULT in the absence of an explicit zone config for the named
			// zone.
			stmt: "ALTER RANGE liveness CONFIGURE ZONE DISCARD",
			expectedUpdates: []expectedUpdate{
				{
					expectedSpan:       keys.NodeLivenessSpan,
					expectedSpanConfig: defaultSpanConfig,
				},
			},
		},
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true, // disable the automatic job creation
				},
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	ts := tc.Server(0 /* idx */)
	execCfg := ts.ExecutorConfig().(sql.ExecutorConfig)
	sqlWatcher := spanconfigsqlwatcher.New(
		keys.SystemSQLCodec,
		&execCfg,
		ts.ClusterSettings(),
		ts.RangeFeedFactory().(*rangefeed.Factory),
		ts.Clock(),
		tc.Stopper(),
		&spanconfig.TestingKnobs{
			SQLWatcherDisableInitialScan: true,
		},
	)

	sqlDB := tc.ServerConn(0 /* idx */)
	for _, tc := range testCases {
		_, err := sqlDB.Exec(tc.setup)
		require.NoError(t, err)

		updateCh, err := sqlWatcher.WatchForSQLUpdates(ctx)
		require.NoError(t, err)

		_, err = sqlDB.Exec(tc.stmt)
		require.NoError(t, err)

		for _, expectedUpdate := range tc.expectedUpdates {
			testutils.SucceedsSoon(t, func() error {
				update := <-updateCh
				if !expectedUpdate.expectedSpan.Equal(update.Entry.Span) {
					return errors.Newf(
						"expected span %v, found %v", expectedUpdate.expectedSpan, update.Entry.Span,
					)
				}
				if expectedUpdate.expectedDeleted != update.Deleted {
					return errors.Newf(
						"expected delete status %v, found %v", expectedUpdate.expectedDeleted, update.Deleted,
					)
				}
				if !expectedUpdate.expectedDeleted {
					expectedUpdate.expectedSpanConfig.Equal(update.Entry.Config)
				}
				return nil
			})
		}
	}
}
