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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
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
	defaultZoneConfig := zonepb.DefaultZoneConfig()
	defaultSpanConfig := defaultZoneConfig.AsSpanConfig()
	defaultSpanConfigWith := func(f func(roachpb.SpanConfig) roachpb.SpanConfig) roachpb.SpanConfig {
		return f(defaultSpanConfig)
	}

	testCases := []struct {
		setup           string
		stmt            string
		expectedUpdates []expectedUpdate
	}{
		{
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
			setup: "CREATE TABLE t3(); ALTER TABLE t3 CONFIGURE ZONE USING gc.ttlseconds = 1;",
			stmt:  "DROP TABLE t3",
			expectedUpdates: []expectedUpdate{
				{
					expectedSpan: roachpb.Span{
						Key:    keys.SystemSQLCodec.TablePrefix(54),
						EndKey: keys.SystemSQLCodec.TablePrefix(54).PrefixEnd(),
					},
					expectedSpanConfig: defaultSpanConfigWith(func(config roachpb.SpanConfig) roachpb.SpanConfig {
						config.GCTTL = 1
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
	sqlWatcher := spanconfigsqlwatcher.New(
		keys.SystemSQLCodec,
		ts.DB(),
		ts.ClusterSettings(),
		ts.RangeFeedFactory().(*rangefeed.Factory),
		ts.Clock(),
		ts.InternalExecutor().(*sql.InternalExecutor),
		ts.LeaseManager().(*lease.Manager),
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

// TestSQLWatcherCatchupScan ensures that the SQLWatcher performs a catchup scan
// by default when it starts watching for updates. The catchup scan should only
// return updates corresponding to the state of the descriptor/zones table when
// the scan started, not the intermediate updates that would've been constructed
// while getting there.
func TestSQLWatcherCatchupScan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	setup := []string{
		"CREATE DATABASE db",
		"ALTER DATABASE db CONFIGURE ZONE USING num_replicas=5;",
		"CREATE TABLE db.t1();", // ID 53
		"CREATE TABLE db.t2();", // ID 54
		"CREATE TABLE db.t3();", // ID 55
		"ALTER TABLE db.t1 CONFIGURE ZONE USING num_replicas=7;",
		"ALTER TABLE db.t2 CONFIGURE ZONE USING num_replicas=3;",
	}

	expectedUpdates := []roachpb.SpanConfigEntry{
		{
			Span: roachpb.Span{
				Key:    keys.SystemSQLCodec.TablePrefix(53),
				EndKey: keys.SystemSQLCodec.TablePrefix(53).PrefixEnd(),
			},
			Config: roachpb.SpanConfig{
				RangeMinBytes:    128 << 20,
				RangeMaxBytes:    512 << 20,
				GCTTL:            25 * 60 * 60,
				GlobalReads:      false,
				NumVoters:        7,
				NumReplicas:      7,
				Constraints:      []roachpb.ConstraintsConjunction{},
				VoterConstraints: []roachpb.ConstraintsConjunction{},
				LeasePreferences: []roachpb.LeasePreference{},
			},
		},
		{
			Span: roachpb.Span{
				Key:    keys.SystemSQLCodec.TablePrefix(54),
				EndKey: keys.SystemSQLCodec.TablePrefix(54).PrefixEnd(),
			},
			Config: roachpb.SpanConfig{
				RangeMinBytes:    128 << 20,
				RangeMaxBytes:    512 << 20,
				GCTTL:            25 * 60 * 60,
				GlobalReads:      false,
				NumVoters:        3,
				NumReplicas:      3,
				Constraints:      []roachpb.ConstraintsConjunction{},
				VoterConstraints: []roachpb.ConstraintsConjunction{},
				LeasePreferences: []roachpb.LeasePreference{},
			},
		},
		{
			Span: roachpb.Span{
				Key:    keys.SystemSQLCodec.TablePrefix(55),
				EndKey: keys.SystemSQLCodec.TablePrefix(55).PrefixEnd(),
			},
			Config: roachpb.SpanConfig{
				RangeMinBytes:    128 << 20,
				RangeMaxBytes:    512 << 20,
				GCTTL:            25 * 60 * 60,
				GlobalReads:      false,
				NumVoters:        5,
				NumReplicas:      5,
				Constraints:      []roachpb.ConstraintsConjunction{},
				VoterConstraints: []roachpb.ConstraintsConjunction{},
				LeasePreferences: []roachpb.LeasePreference{},
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
	sqlWatcher := spanconfigsqlwatcher.New(
		keys.SystemSQLCodec,
		ts.DB(),
		ts.ClusterSettings(),
		ts.RangeFeedFactory().(*rangefeed.Factory),
		ts.Clock(),
		ts.InternalExecutor().(*sql.InternalExecutor),
		ts.LeaseManager().(*lease.Manager),
		tc.Stopper(),
		nil,
	)
	sqlDB := tc.ServerConn(0 /* idx */)

	for _, stmt := range setup {
		_, err := sqlDB.Exec(stmt)
		require.NoError(t, err)
	}

	updateCh, err := sqlWatcher.WatchForSQLUpdates(ctx)
	require.NoError(t, err)

	// We expect there to be updates for system tables at the start, which we
	// keep ignoring using this succeeds soon.
	testutils.SucceedsSoon(t, func() error {
		for _, expectedUpdate := range expectedUpdates {
			update := <-updateCh
			if !update.Entry.Span.Equal(expectedUpdate.Span) {
				return errors.Newf("expected span %v, but got %v", expectedUpdate.Span, update.Entry.Span)
			}
			if !expectedUpdate.Config.Equal(update.Entry.Config) {
				return errors.Newf(
					"expected config %v, but got %v", expectedUpdate.Config, update.Entry.Config,
				)
			}
		}
		return nil
	})
}
