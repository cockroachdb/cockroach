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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqlwatcher"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/fmtsafe/testdata/src/github.com/cockroachdb/errors"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestSQLWatcher verifies that the watcher emits the right updates following
// changes made to system.descriptor or system.zones.
func TestSQLWatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type expectedUpdate struct {
		expectedSpan    roachpb.Span
		expectedDeleted bool
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
					expectedDeleted: false,
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
					expectedDeleted: false,
				},
				{
					expectedSpan: roachpb.Span{
						Key:    keys.SystemSQLCodec.TablePrefix(57),
						EndKey: keys.SystemSQLCodec.TablePrefix(57).PrefixEnd(),
					},
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

		updates, err := sqlWatcher.Watch(ctx)
		require.NoError(t, err)

		_, err = sqlDB.Exec(tc.stmt)
		require.NoError(t, err)

		for _, expectedUpdate := range tc.expectedUpdates {
			testutils.SucceedsSoon(t, func() error {
				update := <-updates
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
				return nil
			})
		}
	}
}
