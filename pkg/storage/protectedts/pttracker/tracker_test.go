// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pttracker_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/pttracker"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ protectedts.Tracker = (*pttracker.Tracker)(nil)

// TestTracker exercises the basic behavior of the Tracker.
func TestTracker(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0)
	p := protectedts.WithDatabase(ptstorage.New(s.ClusterSettings(),
		s.InternalExecutor().(sqlutil.InternalExecutor)), s.DB())

	// Set the poll interval to be very short.
	protectedts.PollInterval.Override(&s.ClusterSettings().SV, 100*time.Microsecond)
	tr := pttracker.New(s.ClusterSettings(), s.DB(), p)
	require.Nil(t, tr.Start(ctx, tc.Stopper()))

	// Make sure that protected timestamp gets updated.
	var ts hlc.Timestamp
	testutils.SucceedsSoon(t, func() error {
		ts = tr.ProtectedBy(ctx, roachpb.Span{}, func(*ptpb.Record) {})
		if ts == (hlc.Timestamp{}) {
			return errors.Errorf("expected an update to occur")
		}
		return nil
	})

	// Make sure that it gets updated again.
	testutils.SucceedsSoon(t, func() error {
		newTS := tr.ProtectedBy(ctx, roachpb.Span{}, func(*ptpb.Record) {})
		if !ts.Less(newTS) {
			return errors.Errorf("expected an update to occur")
		}
		return nil
	})

	// Then we'll add a record and make sure it gets seen.
	spanToProtect := roachpb.Span{
		Key:    roachpb.Key(keys.MakeTablePrefix(42)),
		EndKey: roachpb.Key(keys.MakeTablePrefix(42)).PrefixEnd(),
	}
	protectTS := s.Clock().Now()
	r := ptpb.NewRecord(protectTS, ptpb.PROTECT_AT, "", nil, spanToProtect)
	txn := s.DB().NewTxn(ctx, "test")
	require.NoError(t, p.Protect(ctx, txn, &r))
	require.NoError(t, txn.Commit(ctx))
	_, err := p.GetRecord(ctx, nil, r.ID)
	require.NoError(t, err)
	createdAt := txn.CommitTimestamp()
	testutils.SucceedsSoon(t, func() error {
		var coveredBy []*ptpb.Record
		seenTS := tr.ProtectedBy(ctx, spanToProtect, func(r *ptpb.Record) {
			coveredBy = append(coveredBy, r)
		})
		if len(coveredBy) == 0 {
			assert.True(t, seenTS.Less(createdAt), "%v %v", seenTS, protectTS)
			return errors.Errorf("expected %v to be covered", spanToProtect)
		}
		require.True(t, !seenTS.Less(createdAt), "%v %v", seenTS, protectTS)
		require.EqualValues(t, []*ptpb.Record{&r}, coveredBy)
		return nil
	})

	// Then release the record and make sure that that gets seen.
	require.Nil(t, p.Release(ctx, nil /* txn */, r.ID))
	testutils.SucceedsSoon(t, func() error {
		var coveredBy []*ptpb.Record
		_ = tr.ProtectedBy(ctx, spanToProtect, func(r *ptpb.Record) {
			coveredBy = append(coveredBy, r)
		})
		if len(coveredBy) > 0 {
			return errors.Errorf("expected %v not to be covered", spanToProtect)
		}
		return nil
	})
}

// TODO(ajwerner): add more testing.
