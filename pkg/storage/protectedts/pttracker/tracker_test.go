package pttracker_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	p := protectedts.WithDatabase(ptstorage.NewProvider(s.ClusterSettings()), s.DB())

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
		Key:    keys.ProtectedTimestampMetadata,
		EndKey: keys.ProtectedTimestampMetadata.Next(),
	}
	protectTS := s.Clock().Now()
	r := ptpb.NewRecord(protectTS, ptpb.PROTECT_AT, "", nil, spanToProtect)
	require.Nil(t, p.Protect(ctx, nil /* txn */, &r))
	_, createdAt, err := p.GetRecord(ctx, nil, r.ID)
	require.Nil(t, err)

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
