// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lease_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type testObserver struct {
	notified  chan descpb.DescriptorVersion
	targetID  descpb.ID
	numEvents atomic.Int64
}

func (o *testObserver) OnNewVersion(
	ctx context.Context, id descpb.ID, version descpb.DescriptorVersion, timestamp hlc.Timestamp,
) {
	o.numEvents.Add(1)
	if id != o.targetID {
		return
	}
	o.notified <- version
}

// TestLeaseObserver sanity checks the lease observer implementation,
// by ensuring observers can concurrently acqure and release versions.
// Additionally, they observe all versions.
func TestLeaseObserver(t *testing.T) {
	defer leaktest.AfterTest(t)()

	server, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer server.Stopper().Stop(context.Background())

	sqlRunner := sqlutils.MakeSQLRunner(db)
	sqlRunner.Exec(t, "CREATE TABLE t (k INT PRIMARY KEY)")
	var tableID descpb.ID
	sqlRunner.QueryRow(t, "SELECT 't'::regclass::int").Scan(&tableID)
	obs := &testObserver{
		notified: make(chan descpb.DescriptorVersion, 100),
		targetID: tableID,
	}
	lm := server.LeaseManager().(*lease.Manager)
	_, unregisterFn := lm.RegisterLeaseObserver(obs, []descpb.ID{tableID})

	// Trigger initial acquisition on node 0.
	sqlRunner.Exec(t, "SELECT * FROM t")

	grp := ctxgroup.WithContext(context.Background())
	schemaChangeComplete := make(chan struct{})
	schemaChangeCompleteClosed := false
	maybeCloseSchemaChangeComplete := func() {
		if schemaChangeCompleteClosed {
			return
		}
		close(schemaChangeComplete)
		schemaChangeCompleteClosed = true
	}
	defer maybeCloseSchemaChangeComplete()
	var maxVersion atomic.Int64
	maxVersion.Store(1)
	var grpErr atomic.Pointer[error]
	// Ensure the first version is locked before we start the schema change.
	firstVersionLocked := make(chan struct{})
	grp.GoCtx(func(ctx context.Context) (retErr error) {
		defer func() {
			if retErr != nil {
				grpErr.Store(&retErr)
			}
		}()
		firstVersionLockedClosed := false
		maybeCloseFirstVersionLocked := func() {
			if firstVersionLockedClosed {
				return
			}
			close(firstVersionLocked)
			firstVersionLockedClosed = true
		}
		defer maybeCloseFirstVersionLocked()
		// Get a leaase on the current version.
		ld, err := lm.Acquire(ctx, lease.TimestampToReadTimestamp(kvDB.Clock().Now()), tableID)
		if err != nil {
			return err
		}
		maybeCloseFirstVersionLocked()
		defer func() {
			ld.Release(ctx)
		}()
		for {
			select {
			case v := <-obs.notified:
				if v <= ld.Underlying().GetVersion() {
					continue
				}
				maxVersion.Store(int64(v))
				newLease, err := lm.Acquire(ctx, lease.TimestampToReadTimestamp(kvDB.Clock().Now()), tableID)
				if err != nil {
					return err
				}
				ld.Release(ctx)
				ld = newLease
				if ld.Underlying().GetVersion() < descpb.DescriptorVersion(maxVersion.Load()) {
					return errors.AssertionFailedf(
						"expected version >= %d, got %d", maxVersion.Load(), ld.Underlying().GetVersion(),
					)
				}
				maxVersion.Store(int64(ld.Underlying().GetVersion()))
			case <-schemaChangeComplete:
				return nil
			}
		}
	})
	<-firstVersionLocked
	// Add the first column and validate all versions are observed.
	sqlRunner.Exec(t, "ALTER TABLE t ADD COLUMN v INT")
	// Validate the schema change was observed.
	testutils.SucceedsSoon(t, func() error {
		if e := grpErr.Load(); e != nil {
			return *e
		}
		if maxVersion.Load() <= int64(1) {
			return errors.New("new versions were not detected")
		}
		return nil
	})
	// Confirm the observer stops firing after unregistering.
	unregisterFn()
	maybeCloseSchemaChangeComplete()
	numEvents := obs.numEvents.Load()
	sqlRunner.Exec(t, "ALTER TABLE t ADD COLUMN v2 INT")
	// No new events should be see.
	require.Equal(t, numEvents, obs.numEvents.Load())
	require.NoError(t, grp.Wait())
}

// TestRegisterLeaseObserverReturnsInitialVersions verifies that an observer
// registered after a descriptor version has already been broadcast learns
// about that version from the map returned by RegisterLeaseObserver, rather
// than depending on a re-broadcast that would be suppressed by the
// per-descriptor dedup in maybeAddObserverEvent. Regression test for #169820.
func TestRegisterLeaseObserverReturnsInitialVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	server, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	sqlRunner := sqlutils.MakeSQLRunner(db)
	sqlRunner.Exec(t, "CREATE TABLE t (k INT PRIMARY KEY)")
	var tableID descpb.ID
	sqlRunner.QueryRow(t, "SELECT 't'::regclass::int").Scan(&tableID)

	lm := server.LeaseManager().(*lease.Manager)

	// Register a first observer as a synchronization point: it lets us
	// wait until the lease manager has actually broadcast new versions of
	// the table descriptor (via maybeAddObserverEvent), so that
	// maxVersionNotified is non-trivial when we register the second
	// observer below.
	firstObserver := &testObserver{
		notified: make(chan descpb.DescriptorVersion, 100),
		targetID: tableID,
	}
	_, firstObserverUnreg := lm.RegisterLeaseObserver(firstObserver, []descpb.ID{tableID})

	// Trigger acquisition (so the descriptor is known to the lease
	// manager) and then run schema changes so the broadcast version is
	// strictly greater than 1.
	sqlRunner.Exec(t, "SELECT * FROM t")
	sqlRunner.Exec(t, "ALTER TABLE t ADD COLUMN v INT")
	sqlRunner.Exec(t, "ALTER TABLE t ADD COLUMN w INT")

	var maxSeen descpb.DescriptorVersion
	testutils.SucceedsSoon(t, func() error {
		for {
			select {
			case v := <-firstObserver.notified:
				if v > maxSeen {
					maxSeen = v
				}
			default:
				if maxSeen >= 2 {
					return nil
				}
				return errors.Newf("first observer has only seen version %d so far", maxSeen)
			}
		}
	})
	firstObserverUnreg()

	// Register a second observer. The returned map must include our
	// table at a version at least as high as what the first observer
	// saw. No OnNewVersion plumbing needed: the snapshot is captured
	// atomically with subscription inside RegisterLeaseObserver.
	lateObserver := &testObserver{
		notified: make(chan descpb.DescriptorVersion, 100),
		targetID: tableID,
	}
	initial, lateObserverUnreg := lm.RegisterLeaseObserver(lateObserver, []descpb.ID{tableID})
	defer lateObserverUnreg()

	v, ok := initial[tableID]
	require.True(t, ok, "expected descriptor %d in initialVersions, got %v", tableID, initial)
	require.GreaterOrEqual(t, v, maxSeen,
		"initial version for %d was %d, expected >= %d", tableID, v, maxSeen)
	// initialVersions should never include the zero version sentinel.
	for id, ver := range initial {
		require.NotZero(t, ver, "initialVersions[%d] is zero", id)
	}
}
