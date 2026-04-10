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
	unregisterFn := lm.RegisterLeaseObserver(obs)

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
	// Ensure the first version is locked before we start the schema change.
	firstVersionLocked := make(chan struct{})
	grp.GoCtx(func(ctx context.Context) error {
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
				if ld.Underlying().GetVersion()+1 != v {
					return errors.AssertionFailedf("expected version %d, got %d", ld.Underlying().GetVersion()+1, v)
				}
				require.Equal(t, ld.Underlying().GetVersion()+1, v)
				maxVersion.Store(int64(v))
				newLease, err := lm.Acquire(ctx, lease.TimestampToReadTimestamp(kvDB.Clock().Now()), tableID)
				if err != nil {
					return err
				}
				ld.Release(ctx)
				ld = newLease
				if ld.Underlying().GetVersion() != descpb.DescriptorVersion(maxVersion.Load()) {
					return errors.AssertionFailedf("expected version %d, got %d", maxVersion.Load(), ld.Underlying().GetVersion())
				}
			case <-schemaChangeComplete:
				return nil
			}
		}
	})
	<-firstVersionLocked
	// Add the first column and validate all versions are observed.
	sqlRunner.Exec(t, "ALTER TABLE t ADD COLUMN v INT")
	// Validate the schema change was observed.
	require.Greaterf(t, maxVersion.Load(), int64(1), "new versions were not detected")
	// Confirm the observer stops firing after unregistering.
	unregisterFn()
	maybeCloseSchemaChangeComplete()
	numEvents := obs.numEvents.Load()
	sqlRunner.Exec(t, "ALTER TABLE t ADD COLUMN v2 INT")
	// No new events should be see.
	require.Equal(t, numEvents, obs.numEvents.Load())
	require.NoError(t, grp.Wait())
}
