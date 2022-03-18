package kvserver_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCordonIfPanicDuringApply(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t)

	ctx := context.Background()

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	testArgs := func(dontPanicOnApplyPanicOrFatalError bool) base.TestServerArgs {
		return base.TestServerArgs{
			Settings: cluster.MakeClusterSettings(),
			// We will start up a second server after stopping the first one to
			// simulate a server restart, since cordoning in response to a panic
			// during apply only takes effect after a server restart. So we use
			// the same store for the two servers (they are the same node). This
			// way, the should cordon write to storage done by the first server is
			// seen by the second server at startup.
			StoreSpecs: []base.StoreSpec{{Path: dir + "/store-1"}},
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					DontPanicOnApplyPanicOrFatalError: dontPanicOnApplyPanicOrFatalError,
					// Simulate a panic during apply!
					TestingApplyFilter: func(args kvserverbase.ApplyFilterArgs) (int, *roachpb.Error) {
						for _, ru := range args.Req.Requests {
							key := ru.GetInner().Header().Key
							// The time-series range is continuously written to.
							if bytes.HasPrefix(key, keys.TimeseriesPrefix) {
								panic("boom")
							}
						}
						return 0, nil
					},
				},
			},
		}
	}

	s, _, kvDB := serverutils.StartServer(t,
		// In production, the first time the panic at apply time is experienced, we expect the
		// server to mark the replica as to be cordoned and crash after re-throwing the panic.
		// In this test, we expect the server to mark the replicas as to be cordoned but also
		// to NOT re-throw the panic. Else the test would fail due to a uncaught panic.
		testArgs(true /* dontPanicOnApplyPanicOrFatalError */))

	// TODO(josh): This is gnarly. Can we force the scheduler to run on the range with the
	// apply time panic, instead of sleeping here?
	time.Sleep(10 * time.Second)
	s.Stopper().Stop(ctx)

	s, _, kvDB = serverutils.StartServer(t,
		// On the second run of the server, we don't expect a panic, as on startup, the replica
		// should be cordoned. All raft machinery should stop, so the TestingApplyFilter up above
		// shouldn't run.
		testArgs(false /* dontPanicOnApplyPanicOrFatalError */))
	defer s.Stopper().Stop(ctx)

	time.Sleep(10 * time.Second)

	// On the second run of the server, we expect requests to the cordoned range to fail fast.
	//
	// Note that if this was a three node cluster, and if only one replica was failing to apply
	// some entry, we'd expect that one replica to cordon on restart, which would imply shedding
	// the lease. As a result, we'd expect reads & writes to the range to succeed, rather than
	// fail fast.
	// TODO(josh): Test behavior on a three node cluster.
	_, err := kvDB.Get(ctx, keys.TimeseriesPrefix)
	require.Error(t, err)
	require.Regexp(t, "cordoned", err.Error())
}
