// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness_test

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness"
	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/listenerutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// The tests in this file set up a multi-node multi-store cluster and ensure
// that Store Liveness support is established between all pairs of stores. The
// tests don't use Raft as a driver for establishing Store Liveness support;
// instead, each test initiates heartbeats manually (by calling SupportFrom).
// The tests can be extended to also use Raft as a driver for heartbeats.
//
// Some of the tests then trigger a failure mode (like a node restart, a disk
// stall, or a partial partition), and ensure that only the affected stores lose
// support.

const numNodes = 3
const numStoresPerNode = 2

func checkSupportFrom(
	t *testing.T, sm *storeliveness.SupportManager, storeID slpb.StoreIdent, supportExpected bool,
) {
	testutils.SucceedsSoon(
		t, func() error {
			_, exp := sm.SupportFrom(storeID)
			// The current timestamp should ideally be drawn from the local store's
			// clock, but this timestamp is not used for asserting any time-based
			// correctness property here; it just indicates that enough time has
			// passed since support from the remote store was last provided.
			now := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
			supportProvided := !exp.IsEmpty() && !exp.LessEq(now)
			if supportExpected && !supportProvided {
				return errors.New("support from not provided yet")
			}
			if !supportExpected && supportProvided {
				return errors.New("support from still provided")
			}
			return nil
		},
	)
}

func checkSupportFor(
	t *testing.T, sm *storeliveness.SupportManager, storeID slpb.StoreIdent, supportExpected bool,
) {
	testutils.SucceedsSoon(
		t, func() error {
			_, supportProvided := sm.SupportFor(storeID)
			if supportExpected && !supportProvided {
				return errors.New("support for not provided yet")
			}
			if !supportExpected && supportProvided {
				return errors.New("support for still provided")
			}
			return nil
		},
	)
}

// supportCheckFn uses the IDs and support managers of the local and remote
// stores to assert some properties of the requested/provided support.
type supportCheckFn func(
	localID slpb.StoreIdent, remoteID slpb.StoreIdent,
	localSM *storeliveness.SupportManager, remoteSM *storeliveness.SupportManager,
)

// checkSupport iterates over all pairs of stores and runs the function fn to
// assert the state of expected support.
func checkSupport(t *testing.T, tc *testcluster.TestCluster, fn supportCheckFn) {
	for i := 0; i < numNodes; i++ {
		require.NoError(
			t, tc.Server(i).GetStores().(*kvserver.Stores).VisitStores(
				func(localS *kvserver.Store) error {
					localID := slpb.StoreIdent{NodeID: localS.NodeID(), StoreID: localS.StoreID()}
					localSM := localS.TestingStoreLivenessSupportManager()
					for j := 0; j < numNodes; j++ {
						require.NoError(
							t, tc.Server(j).GetStores().(*kvserver.Stores).VisitStores(
								func(remoteS *kvserver.Store) error {
									remoteID := slpb.StoreIdent{NodeID: remoteS.NodeID(), StoreID: remoteS.StoreID()}
									remoteSM := remoteS.TestingStoreLivenessSupportManager()
									fn(localID, remoteID, localSM, remoteSM)
									return nil
								},
							),
						)
					}
					return nil
				},
			),
		)
	}
}

func ensureAllToAllSupport(t *testing.T, tc *testcluster.TestCluster) {
	checkSupportFn := func(
		localID slpb.StoreIdent, remoteID slpb.StoreIdent,
		localSM *storeliveness.SupportManager, remoteSM *storeliveness.SupportManager,
	) {
		// These tests don't use Raft as the driver of store liveness heartbeats, so
		// we request support-from first to start the heartbeats. I.e. we can't do
		// the following because the remoteID store may not have started sending
		// heartbeats to the localID store:
		// checkSupportFrom(t, localSM, remoteID, true /* supportExpected */)
		// checkSupportFor(t, localSM, remoteID, true /* supportExpected */)
		checkSupportFrom(t, localSM, remoteID, true /* supportExpected */)
		checkSupportFor(t, remoteSM, localID, true /* supportExpected */)
	}

	checkSupport(t, tc, checkSupportFn)
}

func makeMultiStoreArgs(storeKnobs *kvserver.StoreTestingKnobs) base.TestClusterArgs {
	reg := fs.NewStickyRegistry()
	knobs := base.TestingKnobs{Server: &server.TestingKnobs{StickyVFSRegistry: reg}}
	if storeKnobs != nil {
		knobs.Store = storeKnobs
	}
	clusterArgs := base.TestClusterArgs{
		ReplicationMode:   base.ReplicationManual,
		ServerArgsPerNode: map[int]base.TestServerArgs{},
	}
	for srvIdx := 0; srvIdx < numNodes; srvIdx++ {
		serverArgs := base.TestServerArgs{Knobs: knobs}
		for storeIdx := 0; storeIdx < numStoresPerNode; storeIdx++ {
			id := strconv.Itoa(srvIdx*numStoresPerNode + storeIdx + 1)
			serverArgs.StoreSpecs = append(
				serverArgs.StoreSpecs,
				base.StoreSpec{InMemory: true, StickyVFSID: id},
			)
		}
		clusterArgs.ServerArgsPerNode[srvIdx] = serverArgs
	}
	return clusterArgs
}

// TestStoreLivenessAllToAllSupport tests that each store provides and receives
// support from all other stores.
func TestStoreLivenessAllToAllSupport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := makeMultiStoreArgs(nil)
	tc := testcluster.NewTestCluster(t, numNodes, args)
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)

	ensureAllToAllSupport(t, tc)
}

// TestStoreLivenessRestart tests that when a node is stopped all its stores
// lose support; when the node is restarted, all stores re-establish support.
func TestStoreLivenessRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	lisReg := listenerutil.NewListenerRegistry()
	defer lisReg.Close()
	args := makeMultiStoreArgs(nil)
	args.ReusableListenerReg = lisReg
	tc := testcluster.NewTestCluster(t, numNodes, args)
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)

	ensureAllToAllSupport(t, tc)

	// Stop node 1.
	serverIdxToStop := 0
	tc.StopServer(serverIdxToStop)
	stoppedNode := roachpb.NodeID(serverIdxToStop + 1)

	checkSupportFn := func(
		localID slpb.StoreIdent, remoteID slpb.StoreIdent,
		localSM *storeliveness.SupportManager, _ *storeliveness.SupportManager,
	) {
		// If the local store is on the stopped node, its SupportManager is in a
		// frozen state, so it's not useful to try to assert anything based on it.
		// The support provided for and from the local store will be asserted from
		// the perspective of its peers (when the local store becomes the remote
		// store in the checkSupport loop).
		if localID.NodeID == stoppedNode {
			return
		}
		// If the remote store is the stopped one, the local store should lose
		// support from and for the remote node.
		if remoteID.NodeID == stoppedNode {
			checkSupportFrom(t, localSM, remoteID, false /* supportExpected */)
			checkSupportFor(t, localSM, remoteID, false /* supportExpected */)
		} else {
			checkSupportFrom(t, localSM, remoteID, true /* supportExpected */)
			checkSupportFor(t, localSM, remoteID, true /* supportExpected */)
		}
	}
	checkSupport(t, tc, checkSupportFn)

	// Restart node 1 and ensure all-to-all support again.
	require.NoError(t, tc.RestartServer(serverIdxToStop))
	ensureAllToAllSupport(t, tc)
}

// TestStoreLivenessDiskStall tests that a store with a stalled disk loses all
// support while all other stores are unaffected.
func TestStoreLivenessDiskStall(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stalledStore := slpb.StoreIdent{NodeID: roachpb.NodeID(2), StoreID: roachpb.StoreID(3)}
	testEngine := storeliveness.NewTestEngine(stalledStore)
	storeKnobs := &kvserver.StoreTestingKnobs{
		StoreLivenessKnobs: &storeliveness.TestingKnobs{
			SupportManagerKnobs: storeliveness.SupportManagerKnobs{
				TestEngine: testEngine,
			},
		},
	}
	defer testEngine.Close()
	args := makeMultiStoreArgs(storeKnobs)
	tc := testcluster.NewTestCluster(t, numNodes, args)
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)

	ensureAllToAllSupport(t, tc)

	// Start blocking writes.
	testEngine.SetBlockOnWrite(true)

	checkSupportFn := func(
		localID slpb.StoreIdent, remoteID slpb.StoreIdent,
		localSM *storeliveness.SupportManager, _ *storeliveness.SupportManager,
	) {
		// If the local store has a stalled disk, calls to SupportFor or SupportFrom
		// might hang while acquiring the read lock (supporterState.mu or
		// requesterState.mu) if a write is in progress and is
		// stalled while holding the lock. The support provided for and from the
		// local store will be asserted from the perspective of its peers (when the
		// local store becomes the remote store in the checkSupport loop).
		if localID == stalledStore {
			return
		}
		// If the remote store is the stalled one, it should lose support from and
		// for the local store.
		if remoteID == stalledStore {
			checkSupportFrom(t, localSM, remoteID, false /* supportExpected */)
			checkSupportFor(t, localSM, remoteID, false /* supportExpected */)
		} else {
			checkSupportFrom(t, localSM, remoteID, true /* supportExpected */)
			checkSupportFor(t, localSM, remoteID, true /* supportExpected */)
		}
	}
	checkSupport(t, tc, checkSupportFn)

	// Stop blocking writes.
	testEngine.SetBlockOnWrite(false)

	// Ensure all-to-all support again.
	ensureAllToAllSupport(t, tc)
}

// TestStoreLivenessPartialPartition tests that a partially-partitioned store
// loses all support from partitioned peers but keeps support from others.
func TestStoreLivenessPartialPartition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := makeMultiStoreArgs(nil)
	tc := testcluster.NewTestCluster(t, numNodes, args)
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)

	// Pick two stores, one from node 1 and one from node 2; call them left and
	// right. The test will set up either a symmetric or an asymmetric partition
	// between them.
	// Connected:            left <----> right
	// Symmetric partition:  left x----x right
	// Asymmetric partition: left x----> right
	server0 := tc.Server(0)
	left, err := server0.GetStores().(*kvserver.Stores).GetStore(server0.GetFirstStoreID())
	require.NoError(t, err)
	leftStoreID := slpb.StoreIdent{NodeID: left.NodeID(), StoreID: left.StoreID()}
	server1 := tc.Server(1)
	right, err := server1.GetStores().(*kvserver.Stores).GetStore(server1.GetFirstStoreID())
	require.NoError(t, err)
	rightStoreID := slpb.StoreIdent{NodeID: right.NodeID(), StoreID: right.StoreID()}

	var shouldDropMsgs atomic.Bool
	// createPartition drops all messages from sender to receiver by registering
	// an unreliable handler at the receiver's store liveness transport.
	createPartition := func(sender *kvserver.Store, receiver *kvserver.Store) {
		receiverServerIdx := int(receiver.NodeID()) - 1
		transport := tc.Server(receiverServerIdx).StoreLivenessTransport().(*storeliveness.Transport)
		transport.ListenMessages(
			receiver.StoreID(), &storeliveness.UnreliableHandler{
				MessageHandler: receiver.TestingStoreLivenessSupportManager(),
				UnreliableHandlerFuncs: storeliveness.UnreliableHandlerFuncs{
					DropStoreLivenessMsg: func(msg *slpb.Message) bool {
						return shouldDropMsgs.Load() && msg.From.StoreID == sender.StoreID()
					},
				},
			},
		)
	}

	testutils.RunTrueAndFalse(
		t, "symmetric", func(t *testing.T, symmetric bool) {
			ensureAllToAllSupport(t, tc)

			// Always block messages from right to left (left x----> right).
			createPartition(right, left)
			// If the partition is symmetric, also block messages from left to right.
			// (left x----x right)
			if symmetric {
				createPartition(left, right)
			}

			// Start dropping messages.
			shouldDropMsgs.Store(true)

			checkSupportFn := func(
				localID slpb.StoreIdent, remoteID slpb.StoreIdent,
				localSM *storeliveness.SupportManager, _ *storeliveness.SupportManager,
			) {
				if localID == leftStoreID && remoteID == rightStoreID {
					// In either the symmetric or asymmetric case, the left (local) store
					// doesn't receive any messages, so it loses support from the remote
					// store, and also withdraws support for it.
					checkSupportFrom(t, localSM, remoteID, false /* supportExpected */)
					checkSupportFor(t, localSM, remoteID, false /* supportExpected */)
				} else if localID == rightStoreID && remoteID == leftStoreID {
					// In either the symmetric or asymmetric case, the left (remote) store
					// doesn't receive the right (local) store's heartbeats, so the left
					// store withdraws support from the right store.
					checkSupportFrom(t, localSM, remoteID, false /* supportExpected */)
					// In the symmetric case, the right (local) store also doesn't
					// receive the left (remote) store's heartbeats, so it withdraws
					// support for it. However, in the asymmetric case, the right store
					// received the left store's heartbeats, so support is extended.
					if symmetric {
						checkSupportFor(t, localSM, remoteID, false /* supportExpected */)
					} else {
						checkSupportFor(t, localSM, remoteID, true /* supportExpected */)
					}
				} else {
					// All other pairs of stores are unaffected.
					checkSupportFrom(t, localSM, remoteID, true /* supportExpected */)
					checkSupportFor(t, localSM, remoteID, true /* supportExpected */)
				}
			}
			checkSupport(t, tc, checkSupportFn)

			// Stop dropping messages and ensure all-to-all support again.
			shouldDropMsgs.Store(false)
			ensureAllToAllSupport(t, tc)
		},
	)
}
