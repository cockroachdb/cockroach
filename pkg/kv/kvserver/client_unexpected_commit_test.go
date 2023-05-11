// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

type interceptingTransport struct {
	kvcoord.Transport
	intercept func(context.Context, *kvpb.BatchRequest) (*kvpb.BatchResponse, error)
}

func (t *interceptingTransport) SendNext(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, error) {
	return t.intercept(ctx, ba)
}

func tempStoreSpec(t *testing.T, serverIdx int) base.StoreSpec {
	storeSpec := base.DefaultTestStoreSpec
	// TODO(sarkesian): Cleanup.
	//storeSpec.InMemory = false
	//storeSpec.Path = t.TempDir()
	//t.Logf("svr %d using store: %s", serverIdx, storeSpec.Path)
	return storeSpec
}

func getJamKey(txnName string, fromNodeID, toNodeID roachpb.NodeID, rangeID roachpb.RangeID) string {
	return fmt.Sprintf("%s:n%d->n%d:r%d", txnName, fromNodeID, toNodeID, rangeID)
}

// TestTransactionUnexpectedlyCommitted ...
func TestTransactionUnexpectedlyCommitted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t)
	skip.UnderRace(t)

	const enableDebug = true
	var blockedNodeLinks sync.Map // map[roachpb.NodeID]roachpb.NodeID
	var jammedOps sync.Map        // map[string]chan struct{}

	// TODO(sarkesian): Refactor/improve this.
	recoverReady := make(chan struct{})
	opID := 0

	// Test helpers.
	dbg := func(format string, args ...any) {
		t.Helper()
		if enableDebug {
			t.Logf("[dbg] "+format, args...)
		}
	}

	getInterceptingTransportFactory := func(nID roachpb.NodeID) kvcoord.TransportFactory {
		return func(options kvcoord.SendOptions, dialer *nodedialer.Dialer, slice kvcoord.ReplicaSlice) (kvcoord.Transport, error) {
			transport, err := kvcoord.GRPCTransportFactory(options, dialer, slice)
			interceptor := &interceptingTransport{
				Transport: transport,
				intercept: func(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, error) {
					fromNodeID := nID
					toNodeID := transport.NextReplica().NodeID
					toRangeID := ba.RangeID
					toReplicaID := transport.NextReplica().ReplicaID
					txnName := "_"
					if ba.Txn != nil {
						txnName = ba.Txn.Name
					}
					jamKey := getJamKey(txnName, fromNodeID, toNodeID, toRangeID)
					var blockedNodeID roachpb.NodeID
					var blocked bool
					var jammed bool
					var paused bool
					var jamChan chan struct{}
					var tags strings.Builder

					if nodeToBlock, ok := blockedNodeLinks.Load(fromNodeID); ok {
						blockedNodeID = nodeToBlock.(roachpb.NodeID)
					}
					if jamVal, ok := jammedOps.Load(jamKey); ok {
						jamChan = jamVal.(chan struct{})
					}

					if txnName == "txn1" || txnName == "txn2" {
						opID++
						if jamChan != nil {
							jammed = true
							fmt.Fprintf(&tags, "[jammed %d] ", opID)
						} else if toNodeID == blockedNodeID {
							blocked = true
							fmt.Fprintf(&tags, "[blocked %d] ", opID)
						} else if txnName == "txn1" && (ba.IsSingleEndTxnRequest() || ba.IsSingleHeartbeatTxnRequest()) {
							// Hold back txn1's single EndTxn, after it refreshes.
							paused = true
							fmt.Fprintf(&tags, "[paused %d] ", opID)
						}

						fmt.Fprintf(&tags, "(%s) n%d->n%d:r%d/%d ",
							ba.Txn.Name, fromNodeID, toNodeID, toRangeID, toReplicaID,
						)

						t.Logf("%sbatchReq={%s}, meta={key=%s ts=%s seq=%d, epoch=%d}", tags.String(), ba,
							roachpb.Key(ba.Txn.TxnMeta.Key), ba.Txn.TxnMeta.WriteTimestamp, ba.Txn.TxnMeta.Sequence,
							ba.Txn.TxnMeta.Epoch)
					}

					if paused {
						// TODO(sarkesian): This is confusing, refactor/improve this.
						if ba.IsSingleEndTxnRequest() {
							close(recoverReady)
						}
						<-time.After(5 * time.Second)
						t.Logf("%sop unpaused", tags.String())
					}

					br, rpcErr := transport.SendNext(ctx, ba)
					if jammed {
						<-jamChan
						t.Logf("%sop released", tags.String())
						return nil, grpcstatus.Errorf(codes.Unavailable, "response jammed on n%d<-n%d", fromNodeID, toNodeID)
					} else if blocked {
						t.Logf("%sop returning error", tags.String())
						return nil, grpcstatus.Errorf(codes.Unavailable, "response blocked on n%d<-n%d", fromNodeID, toNodeID)
					}

					// TODO(sarkesian): Remove, but - Let's make this an error and see what happens...
					//if paused && ba.IsSingleEndTxnRequest() {
					//	t.Logf("%sop returning error", tags.String())
					//	return nil, grpcstatus.Errorf(codes.Unavailable, "response blocked on n%d<-n%d", fromNodeID, toNodeID)
					//}

					return br, rpcErr
				},
			}
			return interceptor, err
		}
	}

	dumpKVs := func(scanResults []kv.KeyValue) {
		t.Helper()
		for _, kv := range scanResults {
			mvccValue, err := storage.DecodeMVCCValue(kv.Value.RawBytes)
			require.NoError(t, err)
			t.Logf("key: %s, value: %s", kv.Key, mvccValue)
		}
	}

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	// Disable closed timestamps for greater control over when transaction gets bumped.
	closedts.TargetDuration.Override(ctx, &st.SV, 1*time.Hour)

	startTime := time.Now()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {
				Settings:   st,
				Insecure:   true,
				StoreSpecs: []base.StoreSpec{tempStoreSpec(t, 0)},
				Knobs: base.TestingKnobs{
					KVClient: &kvcoord.ClientTestingKnobs{
						DisableCommitSanityCheck: true,
						TransportFactory:         getInterceptingTransportFactory(roachpb.NodeID(1)),
					},
					Store: &kvserver.StoreTestingKnobs{
						TestingRequestFilter: func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
							// TODO(sarkesian): Refactor/improve this.
							if ba.IsSingleRecoverTxnRequest() {
								<-recoverReady
								t.Logf("RECOVERY: batchReq={%s}", ba)
							}
							return nil
						},
					},
				},
			},
			1: {
				Settings:   st,
				Insecure:   true,
				StoreSpecs: []base.StoreSpec{tempStoreSpec(t, 1)},
			},
			2: {
				Settings:   st,
				Insecure:   true,
				StoreSpecs: []base.StoreSpec{tempStoreSpec(t, 2)},
			},
		},
	})

	defer tc.Stopper().Stop(ctx)
	dbg("cluster startup took %s", time.Since(startTime))
	require.NoError(t, tc.WaitForFullReplication())
	db := tc.Server(0).DB()

	requireRangeLease := func(desc roachpb.RangeDescriptor, serverIdx int) {
		t.Helper()
		testutils.SucceedsSoon(t, func() error {
			li, _, err := tc.FindRangeLeaseEx(ctx, desc, nil)
			if err != nil {
				return errors.Wrapf(err, "could not find lease for %s", desc)
			}
			curLease := li.Current()
			if curLease.Empty() {
				return errors.Errorf("could not find lease for %s", desc)
			}
			expStoreID := tc.Target(serverIdx).StoreID
			if !curLease.OwnedBy(expStoreID) {
				return errors.Errorf("expected s%d to own the lease for %s\n"+
					"actual lease info: %s",
					expStoreID, desc, curLease)
			}
			return nil
		})
	}

	// Create a table, and split it so we have multiple ranges.
	// Corresponds to:
	_ = `
		CREATE TABLE bank (id INT PRIMARY KEY, balance INT);
		INSERT INTO bank VALUES (1, 50), (2, 50);
		ALTER TABLE bank SPLIT AT VALUES (2);`
	tablePrefix := bootstrap.TestingUserTableDataMin()
	key1 := roachpb.Key(encoding.EncodeUvarintAscending(tablePrefix.Clone(), 1))
	key2 := roachpb.Key(encoding.EncodeUvarintAscending(tablePrefix.Clone(), 2))
	dbg("Putting initial keys")
	require.NoError(t, db.Put(ctx, key1, 50))
	require.NoError(t, db.Put(ctx, key2, 50))
	// NB: The ranges are split in descending key order so that the "first" range will have a higher rangeID.
	dbg("Splitting ranges")
	_, secondRange := tc.SplitRangeOrFatal(t, key2)
	_, firstRange := tc.SplitRangeOrFatal(t, key1)
	t.Logf("first range: %s", firstRange)
	t.Logf("second range: %s", secondRange)

	scannedKVs, err := db.Scan(ctx, tablePrefix, tablePrefix.PrefixEnd(), 0)
	require.NoError(t, err)
	dumpKVs(scannedKVs)

	// Dump KVs at end of test.
	defer func() {
		scannedKVs, err := db.Scan(ctx, tablePrefix, tablePrefix.PrefixEnd(), 0)
		require.NoError(t, err)
		dumpKVs(scannedKVs)
	}()

	// Separate the leases for each range so they are not on the same node.
	dbg("Moving leases")
	tc.TransferRangeLeaseOrFatal(t, firstRange, tc.Target(0))
	requireRangeLease(firstRange, 0)
	tc.TransferRangeLeaseOrFatal(t, secondRange, tc.Target(1))
	requireRangeLease(secondRange, 1)

	// Execute implicit transaction to move $10 from account 1 to account 2.
	// Corresponds to:
	_ = `
		UPDATE bank SET balance =
			CASE id
				WHEN $1 THEN balance - $3
				WHEN $2 THEN balance + $3
			END
			WHERE id IN ($1, $2)`

	getInBatch := func(ctx context.Context, txn *kv.Txn, keys ...roachpb.Key) []int64 {
		batch := txn.NewBatch()
		for _, key := range keys {
			batch.GetForUpdate(key)
		}
		assert.NoError(t, txn.Run(ctx, batch))
		assert.Len(t, batch.Results, len(keys))
		vals := make([]int64, len(keys))
		for i, result := range batch.Results {
			assert.Len(t, result.Rows, 1)
			vals[i] = result.Rows[0].ValueInt()
		}
		return vals
	}

	const xferAmount = 10
	const txnTimeout = 10 * time.Second

	// Checkpoints in test.
	txn1Ready := make(chan struct{})
	txn2Ready := make(chan struct{})
	leaseMoveReady := make(chan struct{})
	networkManipReady := make(chan struct{})
	networkManipDone := make(chan struct{})

	// Concurrent transactions.
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		// Wait until txn1 is ready to start.
		select {
		case <-txn1Ready:
		case <-tc.Stopper().ShouldQuiesce():
			t.Logf("txn1 quiescing...")
		}
		tCtx := context.Background()
		txn := db.NewTxn(tCtx, "txn1")
		vals := getInBatch(tCtx, txn, key1, key2)

		// Signal for network jamming to occur.
		close(networkManipReady)
		<-networkManipDone

		// Signal that txn2 can start after a few seconds,
		// and a few seconds after that the lease transfer can occur.
		// TODO(sarkesian): Should be tied to requests being in-flight, rather than time.
		go func() {
			// TODO(sarkesian): Refactor/improve this. Requires txn expiry defaults currently.
			<-time.After(6 * time.Second)
			close(txn2Ready)
			<-time.After(2 * time.Second)
			close(leaseMoveReady)
		}()

		batch := txn.NewBatch()
		batch.Put(key1, vals[0]-xferAmount)
		batch.Put(key2, vals[1]+xferAmount)
		assert.NoError(t, txn.CommitInBatch(tCtx, batch))
	}()
	go func() {
		defer wg.Done()
		// Wait until txn2 is ready to start.
		select {
		case <-txn2Ready:
		case <-tc.Stopper().ShouldQuiesce():
			t.Logf("txn2 quiescing...")
		}
		tCtx := context.Background()
		txn := db.NewTxn(tCtx, "txn2")
		vals := getInBatch(tCtx, txn, key1, key2)

		batch := txn.NewBatch()
		batch.Put(key1, vals[0]-xferAmount)
		batch.Put(key2, vals[1]+xferAmount)
		assert.NoError(t, txn.CommitInBatch(tCtx, batch))
	}()
	go func() {
		defer wg.Done()
		// Wait until lease move is ready.
		select {
		case <-leaseMoveReady:
		case <-tc.Stopper().ShouldQuiesce():
			t.Logf("lease mover quiescing...")
		}

		dbg("Transferring r%d lease to n%d", secondRange.RangeID, tc.Target(0).NodeID)
		tc.TransferRangeLeaseOrFatal(t, secondRange, tc.Target(0))
	}()

	// Network jamming helper functions.
	jamOperation := func(jamKey string, duration time.Duration) {
		jamChan := make(chan struct{})
		jammedOps.Store(jamKey, jamChan)
		if duration > 0 {
			go func() {
				defer jammedOps.Delete(jamKey)
				defer close(jamChan)
				select {
				case <-time.After(duration):
				case <-tc.Stopper().ShouldQuiesce():
					t.Logf("jammer %s quiescing...", jamKey)
				}
			}()
		}
	}

	// Test synchronization.
	close(txn1Ready)

	// Jam network connections when ready.
	<-networkManipReady
	dbg("Manipulating network links")
	// TODO(sarkesian): Cleanup.
	//blockedNodeLinks.Store(tc.Server(0).NodeID(), tc.Server(1).NodeID())
	//dbg("Blocked n%d<-n%d", tc.Server(0).NodeID(), tc.Server(1).NodeID())
	jamTxn1Key := getJamKey("txn1", tc.Server(0).NodeID(), tc.Server(1).NodeID(), secondRange.RangeID)
	//jamTxn2Key := getJamKey("txn2", tc.Server(0).NodeID(), tc.Server(1).NodeID(), secondRange.RangeID)
	jamOperation(jamTxn1Key, 10*time.Second)
	//jamOperation(jamTxn2Key, 10*time.Second)
	jammedOps.Range(func(key, value any) bool {
		jamKey := key.(string)
		dbg("Jamming %s", jamKey)
		return true
	})
	close(networkManipDone)

	wg.Wait()

	requireRangeLease(secondRange, 0)
}
