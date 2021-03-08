// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/* Package storage_test provides a means of testing store
functionality which depends on a fully-functional KV client. This
cannot be done within the storage package because of circular
dependencies.

By convention, tests in package storage_test have names of the form
client_*.go.
*/
package kvserver_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

// createTestStore creates a test store using an in-memory
// engine.
func createTestStore(t testing.TB, stopper *stop.Stopper) (*kvserver.Store, *hlc.ManualClock) {
	manual := hlc.NewManualClock(123)
	cfg := kvserver.TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	store := createTestStoreWithOpts(t, testStoreOpts{cfg: &cfg}, stopper)
	return store, manual
}

// DEPRECATED. Use createTestStoreWithOpts().
func createTestStoreWithConfig(
	t testing.TB, stopper *stop.Stopper, storeCfg kvserver.StoreConfig,
) *kvserver.Store {
	store := createTestStoreWithOpts(t,
		testStoreOpts{
			cfg: &storeCfg,
		},
		stopper,
	)
	return store
}

// testStoreOpts affords control over aspects of store creation.
type testStoreOpts struct {
	// dontBootstrap, if set, means that the engine will not be bootstrapped.
	dontBootstrap bool
	// dontCreateSystemRanges is relevant only if dontBootstrap is not set.
	// If set, the store will have a single range. If not set, the store will have
	// all the system ranges that are generally created for a cluster at boostrap.
	dontCreateSystemRanges bool

	cfg *kvserver.StoreConfig
	eng storage.Engine
}

// createTestStoreWithOpts creates a test store using the given engine and clock.
// TestStoreConfig() can be used for creating a config suitable for most
// tests.
func createTestStoreWithOpts(
	t testing.TB, opts testStoreOpts, stopper *stop.Stopper,
) *kvserver.Store {
	var storeCfg kvserver.StoreConfig
	if opts.cfg == nil {
		manual := hlc.NewManualClock(123)
		storeCfg = kvserver.TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	} else {
		storeCfg = *opts.cfg
	}
	eng := opts.eng
	if eng == nil {
		eng = storage.NewDefaultInMemForTesting()
		stopper.AddCloser(eng)
	}

	tracer := storeCfg.Settings.Tracer
	ac := log.AmbientContext{Tracer: tracer}
	storeCfg.AmbientCtx = ac

	rpcContext := rpc.NewContext(rpc.ContextOptions{
		TenantID:   roachpb.SystemTenantID,
		AmbientCtx: ac,
		Config:     &base.Config{Insecure: true},
		Clock:      storeCfg.Clock,
		Stopper:    stopper,
		Settings:   storeCfg.Settings,
	})
	// Ensure that tests using this test context and restart/shut down
	// their servers do not inadvertently start talking to servers from
	// unrelated concurrent tests.
	rpcContext.ClusterID.Set(context.Background(), uuid.MakeV4())
	nodeDesc := &roachpb.NodeDescriptor{
		NodeID:  1,
		Address: util.MakeUnresolvedAddr("tcp", "invalid.invalid:26257"),
	}
	server := rpc.NewServer(rpcContext) // never started
	storeCfg.Gossip = gossip.NewTest(
		nodeDesc.NodeID, rpcContext, server, stopper, metric.NewRegistry(), storeCfg.DefaultZoneConfig,
	)
	storeCfg.ScanMaxIdleTime = 1 * time.Second
	stores := kvserver.NewStores(ac, storeCfg.Clock)

	if err := storeCfg.Gossip.SetNodeDescriptor(nodeDesc); err != nil {
		t.Fatal(err)
	}

	retryOpts := base.DefaultRetryOptions()
	retryOpts.Closer = stopper.ShouldQuiesce()
	distSender := kvcoord.NewDistSender(kvcoord.DistSenderConfig{
		AmbientCtx:         ac,
		Settings:           storeCfg.Settings,
		Clock:              storeCfg.Clock,
		NodeDescs:          storeCfg.Gossip,
		RPCContext:         rpcContext,
		RPCRetryOptions:    &retryOpts,
		FirstRangeProvider: storeCfg.Gossip,
		TestingKnobs: kvcoord.ClientTestingKnobs{
			TransportFactory: kvcoord.SenderTransportFactory(tracer, stores),
		},
	})

	tcsFactory := kvcoord.NewTxnCoordSenderFactory(
		kvcoord.TxnCoordSenderFactoryConfig{
			AmbientCtx: ac,
			Settings:   storeCfg.Settings,
			Clock:      storeCfg.Clock,
			Stopper:    stopper,
		},
		distSender,
	)
	storeCfg.DB = kv.NewDB(ac, tcsFactory, storeCfg.Clock, stopper)
	storeCfg.StorePool = kvserver.NewTestStorePool(storeCfg)
	storeCfg.Transport = kvserver.NewDummyRaftTransport(storeCfg.Settings)
	// TODO(bdarnell): arrange to have the transport closed.
	ctx := context.Background()
	if !opts.dontBootstrap {
		require.NoError(t, kvserver.WriteClusterVersion(ctx, eng, clusterversion.TestingClusterVersion))
		if err := kvserver.InitEngine(
			ctx, eng, roachpb.StoreIdent{NodeID: 1, StoreID: 1},
		); err != nil {
			t.Fatal(err)
		}
	}
	store := kvserver.NewStore(ctx, storeCfg, eng, nodeDesc)
	if !opts.dontBootstrap {
		var kvs []roachpb.KeyValue
		var splits []roachpb.RKey
		kvs, tableSplits := bootstrap.MakeMetadataSchema(
			keys.SystemSQLCodec, storeCfg.DefaultZoneConfig, storeCfg.DefaultSystemZoneConfig,
		).GetInitialValues()
		if !opts.dontCreateSystemRanges {
			splits = config.StaticSplits()
			splits = append(splits, tableSplits...)
			sort.Slice(splits, func(i, j int) bool {
				return splits[i].Less(splits[j])
			})
		}
		err := kvserver.WriteInitialClusterData(
			ctx,
			eng,
			kvs, /* initialValues */
			clusterversion.TestingBinaryVersion,
			1 /* numStores */, splits, storeCfg.Clock.PhysicalNow(),
			storeCfg.TestingKnobs,
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Start(ctx, stopper); err != nil {
		t.Fatal(err)
	}
	stores.AddStore(store)

	// Connect to gossip and gossip the store's capacity.
	<-store.Gossip().Connected
	if err := store.GossipStore(ctx, false /* useCached */); err != nil {
		t.Fatal(err)
	}
	// Wait for the store's single range to have quorum before proceeding.
	repl := store.LookupReplica(roachpb.RKeyMin)

	// Send a request through the range to make sure everything is warmed up
	// and works.
	// NB: it's unclear if this code is necessary.
	var ba roachpb.BatchRequest
	get := roachpb.GetRequest{}
	get.Key = keys.LocalMax
	ba.Header.Replica = repl.Desc().Replicas().VoterDescriptors()[0]
	ba.Header.RangeID = repl.RangeID
	ba.Add(&get)
	_, pErr := store.Send(ctx, ba)
	require.NoError(t, pErr.GoError())

	// Wait for the system config to be available in gossip. All sorts of things
	// might not work properly while the system config is not available.
	testutils.SucceedsSoon(t, func() error {
		if cfg := store.Gossip().GetSystemConfig(); cfg == nil {
			return errors.Errorf("system config not available in gossip yet")
		}
		return nil
	})

	// Make all the initial ranges part of replication queue purgatory. This is
	// similar to what a real cluster does after bootstrap - we want the initial
	// ranges to up-replicate as soon as other nodes join.
	if err := store.ForceReplicationScanAndProcess(); err != nil {
		t.Fatal(err)
	}

	return store
}

// getArgs returns a GetRequest and GetResponse pair addressed to
// the default replica for the specified key.
func getArgs(key roachpb.Key) *roachpb.GetRequest {
	return &roachpb.GetRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
	}
}

// putArgs returns a PutRequest and PutResponse pair addressed to
// the default replica for the specified key / value.
func putArgs(key roachpb.Key, value []byte) *roachpb.PutRequest {
	return &roachpb.PutRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Value: roachpb.MakeValueFromBytes(value),
	}
}

// incrementArgs returns an IncrementRequest addressed to the default replica
// for the specified key.
func incrementArgs(key roachpb.Key, inc int64) *roachpb.IncrementRequest {
	return &roachpb.IncrementRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Increment: inc,
	}
}

func truncateLogArgs(index uint64, rangeID roachpb.RangeID) *roachpb.TruncateLogRequest {
	return &roachpb.TruncateLogRequest{
		Index:   index,
		RangeID: rangeID,
	}
}

func heartbeatArgs(
	txn *roachpb.Transaction, now hlc.Timestamp,
) (*roachpb.HeartbeatTxnRequest, roachpb.Header) {
	return &roachpb.HeartbeatTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
		Now: now,
	}, roachpb.Header{Txn: txn}
}

func pushTxnArgs(
	pusher, pushee *roachpb.Transaction, pushType roachpb.PushTxnType,
) *roachpb.PushTxnRequest {
	return &roachpb.PushTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: pushee.Key,
		},
		PushTo:    pusher.WriteTimestamp.Next(),
		PusherTxn: *pusher,
		PusheeTxn: pushee.TxnMeta,
		PushType:  pushType,
	}
}

func migrateArgs(start, end roachpb.Key, version roachpb.Version) *roachpb.MigrateRequest {
	return &roachpb.MigrateRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    start,
			EndKey: end,
		},
		Version: version,
	}
}

func adminTransferLeaseArgs(key roachpb.Key, target roachpb.StoreID) roachpb.Request {
	return &roachpb.AdminTransferLeaseRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Target: target,
	}
}

func verifyRangeStats(
	reader storage.Reader, rangeID roachpb.RangeID, expMS enginepb.MVCCStats,
) error {
	ms, err := stateloader.Make(rangeID).LoadMVCCStats(context.Background(), reader)
	if err != nil {
		return err
	}
	// When used with a real wall clock these will not be the same, since it
	// takes time to load stats.
	expMS.AgeTo(ms.LastUpdateNanos)
	// Clear system counts as these are expected to vary.
	ms.SysBytes, ms.SysCount, ms.AbortSpanBytes = 0, 0, 0
	if ms != expMS {
		return errors.Errorf("expected and actual stats differ:\n%s", pretty.Diff(expMS, ms))
	}
	return nil
}

func verifyRecomputedStats(
	reader storage.Reader, d *roachpb.RangeDescriptor, expMS enginepb.MVCCStats, nowNanos int64,
) error {
	ms, err := rditer.ComputeStatsForRange(d, reader, nowNanos)
	if err != nil {
		return err
	}
	// When used with a real wall clock these will not be the same, since it
	// takes time to load stats.
	expMS.AgeTo(ms.LastUpdateNanos)
	if expMS != ms {
		return fmt.Errorf("expected range's stats to agree with recomputation: got\n%+v\nrecomputed\n%+v", expMS, ms)
	}
	return nil
}

func waitForTombstone(
	t *testing.T, reader storage.Reader, rangeID roachpb.RangeID,
) (tombstone roachpb.RangeTombstone) {
	testutils.SucceedsSoon(t, func() error {
		tombstoneKey := keys.RangeTombstoneKey(rangeID)
		ok, err := storage.MVCCGetProto(
			context.Background(), reader, tombstoneKey, hlc.Timestamp{}, &tombstone, storage.MVCCGetOptions{},
		)
		if err != nil {
			t.Fatalf("failed to read tombstone: %v", err)
		}
		if !ok {
			return fmt.Errorf("tombstone not found for range %d", rangeID)
		}
		return nil
	})
	return tombstone
}
