// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/gossip/simulation"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	//
	// Meta RangeDescriptors
	//
	testMetaEndKey = roachpb.RKey(keys.SystemPrefix)
	// single meta1 and meta2 range with one replica.
	testMetaRangeDescriptor = roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKeyMin,
		EndKey:   testMetaEndKey,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}

	//
	// User-Space RangeDescriptors
	//
	// single user-space descriptor with one replica.
	testUserRangeDescriptor = roachpb.RangeDescriptor{
		RangeID:  2,
		StartKey: testMetaEndKey,
		EndKey:   roachpb.RKeyMax,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	// single user-space descriptor with three replicas.
	testUserRangeDescriptor3Replicas = roachpb.RangeDescriptor{
		RangeID:  2,
		StartKey: testMetaEndKey,
		EndKey:   roachpb.RKeyMax,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				ReplicaID: 1,
				NodeID:    1,
				StoreID:   1,
			},
			{
				ReplicaID: 2,
				NodeID:    2,
				StoreID:   2,
			},
			{
				ReplicaID: 3,
				NodeID:    3,
				StoreID:   3,
			},
		},
	}
)

var testAddress = util.NewUnresolvedAddr("tcp", "node1")

// simpleSendFn is the function type used to dispatch RPC calls in simpleTransportAdapter
type simpleSendFn func(
	context.Context,
	SendOptions,
	ReplicaSlice,
	roachpb.BatchRequest,
) (*roachpb.BatchResponse, error)

// stubRPCSendFn is an rpcSendFn that simply creates a reply for the
// BatchRequest without performing an RPC call or triggering any
// test instrumentation.
var stubRPCSendFn simpleSendFn = func(
	_ context.Context, _ SendOptions, _ ReplicaSlice, args roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	return args.CreateReply(), nil
}

// adaptSimpleTransport converts the RPCSend functions used in these
// tests to the newer transport interface.
func adaptSimpleTransport(fn simpleSendFn) TransportFactory {
	return func(
		opts SendOptions,
		nodeDialer *nodedialer.Dialer,
		replicas ReplicaSlice,
	) (Transport, error) {
		return &simpleTransportAdapter{
			fn:       fn,
			opts:     opts,
			replicas: replicas}, nil
	}
}

type simpleTransportAdapter struct {
	fn          simpleSendFn
	opts        SendOptions
	replicas    ReplicaSlice
	nextReplica int
}

func (l *simpleTransportAdapter) IsExhausted() bool {
	return l.nextReplica >= len(l.replicas)
}

func (l *simpleTransportAdapter) SendNext(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	ba.Replica = l.replicas[l.nextReplica].ReplicaDescriptor
	l.nextReplica++
	return l.fn(ctx, l.opts, l.replicas, ba)
}

func (l *simpleTransportAdapter) NextInternalClient(
	ctx context.Context,
) (context.Context, roachpb.InternalClient, error) {
	panic("unimplemented")
}

func (l *simpleTransportAdapter) NextReplica() roachpb.ReplicaDescriptor {
	if !l.IsExhausted() {
		return l.replicas[l.nextReplica].ReplicaDescriptor
	}
	return roachpb.ReplicaDescriptor{}
}

func (*simpleTransportAdapter) MoveToFront(roachpb.ReplicaDescriptor) {
}

func makeGossip(t *testing.T, stopper *stop.Stopper, rpcContext *rpc.Context) *gossip.Gossip {
	server := rpc.NewServer(rpcContext)

	const nodeID = 1
	g := gossip.NewTest(nodeID, rpcContext, server, stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())
	if err := g.SetNodeDescriptor(newNodeDesc(nodeID)); err != nil {
		t.Fatal(err)
	}
	if err := g.AddInfo(gossip.KeySentinel, nil, time.Hour); err != nil {
		t.Fatal(err)
	}

	return g
}

func newNodeDesc(nodeID roachpb.NodeID) *roachpb.NodeDescriptor {
	return &roachpb.NodeDescriptor{
		NodeID:  nodeID,
		Address: util.MakeUnresolvedAddr("tcp", fmt.Sprintf("invalid.invalid:%d", nodeID)),
	}
}

// TestSendRPCOrder verifies that sendRPC correctly takes into account the
// lease holder, attributes and required consistency to determine where to send
// remote requests.
func TestSendRPCOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	rangeID := roachpb.RangeID(99)

	nodeTiers := map[int32][]roachpb.Tier{
		1: {}, // The local node, set in each test case.
		2: {roachpb.Tier{Key: "country", Value: "us"}, roachpb.Tier{Key: "region", Value: "west"}},
		3: {roachpb.Tier{Key: "country", Value: "eu"}, roachpb.Tier{Key: "city", Value: "dublin"}},
		4: {roachpb.Tier{Key: "country", Value: "us"}, roachpb.Tier{Key: "region", Value: "east"}, roachpb.Tier{Key: "city", Value: "nyc"}},
		5: {roachpb.Tier{Key: "country", Value: "us"}, roachpb.Tier{Key: "region", Value: "east"}, roachpb.Tier{Key: "city", Value: "mia"}},
	}

	// Gets filled below to identify the replica by its address.
	makeVerifier := func(expAddrs []roachpb.NodeID) func(SendOptions, ReplicaSlice) error {
		return func(o SendOptions, replicas ReplicaSlice) error {
			var actualAddrs []roachpb.NodeID
			for i, r := range replicas {
				if len(expAddrs) <= i {
					return errors.Errorf("got unexpected address: %s", r.NodeDesc.Address)
				}
				if expAddrs[i] == 0 {
					actualAddrs = append(actualAddrs, 0)
				} else {
					actualAddrs = append(actualAddrs, r.NodeDesc.NodeID)
				}
			}
			if !reflect.DeepEqual(expAddrs, actualAddrs) {
				return errors.Errorf("expected %d, but found %d", expAddrs, actualAddrs)
			}
			return nil
		}
	}

	testCases := []struct {
		args        roachpb.Request
		tiers       []roachpb.Tier
		expReplica  []roachpb.NodeID
		leaseHolder int32 // 0 for not caching a lease holder.
		// Naming is somewhat off, as eventually consistent reads usually
		// do not have to go to the lease holder when a node has a read lease.
		// Would really want CONSENSUS here, but that is not implemented.
		// Likely a test setup here will never have a read lease, but good
		// to keep in mind.
		consistent bool
	}{
		// Inconsistent Scan without matching attributes.
		{
			args:       &roachpb.ScanRequest{},
			tiers:      []roachpb.Tier{},
			expReplica: []roachpb.NodeID{1, 2, 3, 4, 5},
		},
		// Inconsistent Scan with matching attributes.
		// Should move the two nodes matching the attributes to the front and
		// go stable.
		{
			args:  &roachpb.ScanRequest{},
			tiers: nodeTiers[5],
			// Compare only the first two resulting addresses.
			expReplica: []roachpb.NodeID{5, 4, 0, 0, 0},
		},

		// Scan without matching attributes that requires but does not find
		// a lease holder.
		{
			args:       &roachpb.ScanRequest{},
			tiers:      []roachpb.Tier{},
			expReplica: []roachpb.NodeID{1, 2, 3, 4, 5},
			consistent: true,
		},
		// Put without matching attributes that requires but does not find lease holder.
		// Should go random and not change anything.
		{
			args:       &roachpb.PutRequest{},
			tiers:      []roachpb.Tier{{Key: "nomatch", Value: ""}},
			expReplica: []roachpb.NodeID{1, 2, 3, 4, 5},
		},
		// Put with matching attributes but no lease holder.
		// Should move the two nodes matching the attributes to the front.
		{
			args:  &roachpb.PutRequest{},
			tiers: append(nodeTiers[5], roachpb.Tier{Key: "irrelevant", Value: ""}),
			// Compare only the first two resulting addresses.
			expReplica: []roachpb.NodeID{5, 4, 0, 0, 0},
		},
		// Put with matching attributes that finds the lease holder (node 2).
		// Should address the lease holder and the two nodes matching the attributes
		// (the last and second to last) in that order.
		{
			args:  &roachpb.PutRequest{},
			tiers: append(nodeTiers[5], roachpb.Tier{Key: "irrelevant", Value: ""}),
			// Compare only the first resulting address as we have a lease holder
			// and that means we're only trying to send there.
			expReplica:  []roachpb.NodeID{2, 0, 0, 0, 0},
			leaseHolder: 2,
		},
		// Inconsistent Get without matching attributes but lease holder (node 3). Should just
		// go random as the lease holder does not matter.
		{
			args:        &roachpb.GetRequest{},
			tiers:       []roachpb.Tier{},
			expReplica:  []roachpb.NodeID{1, 2, 3, 4, 5},
			leaseHolder: 2,
		},
	}

	descriptor := roachpb.RangeDescriptor{
		StartKey:      roachpb.RKeyMin,
		EndKey:        roachpb.RKeyMax,
		RangeID:       rangeID,
		NextReplicaID: 1,
	}
	for i := int32(1); i <= 5; i++ {
		addr := util.MakeUnresolvedAddr("tcp", fmt.Sprintf("node%d:1", i))
		nd := &roachpb.NodeDescriptor{
			NodeID:  roachpb.NodeID(i),
			Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
			Locality: roachpb.Locality{
				Tiers: nodeTiers[i],
			},
		}
		if err := g.AddInfoProto(gossip.MakeNodeIDKey(roachpb.NodeID(i)), nd, time.Hour); err != nil {
			t.Fatal(err)
		}
		descriptor.AddReplica(roachpb.NodeID(i), roachpb.StoreID(i), roachpb.VOTER_FULL)
	}

	// Stub to be changed in each test case.
	var verifyCall func(SendOptions, ReplicaSlice) error

	var testFn simpleSendFn = func(
		_ context.Context,
		opts SendOptions,
		replicas ReplicaSlice,
		args roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		if err := verifyCall(opts, replicas); err != nil {
			return nil, err
		}
		return args.CreateReply(), nil
	}

	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(testFn),
		},
		RangeDescriptorDB: mockRangeDescriptorDBForDescs(descriptor),
		NodeDialer:        nodedialer.New(rpcContext, gossip.AddressResolver(g)),
		Settings:          cluster.MakeTestingClusterSettings(),
	}

	ds := NewDistSender(cfg)

	for n, tc := range testCases {
		t.Run("", func(t *testing.T) {
			verifyCall = makeVerifier(tc.expReplica)

			{
				// The local node needs to get its attributes during sendRPC.
				nd := &roachpb.NodeDescriptor{
					NodeID:  6,
					Address: util.MakeUnresolvedAddr("tcp", fmt.Sprintf("invalid.invalid:6")),
					Locality: roachpb.Locality{
						Tiers: tc.tiers,
					},
				}
				g.NodeID.Reset(nd.NodeID)
				if err := g.SetNodeDescriptor(nd); err != nil {
					t.Fatal(err)
				}
			}

			ds.rangeCache.Clear()
			var lease roachpb.Lease
			if tc.leaseHolder != 0 {
				lease.Replica = descriptor.InternalReplicas[tc.leaseHolder-1]
			}
			ds.rangeCache.Insert(ctx, roachpb.RangeInfo{
				Desc:  descriptor,
				Lease: lease,
			})

			args := tc.args
			{
				header := args.Header()
				header.Key = roachpb.Key("a")
				args.SetHeader(header)
			}
			if roachpb.IsRange(args) {
				header := args.Header()
				header.EndKey = args.Header().Key.Next()
				args.SetHeader(header)
			}
			consistency := roachpb.CONSISTENT
			if !tc.consistent {
				consistency = roachpb.INCONSISTENT
			}
			// Kill the cached NodeDescriptor, enforcing a lookup from Gossip.
			ds.nodeDescriptor = nil
			if _, err := kv.SendWrappedWith(ctx, ds, roachpb.Header{
				RangeID:         rangeID, // Not used in this test, but why not.
				ReadConsistency: consistency,
			}, args); err != nil {
				t.Errorf("%d: %s", n, err)
			}
		})
	}
}

// MockRangeDescriptorDB is an implementation of RangeDescriptorDB. Unlike
// DistSender's implementation, MockRangeDescriptorDB does not call back into
// the RangeDescriptorCache by default to perform RangeLookups. Because of this,
// tests should not rely on that behavior and should implement it themselves if
// they need it.
type MockRangeDescriptorDB func(roachpb.RKey, bool) (rs, preRs []roachpb.RangeDescriptor, err error)

func (mdb MockRangeDescriptorDB) RangeLookup(
	ctx context.Context, key roachpb.RKey, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error) {
	return mdb(key, useReverseScan)
}

func (mdb MockRangeDescriptorDB) FirstRange() (*roachpb.RangeDescriptor, error) {
	rs, _, err := mdb(roachpb.RKey(roachpb.KeyMin), false)
	if err != nil || len(rs) == 0 {
		return nil, err
	}
	return &rs[0], nil
}

// withMetaRecursion returns a new MockRangeDescriptorDB that will behave the
// same as the receiver, but will also recurse into the provided
// RangeDescriptorCache on each lookup to simulate the use of a descriptor's
// parent descriptor during the RangeLookup scan. This is important for tests
// that expect the RangeLookup for a user space descriptor to trigger a lookup
// for a meta descriptor.
func (mdb MockRangeDescriptorDB) withMetaRecursion(
	rdc *RangeDescriptorCache,
) MockRangeDescriptorDB {
	return func(key roachpb.RKey, useReverseScan bool) (rs, preRs []roachpb.RangeDescriptor, err error) {
		metaKey := keys.RangeMetaKey(key)
		if !metaKey.Equal(roachpb.RKeyMin) {
			_, err := rdc.LookupWithEvictionToken(context.Background(), metaKey, EvictionToken{}, useReverseScan)
			if err != nil {
				return nil, nil, err
			}
		}
		return mdb(key, useReverseScan)
	}
}

// withMetaRecursion calls MockRangeDescriptorDB.withMetaRecursion on the
// DistSender's RangeDescriptorDB.
func (ds *DistSender) withMetaRecursion() *DistSender {
	ds.rangeCache.db = ds.rangeCache.db.(MockRangeDescriptorDB).withMetaRecursion(ds.rangeCache)
	return ds
}

func mockRangeDescriptorDBForDescs(descs ...roachpb.RangeDescriptor) MockRangeDescriptorDB {
	return MockRangeDescriptorDB(func(key roachpb.RKey, useReverseScan bool) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error) {
		var matchingDescs []roachpb.RangeDescriptor
		for _, desc := range descs {
			contains := desc.ContainsKey
			if useReverseScan {
				contains = desc.ContainsKeyInverted
			}
			if contains(key) {
				matchingDescs = append(matchingDescs, desc)
			}
		}
		switch len(matchingDescs) {
		case 0:
			panic(fmt.Sprintf("found no matching descriptors for key %s", key))
		case 1:
			return matchingDescs, nil, nil
		default:
			panic(fmt.Sprintf("found multiple matching descriptors for key %s: %v", key, matchingDescs))
		}
	})
}

var defaultMockRangeDescriptorDB = mockRangeDescriptorDBForDescs(
	testMetaRangeDescriptor,
	testUserRangeDescriptor,
)
var threeReplicaMockRangeDescriptorDB = mockRangeDescriptorDBForDescs(
	testMetaRangeDescriptor,
	testUserRangeDescriptor3Replicas,
)

func TestImmutableBatchArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	var testFn simpleSendFn = func(
		_ context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		args roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		reply := args.CreateReply()
		reply.Txn = args.Txn.Clone()
		reply.Txn.WriteTimestamp = hlc.MaxTimestamp
		return reply, nil
	}

	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(testFn),
		},
		RangeDescriptorDB: defaultMockRangeDescriptorDB,
		NodeDialer:        nodedialer.New(rpcContext, gossip.AddressResolver(g)),
		Settings:          cluster.MakeTestingClusterSettings(),
	}

	ds := NewDistSender(cfg)

	txn := roachpb.MakeTransaction(
		"test", nil /* baseKey */, roachpb.NormalUserPriority,
		clock.Now(), clock.MaxOffset().Nanoseconds(),
	)
	origTxnTs := txn.WriteTimestamp

	// An optimization does copy-on-write if we haven't observed anything,
	// so make sure we're not in that case.
	txn.UpdateObservedTimestamp(1, hlc.MaxTimestamp)

	put := roachpb.NewPut(roachpb.Key("don't"), roachpb.Value{})
	if _, pErr := kv.SendWrappedWith(context.Background(), ds, roachpb.Header{
		Txn: &txn,
	}, put); pErr != nil {
		t.Fatal(pErr)
	}

	if txn.WriteTimestamp != origTxnTs {
		t.Fatal("Transaction was mutated by DistSender")
	}
}

// TestRetryOnNotLeaseHolderError verifies that the DistSender correctly updates
// the leaseholder in the range cache and retries when receiving a
// NotLeaseHolderError.
func TestRetryOnNotLeaseHolderError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	recognizedLeaseHolder := testUserRangeDescriptor3Replicas.Replicas().Voters()[1]
	unrecognizedLeaseHolder := roachpb.ReplicaDescriptor{
		NodeID:  99,
		StoreID: 999,
	}

	testutils.RunTrueAndFalse(t, "leaseholder in descriptor", func(t *testing.T, lhRecognized bool) {
		stopper := stop.NewStopper()
		defer stopper.Stop(context.Background())

		clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
		rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
		g := makeGossip(t, stopper, rpcContext)
		for _, n := range testUserRangeDescriptor3Replicas.Replicas().Voters() {
			require.NoError(t, g.AddInfoProto(
				gossip.MakeNodeIDKey(n.NodeID),
				newNodeDesc(n.NodeID),
				gossip.NodeDescriptorTTL,
			))
		}

		first := true

		var testFn simpleSendFn = func(
			_ context.Context,
			_ SendOptions,
			_ ReplicaSlice,
			args roachpb.BatchRequest,
		) (*roachpb.BatchResponse, error) {
			reply := &roachpb.BatchResponse{}
			if first {
				leaseHolder := recognizedLeaseHolder
				if !lhRecognized {
					leaseHolder = unrecognizedLeaseHolder
				}

				reply.Error = roachpb.NewError(
					&roachpb.NotLeaseHolderError{LeaseHolder: &leaseHolder})
				first = false
				return reply, nil
			}
			// Return an error to avoid activating a code path that would update the
			// cache with the leaseholder from the successful response. That's not
			// what this test wants to test.
			reply.Error = roachpb.NewErrorf("boom")
			return reply, nil
		}

		cfg := DistSenderConfig{
			AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
			Clock:      clock,
			NodeDescs:  g,
			RPCContext: rpcContext,
			TestingKnobs: ClientTestingKnobs{
				TransportFactory: adaptSimpleTransport(testFn),
			},
			RangeDescriptorDB: threeReplicaMockRangeDescriptorDB,
			NodeDialer:        nodedialer.New(rpcContext, gossip.AddressResolver(g)),
			Settings:          cluster.MakeTestingClusterSettings(),
		}
		ds := NewDistSender(cfg)
		v := roachpb.MakeValueFromString("value")
		put := roachpb.NewPut(roachpb.Key("a"), v)
		if _, pErr := kv.SendWrapped(context.Background(), ds, put); !testutils.IsPError(pErr, "boom") {
			t.Fatalf("unexpected error: %v", pErr)
		}
		if first {
			t.Fatal("the request did not retry")
		}
		rng := ds.rangeCache.GetCached(testUserRangeDescriptor.StartKey, false /* inverted */)
		require.NotNil(t, rng)

		if lhRecognized {
			require.Equal(t, recognizedLeaseHolder, rng.Lease.Replica)
		} else {
			require.Equal(t, roachpb.ReplicaDescriptor{}, rng.Lease.Replica)
		}
	})
}

// TestBackoffOnNotLeaseHolderErrorDuringTransfer verifies that the DistSender
// backs off upon receiving multiple NotLeaseHolderErrors without observing an
// increase in LeaseSequence.
func TestBackoffOnNotLeaseHolderErrorDuringTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	leaseHolders := testUserRangeDescriptor3Replicas.InternalReplicas
	for _, n := range leaseHolders {
		if err := g.AddInfoProto(
			gossip.MakeNodeIDKey(n.NodeID),
			newNodeDesc(n.NodeID),
			gossip.NodeDescriptorTTL,
		); err != nil {
			t.Fatal(err)
		}
	}
	var sequences []roachpb.LeaseSequence
	var testFn simpleSendFn = func(
		_ context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		args roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		reply := &roachpb.BatchResponse{}
		if len(sequences) > 0 {
			seq := sequences[0]
			sequences = sequences[1:]
			lease := roachpb.Lease{
				Sequence: seq,
				Replica:  leaseHolders[int(seq)%2],
			}
			reply.Error = roachpb.NewError(
				&roachpb.NotLeaseHolderError{
					Replica:     leaseHolders[int(seq)%2],
					LeaseHolder: &leaseHolders[(int(seq)+1)%2],
					Lease:       &lease,
				})
			return reply, nil
		}
		// Return an error to bail out of retries.
		reply.Error = roachpb.NewErrorf("boom")
		return reply, nil
	}

	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(testFn),
		},
		RangeDescriptorDB: threeReplicaMockRangeDescriptorDB,
		NodeDialer:        nodedialer.New(rpcContext, gossip.AddressResolver(g)),
		RPCRetryOptions: &retry.Options{
			InitialBackoff: time.Microsecond,
			MaxBackoff:     time.Microsecond,
		},
		Settings: cluster.MakeTestingClusterSettings(),
	}
	for i, c := range []struct {
		leaseSequences []roachpb.LeaseSequence
		expected       int64
	}{
		{[]roachpb.LeaseSequence{2, 1, 2, 3}, 2},
		{[]roachpb.LeaseSequence{0}, 0},
		{[]roachpb.LeaseSequence{2, 1, 2, 3, 2}, 3},
	} {
		t.Run("", func(t *testing.T) {
			sequences = c.leaseSequences
			ds := NewDistSender(cfg)
			v := roachpb.MakeValueFromString("value")
			put := roachpb.NewPut(roachpb.Key("a"), v)
			if _, pErr := kv.SendWrapped(ctx, ds, put); !testutils.IsPError(pErr, "boom") {
				t.Fatalf("%d: unexpected error: %v", i, pErr)
			}
			if got := ds.Metrics().InLeaseTransferBackoffs.Count(); got != c.expected {
				t.Fatalf("%d: expected %d backoffs, got %d", i, c.expected, got)
			}
		})
	}
}

// This test verifies that when we have a cached leaseholder that is down
// it is ejected from the cache.
func TestDistSenderDownNodeEvictLeaseholder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	if err := g.AddInfoProto(
		gossip.MakeNodeIDKey(roachpb.NodeID(2)),
		newNodeDesc(2),
		gossip.NodeDescriptorTTL,
	); err != nil {
		t.Fatal(err)
	}

	var contacted1, contacted2 bool

	transport := func(
		ctx context.Context,
		opts SendOptions,
		replicas ReplicaSlice,
		ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		switch ba.Replica.StoreID {
		case 1:
			contacted1 = true
			return nil, errors.New("mock RPC error")
		case 2:
			contacted2 = true
			return ba.CreateReply(), nil
		default:
			panic("unexpected replica: " + ba.Replica.String())
		}
	}

	desc := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKeyMax,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
			{
				NodeID:  2,
				StoreID: 2,
			},
		},
	}
	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(transport),
		},
		RangeDescriptorDB: mockRangeDescriptorDBForDescs(desc),
		NodeDialer:        nodedialer.New(rpcContext, gossip.AddressResolver(g)),
		Settings:          cluster.MakeTestingClusterSettings(),
	}

	ds := NewDistSender(cfg)
	var lease roachpb.Lease
	lease.Replica = roachpb.ReplicaDescriptor{StoreID: 1}
	ds.rangeCache.Insert(ctx, roachpb.RangeInfo{
		Desc:  desc,
		Lease: lease,
	})

	var ba roachpb.BatchRequest
	ba.RangeID = 1
	get := &roachpb.GetRequest{}
	get.Key = roachpb.Key("a")
	ba.Add(get)

	if _, pErr := ds.Send(ctx, ba); pErr != nil {
		t.Fatal(pErr)
	}

	if !contacted1 || !contacted2 {
		t.Errorf("contacted n1: %t, contacted n2: %t", contacted1, contacted2)
	}

	rng := ds.rangeCache.GetCached(testUserRangeDescriptor.StartKey, false /* inverted */)
	require.Equal(t, roachpb.StoreID(2), rng.Lease.Replica.StoreID)
}

// TestRetryOnDescriptorLookupError verifies that the DistSender retries a descriptor
// lookup on any error.
func TestRetryOnDescriptorLookupError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)

	errs := []error{
		errors.New("boom"),
		nil,
	}

	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(stubRPCSendFn),
		},
		RangeDescriptorDB: MockRangeDescriptorDB(func(key roachpb.RKey, _ bool) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error) {
			// Don't return an error on the FirstRange lookup.
			if key.Equal(roachpb.KeyMin) {
				return []roachpb.RangeDescriptor{testMetaRangeDescriptor}, nil, nil
			}

			// Return next error and truncate the prefix of the errors array.
			err := errs[0]
			errs = errs[1:]
			return []roachpb.RangeDescriptor{testUserRangeDescriptor}, nil, err
		}),
		NodeDialer: nodedialer.New(rpcContext, gossip.AddressResolver(g)),
		Settings:   cluster.MakeTestingClusterSettings(),
	}
	ds := NewDistSender(cfg)
	put := roachpb.NewPut(roachpb.Key("a"), roachpb.MakeValueFromString("value"))
	// Error on descriptor lookup, second attempt successful.
	if _, pErr := kv.SendWrapped(context.Background(), ds, put); pErr != nil {
		t.Errorf("unexpected error: %s", pErr)
	}
	if len(errs) != 0 {
		t.Fatalf("expected more descriptor lookups, leftover errs: %+v", errs)
	}
}

// TestEvictOnFirstRangeGossip verifies that we evict the first range
// descriptor from the descriptor cache when a gossip update is received for
// the first range.
func TestEvictOnFirstRangeGossip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)

	sender := func(
		_ context.Context, ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error) {
		return ba.CreateReply(), nil
	}

	desc := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKeyMax,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}

	var numFirstRange int32
	rDB := MockRangeDescriptorDB(func(key roachpb.RKey, _ bool) (
		[]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error,
	) {
		if key.Equal(roachpb.KeyMin) {
			atomic.AddInt32(&numFirstRange, 1)
		}
		return []roachpb.RangeDescriptor{desc}, nil, nil
	})

	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: SenderTransportFactory(
				tracing.NewTracer(),
				kv.SenderFunc(sender),
			),
		},
		// Provide both FirstRangeProvider and RangeDescriptorDB to listen to
		// changes to the first range while still using a MockRangeDescriptorDB.
		FirstRangeProvider: g,
		RangeDescriptorDB:  rDB,
		NodeDialer:         nodedialer.New(rpcContext, gossip.AddressResolver(g)),
		Settings:           cluster.MakeTestingClusterSettings(),
	}

	ds := NewDistSender(cfg).withMetaRecursion()

	anyKey := roachpb.Key("anything")
	rAnyKey := keys.MustAddr(anyKey)

	call := func() {
		if _, err := ds.rangeCache.LookupWithEvictionToken(
			context.Background(), rAnyKey, EvictionToken{}, false,
		); err != nil {
			t.Fatal(err)
		}
	}

	// Perform multiple calls and check that the first range is only looked up
	// once, with subsequent calls hitting the cache.
	//
	// This potentially races with the cache-evicting gossip callback on the
	// first range, so it is important that the first range descriptor's state
	// in gossip is stable from this point forward.
	for i := 0; i < 3; i++ {
		call()
		if num := atomic.LoadInt32(&numFirstRange); num != 1 {
			t.Fatalf("expected one first range lookup, got %d", num)
		}
	}
	// Tweak the descriptor so that the gossip callback will be invoked.
	desc.Generation = 1
	if err := g.AddInfoProto(gossip.KeyFirstRangeDescriptor, &desc, 0); err != nil {
		t.Fatal(err)
	}

	// Once Gossip fires the callbacks, we should see a cache eviction and thus,
	// a new cache hit.
	testutils.SucceedsSoon(t, func() error {
		call()
		if exp, act := int32(2), atomic.LoadInt32(&numFirstRange); exp != act {
			return errors.Errorf("expected %d first range lookups, got %d", exp, act)
		}
		return nil
	})
}

func TestEvictCacheOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The first attempt gets a BatchResponse with replicaError in the header, if
	// replicaError set. If not set, the first attempt gets an RPC error. The
	// second attempt, if any, succeeds.
	// Currently lease holder and cached range descriptor are treated equally.
	// TODO(bdarnell): refactor to cover different types of retryable errors.
	const errString = "boom"

	// One of the subtests returns a RangeKeyMismatchError simulating the request
	// falling on the lhs after a split, whereas the request wanted the rhs.
	splitKey := roachpb.RKey("a")
	lhs := roachpb.RangeDescriptor{
		RangeID:  testUserRangeDescriptor.RangeID,
		StartKey: testUserRangeDescriptor.StartKey,
		EndKey:   splitKey,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	rhs := roachpb.RangeDescriptor{
		RangeID:  testUserRangeDescriptor.RangeID,
		StartKey: splitKey,
		EndKey:   testUserRangeDescriptor.EndKey,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}

	rangeMismachErr := roachpb.NewRangeKeyMismatchError(
		context.Background(), nil, nil, &lhs, nil /* lease */)
	rangeMismachErr.AppendRangeInfo(context.Background(), rhs, roachpb.Lease{})

	testCases := []struct {
		canceledCtx            bool
		replicaError           error
		shouldClearLeaseHolder bool
		shouldClearReplica     bool
	}{
		{false, errors.New(errString), false, false},         // non-retryable replica error
		{false, rangeMismachErr, false, false},               // RangeKeyMismatch replica error
		{false, &roachpb.RangeNotFoundError{}, false, false}, // RangeNotFound replica error
		{false, nil, false, false},                           // RPC error
		{true, nil, false, false},                            // canceled context
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			stopper := stop.NewStopper()
			defer stopper.Stop(context.Background())

			clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
			rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
			g := makeGossip(t, stopper, rpcContext)
			leaseHolder := roachpb.ReplicaDescriptor{
				NodeID:  99,
				StoreID: 999,
			}
			first := true

			ctx, cancel := context.WithCancel(context.Background())

			var testFn simpleSendFn = func(
				ctx context.Context,
				_ SendOptions,
				_ ReplicaSlice,
				args roachpb.BatchRequest,
			) (*roachpb.BatchResponse, error) {
				if !first {
					return args.CreateReply(), nil
				}
				first = false
				if tc.canceledCtx {
					cancel()
					return nil, ctx.Err()
				}
				if tc.replicaError == nil {
					return nil, errors.New(errString)
				}
				reply := &roachpb.BatchResponse{}
				reply.Error = roachpb.NewError(tc.replicaError)
				return reply, nil
			}

			cfg := DistSenderConfig{
				AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
				Clock:      clock,
				NodeDescs:  g,
				RPCContext: rpcContext,
				TestingKnobs: ClientTestingKnobs{
					TransportFactory: adaptSimpleTransport(testFn),
				},
				RangeDescriptorDB: defaultMockRangeDescriptorDB,
				NodeDialer:        nodedialer.New(rpcContext, gossip.AddressResolver(g)),
				Settings:          cluster.MakeTestingClusterSettings(),
			}
			ds := NewDistSender(cfg)

			var lease roachpb.Lease
			lease.Replica = leaseHolder
			ds.rangeCache.Insert(ctx, roachpb.RangeInfo{
				Desc:  testUserRangeDescriptor,
				Lease: lease,
			})

			key := roachpb.Key("b")
			put := roachpb.NewPut(key, roachpb.MakeValueFromString("value"))

			if _, pErr := kv.SendWrapped(ctx, ds, put); pErr != nil && !testutils.IsPError(pErr, errString) && !testutils.IsError(pErr.GoError(), ctx.Err().Error()) {
				t.Errorf("put encountered unexpected error: %s", pErr)
			}
			rng := ds.rangeCache.GetCached(testUserRangeDescriptor.StartKey, false /* inverted */)
			if tc.shouldClearReplica {
				require.Nil(t, rng)
			} else if tc.shouldClearLeaseHolder {
				require.True(t, rng.Lease.Empty())
			}
		})
	}
}

func TestEvictCacheOnUnknownLeaseHolder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)

	// Gossip the two nodes referred to in testUserRangeDescriptor3Replicas.
	for i := 2; i <= 3; i++ {
		nd := newNodeDesc(roachpb.NodeID(i))
		if err := g.AddInfoProto(gossip.MakeNodeIDKey(roachpb.NodeID(i)), nd, time.Hour); err != nil {
			t.Fatal(err)
		}
	}

	var count int32
	var testFn simpleSendFn = func(
		_ context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		args roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		var err error
		switch count {
		case 0, 1:
			err = &roachpb.NotLeaseHolderError{LeaseHolder: &roachpb.ReplicaDescriptor{NodeID: 99, StoreID: 999}}
		case 2:
			err = roachpb.NewRangeNotFoundError(0, 0)
		default:
			return args.CreateReply(), nil
		}
		count++
		reply := &roachpb.BatchResponse{}
		reply.Error = roachpb.NewError(err)
		return reply, nil
	}

	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(testFn),
		},
		RangeDescriptorDB: threeReplicaMockRangeDescriptorDB,
		NodeDialer:        nodedialer.New(rpcContext, gossip.AddressResolver(g)),
		Settings:          cluster.MakeTestingClusterSettings(),
	}
	ds := NewDistSender(cfg)
	key := roachpb.Key("a")
	put := roachpb.NewPut(key, roachpb.MakeValueFromString("value"))

	if _, pErr := kv.SendWrapped(context.Background(), ds, put); pErr != nil {
		t.Errorf("put encountered unexpected error: %s", pErr)
	}
	if count != 3 {
		t.Errorf("expected three retries; got %d", count)
	}
}

// TestRetryOnWrongReplicaError sets up a DistSender on a minimal gossip
// network and a mock of Send, and verifies that the DistSender correctly
// retries upon encountering a stale entry in its range descriptor cache.
func TestRetryOnWrongReplicaError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	if err := g.AddInfoProto(gossip.KeyFirstRangeDescriptor, &testMetaRangeDescriptor, time.Hour); err != nil {
		t.Fatal(err)
	}

	// Updated below, after it has first been returned.
	badEndKey := roachpb.RKey("m")
	newRangeDescriptor := testUserRangeDescriptor
	goodEndKey := newRangeDescriptor.EndKey
	newRangeDescriptor.EndKey = badEndKey
	descStale := true

	var testFn simpleSendFn = func(
		_ context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		rs, err := keys.Range(ba.Requests)
		if err != nil {
			t.Fatal(err)
		}
		if kv.TestingIsRangeLookup(ba) {
			if bytes.HasPrefix(rs.Key, keys.Meta1Prefix) {
				br := &roachpb.BatchResponse{}
				r := &roachpb.ScanResponse{}
				var kv roachpb.KeyValue
				if err := kv.Value.SetProto(&testMetaRangeDescriptor); err != nil {
					t.Fatal(err)
				}
				r.Rows = append(r.Rows, kv)
				br.Add(r)
				return br, nil
			}

			if !descStale && bytes.HasPrefix(rs.Key, keys.Meta2Prefix) {
				t.Fatalf("unexpected extra lookup for non-stale replica descriptor at %s", rs.Key)
			}

			br := &roachpb.BatchResponse{}
			r := &roachpb.ScanResponse{}
			var kv roachpb.KeyValue
			if err := kv.Value.SetProto(&newRangeDescriptor); err != nil {
				t.Fatal(err)
			}
			r.Rows = append(r.Rows, kv)
			br.Add(r)
			// If we just returned the stale descriptor, set up returning the
			// good one next time.
			if bytes.HasPrefix(rs.Key, keys.Meta2Prefix) {
				if newRangeDescriptor.EndKey.Equal(badEndKey) {
					newRangeDescriptor.EndKey = goodEndKey
				} else {
					descStale = false
				}
			}
			return br, nil
		}
		// When the Scan first turns up, update the descriptor for future
		// range descriptor lookups.
		if !newRangeDescriptor.EndKey.Equal(goodEndKey) {
			return nil, &roachpb.RangeKeyMismatchError{
				RequestStartKey: rs.Key.AsRawKey(),
				RequestEndKey:   rs.EndKey.AsRawKey(),
			}
		}
		return ba.CreateReply(), nil
	}

	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(testFn),
		},
		NodeDialer:         nodedialer.New(rpcContext, gossip.AddressResolver(g)),
		FirstRangeProvider: g,
		Settings:           cluster.MakeTestingClusterSettings(),
	}
	ds := NewDistSender(cfg)
	scan := roachpb.NewScan(roachpb.Key("a"), roachpb.Key("d"), false)
	if _, err := kv.SendWrapped(context.Background(), ds, scan); err != nil {
		t.Errorf("scan encountered error: %s", err)
	}
}

// TestRetryOnWrongReplicaErrorWithSuggestion sets up a DistSender on a
// minimal gossip network and a mock of Send, and verifies that the DistSender
// correctly retries upon encountering a stale entry in its range descriptor cache
// without needing to perform a second RangeLookup when the mismatch error
// provides a suggestion.
func TestRetryOnWrongReplicaErrorWithSuggestion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	if err := g.AddInfoProto(gossip.KeyFirstRangeDescriptor, &testMetaRangeDescriptor, time.Hour); err != nil {
		t.Fatal(err)
	}

	// The test is gonna send the request first to staleDesc, but it reaches the
	// rhsDesc, which redirects it to lhsDesc.
	staleDesc := testUserRangeDescriptor
	lhsDesc := testUserRangeDescriptor
	lhsDesc.EndKey = roachpb.RKey("m")
	lhsDesc.RangeID = staleDesc.RangeID + 1
	lhsDesc.Generation = staleDesc.Generation + 1
	rhsDesc := testUserRangeDescriptor
	rhsDesc.StartKey = roachpb.RKey("m")
	rhsDesc.RangeID = staleDesc.RangeID + 2
	rhsDesc.Generation = staleDesc.Generation + 2
	firstLookup := true

	var testFn simpleSendFn = func(
		ctx context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		rs, err := keys.Range(ba.Requests)
		if err != nil {
			t.Fatal(err)
		}
		if kv.TestingIsRangeLookup(ba) {
			if bytes.HasPrefix(rs.Key, keys.Meta1Prefix) {
				br := &roachpb.BatchResponse{}
				r := &roachpb.ScanResponse{}
				var kv roachpb.KeyValue
				if err := kv.Value.SetProto(&testMetaRangeDescriptor); err != nil {
					t.Fatal(err)
				}
				r.Rows = append(r.Rows, kv)
				br.Add(r)
				return br, nil
			}

			if !firstLookup {
				t.Fatalf("unexpected extra lookup for non-stale replica descriptor at %s", rs.Key)
			}
			firstLookup = false

			br := &roachpb.BatchResponse{}
			r := &roachpb.ScanResponse{}
			var kv roachpb.KeyValue
			if err := kv.Value.SetProto(&staleDesc); err != nil {
				t.Fatal(err)
			}
			r.Rows = append(r.Rows, kv)
			br.Add(r)
			return br, nil
		}

		// When the Scan first turns up, provide the correct descriptor as a
		// suggestion for future range descriptor lookups.
		if ba.RangeID == staleDesc.RangeID {
			var br roachpb.BatchResponse
			err := roachpb.NewRangeKeyMismatchError(ctx, rs.Key.AsRawKey(), rs.EndKey.AsRawKey(), &rhsDesc, nil /* lease */)
			err.AppendRangeInfo(ctx, lhsDesc, roachpb.Lease{})
			br.Error = roachpb.NewError(err)
			return &br, nil
		} else if ba.RangeID != lhsDesc.RangeID {
			t.Fatalf("unexpected RangeID %d provided in request %v. expected: %s", ba.RangeID, ba, lhsDesc.RangeID)
		}
		return ba.CreateReply(), nil
	}

	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(testFn),
		},
		NodeDialer:         nodedialer.New(rpcContext, gossip.AddressResolver(g)),
		FirstRangeProvider: g,
		Settings:           cluster.MakeTestingClusterSettings(),
	}
	ds := NewDistSender(cfg)
	scan := roachpb.NewScan(roachpb.Key("a"), roachpb.Key("d"), false)
	if _, err := kv.SendWrapped(context.Background(), ds, scan); err != nil {
		t.Errorf("scan encountered error: %s", err)
	}
}

func TestGetFirstRangeDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	n := simulation.NewNetwork(stopper, 3, true, zonepb.DefaultZoneConfigRef())
	for _, node := range n.Nodes {
		// TODO(spencer): remove the use of gossip/simulation here.
		node.Gossip.EnableSimulationCycler(false)
	}
	n.Start()
	ds := NewDistSender(DistSenderConfig{
		AmbientCtx:         log.AmbientContext{Tracer: tracing.NewTracer()},
		NodeDescs:          n.Nodes[0].Gossip,
		RPCContext:         n.RPCContext,
		NodeDialer:         nodedialer.New(n.RPCContext, gossip.AddressResolver(n.Nodes[0].Gossip)),
		FirstRangeProvider: n.Nodes[0].Gossip,
		Settings:           cluster.MakeTestingClusterSettings(),
	})
	if _, err := ds.FirstRange(); err == nil {
		t.Errorf("expected not to find first range descriptor")
	}
	expectedDesc := &roachpb.RangeDescriptor{}
	expectedDesc.StartKey = roachpb.RKey("a")
	expectedDesc.EndKey = roachpb.RKey("c")

	// Add first RangeDescriptor to a node different from the node for
	// this dist sender and ensure that this dist sender has the
	// information within a given time.
	if err := n.Nodes[1].Gossip.AddInfoProto(gossip.KeyFirstRangeDescriptor, expectedDesc, time.Hour); err != nil {
		t.Fatal(err)
	}
	const maxCycles = 25
	n.SimulateNetwork(func(cycle int, network *simulation.Network) bool {
		desc, err := ds.FirstRange()
		if err != nil {
			if cycle >= maxCycles {
				t.Errorf("could not get range descriptor after %d cycles", cycle)
				return false
			}
			return true
		}
		if !bytes.Equal(desc.StartKey, expectedDesc.StartKey) ||
			!bytes.Equal(desc.EndKey, expectedDesc.EndKey) {
			t.Errorf("expected first range descriptor %v, instead was %v",
				expectedDesc, desc)
		}
		return false
	})
}

// TestSendRPCRetry verifies that sendRPC failed on first address but succeed on
// second address, the second reply should be successfully returned back.
func TestSendRPCRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	if err := g.SetNodeDescriptor(newNodeDesc(1)); err != nil {
		t.Fatal(err)
	}

	// Fill RangeDescriptor with 2 replicas.
	var descriptor = roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("z"),
	}
	for i := 1; i <= 2; i++ {
		addr := util.MakeUnresolvedAddr("tcp", fmt.Sprintf("node%d", i))
		nd := &roachpb.NodeDescriptor{
			NodeID:  roachpb.NodeID(i),
			Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
		}
		if err := g.AddInfoProto(gossip.MakeNodeIDKey(roachpb.NodeID(i)), nd, time.Hour); err != nil {
			t.Fatal(err)
		}

		descriptor.InternalReplicas = append(descriptor.InternalReplicas, roachpb.ReplicaDescriptor{
			NodeID:  roachpb.NodeID(i),
			StoreID: roachpb.StoreID(i),
		})
	}
	descDB := mockRangeDescriptorDBForDescs(
		testMetaRangeDescriptor,
		descriptor,
	)

	var testFn simpleSendFn = func(
		_ context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		args roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		batchReply := &roachpb.BatchResponse{}
		reply := &roachpb.ScanResponse{}
		batchReply.Add(reply)
		reply.Rows = append([]roachpb.KeyValue{}, roachpb.KeyValue{Key: roachpb.Key("b"), Value: roachpb.Value{}})
		return batchReply, nil
	}
	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(testFn),
		},
		RangeDescriptorDB: descDB,
		Settings:          cluster.MakeTestingClusterSettings(),
	}
	ds := NewDistSender(cfg)
	scan := roachpb.NewScan(roachpb.Key("a"), roachpb.Key("d"), false)
	sr, err := kv.SendWrappedWith(context.Background(), ds, roachpb.Header{MaxSpanRequestKeys: 1}, scan)
	if err != nil {
		t.Fatal(err)
	}
	if l := len(sr.(*roachpb.ScanResponse).Rows); l != 1 {
		t.Fatalf("expected 1 row; got %d", l)
	}
}

// This test reproduces the main problem in:
// https://github.com/cockroachdb/cockroach/issues/30613.
// by verifying that if a RangeNotFoundError is returned from a Replica,
// the next Replica is tried.
func TestSendRPCRangeNotFoundError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	if err := g.SetNodeDescriptor(newNodeDesc(1)); err != nil {
		t.Fatal(err)
	}

	// Fill RangeDescriptor with three replicas.
	var descriptor = roachpb.RangeDescriptor{
		RangeID:       1,
		StartKey:      roachpb.RKey("a"),
		EndKey:        roachpb.RKey("z"),
		NextReplicaID: 1,
	}
	for i := 1; i <= 3; i++ {
		addr := util.MakeUnresolvedAddr("tcp", fmt.Sprintf("node%d", i))
		nd := &roachpb.NodeDescriptor{
			NodeID:  roachpb.NodeID(i),
			Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
		}
		if err := g.AddInfoProto(gossip.MakeNodeIDKey(roachpb.NodeID(i)), nd, time.Hour); err != nil {
			t.Fatal(err)
		}

		descriptor.AddReplica(roachpb.NodeID(i), roachpb.StoreID(i), roachpb.VOTER_FULL)
	}
	descDB := mockRangeDescriptorDBForDescs(
		testMetaRangeDescriptor,
		descriptor,
	)

	seen := map[roachpb.ReplicaID]struct{}{}
	var leaseholderStoreID roachpb.StoreID
	var ds *DistSender
	var testFn simpleSendFn = func(
		_ context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		br := ba.CreateReply()
		if _, ok := seen[ba.Replica.ReplicaID]; ok {
			br.Error = roachpb.NewErrorf("visited replica %+v twice", ba.Replica)
			return br, nil
		}
		seen[ba.Replica.ReplicaID] = struct{}{}
		if len(seen) <= 2 {
			if len(seen) == 1 {
				// Pretend that this replica is the leaseholder in the cache to verify
				// that the response evicts it.
				rng := ds.rangeCache.GetCached(descriptor.StartKey, false /* inverse */)
				rng.Lease.Replica = ba.Replica
			}
			br.Error = roachpb.NewError(roachpb.NewRangeNotFoundError(ba.RangeID, ba.Replica.StoreID))
			return br, nil
		}
		leaseholderStoreID = ba.Replica.StoreID
		return br, nil
	}
	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(testFn),
		},
		RangeDescriptorDB: descDB,
		Settings:          cluster.MakeTestingClusterSettings(),
	}
	ds = NewDistSender(cfg)
	get := roachpb.NewGet(roachpb.Key("b"))
	_, err := kv.SendWrapped(ctx, ds, get)
	if err != nil {
		t.Fatal(err)
	}

	rng := ds.rangeCache.GetCached(descriptor.StartKey, false /* inverted */)
	require.NotNil(t, rng)
	require.Equal(t, leaseholderStoreID, rng.Lease.Replica.StoreID)
}

// TestGetNodeDescriptor checks that the Node descriptor automatically gets
// looked up from Gossip.
func TestGetNodeDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	ds := NewDistSender(DistSenderConfig{
		AmbientCtx:         log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:              clock,
		NodeDescs:          g,
		RPCContext:         rpcContext,
		FirstRangeProvider: g,
		Settings:           cluster.MakeTestingClusterSettings(),
	})
	g.NodeID.Reset(5)
	if err := g.SetNodeDescriptor(newNodeDesc(5)); err != nil {
		t.Fatal(err)
	}
	testutils.SucceedsSoon(t, func() error {
		desc := ds.getNodeDescriptor()
		if desc != nil && desc.NodeID == 5 {
			return nil
		}
		return errors.Errorf("wanted NodeID 5, got %v", desc)
	})
}

func TestMultiRangeGapReverse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)

	var descs []roachpb.RangeDescriptor
	splits := []roachpb.Key{roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c"), roachpb.Key("d")}
	for i, split := range splits {
		var startKey roachpb.RKey
		if i > 0 {
			startKey = descs[i-1].EndKey
		}
		descs = append(descs, roachpb.RangeDescriptor{
			RangeID:  roachpb.RangeID(i + 1),
			StartKey: startKey,
			EndKey:   keys.MustAddr(split),
			InternalReplicas: []roachpb.ReplicaDescriptor{
				{
					NodeID:  1,
					StoreID: 1,
				},
			},
		})
	}

	sender := kv.SenderFunc(
		func(_ context.Context, args roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			rb := args.CreateReply()
			return rb, nil
		})

	rdb := MockRangeDescriptorDB(func(key roachpb.RKey, reverse bool) (
		[]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error,
	) {
		n := sort.Search(len(descs), func(i int) bool {
			if !reverse {
				return key.Less(descs[i].EndKey)
			}
			// In reverse mode, the range boundary behavior is "inverted".
			// If we scan [a,z) in reverse mode, we'd look up key z.
			return !descs[i].EndKey.Less(key) // key <= EndKey
		})
		if n < 0 {
			n = 0
		}
		if n >= len(descs) {
			panic(fmt.Sprintf("didn't set up descriptor for key %q", key))
		}
		return descs[n : n+1], nil, nil
	})

	cfg := DistSenderConfig{
		AmbientCtx:        log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:             clock,
		NodeDescs:         g,
		RPCContext:        rpcContext,
		RangeDescriptorDB: rdb,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: SenderTransportFactory(
				tracing.NewTracer(),
				sender,
			),
		},
		Settings: cluster.MakeTestingClusterSettings(),
	}

	ds := NewDistSender(cfg)

	txn := roachpb.MakeTransaction("foo", nil, 1.0, clock.Now(), 0)

	var ba roachpb.BatchRequest
	ba.Txn = &txn
	ba.Add(roachpb.NewReverseScan(splits[0], splits[1], false))
	ba.Add(roachpb.NewReverseScan(splits[2], splits[3], false))

	// Before fixing https://github.com/cockroachdb/cockroach/issues/18174, this
	// would error with:
	//
	// truncation resulted in empty batch on {b-c}: ReverseScan ["a","b"), ReverseScan ["c","d")
	if _, pErr := ds.Send(context.Background(), ba); pErr != nil {
		t.Fatal(pErr)
	}
}

// TestMultiRangeMergeStaleDescriptor simulates the situation in which the
// DistSender executes a multi-range scan which encounters the stale descriptor
// of a range which has since incorporated its right neighbor by means of a
// merge. It is verified that the DistSender scans the correct keyrange exactly
// once.
func TestMultiRangeMergeStaleDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	// Assume we have two ranges, [a-b) and [b-KeyMax).
	merged := false
	// The stale first range descriptor which is unaware of the merge.
	var firstRange = roachpb.RangeDescriptor{
		RangeID:  2,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("b"),
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	// The merged descriptor, which will be looked up after having processed
	// the stale range [a,b).
	var mergedRange = roachpb.RangeDescriptor{
		RangeID:  2,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKeyMax,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	// Assume we have two key-value pairs, a=1 and c=2.
	existingKVs := []roachpb.KeyValue{
		{Key: roachpb.Key("a"), Value: roachpb.MakeValueFromString("1")},
		{Key: roachpb.Key("c"), Value: roachpb.MakeValueFromString("2")},
	}
	var testFn simpleSendFn = func(
		_ context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		rs, err := keys.Range(ba.Requests)
		if err != nil {
			t.Fatal(err)
		}
		batchReply := &roachpb.BatchResponse{}
		reply := &roachpb.ScanResponse{}
		batchReply.Add(reply)
		results := []roachpb.KeyValue{}
		for _, curKV := range existingKVs {
			curKeyAddr, err := keys.Addr(curKV.Key)
			if err != nil {
				t.Fatal(err)
			}
			if rs.Key.Less(curKeyAddr.Next()) && curKeyAddr.Less(rs.EndKey) {
				results = append(results, curKV)
			}
		}
		reply.Rows = results
		return batchReply, nil
	}
	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(testFn),
		},
		RangeDescriptorDB: MockRangeDescriptorDB(func(key roachpb.RKey, _ bool) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error) {
			if key.Less(testMetaRangeDescriptor.EndKey) {
				return []roachpb.RangeDescriptor{testMetaRangeDescriptor}, nil, nil
			}
			if !merged {
				// Assume a range merge operation happened.
				merged = true
				return []roachpb.RangeDescriptor{firstRange}, nil, nil
			}
			return []roachpb.RangeDescriptor{mergedRange}, nil, nil
		}),
		Settings: cluster.MakeTestingClusterSettings(),
	}
	ds := NewDistSender(cfg)
	scan := roachpb.NewScan(roachpb.Key("a"), roachpb.Key("d"), false)
	// Set the Txn info to avoid an OpRequiresTxnError.
	reply, err := kv.SendWrappedWith(context.Background(), ds, roachpb.Header{
		MaxSpanRequestKeys: 10,
		Txn:                &roachpb.Transaction{},
	}, scan)
	if err != nil {
		t.Fatalf("scan encountered error: %s", err)
	}
	sr := reply.(*roachpb.ScanResponse)
	if !reflect.DeepEqual(existingKVs, sr.Rows) {
		t.Fatalf("expect get %v, actual get %v", existingKVs, sr.Rows)
	}
}

// TestRangeLookupOptionOnReverseScan verifies that a lookup triggered by a
// ReverseScan request has the useReverseScan specified.
func TestRangeLookupOptionOnReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(stubRPCSendFn),
		},
		RangeDescriptorDB: MockRangeDescriptorDB(func(key roachpb.RKey, useReverseScan bool) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error) {
			if !key.Equal(roachpb.KeyMin) && !useReverseScan {
				t.Fatalf("expected UseReverseScan to be set")
			}
			if key.Less(testMetaRangeDescriptor.EndKey) {
				return []roachpb.RangeDescriptor{testMetaRangeDescriptor}, nil, nil
			}
			return []roachpb.RangeDescriptor{testUserRangeDescriptor}, nil, nil
		}),
		Settings: cluster.MakeTestingClusterSettings(),
	}
	ds := NewDistSender(cfg)
	rScan := &roachpb.ReverseScanRequest{
		RequestHeader: roachpb.RequestHeader{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
	}
	if _, err := kv.SendWrapped(context.Background(), ds, rScan); err != nil {
		t.Fatal(err)
	}
}

// TestClockUpdateOnResponse verifies that the DistSender picks up
// the timestamp of the remote party embedded in responses.
func TestClockUpdateOnResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	cfg := DistSenderConfig{
		AmbientCtx:        log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:             clock,
		NodeDescs:         g,
		RPCContext:        rpcContext,
		RangeDescriptorDB: defaultMockRangeDescriptorDB,
		NodeDialer:        nodedialer.New(rpcContext, gossip.AddressResolver(g)),
		Settings:          cluster.MakeTestingClusterSettings(),
	}
	ds := NewDistSender(cfg)

	expectedErr := roachpb.NewError(errors.New("boom"))

	// Prepare the test function
	put := roachpb.NewPut(roachpb.Key("a"), roachpb.MakeValueFromString("value"))
	doCheck := func(sender kv.Sender, fakeTime hlc.Timestamp) {
		ds.transportFactory = SenderTransportFactory(tracing.NewTracer(), sender)
		_, err := kv.SendWrapped(context.Background(), ds, put)
		if err != nil && err != expectedErr {
			t.Fatal(err)
		}
		newTime := ds.clock.Now()
		if newTime.Less(fakeTime) {
			t.Fatalf("clock was not advanced: expected >= %s; got %s", fakeTime, newTime)
		}
	}

	// Test timestamp propagation on valid BatchResults.
	fakeTime := ds.clock.Now().Add(10000000000 /*10s*/, 0)
	replyNormal := kv.SenderFunc(
		func(_ context.Context, args roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			rb := args.CreateReply()
			rb.Now = fakeTime
			return rb, nil
		})
	doCheck(replyNormal, fakeTime)

	// Test timestamp propagation on errors.
	fakeTime = ds.clock.Now().Add(10000000000 /*10s*/, 0)
	replyError := kv.SenderFunc(
		func(_ context.Context, _ roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			pErr := expectedErr
			pErr.Now = fakeTime
			return nil, pErr
		})
	doCheck(replyError, fakeTime)
}

// TestTruncateWithSpanAndDescriptor verifies that a batch request is truncated with a
// range span and the range of a descriptor found in cache.
func TestTruncateWithSpanAndDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	if err := g.SetNodeDescriptor(newNodeDesc(1)); err != nil {
		t.Fatal(err)
	}
	nd := &roachpb.NodeDescriptor{
		NodeID:  roachpb.NodeID(1),
		Address: util.MakeUnresolvedAddr(testAddress.Network(), testAddress.String()),
	}
	if err := g.AddInfoProto(gossip.MakeNodeIDKey(roachpb.NodeID(1)), nd, time.Hour); err != nil {
		t.Fatal(err)
	}

	// Fill MockRangeDescriptorDB with two descriptors. When a
	// range descriptor is looked up by key "b", return the second
	// descriptor whose range is ["a", "c") and partially overlaps
	// with the first descriptor's range.
	var descriptor1 = roachpb.RangeDescriptor{
		RangeID:  2,
		StartKey: testMetaEndKey,
		EndKey:   roachpb.RKey("b"),
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
		Generation: 1,
	}
	var descriptor2 = roachpb.RangeDescriptor{
		RangeID:  3,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("c"),
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
		Generation: 2,
	}
	descDB := MockRangeDescriptorDB(func(key roachpb.RKey, _ bool) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error) {
		if key.Less(testMetaRangeDescriptor.EndKey) {
			return []roachpb.RangeDescriptor{testMetaRangeDescriptor}, nil, nil
		}
		desc := descriptor1
		if key.Equal(roachpb.RKey("b")) {
			desc = descriptor2
		}
		return []roachpb.RangeDescriptor{desc}, nil, nil
	})

	// Define our rpcSend stub which checks the span of the batch
	// requests. Because of parallelization, there's no guarantee
	// on the ordering of requests.
	var haveA, haveB bool
	sendStub := func(
		_ context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		rs, err := keys.Range(ba.Requests)
		if err != nil {
			t.Fatal(err)
		}
		if rs.Key.Equal(roachpb.RKey("a")) && rs.EndKey.Equal(roachpb.RKey("a").Next()) {
			haveA = true
		} else if rs.Key.Equal(roachpb.RKey("b")) && rs.EndKey.Equal(roachpb.RKey("b").Next()) {
			haveB = true
		} else {
			t.Fatalf("Unexpected span %s", rs)
		}

		batchReply := &roachpb.BatchResponse{}
		reply := &roachpb.PutResponse{}
		batchReply.Add(reply)
		return batchReply, nil
	}

	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(sendStub),
		},
		RangeDescriptorDB: descDB,
		Settings:          cluster.MakeTestingClusterSettings(),
	}
	ds := NewDistSender(cfg)

	// Send a batch request containing two puts. In the first
	// attempt, the span of the descriptor found in the cache is
	// ["a", "b"). The request is truncated to contain only the put
	// on "a".
	//
	// In the second attempt, The range of the descriptor found in
	// the cache is ["a", "c"), but the put on "a" will not be
	// present. The request is truncated to contain only the put on "b".
	ba := roachpb.BatchRequest{}
	ba.Txn = &roachpb.Transaction{Name: "test"}
	{
		val := roachpb.MakeValueFromString("val")
		ba.Add(roachpb.NewPut(keys.MakeRangeKeyPrefix(roachpb.RKey("a")), val))
	}
	{
		val := roachpb.MakeValueFromString("val")
		ba.Add(roachpb.NewPut(keys.MakeRangeKeyPrefix(roachpb.RKey("b")), val))
	}

	if _, pErr := ds.Send(context.Background(), ba); pErr != nil {
		t.Fatal(pErr)
	}

	if !haveA || !haveB {
		t.Errorf("expected two requests for \"a\" and \"b\": %t, %t", haveA, haveB)
	}
}

// TestTruncateWithLocalSpanAndDescriptor verifies that a batch request with local keys
// is truncated with a range span and the range of a descriptor found in cache.
func TestTruncateWithLocalSpanAndDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	if err := g.SetNodeDescriptor(newNodeDesc(1)); err != nil {
		t.Fatal(err)
	}
	nd := &roachpb.NodeDescriptor{
		NodeID:  roachpb.NodeID(1),
		Address: util.MakeUnresolvedAddr(testAddress.Network(), testAddress.String()),
	}
	if err := g.AddInfoProto(gossip.MakeNodeIDKey(roachpb.NodeID(1)), nd, time.Hour); err != nil {
		t.Fatal(err)
	}

	// Fill MockRangeDescriptorDB with two descriptors.
	var descriptor1 = roachpb.RangeDescriptor{
		RangeID:  2,
		StartKey: testMetaEndKey,
		EndKey:   roachpb.RKey("b"),
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	var descriptor2 = roachpb.RangeDescriptor{
		RangeID:  3,
		StartKey: roachpb.RKey("b"),
		EndKey:   roachpb.RKey("c"),
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	var descriptor3 = roachpb.RangeDescriptor{
		RangeID:  4,
		StartKey: roachpb.RKey("c"),
		EndKey:   roachpb.RKeyMax,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	descDB := mockRangeDescriptorDBForDescs(
		testMetaRangeDescriptor,
		descriptor1,
		descriptor2,
		descriptor3,
	)

	// Define our rpcSend stub which checks the span of the batch
	// requests.
	haveRequest := []bool{false, false, false}
	sendStub := func(
		_ context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		h := ba.Requests[0].GetInner().Header()
		if h.Key.Equal(keys.RangeDescriptorKey(roachpb.RKey("a"))) && h.EndKey.Equal(keys.MakeRangeKeyPrefix(roachpb.RKey("b"))) {
			haveRequest[0] = true
		} else if h.Key.Equal(keys.MakeRangeKeyPrefix(roachpb.RKey("b"))) && h.EndKey.Equal(keys.MakeRangeKeyPrefix(roachpb.RKey("c"))) {
			haveRequest[1] = true
		} else if h.Key.Equal(keys.MakeRangeKeyPrefix(roachpb.RKey("c"))) && h.EndKey.Equal(keys.RangeDescriptorKey(roachpb.RKey("c"))) {
			haveRequest[2] = true
		} else {
			t.Fatalf("Unexpected span [%s,%s)", h.Key, h.EndKey)
		}

		batchReply := &roachpb.BatchResponse{}
		reply := &roachpb.ScanResponse{}
		batchReply.Add(reply)
		return batchReply, nil
	}

	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(sendStub),
		},
		RangeDescriptorDB: descDB,
		Settings:          cluster.MakeTestingClusterSettings(),
	}
	ds := NewDistSender(cfg)

	// Send a batch request contains two scans. In the first
	// attempt, the range of the descriptor found in the cache is
	// ["", "b"). The request is truncated to contain only the scan
	// on local keys that address up to "b".
	//
	// In the second attempt, The range of the descriptor found in
	// the cache is ["b", "d"), The request is truncated to contain
	// only the scan on local keys that address from "b" to "d".
	ba := roachpb.BatchRequest{}
	ba.Txn = &roachpb.Transaction{Name: "test"}
	ba.Add(roachpb.NewScan(
		keys.RangeDescriptorKey(roachpb.RKey("a")),
		keys.RangeDescriptorKey(roachpb.RKey("c")),
		false /* forUpdate */))

	if _, pErr := ds.Send(context.Background(), ba); pErr != nil {
		t.Fatal(pErr)
	}
	for i, found := range haveRequest {
		if !found {
			t.Errorf("request %d not received", i)
		}
	}
}

// TestMultiRangeWithEndTxn verifies that when a chunk of batch looks like it's
// going to be dispatched to more than one range, it will be split up if it
// contains an EndTxn that is not performing a parallel commit. However, it will
// not be split up if it contains an EndTxn that is performing a parallel
// commit.
func TestMultiRangeWithEndTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	testCases := []struct {
		put1, put2, et roachpb.Key
		parCommit      bool
		exp            [][]roachpb.Method
	}{
		{
			// Everything hits the first range, so we get a 1PC txn.
			put1:      roachpb.Key("a1"),
			put2:      roachpb.Key("a2"),
			et:        roachpb.Key("a3"),
			parCommit: false,
			exp:       [][]roachpb.Method{{roachpb.Put, roachpb.Put, roachpb.EndTxn}},
		},
		{
			// Everything hits the first range, so we get a 1PC txn.
			// Parallel commit doesn't matter.
			put1:      roachpb.Key("a1"),
			put2:      roachpb.Key("a2"),
			et:        roachpb.Key("a3"),
			parCommit: true,
			exp:       [][]roachpb.Method{{roachpb.Put, roachpb.Put, roachpb.EndTxn}},
		},
		{
			// Only EndTxn hits the second range.
			put1:      roachpb.Key("a1"),
			put2:      roachpb.Key("a2"),
			et:        roachpb.Key("b"),
			parCommit: false,
			exp:       [][]roachpb.Method{{roachpb.Put, roachpb.Put}, {roachpb.EndTxn}},
		},
		{
			// Only EndTxn hits the second range. However, since the EndTxn is
			// performing a parallel commit, it is sent in parallel, which we
			// can't detect directly because the EndTxn batch is sent to the
			// second range and a strict ordering of batches is enforced by
			// DisableParallelBatches.
			put1:      roachpb.Key("a1"),
			put2:      roachpb.Key("a2"),
			et:        roachpb.Key("b"),
			parCommit: true,
			exp:       [][]roachpb.Method{{roachpb.Put, roachpb.Put}, {roachpb.EndTxn}},
		},
		{
			// One write hits the second range, so EndTxn has to be split off.
			// In this case, going in the usual order without splitting off
			// would actually be fine, but it doesn't seem worth optimizing at
			// this point.
			put1:      roachpb.Key("a1"),
			put2:      roachpb.Key("b1"),
			et:        roachpb.Key("a1"),
			parCommit: false,
			exp:       [][]roachpb.Method{{roachpb.Put}, {roachpb.Put}, {roachpb.EndTxn}},
		},
		{
			// One write hits the second range. Again, EndTxn does not need to
			// be split off because it is performing a parallel commit, so the
			// only split is due to the range boundary.
			put1:      roachpb.Key("a1"),
			put2:      roachpb.Key("b1"),
			et:        roachpb.Key("a1"),
			parCommit: true,
			exp:       [][]roachpb.Method{{roachpb.Put, roachpb.EndTxn}, {roachpb.Put}},
		},
		{
			// Both writes go to the second range, but not EndTxn. It is split
			// from the writes and sent after.
			put1:      roachpb.Key("b1"),
			put2:      roachpb.Key("b2"),
			et:        roachpb.Key("a1"),
			parCommit: false,
			exp:       [][]roachpb.Method{{roachpb.Put, roachpb.Put}, {roachpb.EndTxn}},
		},
		{
			// Both writes go to the second range, but not EndTxn. Since the
			// EndTxn is performing a parallel commit, it is sent in parallel.
			// We can tell this because the EndTxn batch is sent to the first
			// range and ends up being delivered first, unlike in the previous
			// case.
			put1:      roachpb.Key("b1"),
			put2:      roachpb.Key("b2"),
			et:        roachpb.Key("a1"),
			parCommit: true,
			exp:       [][]roachpb.Method{{roachpb.EndTxn}, {roachpb.Put, roachpb.Put}},
		},
	}

	if err := g.SetNodeDescriptor(newNodeDesc(1)); err != nil {
		t.Fatal(err)
	}
	nd := &roachpb.NodeDescriptor{
		NodeID:  roachpb.NodeID(1),
		Address: util.MakeUnresolvedAddr(testAddress.Network(), testAddress.String()),
	}
	if err := g.AddInfoProto(gossip.MakeNodeIDKey(roachpb.NodeID(1)), nd, time.Hour); err != nil {
		t.Fatal(err)

	}

	// Fill MockRangeDescriptorDB with two descriptors.
	var descriptor1 = roachpb.RangeDescriptor{
		RangeID:  2,
		StartKey: testMetaEndKey,
		EndKey:   roachpb.RKey("b"),
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	var descriptor2 = roachpb.RangeDescriptor{
		RangeID:  3,
		StartKey: roachpb.RKey("b"),
		EndKey:   roachpb.RKeyMax,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	descDB := mockRangeDescriptorDBForDescs(
		testMetaRangeDescriptor,
		descriptor1,
		descriptor2,
	)

	for i, test := range testCases {
		var act [][]roachpb.Method
		var testFn simpleSendFn = func(
			_ context.Context,
			_ SendOptions,
			_ ReplicaSlice,
			ba roachpb.BatchRequest,
		) (*roachpb.BatchResponse, error) {
			var cur []roachpb.Method
			for _, union := range ba.Requests {
				cur = append(cur, union.GetInner().Method())
			}
			act = append(act, cur)
			return ba.CreateReply(), nil
		}

		cfg := DistSenderConfig{
			AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
			Clock:      clock,
			NodeDescs:  g,
			RPCContext: rpcContext,
			TestingKnobs: ClientTestingKnobs{
				TransportFactory: adaptSimpleTransport(testFn),
			},
			RangeDescriptorDB: descDB,
			Settings:          cluster.MakeTestingClusterSettings(),
		}
		ds := NewDistSender(cfg)
		ds.DisableParallelBatches()

		// Send a batch request containing two puts.
		var ba roachpb.BatchRequest
		ba.Txn = &roachpb.Transaction{Name: "test"}
		ba.Add(roachpb.NewPut(test.put1, roachpb.MakeValueFromString("val1")))
		ba.Add(roachpb.NewPut(test.put2, roachpb.MakeValueFromString("val2")))
		et := &roachpb.EndTxnRequest{
			RequestHeader: roachpb.RequestHeader{Key: test.et},
			Commit:        true,
		}
		if test.parCommit {
			et.InFlightWrites = []roachpb.SequencedWrite{
				{Key: test.put1, Sequence: 1}, {Key: test.put2, Sequence: 2},
			}
		}
		ba.Add(et)

		if _, pErr := ds.Send(context.Background(), ba); pErr != nil {
			t.Fatal(pErr)
		}

		for j, batchMethods := range act {
			if !reflect.DeepEqual(test.exp[j], batchMethods) {
				t.Fatalf("test %d: expected [%d] %v, got %v", i, j, test.exp[j], batchMethods)
			}
		}
	}
}

// TestParallelCommitSplitFromQueryIntents verifies that a parallel-committing
// batch is split into sub-batches - one containing all pre-commit QueryIntent
// requests and one containing everything else.
//
// The test only uses a single range, so it only tests the split of ranges in
// divideAndSendParallelCommit. See TestMultiRangeWithEndTxn for a test that
// verifies proper behavior of batches containing EndTxn requests which span
// ranges.
func TestParallelCommitSplitFromQueryIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)

	keyA, keyB := roachpb.Key("a"), roachpb.Key("ab")
	put1 := roachpb.NewPut(keyA, roachpb.MakeValueFromString("val1"))
	put2 := roachpb.NewPut(keyB, roachpb.MakeValueFromString("val2"))
	qi := &roachpb.QueryIntentRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	et := &roachpb.EndTxnRequest{
		RequestHeader: roachpb.RequestHeader{Key: keyA},
		Commit:        true,
	}
	etPar := &roachpb.EndTxnRequest{
		RequestHeader:  roachpb.RequestHeader{Key: keyA},
		Commit:         true,
		InFlightWrites: []roachpb.SequencedWrite{{Key: keyA, Sequence: 1}, {Key: keyB, Sequence: 2}},
	}

	testCases := []struct {
		name string
		reqs []roachpb.Request
		exp  [][]roachpb.Method
	}{
		{
			name: "no parallel commits or query intents",
			reqs: []roachpb.Request{put1, put2, et},
			exp:  [][]roachpb.Method{{roachpb.Put, roachpb.Put, roachpb.EndTxn}},
		},
		{
			name: "no parallel commits, but regular and pre-commit query intents",
			reqs: []roachpb.Request{qi, put1, put2, qi, et},
			exp: [][]roachpb.Method{
				{roachpb.QueryIntent, roachpb.Put, roachpb.Put, roachpb.QueryIntent, roachpb.EndTxn},
			},
		},
		{
			name: "parallel commits without query intents",
			reqs: []roachpb.Request{put1, put2, etPar},
			exp:  [][]roachpb.Method{{roachpb.Put, roachpb.Put, roachpb.EndTxn}},
		},
		{
			name: "parallel commits with pre-commit query intents",
			reqs: []roachpb.Request{put1, put2, qi, qi, etPar},
			exp: [][]roachpb.Method{
				{roachpb.QueryIntent, roachpb.QueryIntent},
				{roachpb.Put, roachpb.Put, roachpb.EndTxn},
			},
		},
		{
			name: "parallel commits with regular query intents",
			reqs: []roachpb.Request{qi, put1, qi, put2, etPar},
			exp: [][]roachpb.Method{
				{roachpb.QueryIntent, roachpb.Put, roachpb.QueryIntent, roachpb.Put, roachpb.EndTxn},
			},
		},
		{
			name: "parallel commits with regular and pre-commit query intents",
			reqs: []roachpb.Request{qi, put1, put2, qi, qi, qi, etPar},
			exp: [][]roachpb.Method{
				{roachpb.QueryIntent, roachpb.QueryIntent, roachpb.QueryIntent},
				{roachpb.QueryIntent, roachpb.Put, roachpb.Put, roachpb.EndTxn},
			},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			var act [][]roachpb.Method
			var testFn simpleSendFn = func(
				_ context.Context,
				_ SendOptions,
				_ ReplicaSlice,
				ba roachpb.BatchRequest,
			) (*roachpb.BatchResponse, error) {
				var cur []roachpb.Method
				for _, union := range ba.Requests {
					cur = append(cur, union.GetInner().Method())
				}
				act = append(act, cur)
				return ba.CreateReply(), nil
			}

			cfg := DistSenderConfig{
				AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
				Clock:      clock,
				NodeDescs:  g,
				RPCContext: rpcContext,
				TestingKnobs: ClientTestingKnobs{
					TransportFactory: adaptSimpleTransport(testFn),
				},
				RangeDescriptorDB: defaultMockRangeDescriptorDB,
				Settings:          cluster.MakeTestingClusterSettings(),
			}
			ds := NewDistSender(cfg)
			ds.DisableParallelBatches()

			// Send a batch request containing the requests.
			var ba roachpb.BatchRequest
			ba.Txn = &roachpb.Transaction{Name: "test"}
			ba.Add(test.reqs...)

			if _, pErr := ds.Send(context.Background(), ba); pErr != nil {
				t.Fatal(pErr)
			}

			for j, batchMethods := range act {
				if !reflect.DeepEqual(test.exp[j], batchMethods) {
					t.Fatalf("expected [%d] %v, got %v", j, test.exp[j], batchMethods)
				}
			}
		})
	}
}

// TestParallelCommitsDetectIntentMissingCause tests the functionality in
// DistSender.detectIntentMissingDueToIntentResolution.
func TestParallelCommitsDetectIntentMissingCause(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)

	key := roachpb.Key("a")
	txn := roachpb.MakeTransaction(
		"test", key, roachpb.NormalUserPriority,
		clock.Now(), clock.MaxOffset().Nanoseconds(),
	)

	testCases := []struct {
		name       string
		queryTxnFn func() (roachpb.TransactionStatus, error)
		expErr     string
	}{
		{
			name: "transaction record PENDING, real intent missing error",
			queryTxnFn: func() (roachpb.TransactionStatus, error) {
				return roachpb.PENDING, nil
			},
			expErr: "intent missing",
		},
		{
			name: "transaction record STAGING, real intent missing error",
			queryTxnFn: func() (roachpb.TransactionStatus, error) {
				return roachpb.STAGING, nil
			},
			expErr: "intent missing",
		},
		{
			name: "transaction record COMMITTED, intent missing error caused by intent resolution",
			queryTxnFn: func() (roachpb.TransactionStatus, error) {
				return roachpb.COMMITTED, nil
			},
		},
		{
			name: "transaction record ABORTED, ambiguous intent missing error",
			queryTxnFn: func() (roachpb.TransactionStatus, error) {
				return roachpb.ABORTED, nil
			},
			expErr: "result is ambiguous (intent missing and record aborted)",
		},
		{
			name: "QueryTxn error, unresolved ambiguity",
			queryTxnFn: func() (roachpb.TransactionStatus, error) {
				return 0, errors.New("unable to query txn")
			},
			expErr: "result is ambiguous (error=unable to query txn [intent missing])",
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			var testFn simpleSendFn = func(
				_ context.Context,
				_ SendOptions,
				_ ReplicaSlice,
				ba roachpb.BatchRequest,
			) (*roachpb.BatchResponse, error) {
				br := ba.CreateReply()
				switch ba.Requests[0].GetInner().Method() {
				case roachpb.QueryIntent:
					br.Error = roachpb.NewError(roachpb.NewIntentMissingError(key, nil))
				case roachpb.QueryTxn:
					status, err := test.queryTxnFn()
					if err != nil {
						br.Error = roachpb.NewError(err)
					} else {
						respTxn := txn
						respTxn.Status = status
						br.Responses[0].GetQueryTxn().QueriedTxn = respTxn
					}
				case roachpb.EndTxn:
					br.Txn = ba.Txn.Clone()
					br.Txn.Status = roachpb.STAGING
				}
				return br, nil
			}

			cfg := DistSenderConfig{
				AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
				Clock:      clock,
				NodeDescs:  g,
				RPCContext: rpcContext,
				TestingKnobs: ClientTestingKnobs{
					TransportFactory: adaptSimpleTransport(testFn),
				},
				RangeDescriptorDB: defaultMockRangeDescriptorDB,
				Settings:          cluster.MakeTestingClusterSettings(),
			}
			ds := NewDistSender(cfg)

			// Send a parallel commit batch request.
			var ba roachpb.BatchRequest
			ba.Txn = txn.Clone()
			ba.Add(&roachpb.QueryIntentRequest{
				RequestHeader:  roachpb.RequestHeader{Key: key},
				Txn:            txn.TxnMeta,
				ErrorIfMissing: true,
			})
			ba.Add(&roachpb.EndTxnRequest{
				RequestHeader:  roachpb.RequestHeader{Key: key},
				Commit:         true,
				InFlightWrites: []roachpb.SequencedWrite{{Key: key, Sequence: 1}},
			})

			// Verify that the response is expected.
			_, pErr := ds.Send(context.Background(), ba)
			if test.expErr == "" {
				if pErr != nil {
					t.Fatalf("unexpected error %v", pErr)
				}
			} else {
				if !testutils.IsPError(pErr, regexp.QuoteMeta(test.expErr)) {
					t.Fatalf("expected error %q; found %v", test.expErr, pErr)
				}
			}
		})
	}
}

func TestCountRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	// Create a slice of fake descriptors.
	const numDescriptors = 9
	const firstKeyBoundary = 'a'
	var descriptors [numDescriptors]roachpb.RangeDescriptor
	for i := range descriptors {
		startKey := testMetaEndKey
		if i > 0 {
			startKey = roachpb.RKey(string(firstKeyBoundary + i - 1))
		}
		endKey := roachpb.RKeyMax
		if i < len(descriptors)-1 {
			endKey = roachpb.RKey(string(firstKeyBoundary + i))
		}

		descriptors[i] = roachpb.RangeDescriptor{
			RangeID:  roachpb.RangeID(i + 2),
			StartKey: startKey,
			EndKey:   endKey,
			InternalReplicas: []roachpb.ReplicaDescriptor{
				{
					NodeID:  1,
					StoreID: 1,
				},
			},
		}
	}

	// Mock out descriptor DB and sender function.
	descDB := mockRangeDescriptorDBForDescs(append(descriptors[:], testMetaRangeDescriptor)...)
	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(stubRPCSendFn),
		},
		RangeDescriptorDB: descDB,
		Settings:          cluster.MakeTestingClusterSettings(),
	}
	ds := NewDistSender(cfg)

	// Verify counted ranges.
	keyIn := func(desc roachpb.RangeDescriptor) roachpb.RKey {
		return append(desc.StartKey, 'a')
	}
	testcases := []struct {
		key    roachpb.RKey
		endKey roachpb.RKey
		count  int64
	}{
		{testMetaEndKey, roachpb.RKey(string(firstKeyBoundary)), 1},
		{testMetaEndKey, keyIn(descriptors[0]), 1},
		{testMetaEndKey, descriptors[len(descriptors)-1].StartKey, numDescriptors - 1},
		{descriptors[0].EndKey, roachpb.RKeyMax, numDescriptors - 1},
		// Everything from the min key to a key within the last range.
		{testMetaEndKey, keyIn(descriptors[len(descriptors)-1]), numDescriptors},
		{testMetaEndKey, roachpb.RKeyMax, numDescriptors},
	}
	for i, tc := range testcases {
		count, pErr := ds.CountRanges(context.Background(), roachpb.RSpan{Key: tc.key, EndKey: tc.endKey})
		if pErr != nil {
			t.Fatalf("%d: %s", i, pErr)
		}
		if a, e := count, tc.count; a != e {
			t.Errorf("%d: # of ranges %d != expected %d", i, a, e)
		}
	}
}

func TestSenderTransport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	transport, err := SenderTransportFactory(
		tracing.NewTracer(),
		kv.SenderFunc(
			func(
				_ context.Context,
				_ roachpb.BatchRequest,
			) (r *roachpb.BatchResponse, e *roachpb.Error) {
				return
			},
		))(SendOptions{}, &nodedialer.Dialer{}, ReplicaSlice{{}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = transport.SendNext(context.Background(), roachpb.BatchRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !transport.IsExhausted() {
		t.Fatalf("transport is not exhausted")
	}
}

func TestGatewayNodeID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	const expNodeID = 42
	nd := newNodeDesc(expNodeID)
	g.NodeID.Reset(nd.NodeID)
	if err := g.SetNodeDescriptor(nd); err != nil {
		t.Fatal(err)
	}
	if err := g.AddInfoProto(gossip.MakeNodeIDKey(expNodeID), nd, time.Hour); err != nil {
		t.Fatal(err)
	}

	var observedNodeID roachpb.NodeID
	var testFn simpleSendFn = func(
		_ context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		observedNodeID = ba.Header.GatewayNodeID
		return ba.CreateReply(), nil
	}

	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(testFn),
		},
		RangeDescriptorDB: defaultMockRangeDescriptorDB,
		Settings:          cluster.MakeTestingClusterSettings(),
	}
	ds := NewDistSender(cfg)
	var ba roachpb.BatchRequest
	ba.Add(roachpb.NewPut(roachpb.Key("a"), roachpb.MakeValueFromString("value")))
	if _, err := ds.Send(context.Background(), ba); err != nil {
		t.Fatalf("put encountered error: %s", err)
	}
	if observedNodeID != expNodeID {
		t.Errorf("got GatewayNodeID=%d, want %d", observedNodeID, expNodeID)
	}
}

// TestMultipleErrorsMerged tests that DistSender prioritizes errors that are
// returned from concurrent partial batches and returns the "best" one after
// merging the transaction metadata passed on the errors.
func TestMultipleErrorsMerged(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)

	if err := g.SetNodeDescriptor(newNodeDesc(1)); err != nil {
		t.Fatal(err)
	}
	nd := &roachpb.NodeDescriptor{
		NodeID:  roachpb.NodeID(1),
		Address: util.MakeUnresolvedAddr(testAddress.Network(), testAddress.String()),
	}
	if err := g.AddInfoProto(gossip.MakeNodeIDKey(roachpb.NodeID(1)), nd, time.Hour); err != nil {
		t.Fatal(err)
	}

	// Fill MockRangeDescriptorDB with two descriptors.
	var descriptor1 = roachpb.RangeDescriptor{
		RangeID:  2,
		StartKey: testMetaEndKey,
		EndKey:   roachpb.RKey("b"),
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	var descriptor2 = roachpb.RangeDescriptor{
		RangeID:  3,
		StartKey: roachpb.RKey("b"),
		EndKey:   roachpb.RKeyMax,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	descDB := mockRangeDescriptorDBForDescs(
		testMetaRangeDescriptor,
		descriptor1,
		descriptor2,
	)

	txn := roachpb.MakeTransaction(
		"test", nil /* baseKey */, roachpb.NormalUserPriority,
		clock.Now(), clock.MaxOffset().Nanoseconds(),
	)
	// We're also going to check that the highest bumped WriteTimestamp makes it
	// to the merged error.
	err1WriteTimestamp := txn.WriteTimestamp.Add(100, 0)
	err2WriteTimestamp := txn.WriteTimestamp.Add(200, 0)

	retryErr := roachpb.NewTransactionRetryError(roachpb.RETRY_SERIALIZABLE, "test err")
	abortErr := roachpb.NewTransactionAbortedError(roachpb.ABORT_REASON_ABORTED_RECORD_FOUND)
	conditionFailedErr := &roachpb.ConditionFailedError{}
	sendErr := sendError{}
	ambiguousErr := &roachpb.AmbiguousResultError{}
	randomErr := &roachpb.IntegerOverflowError{}

	testCases := []struct {
		err1, err2 error
		expErr     string
	}{
		{
			err1:   retryErr,
			err2:   nil,
			expErr: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE - test err)",
		},
		{
			err1:   abortErr,
			err2:   nil,
			expErr: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
		},
		{
			err1:   conditionFailedErr,
			err2:   nil,
			expErr: "unexpected value",
		},
		{
			err1:   retryErr,
			err2:   retryErr,
			expErr: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE - test err)",
		},
		{
			err1:   retryErr,
			err2:   abortErr,
			expErr: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
		},
		{
			err1:   abortErr,
			err2:   abortErr,
			expErr: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
		},
		{
			err1:   retryErr,
			err2:   conditionFailedErr,
			expErr: "unexpected value",
		},
		{
			err1:   abortErr,
			err2:   conditionFailedErr,
			expErr: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
		},
		{
			err1:   conditionFailedErr,
			err2:   conditionFailedErr,
			expErr: "unexpected value",
		},
		// ConditionFailedError has a low score since it's "not ambiguous". We want
		// ambiguity to be infectious, so most things have a higher score.
		{
			err1:   conditionFailedErr,
			err2:   ambiguousErr,
			expErr: "result is ambiguous",
		},
		{
			err1:   conditionFailedErr,
			err2:   sendErr,
			expErr: "failed to send RPC",
		},
		{
			err1:   conditionFailedErr,
			err2:   randomErr,
			expErr: "results in overflow",
		},
	}
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			// We run every test case twice, to make sure error merging is commutative.
			testutils.RunTrueAndFalse(t, "reverse", func(t *testing.T, reverse bool) {
				if reverse {
					// Switch the order of errors.
					err1 := tc.err1
					err2 := tc.err2
					tc.err1 = err2
					tc.err2 = err1
				}

				var testFn simpleSendFn = func(
					_ context.Context,
					_ SendOptions,
					_ ReplicaSlice,
					ba roachpb.BatchRequest,
				) (*roachpb.BatchResponse, error) {
					reply := ba.CreateReply()
					if delRng := ba.Requests[0].GetDeleteRange(); delRng == nil {
						return nil, errors.Errorf("expected DeleteRange request, found %v", ba.Requests[0])
					} else if delRng.Key.Equal(roachpb.Key("a")) {
						if tc.err1 != nil {
							errTxn := ba.Txn.Clone()
							errTxn.WriteTimestamp = err1WriteTimestamp
							reply.Error = roachpb.NewErrorWithTxn(tc.err1, errTxn)
						}
					} else if delRng.Key.Equal(roachpb.Key("b")) {
						if tc.err2 != nil {
							errTxn := ba.Txn.Clone()
							errTxn.WriteTimestamp = err2WriteTimestamp
							reply.Error = roachpb.NewErrorWithTxn(tc.err2, errTxn)
						}
					} else {
						return nil, errors.Errorf("unexpected DeleteRange boundaries")
					}
					return reply, nil
				}

				cfg := DistSenderConfig{
					AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
					Clock:      clock,
					NodeDescs:  g,
					RPCContext: rpcContext,
					TestingKnobs: ClientTestingKnobs{
						TransportFactory: adaptSimpleTransport(testFn),
					},
					RangeDescriptorDB: descDB,
					Settings:          cluster.MakeTestingClusterSettings(),
					RPCRetryOptions:   &retry.Options{MaxRetries: 1},
				}
				ds := NewDistSender(cfg)

				var ba roachpb.BatchRequest
				ba.Txn = txn.Clone()
				ba.Add(roachpb.NewDeleteRange(roachpb.Key("a"), roachpb.Key("c"), false /* returnKeys */))

				expWriteTimestamp := txn.WriteTimestamp
				if tc.err1 != nil {
					expWriteTimestamp = err1WriteTimestamp
				}
				if tc.err2 != nil {
					expWriteTimestamp = err2WriteTimestamp
				}

				if _, pErr := ds.Send(context.Background(), ba); pErr == nil {
					t.Fatalf("expected an error to be returned from distSender")
				} else if !testutils.IsPError(pErr, regexp.QuoteMeta(tc.expErr)) {
					t.Fatalf("expected error %q; found %v", tc.expErr, pErr)
				} else if !pErr.GetTxn().WriteTimestamp.Equal(expWriteTimestamp) {
					t.Fatalf("expected bumped ts %s, got: %s", expWriteTimestamp, pErr.GetTxn().WriteTimestamp)
				}
			})
		})
	}
}

// Regression test for #20067.
// If a batch is partitioned into multiple partial batches, the
// roachpb.Error.Index of each batch should correspond to its original index in
// the overall batch.
func TestErrorIndexAlignment(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)

	if err := g.SetNodeDescriptor(newNodeDesc(1)); err != nil {
		t.Fatal(err)
	}
	nd := &roachpb.NodeDescriptor{
		NodeID:  roachpb.NodeID(1),
		Address: util.MakeUnresolvedAddr(testAddress.Network(), testAddress.String()),
	}
	if err := g.AddInfoProto(gossip.MakeNodeIDKey(roachpb.NodeID(1)), nd, time.Hour); err != nil {
		t.Fatal(err)
	}

	// Fill MockRangeDescriptorDB with two descriptors.
	var descriptor1 = roachpb.RangeDescriptor{
		RangeID:  2,
		StartKey: testMetaEndKey,
		EndKey:   roachpb.RKey("b"),
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	var descriptor2 = roachpb.RangeDescriptor{
		RangeID:  3,
		StartKey: roachpb.RKey("b"),
		EndKey:   roachpb.RKey("c"),
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	var descriptor3 = roachpb.RangeDescriptor{
		RangeID:  4,
		StartKey: roachpb.RKey("c"),
		EndKey:   roachpb.RKeyMax,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}

	// The 1st partial batch has 1 request.
	// The 2nd partial batch has 2 requests.
	// The 3rd partial batch has 1 request.
	// Each test case returns an error for the first request of the nth
	// partial batch.
	testCases := []struct {
		// The nth request to return an error.
		nthPartialBatch  int
		expectedFinalIdx int32
	}{
		{0, 0},
		{1, 1},
		{2, 3},
	}

	descDB := mockRangeDescriptorDBForDescs(
		testMetaRangeDescriptor,
		descriptor1,
		descriptor2,
		descriptor3,
	)

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			nthRequest := 0

			var testFn simpleSendFn = func(
				_ context.Context,
				_ SendOptions,
				_ ReplicaSlice,
				ba roachpb.BatchRequest,
			) (*roachpb.BatchResponse, error) {
				reply := ba.CreateReply()
				if nthRequest == tc.nthPartialBatch {
					reply.Error = &roachpb.Error{
						// The relative index is always 0 since
						// we return an error for the first
						// request of the nthPartialBatch.
						Index: &roachpb.ErrPosition{Index: 0},
					}
				}
				nthRequest++
				return reply, nil
			}

			cfg := DistSenderConfig{
				AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
				Clock:      clock,
				NodeDescs:  g,
				RPCContext: rpcContext,
				TestingKnobs: ClientTestingKnobs{
					TransportFactory: adaptSimpleTransport(testFn),
				},
				RangeDescriptorDB: descDB,
				Settings:          cluster.MakeTestingClusterSettings(),
			}
			ds := NewDistSender(cfg)
			ds.DisableParallelBatches()

			var ba roachpb.BatchRequest
			ba.Txn = &roachpb.Transaction{Name: "test"}
			// First batch has 1 request.
			val := roachpb.MakeValueFromString("val")
			ba.Add(roachpb.NewPut(roachpb.Key("a"), val))

			// Second batch has 2 requests.
			val = roachpb.MakeValueFromString("val")
			ba.Add(roachpb.NewPut(roachpb.Key("b"), val))
			val = roachpb.MakeValueFromString("val")
			ba.Add(roachpb.NewPut(roachpb.Key("bb"), val))

			// Third batch has 1 request.
			val = roachpb.MakeValueFromString("val")
			ba.Add(roachpb.NewPut(roachpb.Key("c"), val))

			_, pErr := ds.Send(context.Background(), ba)
			if pErr == nil {
				t.Fatalf("expected an error to be returned from distSender")
			}
			if pErr.Index.Index != tc.expectedFinalIdx {
				t.Errorf("expected error index to be %d, instead got %d", tc.expectedFinalIdx, pErr.Index.Index)
			}
		})
	}
}

// TestCanSendToFollower tests that the DistSender abides by the result it
// get from CanSendToFollower.
func TestCanSendToFollower(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)

	old := CanSendToFollower
	defer func() { CanSendToFollower = old }()
	canSend := true
	CanSendToFollower = func(_ uuid.UUID, _ *cluster.Settings, ba roachpb.BatchRequest) bool {
		return !ba.IsLocking() && canSend
	}

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	leaseHolders := testUserRangeDescriptor3Replicas.InternalReplicas
	for _, n := range leaseHolders {
		if err := g.AddInfoProto(
			gossip.MakeNodeIDKey(n.NodeID),
			newNodeDesc(n.NodeID),
			gossip.NodeDescriptorTTL,
		); err != nil {
			t.Fatal(err)
		}
	}
	var sentTo ReplicaInfo
	var testFn simpleSendFn = func(
		_ context.Context,
		_ SendOptions,
		r ReplicaSlice,
		args roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		sentTo = r[0]
		return args.CreateReply(), nil
	}
	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(testFn),
		},
		RangeDescriptorDB: threeReplicaMockRangeDescriptorDB,
		NodeDialer:        nodedialer.New(rpcContext, gossip.AddressResolver(g)),
		RPCRetryOptions: &retry.Options{
			InitialBackoff: time.Microsecond,
			MaxBackoff:     time.Microsecond,
		},
		Settings: cluster.MakeTestingClusterSettings(),
	}
	for i, c := range []struct {
		canSendToFollower bool
		header            roachpb.Header
		msg               roachpb.Request
		expectedNode      roachpb.NodeID
	}{
		{
			true,
			roachpb.Header{
				Txn: &roachpb.Transaction{},
			},
			roachpb.NewPut(roachpb.Key("a"), roachpb.Value{}),
			2,
		},
		{
			true,
			roachpb.Header{
				Txn: &roachpb.Transaction{},
			},
			roachpb.NewGet(roachpb.Key("a")),
			1,
		},
		{
			true,
			roachpb.Header{},
			roachpb.NewGet(roachpb.Key("a")),
			1,
		},
		{
			false,
			roachpb.Header{},
			roachpb.NewGet(roachpb.Key("a")),
			2,
		},
	} {
		t.Run("", func(t *testing.T) {
			sentTo = ReplicaInfo{}
			canSend = c.canSendToFollower
			ds := NewDistSender(cfg)
			ds.clusterID = &base.ClusterIDContainer{}
			// Make store 2 the leaseholder.
			var lease roachpb.Lease
			lease.Replica = testUserRangeDescriptor3Replicas.InternalReplicas[1]
			ds.rangeCache.Insert(ctx, roachpb.RangeInfo{
				Desc:  testUserRangeDescriptor3Replicas,
				Lease: lease,
			})
			_, pErr := kv.SendWrappedWith(ctx, ds, c.header, c.msg)
			require.Nil(t, pErr)
			if sentTo.NodeID != c.expectedNode {
				t.Fatalf("%d: unexpected replica: %v != %v", i, sentTo.NodeID, c.expectedNode)
			}
			// Check that the leaseholder in the cache doesn't change, even if the
			// request is served by a follower. This tests a regression for a bug
			// we've had where we were always updating the leaseholder on successful
			// RPCs because we erroneously assumed that a success must come from the
			// leaseholder.
			rng := ds.rangeCache.GetCached(testUserRangeDescriptor.StartKey, false /* inverted */)
			require.NotNil(t, rng)
			require.Equal(t, roachpb.StoreID(2), rng.Lease.Replica.StoreID)
		})
	}
}

// TestEvictMetaRange tests that a query on a stale meta2 range should evict it
// from the cache.
func TestEvictMetaRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	testutils.RunTrueAndFalse(t, "hasSuggestedRange", func(t *testing.T, hasSuggestedRange bool) {
		splitKey := keys.RangeMetaKey(roachpb.RKey("b"))

		testMeta1RangeDescriptor := testMetaRangeDescriptor
		testMeta1RangeDescriptor.EndKey = roachpb.RKey(keys.Meta2Prefix)

		testMeta2RangeDescriptor1 := testMetaRangeDescriptor
		testMeta2RangeDescriptor1.RangeID = 2
		testMeta2RangeDescriptor1.StartKey = roachpb.RKey(keys.Meta2Prefix)

		testMeta2RangeDescriptor2 := testMetaRangeDescriptor
		testMeta2RangeDescriptor2.RangeID = 3
		testMeta2RangeDescriptor2.StartKey = roachpb.RKey(keys.Meta2Prefix)

		testUserRangeDescriptor1 := roachpb.RangeDescriptor{
			RangeID:  4,
			StartKey: roachpb.RKey("a"),
			EndKey:   roachpb.RKey("b"),
			InternalReplicas: []roachpb.ReplicaDescriptor{
				{
					NodeID:  1,
					StoreID: 1,
				},
			},
		}

		testUserRangeDescriptor2 := roachpb.RangeDescriptor{
			RangeID:  5,
			StartKey: roachpb.RKey("b"),
			EndKey:   roachpb.RKey("c"),
			InternalReplicas: []roachpb.ReplicaDescriptor{
				{
					NodeID:  1,
					StoreID: 1,
				},
			},
		}

		clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
		rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
		g := makeGossip(t, stopper, rpcContext)
		if err := g.AddInfoProto(gossip.KeyFirstRangeDescriptor, &testMeta1RangeDescriptor, time.Hour); err != nil {
			t.Fatal(err)
		}

		isStale := false

		var testFn simpleSendFn = func(
			_ context.Context,
			_ SendOptions,
			_ ReplicaSlice,
			ba roachpb.BatchRequest,
		) (*roachpb.BatchResponse, error) {
			rs, err := keys.Range(ba.Requests)
			if err != nil {
				t.Fatal(err)
			}
			if !kv.TestingIsRangeLookup(ba) {
				return ba.CreateReply(), nil
			}

			if bytes.HasPrefix(rs.Key, keys.Meta1Prefix) {
				// Querying meta 1 range.
				br := &roachpb.BatchResponse{}
				r := &roachpb.ScanResponse{}
				var kv roachpb.KeyValue
				if rs.Key.Equal(keys.RangeMetaKey(keys.RangeMetaKey(roachpb.RKey("a")).Next()).Next()) {
					// Scan request is [/Meta1/a - /Meta2), so return the first meta1
					// range.
					if err := kv.Value.SetProto(&testMeta2RangeDescriptor1); err != nil {
						t.Fatal(err)
					}
				} else {
					// Scan request is [/Meta1/b - /Meta2), so return the second meta1
					// range. This is needed when no SuggestedRange is returned from the
					// RangeKeyMismatch error and an additional lookup is needed to
					// determine the correct meta2 range descriptor.
					if err := kv.Value.SetProto(&testMeta2RangeDescriptor2); err != nil {
						t.Fatal(err)
					}
				}
				r.Rows = append(r.Rows, kv)
				br.Add(r)
				return br, nil
			}
			// Querying meta2 range.
			br := &roachpb.BatchResponse{}
			r := &roachpb.ScanResponse{}
			var kv roachpb.KeyValue
			if rs.Key.Equal(keys.RangeMetaKey(roachpb.RKey("a")).Next()) {
				// Scan request is [/Meta2/a - /Meta2/b), so return the first
				// user range descriptor.
				if err := kv.Value.SetProto(&testUserRangeDescriptor1); err != nil {
					t.Fatal(err)
				}
			} else if isStale {
				// Scan request is [/Meta2/b - /Meta2/c). Since we simulate a split of
				// [/Meta2 - /System) into [/Meta2 - /Meta2/a) and [/Meta2/b - /System)
				// and we sent the batch request to the stale cached meta2 range
				// descriptor [/Meta2 - /Meta2/a), we return a RangeKeyMismatchError. We
				// test for two cases here:
				// 1) The SuggestedRange is supplied and the correct meta2 range is
				//    directly inserted into the cache.
				// 2) The SuggestedRange is not supplied and we have to an additional
				//    lookup in meta1 to determine the correct meta2 range.

				// Simulate a split.
				testMeta2RangeDescriptor1.EndKey = splitKey
				testMeta2RangeDescriptor2.StartKey = splitKey
				isStale = false

				reply := ba.CreateReply()
				// Return a RangeKeyMismatchError to simulate the range being stale.
				err := roachpb.NewRangeKeyMismatchError(
					ctx, rs.Key.AsRawKey(), rs.EndKey.AsRawKey(), &testMeta2RangeDescriptor1, nil /* lease */)
				if hasSuggestedRange {
					err.AppendRangeInfo(ctx, testMeta2RangeDescriptor2, roachpb.Lease{})
				}
				reply.Error = roachpb.NewError(err)
				return reply, nil
			} else {
				// Scan request is [/Meta2/b - /Meta2/c) and the range descriptor is
				// not stale, so return the second user range descriptor.
				if err := kv.Value.SetProto(&testUserRangeDescriptor2); err != nil {
					t.Fatal(err)
				}
			}
			r.Rows = append(r.Rows, kv)
			br.Add(r)
			return br, nil
		}

		cfg := DistSenderConfig{
			AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
			Clock:      clock,
			NodeDescs:  g,
			RPCContext: rpcContext,
			TestingKnobs: ClientTestingKnobs{
				TransportFactory: adaptSimpleTransport(testFn),
			},
			NodeDialer:         nodedialer.New(rpcContext, gossip.AddressResolver(g)),
			FirstRangeProvider: g,
			Settings:           cluster.MakeTestingClusterSettings(),
		}
		ds := NewDistSender(cfg)

		scan := roachpb.NewScan(roachpb.Key("a"), roachpb.Key("b"), false)
		if _, pErr := kv.SendWrapped(ctx, ds, scan); pErr != nil {
			t.Fatalf("scan encountered error: %s", pErr)
		}

		// Verify that there is one meta2 cached range.
		cachedRange := ds.rangeCache.GetCached(keys.RangeMetaKey(roachpb.RKey("a")), false)
		if !cachedRange.Desc.StartKey.Equal(keys.Meta2Prefix) || !cachedRange.Desc.EndKey.Equal(testMetaEndKey) {
			t.Fatalf("expected cached meta2 range to be [%s, %s), actual [%s, %s)",
				keys.Meta2Prefix, testMetaEndKey, cachedRange.Desc.StartKey, cachedRange.Desc.EndKey)
		}

		// Simulate a split on the meta2 range and mark it as stale.
		isStale = true

		scan = roachpb.NewScan(roachpb.Key("b"), roachpb.Key("c"), false)
		if _, pErr := kv.SendWrapped(ctx, ds, scan); pErr != nil {
			t.Fatalf("scan encountered error: %s", pErr)
		}

		// Verify that there are two meta2 cached ranges.
		cachedRange = ds.rangeCache.GetCached(keys.RangeMetaKey(roachpb.RKey("a")), false)
		if !cachedRange.Desc.StartKey.Equal(keys.Meta2Prefix) || !cachedRange.Desc.EndKey.Equal(splitKey) {
			t.Fatalf("expected cached meta2 range to be [%s, %s), actual [%s, %s)",
				keys.Meta2Prefix, splitKey, cachedRange.Desc.StartKey, cachedRange.Desc.EndKey)
		}
		cachedRange = ds.rangeCache.GetCached(keys.RangeMetaKey(roachpb.RKey("b")), false)
		if !cachedRange.Desc.StartKey.Equal(splitKey) || !cachedRange.Desc.EndKey.Equal(testMetaEndKey) {
			t.Fatalf("expected cached meta2 range to be [%s, %s), actual [%s, %s)",
				splitKey, testMetaEndKey, cachedRange.Desc.StartKey, cachedRange.Desc.EndKey)
		}
	})
}

// TestConnectionClass verifies that the dist sender constructs a transport with
// the appropriate class for a given resolved range.
func TestConnectionClass(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	// Create a mock range descriptor DB that can resolve made up meta1, node
	// liveness and user ranges.
	rDB := MockRangeDescriptorDB(func(key roachpb.RKey, _ bool) (
		[]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error,
	) {
		if key.Equal(roachpb.KeyMin) {
			return []roachpb.RangeDescriptor{{
				RangeID:  1,
				StartKey: roachpb.RKeyMin,
				EndKey:   roachpb.RKey(keys.NodeLivenessPrefix),
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{NodeID: 1, StoreID: 1},
				},
			}}, nil, nil
		} else if bytes.HasPrefix(key, keys.NodeLivenessPrefix) {
			return []roachpb.RangeDescriptor{{
				RangeID:  2,
				StartKey: roachpb.RKey(keys.NodeLivenessPrefix),
				EndKey:   roachpb.RKey(keys.NodeLivenessKeyMax),
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{NodeID: 1, StoreID: 1},
				},
			}}, nil, nil
		}
		return []roachpb.RangeDescriptor{{
			RangeID:  3,
			StartKey: roachpb.RKey(keys.NodeLivenessKeyMax),
			EndKey:   roachpb.RKeyMax,
			InternalReplicas: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1},
			},
		}}, nil, nil
	})
	// Verify that the request carries the class we expect it to for its span.
	verifyClass := func(class rpc.ConnectionClass, args roachpb.BatchRequest) {
		span, err := keys.Range(args.Requests)
		if assert.Nil(t, err) {
			assert.Equalf(t, rpc.ConnectionClassForKey(span.Key), class,
				"unexpected class for span key %v", span.Key)
		}
	}
	var testFn simpleSendFn = func(
		_ context.Context,
		opts SendOptions,
		replicas ReplicaSlice,
		args roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		verifyClass(opts.class, args)
		return args.CreateReply(), nil
	}
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(testFn),
		},
		NodeDialer: nodedialer.New(rpcContext, gossip.AddressResolver(g)),
		RPCRetryOptions: &retry.Options{
			MaxRetries: 1,
		},
		RangeDescriptorDB: rDB,
		Settings:          cluster.MakeTestingClusterSettings(),
	}
	ds := NewDistSender(cfg)

	// Check the three important cases to ensure they are sent with the correct
	// ConnectionClass.
	for _, key := range []roachpb.Key{
		keys.Meta1Prefix,
		keys.NodeLivenessKey(1),
		keys.SystemSQLCodec.TablePrefix(1234), // A non-system table
	} {
		t.Run(key.String(), func(t *testing.T) {
			var ba roachpb.BatchRequest
			ba.Add(&roachpb.GetRequest{
				RequestHeader: roachpb.RequestHeader{
					Key: key,
				},
			})
			_, err := ds.Send(context.Background(), ba)
			require.Nil(t, err)
		})
	}
}

// TestEvictionTokenCoalesce tests when two separate batch requests are a part
// of the same stale range descriptor, they are coalesced when the range lookup
// is retried.
func TestEvictionTokenCoalesce(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	initGen := int64(1)
	testUserRangeDescriptor := roachpb.RangeDescriptor{
		RangeID:  2,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("d"),
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
		Generation: initGen,
	}

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	if err := g.AddInfoProto(gossip.KeyFirstRangeDescriptor, &testMetaRangeDescriptor, time.Hour); err != nil {
		t.Fatal(err)
	}

	sendErrors := int32(0)
	var queriedMetaKeys sync.Map

	var ds *DistSender
	var testFn simpleSendFn = func(
		ctx context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		rs, err := keys.Range(ba.Requests)
		br := ba.CreateReply()
		if err != nil {
			br.Error = roachpb.NewError(err)
			return br, nil
		}
		if !kv.TestingIsRangeLookup(ba) {
			// Return a sendError so DistSender retries the first range lookup in the
			// user key-space for both batches.
			if atomic.AddInt32(&sendErrors, 1) <= 2 {
				return nil, newSendError("boom")
			}
			return br, nil
		}

		if bytes.HasPrefix(rs.Key, keys.Meta1Prefix) {
			// Querying meta 1 range.
			br = &roachpb.BatchResponse{}
			r := &roachpb.ScanResponse{}
			var kv roachpb.KeyValue
			if err := kv.Value.SetProto(&testMetaRangeDescriptor); err != nil {
				br.Error = roachpb.NewError(err)
				return br, nil
			}
			r.Rows = append(r.Rows, kv)
			br.Add(r)
			return br, nil
		}
		// Querying meta2 range.
		br = &roachpb.BatchResponse{}
		r := &roachpb.ScanResponse{}
		var kv roachpb.KeyValue
		if err := kv.Value.SetProto(&testUserRangeDescriptor); err != nil {
			br.Error = roachpb.NewError(err)
			return br, nil
		}
		r.Rows = append(r.Rows, kv)
		br.Add(r)
		// The first query for each batch request key of the meta1 range should be
		// in separate requests because there is no prior eviction token.
		if _, ok := queriedMetaKeys.Load(string(rs.Key)); ok {
			// Wait until we have two in-flight requests.
			if err := testutils.SucceedsSoonError(func() error {
				// Since the previously fetched RangeDescriptor was ["a", "d"), the request keys
				// would be coalesced to "a".
				numCalls := ds.rangeCache.lookupRequests.NumCalls(fmt.Sprintf("a:false:%d", initGen))
				if numCalls != 2 {
					return errors.Errorf("expected %d in-flight requests, got %d", 2, numCalls)
				}
				return nil
			}); err != nil {
				br.Error = roachpb.NewError(err)
				return br, nil
			}
		}
		queriedMetaKeys.Store(string(rs.Key), struct{}{})
		return br, nil
	}

	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		NodeDescs:  g,
		RPCContext: rpcContext,
		RPCRetryOptions: &retry.Options{
			MaxRetries: 1,
		},
		NodeDialer:         nodedialer.New(rpcContext, gossip.AddressResolver(g)),
		FirstRangeProvider: g,
		Settings:           cluster.MakeTestingClusterSettings(),
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(testFn),
		},
	}
	ds = NewDistSender(cfg)

	var batchWaitGroup sync.WaitGroup
	putFn := func(key, value string) {
		defer batchWaitGroup.Done()
		put := roachpb.NewPut(roachpb.Key(key), roachpb.MakeValueFromString("c"))
		if _, pErr := kv.SendWrapped(context.Background(), ds, put); pErr != nil {
			t.Errorf("put encountered error: %s", pErr)
		}
	}
	batchWaitGroup.Add(2)
	go putFn("b", "b")
	go putFn("c", "c")
	batchWaitGroup.Wait()
}

func TestDistSenderSlowLogMessage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const (
		dur      = 8158 * time.Millisecond
		attempts = 120
	)
	desc := &roachpb.RangeDescriptor{RangeID: 9, StartKey: roachpb.RKey("x")}
	{
		exp := `have been waiting 8.16s (120 attempts) for RPC to` +
			` r9:{-} [<no replicas>, next=0, gen=0]: boom`
		act := slowRangeRPCWarningStr(
			dur,
			120,
			desc,
			roachpb.NewError(errors.New("boom")))

		require.Equal(t, exp, act)
	}

	{
		exp := `slow RPC finished after 8.16s (120 attempts)`
		act := slowRangeRPCReturnWarningStr(dur, attempts)
		require.Equal(t, exp, act)
	}
}

// Test the following scenario: the DistSender sends a request that results in a
// sendError, meaning that the descriptor is probably stale. The descriptor is
// then refreshed, and it turns out that the range had split in the meantime.
// Thus, the request now needs to be divided. The test checks that the request
// is divided before being sent to the new descriptor. This acts as a regression
// test, since we used to blindly try the non-divided request against the
// smaller range, only to get an entirely predictable RangeKeyMismatchError.
func TestRequestSubdivisionAfterDescriptorChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)

	// First request will be sent to an unsplit descriptor.
	var initialDesc = roachpb.RangeDescriptor{
		RangeID:    roachpb.RangeID(1),
		Generation: 1,
		StartKey:   roachpb.RKeyMin,
		EndKey:     roachpb.RKeyMax,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}

	// But the 2nd attempt will use the split ones.
	splitKey := roachpb.RKey("b")
	splitDescs := []roachpb.RangeDescriptor{{
		RangeID:    roachpb.RangeID(1),
		Generation: 2,
		StartKey:   roachpb.RKeyMin,
		EndKey:     splitKey,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}, {
		RangeID:    roachpb.RangeID(2),
		Generation: 2,
		StartKey:   splitKey,
		EndKey:     roachpb.RKeyMax,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}}

	initialRDB := MockRangeDescriptorDB(func(key roachpb.RKey, reverse bool) (
		[]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error,
	) {
		return []roachpb.RangeDescriptor{initialDesc}, nil, nil
	})
	splitRDB := MockRangeDescriptorDB(func(key roachpb.RKey, reverse bool) (
		[]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error,
	) {
		if key.Less(splitKey) {
			return splitDescs[0:1], nil, nil
		}
		return splitDescs[1:], nil, nil
	})

	returnErr := true
	var switchToSplitDesc func()
	var transportFn = func(
		_ context.Context,
		opts SendOptions,
		replicas ReplicaSlice,
		ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		if returnErr {
			// First time around we return an RPC error. Next time around, make sure
			// the DistSender tries gets the split descriptors.
			if len(ba.Requests) != 2 {
				// Sanity check - first attempt should have the unsplit batch.
				rep := ba.CreateReply()
				rep.Error = roachpb.NewErrorf("expected divided batches with one request each, got: %s", ba)
				return rep, nil
			}
			switchToSplitDesc()
			returnErr = false
			return nil, errors.New("boom")
		}
		rep := ba.CreateReply()
		if len(ba.Requests) != 1 {
			rep.Error = roachpb.NewErrorf("expected divided batches with one request each, got: %s", ba)
		}
		return rep, nil
	}

	cfg := DistSenderConfig{
		AmbientCtx:        log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:             clock,
		NodeDescs:         g,
		RPCContext:        rpcContext,
		RangeDescriptorDB: initialRDB,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(transportFn),
		},
		Settings: cluster.MakeTestingClusterSettings(),
	}

	ds := NewDistSender(cfg)
	switchToSplitDesc = func() {
		ds.rangeCache.db = splitRDB
	}

	// We're going to send a batch with two gets, on different sides of the split.
	// The DistSender will first use the unsplit descriptor, and we'll inject an
	// RPC error which will cause the eviction of the descriptor from the cache.
	// Then, we'll switch the descriptor db that the DistSender uses to the
	// version that returns a split descriptor (see switchToSplitDesc). From this
	// moment on, we check that the sent batches only consist of single requests -
	// which proves that the original batch was split.

	var ba roachpb.BatchRequest
	get := &roachpb.GetRequest{}
	get.Key = roachpb.Key("a")
	ba.Add(get)
	get = &roachpb.GetRequest{}
	get.Key = roachpb.Key("c")
	ba.Add(get)
	// Inconsistent read because otherwise the batch will ask to be re-sent in a
	// txn when split.
	ba.ReadConsistency = roachpb.INCONSISTENT

	if _, pErr := ds.Send(ctx, ba); pErr != nil {
		t.Fatal(pErr)
	}
}
