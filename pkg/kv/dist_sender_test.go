// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package kv

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/gossip/simulation"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
		Replicas: []roachpb.ReplicaDescriptor{
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
		Replicas: []roachpb.ReplicaDescriptor{
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
		Replicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
			{
				NodeID:  2,
				StoreID: 2,
			},
			{
				NodeID:  3,
				StoreID: 3,
			},
		},
	}
)

var testAddress = util.NewUnresolvedAddr("tcp", "node1")

// rpcSendFn is the function type used to dispatch RPC calls.
type rpcSendFn func(
	context.Context,
	SendOptions,
	ReplicaSlice,
	roachpb.BatchRequest,
	*rpc.Context,
) (*roachpb.BatchResponse, error)

// stubRPCSendFn is an rpcSendFn that simply creates a reply for the
// BatchRequest without performing an RPC call or triggering any
// test instrumentation.
var stubRPCSendFn rpcSendFn = func(
	_ context.Context, _ SendOptions, _ ReplicaSlice, args roachpb.BatchRequest, _ *rpc.Context,
) (*roachpb.BatchResponse, error) {
	return args.CreateReply(), nil
}

// adaptLegacyTransport converts the RPCSend functions used in these
// tests to the newer transport interface.
//
// TODO(bdarnell): phase out this interface.
func adaptLegacyTransport(fn rpcSendFn) TransportFactory {
	return func(
		opts SendOptions,
		rpcContext *rpc.Context,
		replicas ReplicaSlice,
		args roachpb.BatchRequest,
	) (Transport, error) {
		return &legacyTransportAdapter{fn, opts, rpcContext, replicas, args, false}, nil
	}
}

type legacyTransportAdapter struct {
	fn         rpcSendFn
	opts       SendOptions
	rpcContext *rpc.Context
	replicas   ReplicaSlice
	args       roachpb.BatchRequest

	called bool
}

func (l *legacyTransportAdapter) IsExhausted() bool {
	return l.called
}

func (l *legacyTransportAdapter) GetPending() []roachpb.ReplicaDescriptor {
	return nil
}

func (l *legacyTransportAdapter) SendNext(ctx context.Context, done chan<- BatchCall) {
	l.called = true
	br, err := l.fn(ctx, l.opts, l.replicas, l.args, l.rpcContext)
	done <- BatchCall{
		Reply: br,
		Err:   err,
	}
}

func (l *legacyTransportAdapter) NextReplica() roachpb.ReplicaDescriptor {
	return roachpb.ReplicaDescriptor{}
}

func (*legacyTransportAdapter) MoveToFront(roachpb.ReplicaDescriptor) {
}

func (*legacyTransportAdapter) Close() {
}

// TestSendRPCOrder verifies that sendRPC correctly takes into account the
// lease holder, attributes and required consistency to determine where to send
// remote requests.
func TestSendRPCOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
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
		// Put with matching attributes that finds the lease holder (node 3).
		// Should address the lease holder and the two nodes matching the attributes
		// (the last and second to last) in that order.
		{
			args:  &roachpb.PutRequest{},
			tiers: append(nodeTiers[5], roachpb.Tier{Key: "irrelevant", Value: ""}),
			// Compare only the first resulting addresses as we have a lease holder
			// and that means we're only trying to send there.
			expReplica:  []roachpb.NodeID{2, 5, 4, 0, 0},
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
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKeyMax,
		RangeID:  rangeID,
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
		descriptor.Replicas = append(descriptor.Replicas, roachpb.ReplicaDescriptor{
			NodeID:  roachpb.NodeID(i),
			StoreID: roachpb.StoreID(i),
		})
	}

	// Stub to be changed in each test case.
	var verifyCall func(SendOptions, ReplicaSlice) error

	var testFn rpcSendFn = func(
		_ context.Context,
		opts SendOptions,
		replicas ReplicaSlice,
		args roachpb.BatchRequest,
		_ *rpc.Context,
	) (*roachpb.BatchResponse, error) {
		if err := verifyCall(opts, replicas); err != nil {
			return nil, err
		}
		return args.CreateReply(), nil
	}

	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		TestingKnobs: DistSenderTestingKnobs{
			TransportFactory: adaptLegacyTransport(testFn),
		},
		RangeDescriptorDB: mockRangeDescriptorDBForDescs(descriptor),
	}

	ds := NewDistSender(cfg, g)

	for n, tc := range testCases {
		verifyCall = makeVerifier(tc.expReplica)

		{
			// The local node needs to get its attributes during sendRPC.
			nd := &roachpb.NodeDescriptor{
				NodeID: 6,
				Locality: roachpb.Locality{
					Tiers: tc.tiers,
				},
			}
			g.NodeID.Reset(nd.NodeID)
			if err := g.SetNodeDescriptor(nd); err != nil {
				t.Fatal(err)
			}
		}

		ds.leaseHolderCache.Update(
			context.TODO(), rangeID, roachpb.StoreID(0),
		)
		if tc.leaseHolder > 0 {
			ds.leaseHolderCache.Update(
				context.TODO(), rangeID, descriptor.Replicas[tc.leaseHolder-1].StoreID,
			)
		}

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
		if _, err := client.SendWrappedWith(context.Background(), ds, roachpb.Header{
			RangeID:         rangeID, // Not used in this test, but why not.
			ReadConsistency: consistency,
		}, args); err != nil {
			t.Errorf("%d: %s", n, err)
		}
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
			_, _, err := rdc.LookupRangeDescriptor(context.Background(), metaKey, nil, useReverseScan)
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
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
	var testFn rpcSendFn = func(
		_ context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		args roachpb.BatchRequest,
		_ *rpc.Context,
	) (*roachpb.BatchResponse, error) {
		reply := args.CreateReply()
		txnClone := args.Txn.Clone()
		reply.Txn = &txnClone
		reply.Txn.Timestamp = hlc.MaxTimestamp
		return reply, nil
	}

	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		TestingKnobs: DistSenderTestingKnobs{
			TransportFactory: adaptLegacyTransport(testFn),
		},
		RangeDescriptorDB: defaultMockRangeDescriptorDB,
	}

	ds := NewDistSender(cfg, g)

	txn := roachpb.MakeTransaction(
		"test", nil /* baseKey */, roachpb.NormalUserPriority,
		enginepb.SERIALIZABLE, clock.Now(), clock.MaxOffset().Nanoseconds(),
	)
	origTxnTs := txn.Timestamp

	// An optimization does copy-on-write if we haven't observed anything,
	// so make sure we're not in that case.
	txn.UpdateObservedTimestamp(1, hlc.MaxTimestamp)

	put := roachpb.NewPut(roachpb.Key("don't"), roachpb.Value{})
	if _, pErr := client.SendWrappedWith(context.Background(), ds, roachpb.Header{
		Txn: &txn,
	}, put); pErr != nil {
		t.Fatal(pErr)
	}

	if txn.Timestamp != origTxnTs {
		t.Fatal("Transaction was mutated by DistSender")
	}
}

// TestRetryOnNotLeaseHolderError verifies that the DistSender correctly updates the
// lease holder cache and retries when receiving a NotLeaseHolderError.
func TestRetryOnNotLeaseHolderError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
	leaseHolder := roachpb.ReplicaDescriptor{
		NodeID:  99,
		StoreID: 999,
	}
	first := true

	var testFn rpcSendFn = func(
		_ context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		args roachpb.BatchRequest,
		_ *rpc.Context,
	) (*roachpb.BatchResponse, error) {
		if first {
			reply := &roachpb.BatchResponse{}
			reply.Error = roachpb.NewError(
				&roachpb.NotLeaseHolderError{LeaseHolder: &leaseHolder})
			first = false
			return reply, nil
		}
		return args.CreateReply(), nil
	}

	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		TestingKnobs: DistSenderTestingKnobs{
			TransportFactory: adaptLegacyTransport(testFn),
		},
		RangeDescriptorDB: defaultMockRangeDescriptorDB,
	}
	ds := NewDistSender(cfg, g)
	v := roachpb.MakeValueFromString("value")
	put := roachpb.NewPut(roachpb.Key("a"), v)
	if _, err := client.SendWrapped(context.Background(), ds, put); err != nil {
		t.Errorf("put encountered error: %s", err)
	}
	if first {
		t.Errorf("The command did not retry")
	}
	rangeID := roachpb.RangeID(2)
	if cur, ok := ds.leaseHolderCache.Lookup(context.TODO(), rangeID); !ok {
		t.Errorf("lease holder cache was not updated: expected %+v", leaseHolder)
	} else if cur != leaseHolder.StoreID {
		t.Errorf("lease holder cache was not updated: expected %d, got %d", leaseHolder.StoreID, cur)
	}
}

// TestRetryOnDescriptorLookupError verifies that the DistSender retries a descriptor
// lookup on any error.
func TestRetryOnDescriptorLookupError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)

	errs := []error{
		errors.New("boom"),
		nil,
	}

	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		TestingKnobs: DistSenderTestingKnobs{
			TransportFactory: adaptLegacyTransport(stubRPCSendFn),
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
	}
	ds := NewDistSender(cfg, g)
	put := roachpb.NewPut(roachpb.Key("a"), roachpb.MakeValueFromString("value"))
	// Error on descriptor lookup, second attempt successful.
	if _, pErr := client.SendWrapped(context.Background(), ds, put); pErr != nil {
		t.Errorf("unexpected error: %s", pErr)
	}
	if len(errs) != 0 {
		t.Fatalf("expected more descriptor lookups, leftover errs: %+v", errs)
	}
}

func makeGossip(t *testing.T, stopper *stop.Stopper) (*gossip.Gossip, *hlc.Clock) {
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewContext(
		log.AmbientContext{Tracer: tracing.NewTracer()},
		&base.Config{Insecure: true},
		clock,
		stopper,
		&cluster.MakeTestingClusterSettings().Version,
	)
	server := rpc.NewServer(rpcContext)

	const nodeID = 1
	g := gossip.NewTest(nodeID, rpcContext, server, stopper, metric.NewRegistry())
	if err := g.SetNodeDescriptor(&roachpb.NodeDescriptor{
		NodeID:  nodeID,
		Address: util.MakeUnresolvedAddr("tcp", "neverused:9999"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddInfo(gossip.KeySentinel, nil, time.Hour); err != nil {
		t.Fatal(err)
	}

	return g, clock
}

// TestEvictOnFirstRangeGossip verifies that we evict the first range
// descriptor from the descriptor cache when a gossip update is received for
// the first range.
func TestEvictOnFirstRangeGossip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)

	sender := func(
		_ context.Context, ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error) {
		return ba.CreateReply(), nil
	}

	desc := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKeyMax,
		Replicas: []roachpb.ReplicaDescriptor{
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
		TestingKnobs: DistSenderTestingKnobs{
			TransportFactory: SenderTransportFactory(
				tracing.NewTracer(),
				client.SenderFunc(sender),
			),
		},
		RangeDescriptorDB: rDB,
	}

	ds := NewDistSender(cfg, g).withMetaRecursion()

	anyKey := roachpb.Key("anything")
	rAnyKey := keys.MustAddr(anyKey)

	call := func() {
		if _, _, err := ds.rangeCache.LookupRangeDescriptor(
			context.Background(), rAnyKey, nil, false,
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
	// if rpcError is true, the first attempt gets an RPC error, otherwise
	// the RPC call succeeds but there is an error in the RequestHeader.
	// Currently lease holder and cached range descriptor are treated equally.
	// TODO(bdarnell): refactor to cover different types of retryable errors.
	testCases := []struct {
		rpcError               bool
		replicaError           error
		shouldClearLeaseHolder bool
		shouldClearReplica     bool
	}{
		{false, nil, false, false},                              // non-retryable replica error
		{false, &roachpb.RangeKeyMismatchError{}, false, false}, // RangeKeyMismatch replica error
		{true, &roachpb.RangeKeyMismatchError{}, false, false},  // RPC error aka all nodes dead
		{false, &roachpb.RangeNotFoundError{}, false, false},    // RangeNotFound replica error
		{true, &roachpb.RangeNotFoundError{}, false, false},     // RPC error aka all nodes dead
	}

	const errString = "boom"

	for i, tc := range testCases {
		stopper := stop.NewStopper()
		defer stopper.Stop(context.TODO())

		g, clock := makeGossip(t, stopper)
		leaseHolder := roachpb.ReplicaDescriptor{
			NodeID:  99,
			StoreID: 999,
		}
		first := true

		var testFn rpcSendFn = func(
			_ context.Context,
			_ SendOptions,
			_ ReplicaSlice,
			args roachpb.BatchRequest,
			_ *rpc.Context,
		) (*roachpb.BatchResponse, error) {
			if !first {
				return args.CreateReply(), nil
			}
			first = false
			if tc.rpcError {
				return nil, roachpb.NewSendError(errString)
			}
			var err error
			if tc.replicaError != nil {
				err = tc.replicaError
			} else {
				err = errors.New(errString)
			}
			reply := &roachpb.BatchResponse{}
			reply.Error = roachpb.NewError(err)
			return reply, nil
		}

		cfg := DistSenderConfig{
			AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
			Clock:      clock,
			TestingKnobs: DistSenderTestingKnobs{
				TransportFactory: adaptLegacyTransport(testFn),
			},
			RangeDescriptorDB: defaultMockRangeDescriptorDB,
		}
		ds := NewDistSender(cfg, g)
		ds.leaseHolderCache.Update(context.TODO(), 1, leaseHolder.StoreID)
		key := roachpb.Key("a")
		put := roachpb.NewPut(key, roachpb.MakeValueFromString("value"))

		if _, pErr := client.SendWrapped(context.Background(), ds, put); pErr != nil && !testutils.IsPError(pErr, errString) {
			t.Errorf("put encountered unexpected error: %s", pErr)
		}
		if _, ok := ds.leaseHolderCache.Lookup(context.TODO(), 1); ok != !tc.shouldClearLeaseHolder {
			t.Errorf("%d: lease holder cache eviction: shouldClearLeaseHolder=%t, but value is %t", i, tc.shouldClearLeaseHolder, ok)
		}
		if cachedDesc, err := ds.rangeCache.GetCachedRangeDescriptor(roachpb.RKey(key), false /* inverted */); err != nil {
			t.Error(err)
		} else if cachedDesc == nil != tc.shouldClearReplica {
			t.Errorf("%d: unexpected second replica lookup behavior: wanted=%t", i, tc.shouldClearReplica)
		}
	}
}

func TestEvictCacheOnUnknownLeaseHolder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)

	// Gossip the two nodes referred to in testUserRangeDescriptor3Replicas.
	for i := 2; i <= 3; i++ {
		addr := util.MakeUnresolvedAddr("tcp", fmt.Sprintf("node%d", i))
		nd := &roachpb.NodeDescriptor{
			NodeID:  roachpb.NodeID(i),
			Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
		}
		if err := g.AddInfoProto(gossip.MakeNodeIDKey(roachpb.NodeID(i)), nd, time.Hour); err != nil {
			t.Fatal(err)
		}
	}

	var count int32
	var testFn rpcSendFn = func(
		_ context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		args roachpb.BatchRequest,
		_ *rpc.Context,
	) (*roachpb.BatchResponse, error) {
		var err error
		switch count {
		case 0, 1:
			err = &roachpb.NotLeaseHolderError{LeaseHolder: &roachpb.ReplicaDescriptor{NodeID: 99, StoreID: 999}}
		case 2:
			err = roachpb.NewRangeNotFoundError(0)
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
		TestingKnobs: DistSenderTestingKnobs{
			TransportFactory: adaptLegacyTransport(testFn),
		},
		RangeDescriptorDB: threeReplicaMockRangeDescriptorDB,
	}
	ds := NewDistSender(cfg, g)
	key := roachpb.Key("a")
	put := roachpb.NewPut(key, roachpb.MakeValueFromString("value"))

	if _, pErr := client.SendWrapped(context.Background(), ds, put); pErr != nil {
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
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
	if err := g.AddInfoProto(gossip.KeyFirstRangeDescriptor, &testMetaRangeDescriptor, time.Hour); err != nil {
		t.Fatal(err)
	}

	// Updated below, after it has first been returned.
	badEndKey := roachpb.RKey("m")
	newRangeDescriptor := testUserRangeDescriptor
	goodEndKey := newRangeDescriptor.EndKey
	newRangeDescriptor.EndKey = badEndKey
	descStale := true

	var testFn rpcSendFn = func(
		_ context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		ba roachpb.BatchRequest,
		_ *rpc.Context,
	) (*roachpb.BatchResponse, error) {
		rs, err := keys.Range(ba)
		if err != nil {
			t.Fatal(err)
		}
		if client.TestingIsRangeLookup(ba) {
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
		TestingKnobs: DistSenderTestingKnobs{
			TransportFactory: adaptLegacyTransport(testFn),
		},
	}
	ds := NewDistSender(cfg, g)
	scan := roachpb.NewScan(roachpb.Key("a"), roachpb.Key("d"))
	if _, err := client.SendWrapped(context.Background(), ds, scan); err != nil {
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
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
	if err := g.AddInfoProto(gossip.KeyFirstRangeDescriptor, &testMetaRangeDescriptor, time.Hour); err != nil {
		t.Fatal(err)
	}

	// Updated below, after it has first been returned.
	goodRangeDescriptor := testUserRangeDescriptor
	badRangeDescriptor := testUserRangeDescriptor
	badRangeDescriptor.EndKey = roachpb.RKey("zBad")
	badRangeDescriptor.RangeID++
	firstLookup := true

	var testFn rpcSendFn = func(
		_ context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		ba roachpb.BatchRequest,
		_ *rpc.Context,
	) (*roachpb.BatchResponse, error) {
		rs, err := keys.Range(ba)
		if err != nil {
			t.Fatal(err)
		}
		if client.TestingIsRangeLookup(ba) {
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
			if err := kv.Value.SetProto(&badRangeDescriptor); err != nil {
				t.Fatal(err)
			}
			r.Rows = append(r.Rows, kv)
			br.Add(r)
			return br, nil
		}

		// When the Scan first turns up, provide the correct descriptor as a
		// suggestion for future range descriptor lookups.
		if ba.RangeID == badRangeDescriptor.RangeID {
			var br roachpb.BatchResponse
			br.Error = roachpb.NewError(&roachpb.RangeKeyMismatchError{
				RequestStartKey: rs.Key.AsRawKey(),
				RequestEndKey:   rs.EndKey.AsRawKey(),
				SuggestedRange:  &goodRangeDescriptor,
			})
			return &br, nil
		} else if ba.RangeID != goodRangeDescriptor.RangeID {
			t.Fatalf("unexpected RangeID %d provided in request %v", ba.RangeID, ba)
		}
		return ba.CreateReply(), nil
	}

	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		TestingKnobs: DistSenderTestingKnobs{
			TransportFactory: adaptLegacyTransport(testFn),
		},
	}
	ds := NewDistSender(cfg, g)
	scan := roachpb.NewScan(roachpb.Key("a"), roachpb.Key("d"))
	if _, err := client.SendWrapped(context.Background(), ds, scan); err != nil {
		t.Errorf("scan encountered error: %s", err)
	}
}

func TestGetFirstRangeDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	n := simulation.NewNetwork(stopper, 3, true)
	for _, node := range n.Nodes {
		// TODO(spencer): remove the use of gossip/simulation here.
		node.Gossip.EnableSimulationCycler(false)
	}
	n.Start()
	ds := NewDistSender(DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
	}, n.Nodes[0].Gossip)
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
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
	if err := g.SetNodeDescriptor(&roachpb.NodeDescriptor{NodeID: 1}); err != nil {
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

		descriptor.Replicas = append(descriptor.Replicas, roachpb.ReplicaDescriptor{
			NodeID:  roachpb.NodeID(i),
			StoreID: roachpb.StoreID(i),
		})
	}
	descDB := mockRangeDescriptorDBForDescs(
		testMetaRangeDescriptor,
		descriptor,
	)

	var testFn rpcSendFn = func(
		_ context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		args roachpb.BatchRequest,
		_ *rpc.Context,
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
		TestingKnobs: DistSenderTestingKnobs{
			TransportFactory: adaptLegacyTransport(testFn),
		},
		RangeDescriptorDB: descDB,
	}
	ds := NewDistSender(cfg, g)
	scan := roachpb.NewScan(roachpb.Key("a"), roachpb.Key("d"))
	sr, err := client.SendWrappedWith(context.Background(), ds, roachpb.Header{MaxSpanRequestKeys: 1}, scan)
	if err != nil {
		t.Fatal(err)
	}
	if l := len(sr.(*roachpb.ScanResponse).Rows); l != 1 {
		t.Fatalf("expected 1 row; got %d", l)
	}
}

// TestGetNodeDescriptor checks that the Node descriptor automatically gets
// looked up from Gossip.
func TestGetNodeDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
	ds := NewDistSender(DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
	}, g)
	g.NodeID.Reset(5)
	if err := g.SetNodeDescriptor(&roachpb.NodeDescriptor{NodeID: 5}); err != nil {
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
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)

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
			Replicas: []roachpb.ReplicaDescriptor{
				{
					NodeID:  1,
					StoreID: 1,
				},
			},
		})
	}

	sender := client.SenderFunc(
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
		RangeDescriptorDB: rdb,
		TestingKnobs: DistSenderTestingKnobs{
			TransportFactory: SenderTransportFactory(
				tracing.NewTracer(),
				sender,
			),
		},
	}

	ds := NewDistSender(cfg, g)

	txn := roachpb.MakeTransaction("foo", nil, 1.0, enginepb.SERIALIZABLE, clock.Now(), 0)

	var ba roachpb.BatchRequest
	ba.Txn = &txn
	ba.Add(roachpb.NewReverseScan(splits[0], splits[1]))
	ba.Add(roachpb.NewReverseScan(splits[2], splits[3]))

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
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
	// Assume we have two ranges, [a-b) and [b-KeyMax).
	merged := false
	// The stale first range descriptor which is unaware of the merge.
	var firstRange = roachpb.RangeDescriptor{
		RangeID:  2,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("b"),
		Replicas: []roachpb.ReplicaDescriptor{
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
		Replicas: []roachpb.ReplicaDescriptor{
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
	var testFn rpcSendFn = func(
		_ context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		ba roachpb.BatchRequest,
		_ *rpc.Context,
	) (*roachpb.BatchResponse, error) {
		rs, err := keys.Range(ba)
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
		TestingKnobs: DistSenderTestingKnobs{
			TransportFactory: adaptLegacyTransport(testFn),
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
	}
	ds := NewDistSender(cfg, g)
	scan := roachpb.NewScan(roachpb.Key("a"), roachpb.Key("d"))
	// Set the Txn info to avoid an OpRequiresTxnError.
	reply, err := client.SendWrappedWith(context.Background(), ds, roachpb.Header{
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
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		TestingKnobs: DistSenderTestingKnobs{
			TransportFactory: adaptLegacyTransport(stubRPCSendFn),
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
	}
	ds := NewDistSender(cfg, g)
	rScan := &roachpb.ReverseScanRequest{
		Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
	}
	if _, err := client.SendWrapped(context.Background(), ds, rScan); err != nil {
		t.Fatal(err)
	}
}

// TestClockUpdateOnResponse verifies that the DistSender picks up
// the timestamp of the remote party embedded in responses.
func TestClockUpdateOnResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
	cfg := DistSenderConfig{
		AmbientCtx:        log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:             clock,
		RangeDescriptorDB: defaultMockRangeDescriptorDB,
	}
	ds := NewDistSender(cfg, g)

	expectedErr := roachpb.NewError(errors.New("boom"))

	// Prepare the test function
	put := roachpb.NewPut(roachpb.Key("a"), roachpb.MakeValueFromString("value"))
	doCheck := func(sender client.Sender, fakeTime hlc.Timestamp) {
		ds.transportFactory = SenderTransportFactory(tracing.NewTracer(), sender)
		_, err := client.SendWrapped(context.Background(), ds, put)
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
	replyNormal := client.SenderFunc(
		func(_ context.Context, args roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			rb := args.CreateReply()
			rb.Now = fakeTime
			return rb, nil
		})
	doCheck(replyNormal, fakeTime)

	// Test timestamp propagation on errors.
	fakeTime = ds.clock.Now().Add(10000000000 /*10s*/, 0)
	replyError := client.SenderFunc(
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
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
	if err := g.SetNodeDescriptor(&roachpb.NodeDescriptor{NodeID: 1}); err != nil {
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
		Replicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	var descriptor2 = roachpb.RangeDescriptor{
		RangeID:  3,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("c"),
		Replicas: []roachpb.ReplicaDescriptor{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
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
		_ *rpc.Context,
	) (*roachpb.BatchResponse, error) {
		rs, err := keys.Range(ba)
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
		TestingKnobs: DistSenderTestingKnobs{
			TransportFactory: adaptLegacyTransport(sendStub),
		},
		RangeDescriptorDB: descDB,
	}
	ds := NewDistSender(cfg, g)

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
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
	if err := g.SetNodeDescriptor(&roachpb.NodeDescriptor{NodeID: 1}); err != nil {
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
		Replicas: []roachpb.ReplicaDescriptor{
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
		Replicas: []roachpb.ReplicaDescriptor{
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
		Replicas: []roachpb.ReplicaDescriptor{
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
		_ *rpc.Context,
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
		TestingKnobs: DistSenderTestingKnobs{
			TransportFactory: adaptLegacyTransport(sendStub),
		},
		RangeDescriptorDB: descDB,
	}
	ds := NewDistSender(cfg, g)

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
	ba.Add(roachpb.NewScan(keys.RangeDescriptorKey(roachpb.RKey("a")), keys.RangeDescriptorKey(roachpb.RKey("c"))))

	if _, pErr := ds.Send(context.Background(), ba); pErr != nil {
		t.Fatal(pErr)
	}
	for i, found := range haveRequest {
		if !found {
			t.Errorf("request %d not received", i)
		}
	}
}

// TestSequenceUpdate verifies txn sequence number is incremented
// on successive commands.
func TestSequenceUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
	if err := g.SetNodeDescriptor(&roachpb.NodeDescriptor{NodeID: 1}); err != nil {
		t.Fatal(err)
	}
	nd := &roachpb.NodeDescriptor{
		NodeID:  roachpb.NodeID(1),
		Address: util.MakeUnresolvedAddr(testAddress.Network(), testAddress.String()),
	}
	if err := g.AddInfoProto(gossip.MakeNodeIDKey(roachpb.NodeID(1)), nd, time.Hour); err != nil {
		t.Fatal(err)

	}

	var expSequence int32 = 1 // sequence numbers are 1-based.
	var testFn rpcSendFn = func(
		_ context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		ba roachpb.BatchRequest,
		_ *rpc.Context,
	) (*roachpb.BatchResponse, error) {
		expSequence++
		if expSequence != ba.Txn.Sequence {
			t.Errorf("expected sequence %d; got %d", expSequence, ba.Txn.Sequence)
		}
		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	}

	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		TestingKnobs: DistSenderTestingKnobs{
			TransportFactory: adaptLegacyTransport(testFn),
		},
		RangeDescriptorDB: defaultMockRangeDescriptorDB,
	}
	ds := NewDistSender(cfg, g)

	// Send 5 puts and verify sequence number increase.
	txn := roachpb.MakeTransaction(
		"test", nil /* baseKey */, roachpb.NormalUserPriority,
		enginepb.SERIALIZABLE,
		clock.Now(),
		clock.MaxOffset().Nanoseconds(),
	)
	for i := 0; i < 5; i++ {
		var ba roachpb.BatchRequest
		ba.Txn = &txn
		ba.Add(roachpb.NewPut(roachpb.Key("a"), roachpb.MakeValueFromString("foo")).(*roachpb.PutRequest))
		br, pErr := ds.Send(context.Background(), ba)
		if pErr != nil {
			t.Fatal(pErr)
		}
		txn = *br.Txn
	}
}

type batchMethods struct {
	sequence int32
	methods  []roachpb.Method
}
type batchMethodsSlice []batchMethods

func (s batchMethodsSlice) Len() int      { return len(s) }
func (s batchMethodsSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s batchMethodsSlice) Less(i, j int) bool {
	return s[i].sequence < s[j].sequence && s[i].methods[0] != roachpb.EndTransaction
}

// TestMultiRangeSplitEndTransaction verifies that when a chunk of
// batch looks like it's going to be dispatched to more than one
// range, it will be split up if it contains an EndTransaction.
func TestMultiRangeSplitEndTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
	testCases := []struct {
		put1, put2, et roachpb.Key
		exp            [][]roachpb.Method
	}{
		{
			// Everything hits the first range, so we get a 1PC txn.
			roachpb.Key("a1"), roachpb.Key("a2"), roachpb.Key("a3"),
			[][]roachpb.Method{{roachpb.Put, roachpb.Put, roachpb.EndTransaction}},
		},
		{
			// Only EndTransaction hits the second range.
			roachpb.Key("a1"), roachpb.Key("a2"), roachpb.Key("b"),
			[][]roachpb.Method{{roachpb.Put, roachpb.Put}, {roachpb.EndTransaction}},
		},
		{
			// One write hits the second range, so EndTransaction has to be split off.
			// In this case, going in the usual order without splitting off
			// would actually be fine, but it doesn't seem worth optimizing at
			// this point.
			roachpb.Key("a1"), roachpb.Key("b1"), roachpb.Key("a1"),
			[][]roachpb.Method{{roachpb.Put}, {roachpb.Put}, {roachpb.EndTransaction}},
		},
		{
			// Both writes go to the second range, but not EndTransaction.
			roachpb.Key("b1"), roachpb.Key("b2"), roachpb.Key("a1"),
			[][]roachpb.Method{{roachpb.Put, roachpb.Put}, {roachpb.EndTransaction}},
		},
	}

	if err := g.SetNodeDescriptor(&roachpb.NodeDescriptor{NodeID: 1}); err != nil {
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
		Replicas: []roachpb.ReplicaDescriptor{
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
		Replicas: []roachpb.ReplicaDescriptor{
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
		var mu syncutil.Mutex
		act := batchMethodsSlice{}
		var testFn rpcSendFn = func(
			_ context.Context,
			_ SendOptions,
			_ ReplicaSlice, ba roachpb.BatchRequest,
			_ *rpc.Context,
		) (*roachpb.BatchResponse, error) {
			var cur []roachpb.Method
			for _, union := range ba.Requests {
				cur = append(cur, union.GetInner().Method())
			}
			mu.Lock()
			act = append(act, batchMethods{sequence: ba.Txn.Sequence, methods: cur})
			mu.Unlock()
			return ba.CreateReply(), nil
		}

		cfg := DistSenderConfig{
			AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
			Clock:      clock,
			TestingKnobs: DistSenderTestingKnobs{
				TransportFactory: adaptLegacyTransport(testFn),
			},
			RangeDescriptorDB: descDB,
		}
		ds := NewDistSender(cfg, g)

		// Send a batch request containing two puts.
		var ba roachpb.BatchRequest
		ba.Txn = &roachpb.Transaction{Name: "test"}
		val := roachpb.MakeValueFromString("val")
		ba.Add(roachpb.NewPut(test.put1, val))
		val = roachpb.MakeValueFromString("val")
		ba.Add(roachpb.NewPut(test.put2, val))
		ba.Add(&roachpb.EndTransactionRequest{Span: roachpb.Span{Key: test.et}})

		if _, pErr := ds.Send(context.Background(), ba); pErr != nil {
			t.Fatal(pErr)
		}

		sort.Sort(act)
		for j, batchMethods := range act {
			if !reflect.DeepEqual(test.exp[j], batchMethods.methods) {
				t.Fatalf("test %d: expected [%d] %v, got %v", i, j, test.exp[j], batchMethods.methods)
			}
		}
	}
}

func TestCountRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
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
			Replicas: []roachpb.ReplicaDescriptor{
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
		TestingKnobs: DistSenderTestingKnobs{
			TransportFactory: adaptLegacyTransport(stubRPCSendFn),
		},
		RangeDescriptorDB: descDB,
	}
	ds := NewDistSender(cfg, g)

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
		client.SenderFunc(
			func(
				_ context.Context,
				_ roachpb.BatchRequest,
			) (r *roachpb.BatchResponse, e *roachpb.Error) {
				return
			},
		))(SendOptions{}, &rpc.Context{}, nil, roachpb.BatchRequest{})
	if err != nil {
		t.Fatal(err)
	}
	transport.SendNext(context.Background(), make(chan BatchCall, 1))
	if !transport.IsExhausted() {
		t.Fatalf("transport is not exhausted")
	}
	transport.Close()
}

func TestGatewayNodeID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
	const expNodeID = 42
	nd := &roachpb.NodeDescriptor{
		NodeID:  expNodeID,
		Address: util.MakeUnresolvedAddr("tcp", "foobar:1234"),
	}
	g.NodeID.Reset(nd.NodeID)
	if err := g.SetNodeDescriptor(nd); err != nil {
		t.Fatal(err)
	}
	if err := g.AddInfoProto(gossip.MakeNodeIDKey(expNodeID), nd, time.Hour); err != nil {
		t.Fatal(err)
	}

	var observedNodeID roachpb.NodeID
	var testFn rpcSendFn = func(
		_ context.Context,
		_ SendOptions,
		_ ReplicaSlice,
		ba roachpb.BatchRequest,
		_ *rpc.Context,
	) (*roachpb.BatchResponse, error) {
		observedNodeID = ba.Header.GatewayNodeID
		return ba.CreateReply(), nil
	}

	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      clock,
		TestingKnobs: DistSenderTestingKnobs{
			TransportFactory: adaptLegacyTransport(testFn),
		},
		RangeDescriptorDB: defaultMockRangeDescriptorDB,
	}
	ds := NewDistSender(cfg, g)
	var ba roachpb.BatchRequest
	ba.Add(roachpb.NewPut(roachpb.Key("a"), roachpb.MakeValueFromString("value")))
	if _, err := ds.Send(context.Background(), ba); err != nil {
		t.Fatalf("put encountered error: %s", err)
	}
	if observedNodeID != expNodeID {
		t.Errorf("got GatewayNodeID=%d, want %d", observedNodeID, expNodeID)
	}
}

// Regression test for #20067.
// If a batch is partitioned into multiple partial batches, the
// roachpb.Error.Index of each batch should correspond to its original index in
// the overall batch.
func TestErrorIndexAlignment(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)

	if err := g.SetNodeDescriptor(&roachpb.NodeDescriptor{NodeID: 1}); err != nil {
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
		Replicas: []roachpb.ReplicaDescriptor{
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
		Replicas: []roachpb.ReplicaDescriptor{
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
		Replicas: []roachpb.ReplicaDescriptor{
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
		nthPartialBatch      int
		expectedFinalIdx     int32
		expectedMixedSuccess bool
	}{
		{0, 0, false},
		{1, 1, true},
		{2, 3, true},
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

			var testFn rpcSendFn = func(
				_ context.Context,
				_ SendOptions,
				_ ReplicaSlice,
				ba roachpb.BatchRequest,
				_ *rpc.Context,
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
				TestingKnobs: DistSenderTestingKnobs{
					TransportFactory: adaptLegacyTransport(testFn),
				},
				RangeDescriptorDB: descDB,
			}
			ds := NewDistSender(cfg, g)

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
			aPS, ok := pErr.GetDetail().(*roachpb.MixedSuccessError)
			if a, e := ok, tc.expectedMixedSuccess; a != e {
				t.Fatalf("expected mixed success %t; got %t", e, a)
			}
			if ok {
				pErr = aPS.Wrapped
			}

			if pErr.Index == nil {
				t.Fatalf("expected error index to be set for err %T", pErr.GetDetail())
			}

			if pErr.Index.Index != tc.expectedFinalIdx {
				t.Errorf("expected error index to be %d, instead got %d", tc.expectedFinalIdx, pErr.Index.Index)
			}
		})
	}

}
