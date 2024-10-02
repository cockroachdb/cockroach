// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

var alphaRangeDescriptors []roachpb.RangeDescriptor
var alphaRangeDescriptorDB MockRangeDescriptorDB
var tf TransportFactory

func init() {
	lastKey := testMetaEndKey
	for i, b := 0, byte('a'); b <= byte('z'); i, b = i+1, b+1 {
		key := roachpb.RKey([]byte{b})
		alphaRangeDescriptors = append(alphaRangeDescriptors, roachpb.RangeDescriptor{
			RangeID:  roachpb.RangeID(i + 2),
			StartKey: lastKey,
			EndKey:   key,
			InternalReplicas: []roachpb.ReplicaDescriptor{
				{
					NodeID:  1,
					StoreID: 1,
				},
			},
		})
		lastKey = key
	}
	alphaRangeDescriptorDB = mockRangeDescriptorDBForDescs(
		append(alphaRangeDescriptors, TestMetaRangeDescriptor)...,
	)
	tf = func(options SendOptions, slice ReplicaSlice) Transport {
		panic("transport not set up for use")
	}
}

func TestRangeIterForward(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClockForTesting(nil)
	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	ds := NewDistSender(DistSenderConfig{
		AmbientCtx:        log.MakeTestingAmbientCtxWithNewTracer(),
		Clock:             clock,
		NodeDescs:         g,
		Stopper:           stopper,
		RangeDescriptorDB: alphaRangeDescriptorDB,
		Settings:          cluster.MakeTestingClusterSettings(),
		TransportFactory:  tf,
	})

	ri := MakeRangeIterator(ds)
	i := 0
	span := roachpb.RSpan{
		Key:    testMetaEndKey,
		EndKey: roachpb.RKey([]byte("z")),
	}
	for ri.Seek(ctx, span.Key, Ascending); ri.Valid(); ri.Next(ctx) {
		if !reflect.DeepEqual(alphaRangeDescriptors[i], *ri.Desc()) {
			t.Fatalf("%d: expected %v; got %v", i, alphaRangeDescriptors[i], ri.Desc())
		}
		i++
		if !ri.NeedAnother(span) {
			break
		}
	}
}

func TestRangeIterSeekForward(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClockForTesting(nil)
	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	ds := NewDistSender(DistSenderConfig{
		AmbientCtx:        log.MakeTestingAmbientCtxWithNewTracer(),
		Clock:             clock,
		NodeDescs:         g,
		Stopper:           stopper,
		RangeDescriptorDB: alphaRangeDescriptorDB,
		Settings:          cluster.MakeTestingClusterSettings(),
		TransportFactory:  tf,
	})

	ri := MakeRangeIterator(ds)
	i := 0
	for ri.Seek(ctx, testMetaEndKey, Ascending); ri.Valid(); {
		if !reflect.DeepEqual(alphaRangeDescriptors[i], *ri.Desc()) {
			t.Fatalf("%d: expected %v; got %v", i, alphaRangeDescriptors[i], ri.Desc())
		}
		i += 2
		// Skip even ranges.
		nextByte := ri.Desc().EndKey[0] + 1
		if nextByte >= byte('z') {
			break
		}
		seekKey := roachpb.RKey([]byte{nextByte})
		ri.Seek(ctx, seekKey, Ascending)
		if !ri.Key().Equal(seekKey) {
			t.Errorf("expected iterator key %s; got %s", seekKey, ri.Key())
		}
	}
}

func TestRangeIterReverse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClockForTesting(nil)
	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	ds := NewDistSender(DistSenderConfig{
		AmbientCtx:        log.MakeTestingAmbientCtxWithNewTracer(),
		Clock:             clock,
		NodeDescs:         g,
		Stopper:           stopper,
		RangeDescriptorDB: alphaRangeDescriptorDB,
		Settings:          cluster.MakeTestingClusterSettings(),
		TransportFactory:  tf,
	})

	ri := MakeRangeIterator(ds)
	i := len(alphaRangeDescriptors) - 1
	span := roachpb.RSpan{
		Key:    testMetaEndKey,
		EndKey: roachpb.RKey([]byte{'z'}),
	}
	for ri.Seek(ctx, span.EndKey, Descending); ri.Valid(); ri.Next(ctx) {
		if !reflect.DeepEqual(alphaRangeDescriptors[i], *ri.Desc()) {
			t.Fatalf("%d: expected %v; got %v", i, alphaRangeDescriptors[i], ri.Desc())
		}
		i--
		if !ri.NeedAnother(span) {
			break
		}
	}
}

func TestRangeIterSeekReverse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClockForTesting(nil)
	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	ds := NewDistSender(DistSenderConfig{
		AmbientCtx:        log.MakeTestingAmbientCtxWithNewTracer(),
		Clock:             clock,
		NodeDescs:         g,
		Stopper:           stopper,
		RangeDescriptorDB: alphaRangeDescriptorDB,
		Settings:          cluster.MakeTestingClusterSettings(),
		TransportFactory:  tf,
	})

	ri := MakeRangeIterator(ds)
	i := len(alphaRangeDescriptors) - 1
	for ri.Seek(ctx, roachpb.RKey([]byte{'z'}), Descending); ri.Valid(); {
		if !reflect.DeepEqual(alphaRangeDescriptors[i], *ri.Desc()) {
			t.Fatalf("%d: expected %v; got %v", i, alphaRangeDescriptors[i], ri.Desc())
		}
		i -= 2
		// Skip every other range.
		nextByte := ri.Desc().StartKey[0] - 1
		if nextByte <= byte('a') {
			break
		}
		seekKey := roachpb.RKey([]byte{nextByte})
		ri.Seek(ctx, seekKey, Descending)
		if !ri.Key().Equal(seekKey) {
			t.Errorf("expected iterator key %s; got %s", seekKey, ri.Key())
		}
	}
}
