// Copyright 2016 The Cockroach Authors.
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
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

var alphaRangeDescriptors []roachpb.RangeDescriptor
var alphaRangeDescriptorDB MockRangeDescriptorDB

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
		append(alphaRangeDescriptors, testMetaRangeDescriptor)...,
	)
}

func TestRangeIterForward(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	ds := NewDistSender(DistSenderConfig{
		AmbientCtx:        log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:             clock,
		NodeDescs:         g,
		RPCContext:        rpcContext,
		RangeDescriptorDB: alphaRangeDescriptorDB,
		Settings:          cluster.MakeTestingClusterSettings(),
	})

	ctx := context.Background()

	ri := NewRangeIterator(ds)
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	ds := NewDistSender(DistSenderConfig{
		AmbientCtx:        log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:             clock,
		NodeDescs:         g,
		RPCContext:        rpcContext,
		RangeDescriptorDB: alphaRangeDescriptorDB,
		Settings:          cluster.MakeTestingClusterSettings(),
	})

	ctx := context.Background()

	ri := NewRangeIterator(ds)
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	ds := NewDistSender(DistSenderConfig{
		AmbientCtx:        log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:             clock,
		NodeDescs:         g,
		RPCContext:        rpcContext,
		RangeDescriptorDB: alphaRangeDescriptorDB,
		Settings:          cluster.MakeTestingClusterSettings(),
	})

	ctx := context.Background()

	ri := NewRangeIterator(ds)
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	g := makeGossip(t, stopper, rpcContext)
	ds := NewDistSender(DistSenderConfig{
		AmbientCtx:        log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:             clock,
		NodeDescs:         g,
		RPCContext:        rpcContext,
		RangeDescriptorDB: alphaRangeDescriptorDB,
		Settings:          cluster.MakeTestingClusterSettings(),
	})

	ctx := context.Background()

	ri := NewRangeIterator(ds)
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
