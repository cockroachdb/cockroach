// Copyright 2016 The Cockroach Authors.
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
//
// Author: Spencer Kimball (spencer@cockroachlabs.com)

package kv

import (
	"bytes"
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

var alphaRangeDescriptors []*roachpb.RangeDescriptor

var alphaRangeDescriptorDB = MockRangeDescriptorDB(func(key roachpb.RKey, useReverseScan bool) (
	[]roachpb.RangeDescriptor,
	[]roachpb.RangeDescriptor,
	*roachpb.Error,
) {
	if bytes.HasPrefix(key, keys.Meta2Prefix) {
		return []roachpb.RangeDescriptor{testMetaRangeDescriptor}, nil, nil
	}
	var index int
	if len(key) > 0 {
		if useReverseScan {
			index = int(key[0] - byte('a'))
		} else {
			index = int(key[0] - byte('a') + 1)
		}
	}
	if index < 0 {
		index = 0
	} else if index >= len(alphaRangeDescriptors) {
		index = len(alphaRangeDescriptors) - 1
	}
	return []roachpb.RangeDescriptor{*alphaRangeDescriptors[index]}, nil, nil
})

func init() {
	lastKey := roachpb.RKey(keys.MinKey)
	for i, b := 0, byte('a'); b <= byte('z'); i, b = i+1, b+1 {
		key := roachpb.RKey([]byte{b})
		alphaRangeDescriptors = append(alphaRangeDescriptors, &roachpb.RangeDescriptor{
			RangeID:  roachpb.RangeID(i + 2),
			StartKey: lastKey,
			EndKey:   key,
			Replicas: []roachpb.ReplicaDescriptor{
				{
					NodeID:  1,
					StoreID: 1,
				},
			},
		})
		lastKey = key
	}
}

func TestRangeIterForward(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
	ds := NewDistSender(DistSenderConfig{
		Clock:             clock,
		RangeDescriptorDB: alphaRangeDescriptorDB,
	}, g)

	ctx := context.Background()

	ri := NewRangeIterator(ds)
	i := 0
	span := roachpb.RSpan{
		Key:    roachpb.RKey(roachpb.KeyMin),
		EndKey: roachpb.RKey([]byte("z")),
	}
	for ri.Seek(ctx, span.Key, Ascending); ri.Valid(); ri.Next(ctx) {
		if !reflect.DeepEqual(alphaRangeDescriptors[i], ri.Desc()) {
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
	ds := NewDistSender(DistSenderConfig{
		Clock:             clock,
		RangeDescriptorDB: alphaRangeDescriptorDB,
	}, g)

	ctx := context.Background()

	ri := NewRangeIterator(ds)
	i := 0
	for ri.Seek(ctx, roachpb.RKey(roachpb.KeyMin), Ascending); ri.Valid(); {
		if !reflect.DeepEqual(alphaRangeDescriptors[i], ri.Desc()) {
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
	ds := NewDistSender(DistSenderConfig{
		Clock:             clock,
		RangeDescriptorDB: alphaRangeDescriptorDB,
	}, g)

	ctx := context.Background()

	ri := NewRangeIterator(ds)
	i := len(alphaRangeDescriptors) - 1
	span := roachpb.RSpan{
		Key:    roachpb.RKey(roachpb.KeyMin),
		EndKey: roachpb.RKey([]byte{'z'}),
	}
	for ri.Seek(ctx, span.EndKey, Descending); ri.Valid(); ri.Next(ctx) {
		if !reflect.DeepEqual(alphaRangeDescriptors[i], ri.Desc()) {
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	g, clock := makeGossip(t, stopper)
	ds := NewDistSender(DistSenderConfig{
		Clock:             clock,
		RangeDescriptorDB: alphaRangeDescriptorDB,
	}, g)

	ctx := context.Background()

	ri := NewRangeIterator(ds)
	i := len(alphaRangeDescriptors) - 1
	for ri.Seek(ctx, roachpb.RKey([]byte{'z'}), Descending); ri.Valid(); {
		if !reflect.DeepEqual(alphaRangeDescriptors[i], ri.Desc()) {
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
