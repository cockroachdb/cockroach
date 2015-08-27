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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func newInfo(val float64) info {
	now := time.Now()

	return info{
		Info: Info{
			Value: proto.Value{
				Bytes: encoding.EncodeFloat(nil, val),
				Timestamp: &proto.Timestamp{
					WallTime: now.UnixNano(),
				},
			},
			TTLStamp: now.Add(time.Millisecond).UnixNano(),
		},
	}
}

func TestExpired(t *testing.T) {
	defer leaktest.AfterTest(t)

	i := newInfo(float64(1))
	if i.expired(i.Value.Timestamp.WallTime) {
		t.Error("premature expiration")
	}
	if !i.expired(i.TTLStamp) {
		t.Error("info should have expired")
	}
}

func TestIsFresh(t *testing.T) {
	defer leaktest.AfterTest(t)
	const seq = 10

	node1 := proto.NodeID(1)
	node2 := proto.NodeID(2)
	node3 := proto.NodeID(3)
	i := newInfo(float64(1))
	i.NodeID = node1
	i.peerID = node2
	i.seq = seq
	if !i.isFresh(node3, seq-1) {
		t.Error("info should be fresh:", i)
	}
	if i.isFresh(node3, seq+1) {
		t.Error("info should not be fresh:", i)
	}
	if i.isFresh(node1, seq-1) {
		t.Error("info should not be fresh:", i)
	}
	if i.isFresh(node2, seq-1) {
		t.Error("info should not be fresh:", i)
	}
	// Using node 0 will always yield fresh data.
	if !i.isFresh(0, 0) {
		t.Error("info should be fresh from node0:", i)
	}
}
