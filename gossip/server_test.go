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
// Author: Peter Mattis (peter@cockroachlabs.com)

package gossip

import (
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
)

func TestRandomPeer(t *testing.T) {
	noAddr := util.UnresolvedAddr{}
	addr1 := util.MakeUnresolvedAddr("tcp", "10.0.0.1")
	addr2 := util.MakeUnresolvedAddr("tcp", "10.0.0.2")

	testCases := []struct {
		nodeMap      map[util.UnresolvedAddr]roachpb.NodeID
		selfID       roachpb.NodeID
		expectedAddr util.UnresolvedAddr
		expectedID   roachpb.NodeID
	}{
		{
			map[util.UnresolvedAddr]roachpb.NodeID{},
			0, noAddr, 0,
		},
		{
			map[util.UnresolvedAddr]roachpb.NodeID{
				addr1: 1,
			},
			1, noAddr, 0,
		},
		{
			map[util.UnresolvedAddr]roachpb.NodeID{
				addr1: 1,
			},
			2, addr1, 1,
		},
		{
			map[util.UnresolvedAddr]roachpb.NodeID{
				addr1: 1,
				addr2: 2,
			},
			1, addr2, 2,
		},
	}
	for i, c := range testCases {
		peerAddr, peerID := randomPeer(c.nodeMap, c.selfID)
		if c.expectedID != peerID {
			t.Fatalf("%d: expected %d but found %d", i, c.expectedID, peerID)
		}
		if c.expectedAddr != peerAddr {
			t.Fatalf("%d: expected %s, but found %s", i, c.expectedAddr, peerAddr)
		}
	}
}
