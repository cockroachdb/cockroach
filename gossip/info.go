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
	"github.com/cockroachdb/cockroach/roachpb"
)

type info struct {
	Info
	peerID roachpb.NodeID
	seq    int64
}

// expired returns true if the node's time to live (TTL) has expired.
func (i *info) expired(now int64) bool {
	return i.TTLStamp <= now
}

// isFresh returns true if the info has a sequence number newer
// than seq and wasn't either passed directly or originated from
// the same node.
func (i *info) isFresh(nodeID roachpb.NodeID, seq int64) bool {
	if i.seq <= seq {
		return false
	}
	if nodeID != 0 && i.NodeID == nodeID {
		return false
	}
	if nodeID != 0 && i.peerID == nodeID {
		return false
	}
	return true
}

// infoMap is a map of keys to info object pointers.
type infoMap map[string]*info
