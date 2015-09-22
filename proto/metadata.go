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
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package proto

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/util"
)

// NodeID is a custom type for a cockroach node ID. (not a raft node ID)
type NodeID int32

// NodeIDSlice implements sort.Interface.
type NodeIDSlice []NodeID

func (n NodeIDSlice) Len() int           { return len(n) }
func (n NodeIDSlice) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n NodeIDSlice) Less(i, j int) bool { return n[i] < n[j] }

// String implements the fmt.Stringer interface.
// It is used to format the ID for use in Gossip keys.
func (n NodeID) String() string {
	return strconv.FormatInt(int64(n), 10)
}

// StoreID is a custom type for a cockroach store ID.
type StoreID int32

// StoreIDSlice implements sort.Interface.
type StoreIDSlice []StoreID

func (s StoreIDSlice) Len() int           { return len(s) }
func (s StoreIDSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s StoreIDSlice) Less(i, j int) bool { return s[i] < s[j] }

// String implements the fmt.Stringer interface.
// It is used to format the ID for use in Gossip keys.
func (n StoreID) String() string {
	return strconv.FormatInt(int64(n), 10)
}

// ReplicaID is a custom type for a range replica ID.
type ReplicaID int32

// RaftNodeID is a custom type for a Raft node ID. A raft node ID
// is composed of a concatenation of NodeID + StoreID.
type RaftNodeID uint64

// Format implements the fmt.Formatter interface.
func (n RaftNodeID) Format(f fmt.State, verb rune) {
	// Note: this implementation doesn't handle the width and precision
	// specifiers such as "%20.10s".
	fmt.Fprint(f, strconv.FormatInt(int64(n), 16))
}

// MakeRaftNodeID packs a NodeID and StoreID into a single uint64 for use in raft.
func MakeRaftNodeID(n NodeID, s StoreID) RaftNodeID {
	if n < 0 || s <= 0 {
		// Zeroes are likely the result of incomplete initialization.
		// TODO(bdarnell): should we disallow NodeID==0? It should never occur in
		// production but many tests use it.
		panic("NodeID must be >= 0 and StoreID must be > 0")
	}
	return RaftNodeID(n)<<32 | RaftNodeID(s)
}

// DecodeRaftNodeID converts a RaftNodeID into its component NodeID and StoreID.
func DecodeRaftNodeID(n RaftNodeID) (NodeID, StoreID) {
	return NodeID(n >> 32), StoreID(n & 0xffffffff)
}

// IsSubset returns whether attributes list a is a subset of
// attributes list b.
func (a Attributes) IsSubset(b Attributes) bool {
	m := map[string]struct{}{}
	for _, s := range b.Attrs {
		m[s] = struct{}{}
	}
	for _, s := range a.Attrs {
		if _, ok := m[s]; !ok {
			return false
		}
	}
	return true
}

func (a Attributes) uniqueAttrs() []string {
	var attrs []string
	m := map[string]struct{}{}
	for _, s := range a.Attrs {
		if _, ok := m[s]; !ok {
			m[s] = struct{}{}
			attrs = append(attrs, s)
		}
	}
	return attrs
}

func (a Attributes) String() string {
	return strings.Join(a.uniqueAttrs(), ",")
}

// SortedString returns a sorted, de-duplicated, comma-separated list
// of the attributes.
func (a Attributes) SortedString() string {
	attrs := a.uniqueAttrs()
	sort.Strings(attrs)
	return strings.Join(attrs, ",")
}

// ContainsKey returns whether this RangeDescriptor contains the specified key.
func (r *RangeDescriptor) ContainsKey(key []byte) bool {
	return bytes.Compare(key, r.StartKey) >= 0 && bytes.Compare(key, r.EndKey) < 0
}

// ContainsKeyRange returns whether this RangeDescriptor contains the specified
// key range from start (inclusive) to end (exclusive).
// If end is empty, returns ContainsKey(start).
func (r *RangeDescriptor) ContainsKeyRange(start, end []byte) bool {
	if len(end) == 0 {
		return r.ContainsKey(start)
	}
	if comp := bytes.Compare(end, start); comp < 0 {
		return false
	} else if comp == 0 {
		return r.ContainsKey(start)
	}
	return bytes.Compare(start, r.StartKey) >= 0 && bytes.Compare(r.EndKey, end) >= 0
}

// FindReplica returns the replica which matches the specified store
// ID. If no replica matches, (-1, nil) is returned.
func (r *RangeDescriptor) FindReplica(storeID StoreID) (int, *Replica) {
	for i := range r.Replicas {
		if r.Replicas[i].StoreID == storeID {
			return i, &r.Replicas[i]
		}
	}
	return -1, nil
}

// Validate performs some basic validation of the contents of a range descriptor.
func (r *RangeDescriptor) Validate() error {
	if r.NextReplicaID == 0 {
		return util.Errorf("NextReplicaID must be non-zero")
	}
	seen := map[ReplicaID]struct{}{}
	for _, rep := range r.Replicas {
		if _, ok := seen[rep.ReplicaID]; ok {
			return util.Errorf("ReplicaID %d was reused", rep.ReplicaID)
		}
		seen[rep.ReplicaID] = struct{}{}
		if rep.ReplicaID >= r.NextReplicaID {
			return util.Errorf("ReplicaID %d must be less than NextReplicaID %d",
				rep.ReplicaID, r.NextReplicaID)
		}
	}
	return nil
}

// FractionUsed computes the fraction of storage capacity that is in use.
func (sc StoreCapacity) FractionUsed() float64 {
	if sc.Capacity == 0 {
		return 0
	}
	return float64(sc.Capacity-sc.Available) / float64(sc.Capacity)
}

// CombinedAttrs returns the full list of attributes for the store, including
// both the node and store attributes.
func (s StoreDescriptor) CombinedAttrs() *Attributes {
	var a []string
	a = append(a, s.Node.Attrs.Attrs...)
	a = append(a, s.Attrs.Attrs...)
	return &Attributes{Attrs: a}
}
