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
// Author: Bram Gruneir (bram.gruneir@gmail.com)

package proto

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/util"
	"github.com/gogo/protobuf/proto"
)

// NodeID is a custom type for a cockroach node ID. (not a raft node ID)
type NodeID int32

// String implements the fmt.Stringer interface.
// It is used to format the ID for use in Gossip keys.
func (n NodeID) String() string {
	return strconv.FormatInt(int64(n), 10)
}

// Marshal implements the gogoproto Marshaler interface.
func (n NodeID) Marshal() ([]byte, error) {
	return proto.EncodeVarint(uint64(n)), nil
}

// Unmarshal implements the gogoproto Unmarshaler interface.
func (n *NodeID) Unmarshal(bytes []byte) error {
	x, length := proto.DecodeVarint(bytes)
	if length != len(bytes) {
		return util.Errorf("invalid varint")
	}
	*n = NodeID(x)
	return nil
}

// StoreID is a custom type for a cockroach store ID.
type StoreID int32

// String implements the fmt.Stringer interface.
// It is used to format the ID for use in Gossip keys.
func (n StoreID) String() string {
	return strconv.FormatInt(int64(n), 10)
}

// Marshal implements the gogoproto Marshaler interface.
func (n StoreID) Marshal() ([]byte, error) {
	return proto.EncodeVarint(uint64(n)), nil
}

// Unmarshal implements the gogoproto Unmarshaler interface.
func (n *StoreID) Unmarshal(bytes []byte) error {
	x, length := proto.DecodeVarint(bytes)
	if length != len(bytes) {
		return util.Errorf("invalid varint")
	}
	*n = StoreID(x)
	return nil
}

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

// SortedString returns a sorted, de-duplicated, comma-separated list
// of the attributes.
func (a Attributes) SortedString() string {
	m := map[string]struct{}{}
	for _, s := range a.Attrs {
		m[s] = struct{}{}
	}
	var attrs []string
	for a := range m {
		attrs = append(attrs, a)
	}
	sort.Strings(attrs)
	return strings.Join(attrs, ",")
}

// ContainsKey returns whether this RangeDescriptor contains the specified key.
func (r *RangeDescriptor) ContainsKey(key []byte) bool {
	return bytes.Compare(key, r.StartKey) >= 0 && bytes.Compare(key, r.EndKey) < 0
}

// ContainsKeyRange returns whether this RangeDescriptor contains the specified
// key range from start (inclusive) to end (exclusive).
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
	return ReplicaSlice(r.Replicas).FindReplica(storeID)
}

// CanRead does a linear search for user to verify read permission.
func (p *PermConfig) CanRead(user string) bool {
	for _, u := range p.Read {
		if u == user {
			return true
		}
	}
	return false
}

// CanWrite does a linear search for user to verify write permission.
func (p *PermConfig) CanWrite(user string) bool {
	for _, u := range p.Write {
		if u == user {
			return true
		}
	}
	return false
}

// A ReplicaSlice is a slice of Replicas.
type ReplicaSlice []Replica

// Swap interchanges the replicas stored at the given indices.
func (rs ReplicaSlice) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

// FindReplica returns the replica which matches the specified store
// ID. If no replica matches, (-1, nil) is returned.
func (rs ReplicaSlice) FindReplica(storeID StoreID) (int, *Replica) {
	for i := range rs {
		if rs[i].StoreID == storeID {
			return i, &rs[i]
		}
	}
	return -1, nil
}

// SortByCommonAttributePrefix rearranges the ReplicaSlice by comparing the
// attributes to the given reference attributes. The basis for the comparison
// is that of the common prefix of replica attributes (i.e. the number of equal
// attributes, starting at the first), with a longer prefix sorting first.
func (rs ReplicaSlice) SortByCommonAttributePrefix(attrs []string) int {
	if len(rs) < 2 {
		return 0
	}
	topIndex := len(rs) - 1
	for bucket := 0; bucket < len(attrs); bucket++ {
		firstNotOrdered := 0
		for i := 0; i <= topIndex; i++ {
			if bucket < len(rs[i].Attrs.Attrs) && rs[i].Attrs.Attrs[bucket] == attrs[bucket] {
				// Move replica which matches this attribute to an earlier
				// place in the array, just behind the last matching replica.
				// This packs all matching replicas together.
				rs.Swap(firstNotOrdered, i)
				firstNotOrdered++
			}
		}
		if firstNotOrdered == 0 {
			return bucket
		}
		topIndex = firstNotOrdered - 1
	}
	return len(attrs)
}

// MoveToFront moves the replica at the given index to the front
// of the slice, keeping the order of the remaining elements stable.
// The function will panic when invoked with an invalid index.
func (rs ReplicaSlice) MoveToFront(i int) {
	l := len(rs) - 1
	if i > l {
		panic("out of bound index")
	}
	front := rs[i]
	// Move the first i-1 elements to the right
	copy(rs[1:i+1], rs[0:i])
	rs[0] = front
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
