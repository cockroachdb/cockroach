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
	"strings"

	"github.com/cockroachdb/cockroach/util"
	"github.com/gogo/protobuf/proto"
)

// NodeID is a custom type for a cockroach node ID. (not a raft node ID)
type NodeID int32

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
// key range from start to end.
func (r *RangeDescriptor) ContainsKeyRange(start, end []byte) bool {
	if len(end) == 0 {
		end = append(append([]byte(nil), start...), byte(0))
	}
	if bytes.Compare(end, start) < 0 {
		panic(fmt.Sprintf("start key is larger than end key %q > %q", string(start), string(end)))
	}
	return bytes.Compare(start, r.StartKey) >= 0 && bytes.Compare(r.EndKey, end) >= 0
}

// FindReplica returns the replica which matches the specified store
// ID. Panic in the event that no replica matches.
func (r *RangeDescriptor) FindReplica(storeID StoreID) *Replica {
	for i := range r.Replicas {
		if r.Replicas[i].StoreID == storeID {
			return &r.Replicas[i]
		}
	}
	panic(fmt.Sprintf("unable to find matching replica for store %d: %v", storeID, r.Replicas))
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
