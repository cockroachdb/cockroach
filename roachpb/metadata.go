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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package roachpb

import (
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// NodeID is a custom type for a cockroach node ID. (not a raft node ID)
type NodeID int32

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

// String implements the fmt.Stringer interface.
func (r ReplicaID) String() string {
	return strconv.FormatInt(int64(r), 10)
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

// RSpan returns the RangeDescriptor's resolved span.
func (r RangeDescriptor) RSpan() RSpan {
	return RSpan{Key: r.StartKey, EndKey: r.EndKey}
}

// ContainsKey returns whether this RangeDescriptor contains the specified key.
func (r RangeDescriptor) ContainsKey(key RKey) bool {
	return r.RSpan().ContainsKey(key)
}

// ContainsExclusiveEndKey returns whether this RangeDescriptor contains the specified end key.
func (r RangeDescriptor) ContainsExclusiveEndKey(key RKey) bool {
	return r.RSpan().ContainsExclusiveEndKey(key)
}

// ContainsKeyRange returns whether this RangeDescriptor contains the specified
// key range from start (inclusive) to end (exclusive).
// If end is empty, returns ContainsKey(start).
func (r RangeDescriptor) ContainsKeyRange(start, end RKey) bool {
	return r.RSpan().ContainsKeyRange(start, end)
}

// GetReplicaDescriptor returns the replica which matches the specified store
// ID.
func (r RangeDescriptor) GetReplicaDescriptor(storeID StoreID) (ReplicaDescriptor, bool) {
	for _, repDesc := range r.Replicas {
		if repDesc.StoreID == storeID {
			return repDesc, true
		}
	}
	return ReplicaDescriptor{}, false
}

// GetReplicaDescriptorByID returns the replica which matches the specified store
// ID.
func (r RangeDescriptor) GetReplicaDescriptorByID(replicaID ReplicaID) (ReplicaDescriptor, bool) {
	for _, repDesc := range r.Replicas {
		if repDesc.ReplicaID == replicaID {
			return repDesc, true
		}
	}
	return ReplicaDescriptor{}, false
}

// IsInitialized returns false if this descriptor represents an
// uninitialized range.
// TODO(bdarnell): unify this with Validate().
func (r RangeDescriptor) IsInitialized() bool {
	return len(r.EndKey) != 0
}

// Validate performs some basic validation of the contents of a range descriptor.
func (r RangeDescriptor) Validate() error {
	if r.NextReplicaID == 0 {
		return errors.Errorf("NextReplicaID must be non-zero")
	}
	seen := map[ReplicaID]struct{}{}
	for i, rep := range r.Replicas {
		if err := rep.Validate(); err != nil {
			return errors.Errorf("replica %d is invalid: %s", i, err)
		}
		if _, ok := seen[rep.ReplicaID]; ok {
			return errors.Errorf("ReplicaID %d was reused", rep.ReplicaID)
		}
		seen[rep.ReplicaID] = struct{}{}
		if rep.ReplicaID >= r.NextReplicaID {
			return errors.Errorf("ReplicaID %d must be less than NextReplicaID %d",
				rep.ReplicaID, r.NextReplicaID)
		}
	}
	return nil
}

// Validate performs some basic validation of the contents of a replica descriptor.
func (r ReplicaDescriptor) Validate() error {
	if r.NodeID == 0 {
		return errors.Errorf("NodeID must not be zero")
	}
	if r.StoreID == 0 {
		return errors.Errorf("StoreID must not be zero")
	}
	if r.ReplicaID == 0 {
		return errors.Errorf("ReplicaID must not be zero")
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
