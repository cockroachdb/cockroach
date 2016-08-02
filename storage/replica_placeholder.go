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

package storage

import (
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/google/btree"
)

// ReplicaPlaceholder is created by a Store in anticipation of replacing it at
// some point in the future with a Replica. It has a RangeDescriptor.
type ReplicaPlaceholder struct {
	RangeID   roachpb.RangeID
	store     *Store
	rangeDesc *roachpb.RangeDescriptor
}

// Desc returns the range Placeholder's descriptor.
func (r *ReplicaPlaceholder) Desc() *roachpb.RangeDescriptor {
	return r.rangeDesc
}

func (r *ReplicaPlaceholder) endKey() roachpb.RKey {
	return r.Desc().EndKey
}

// Less returns true if the range's end key is less than the given item's key.
func (r *ReplicaPlaceholder) Less(i btree.Item) bool {
	return r.Desc().EndKey.Less(i.(rangeKeyItem).endKey())
}
