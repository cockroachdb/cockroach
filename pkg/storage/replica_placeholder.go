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
// Author: Arjun Narayan (arjun@cockroachlabs.com)

package storage

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/google/btree"
)

// ReplicaPlaceholder is created by a Store in anticipation of replacing it at
// some point in the future with a Replica. It has a RangeDescriptor.
type ReplicaPlaceholder struct {
	rangeDesc roachpb.RangeDescriptor
}

var _ KeyRange = &ReplicaPlaceholder{}

// Desc returns the range Placeholder's descriptor.
func (r *ReplicaPlaceholder) Desc() *roachpb.RangeDescriptor {
	return &r.rangeDesc
}

func (r *ReplicaPlaceholder) endKey() roachpb.RKey {
	return r.Desc().EndKey
}

// Less implements the btree.Item interface.
func (r *ReplicaPlaceholder) Less(i btree.Item) bool {
	return r.Desc().EndKey.Less(i.(rangeKeyItem).endKey())
}

func (r *ReplicaPlaceholder) String() string {
	return fmt.Sprintf("range=%d [%s-%s) (placeholder)",
		r.Desc().RangeID, r.rangeDesc.StartKey, r.rangeDesc.EndKey)
}
