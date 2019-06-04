// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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

func (r *ReplicaPlaceholder) startKey() roachpb.RKey {
	return r.Desc().StartKey
}

// Less implements the btree.Item interface.
func (r *ReplicaPlaceholder) Less(i btree.Item) bool {
	return r.Desc().StartKey.Less(i.(rangeKeyItem).startKey())
}

func (r *ReplicaPlaceholder) String() string {
	return fmt.Sprintf("range=%d [%s-%s) (placeholder)",
		r.Desc().RangeID, r.rangeDesc.StartKey, r.rangeDesc.EndKey)
}
