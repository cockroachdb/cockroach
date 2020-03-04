// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package idalloc

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// SetIDKey sets the key which the allocator increments.
func (ia *Allocator) SetIDKey(idKey roachpb.Key) {
	ia.idKey.Store(idKey)
}

// IDs returns the channel that the allocator uses to buffer available ids.
func (ia *Allocator) IDs() <-chan uint32 {
	return ia.ids
}
