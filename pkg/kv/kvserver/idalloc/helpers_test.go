// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package idalloc

// IDs returns the channel that the allocator uses to buffer available ids.
func (ia *Allocator) IDs() <-chan int64 {
	return ia.ids
}
