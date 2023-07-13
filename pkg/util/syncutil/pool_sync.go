// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !race
// +build !race

package syncutil

import "sync"

// A Pool is a set of temporary objects that may be individually saved and
// retrieved.
//
// See the comment on sync.Pool for details.
type Pool sync.Pool

// Put adds x to the pool.
//
//gcassert:inline
func (p *Pool) Put(x any) {
	(*sync.Pool)(p).Put(x)
}

// Get selects an arbitrary item from the Pool, removes it from the
// Pool, and returns it to the caller.
//
// See the comment on (*sync.Pool).Get for details.
//
//gcassert:inline
func (p *Pool) Get() any {
	return (*sync.Pool)(p).Get()
}
