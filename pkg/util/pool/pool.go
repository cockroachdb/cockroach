// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package pool

import (
	"reflect"
	"sync"
)

// Pool is a generic implementation of sync.Pool.
// Normally, sync.Pool stores objects of the same type.
// This implementation adopts sync.Pool to type T, and
// reuses a single underlying sync.Pool per type.
type Pool[T any] struct {
	*sync.Pool
	initFn func(*T)
}

var typedPools sync.Map

// MakePool creates Pool[T] which returns *T, and initializes those
// pointers as per init function.
func MakePool[T any](initFn func(*T)) Pool[T] {
	var zero T
	typ := reflect.TypeOf(zero)

	p, ok := typedPools.Load(typ)
	if !ok {
		p, _ = typedPools.LoadOrStore(typ, &sync.Pool{New: func() any { return new(T) }})
	}
	return Pool[T]{Pool: p.(*sync.Pool), initFn: initFn}
}

// Get returns initialized *T.
func (p *Pool[T]) Get() *T {
	v := p.Pool.Get().(*T)
	p.initFn(v)
	return v
}

// Put returns value to this pool.
func (p *Pool[T]) Put(v *T) {
	p.Pool.Put(v)
}
