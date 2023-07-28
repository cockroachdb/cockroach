// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build race
// +build race

package syncutil

import (
	"fmt"
	"reflect"
	"runtime/debug"
)

// A Pool is a set of temporary objects that may be individually saved and
// retrieved.
type Pool struct {
	// New optionally specifies a function to generate
	// a value when Get would otherwise return nil.
	// It may not be changed concurrently with calls to Get.
	New func() any

	mu   Mutex
	pool []poolObj
}

type poolObj struct {
	x     any
	ptr   uintptr
	stack string
}

const maxPoolCap = 64

// TODO: explain.
const debugStacks = false
const debugStacksDisabledStr = "<disabled, set debugStacks = true for more information>"

// Put adds x to the pool.
func (p *Pool) Put(x any) {
	obj := poolObj{x: x, ptr: asPtr(x), stack: debugStacksDisabledStr}
	if debugStacks {
		obj.stack = string(debug.Stack())
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if obj.ptr != 0 {
		for _, other := range p.pool {
			if obj.ptr == other.ptr {
				panic(fmt.Sprintf("syncutil.Pool: double-free detected, object already in pool!"+
					"\n\nfirst stack:\n%s\n\nsecond stack:\n%s\n", other.stack, string(debug.Stack())))
			}
		}
	}
	if len(p.pool) == maxPoolCap {
		p.pool = p.pool[1:] // rotate
	}
	p.pool = append(p.pool, obj)
}

// Get selects an arbitrary item from the Pool, removes it from the
// Pool, and returns it to the caller.
//
// See the comment on (*sync.Pool).Get for details.
func (p *Pool) Get() any {
	x := p.getIfAvail()
	if x == nil && p.New != nil {
		x = p.New()
	}
	return x
}

func (p *Pool) getIfAvail() any {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.pool) == 0 {
		return nil
	}
	obj := p.pool[0]
	p.pool = p.pool[1:]
	return obj.x
}

func asPtr(x any) uintptr {
	v := reflect.ValueOf(x)
	if v.Kind() != reflect.Ptr {
		return 0
	}
	return v.Pointer()
}
