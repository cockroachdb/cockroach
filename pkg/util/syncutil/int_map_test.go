// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

// This code originated in Go's sync package.

package syncutil

import (
	"math/rand"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"testing/quick"
	"unsafe"
)

type mapOp string

const (
	opLoad        = mapOp("Load")
	opStore       = mapOp("Store")
	opLoadOrStore = mapOp("LoadOrStore")
	opDelete      = mapOp("Delete")
)

var mapOps = [...]mapOp{opLoad, opStore, opLoadOrStore, opDelete}

// mapCall is a quick.Generator for calls on mapInterface.
type mapCall struct {
	op mapOp
	k  int64
	v  unsafe.Pointer
}

func (c mapCall) apply(m mapInterface) (unsafe.Pointer, bool) {
	switch c.op {
	case opLoad:
		return m.Load(c.k)
	case opStore:
		m.Store(c.k, c.v)
		return nil, false
	case opLoadOrStore:
		return m.LoadOrStore(c.k, c.v)
	case opDelete:
		m.Delete(c.k)
		return nil, false
	default:
		panic("invalid mapOp")
	}
}

type mapResult struct {
	value interface{}
	ok    bool
}

func randValue(r *rand.Rand) unsafe.Pointer {
	i := new(int)
	*i = r.Int()
	return unsafe.Pointer(i)
}

func (mapCall) Generate(r *rand.Rand, size int) reflect.Value {
	c := mapCall{op: mapOps[rand.Intn(len(mapOps))], k: r.Int63()}
	switch c.op {
	case opStore, opLoadOrStore:
		c.v = randValue(r)
	}
	return reflect.ValueOf(c)
}

func applyCalls(
	m mapInterface, calls []mapCall,
) (results []mapResult, final map[interface{}]interface{}) {
	for _, c := range calls {
		v, ok := c.apply(m)
		results = append(results, mapResult{v, ok})
	}

	final = make(map[interface{}]interface{})
	m.Range(func(k int64, v unsafe.Pointer) bool {
		final[k] = v
		return true
	})

	return results, final
}

func applyMap(calls []mapCall) ([]mapResult, map[interface{}]interface{}) {
	return applyCalls(new(IntMap), calls)
}

func applyRWMutexMap(calls []mapCall) ([]mapResult, map[interface{}]interface{}) {
	return applyCalls(new(RWMutexMap), calls)
}

func applyDeepCopyMap(calls []mapCall) ([]mapResult, map[interface{}]interface{}) {
	return applyCalls(new(DeepCopyMap), calls)
}

func TestMapMatchesRWMutex(t *testing.T) {
	if err := quick.CheckEqual(applyMap, applyRWMutexMap, nil); err != nil {
		t.Error(err)
	}
}

func TestMapMatchesDeepCopy(t *testing.T) {
	if err := quick.CheckEqual(applyMap, applyDeepCopyMap, nil); err != nil {
		t.Error(err)
	}
}

func TestConcurrentRange(t *testing.T) {
	const mapSize = 1 << 10

	m := new(IntMap)
	for n := int64(1); n <= mapSize; n++ {
		v := new(int64)
		*v = n
		m.Store(n, unsafe.Pointer(v))
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	defer func() {
		close(done)
		wg.Wait()
	}()
	for g := int64(runtime.GOMAXPROCS(0)); g > 0; g-- {
		r := rand.New(rand.NewSource(g))
		wg.Add(1)
		go func(g int64) {
			defer wg.Done()
			for i := int64(0); ; i++ {
				select {
				case <-done:
					return
				default:
				}
				for n := int64(1); n < mapSize; n++ {
					if r.Int63n(mapSize) == 0 {
						v := new(int64)
						*v = n * i * g
						m.Store(n, unsafe.Pointer(v))
					} else {
						m.Load(n)
					}
				}
			}
		}(g)
	}

	iters := 1 << 10
	if testing.Short() {
		iters = 16
	}
	for n := iters; n > 0; n-- {
		seen := make(map[int64]bool, mapSize)

		m.Range(func(ki int64, vi unsafe.Pointer) bool {
			k, v := ki, *(*int64)(vi)
			if v%k != 0 {
				t.Fatalf("while Storing multiples of %v, Range saw value %v", k, v)
			}
			if seen[k] {
				t.Fatalf("Range visited key %v twice", k)
			}
			seen[k] = true
			return true
		})

		if len(seen) != mapSize {
			t.Fatalf("Range visited %v elements of %v-element Map", len(seen), mapSize)
		}
	}
}
