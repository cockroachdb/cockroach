// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

// This code originated in Go's sync package.

package syncutil

import (
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

type bench struct {
	setup func(*testing.B, intMapInterface)
	perG  func(b *testing.B, pb *testing.PB, i int, m intMapInterface)
}

func benchMap(b *testing.B, bench bench) {
	for _, m := range [...]intMapInterface{
		&DeepCopyMap[int64, int64]{},
		&RWMutexMap[int64, int64]{},
		&Map[int64, int64]{},
	} {
		name := fmt.Sprintf("%T", m)
		name = strings.Replace(name, "[int64,int64]", "", -1)
		b.Run(name, func(b *testing.B) {
			m = reflect.New(reflect.TypeOf(m).Elem()).Interface().(intMapInterface)
			if bench.setup != nil {
				bench.setup(b, m)
			}

			b.ResetTimer()

			var i int64
			b.RunParallel(func(pb *testing.PB) {
				id := int(atomic.AddInt64(&i, 1) - 1)
				bench.perG(b, pb, id*b.N, m)
			})
		})
	}
}

func BenchmarkLoadMostlyHits(b *testing.B) {
	const hits, misses = 1023, 1
	v := new(int64)

	benchMap(b, bench{
		setup: func(_ *testing.B, m intMapInterface) {
			for i := 0; i < hits; i++ {
				m.LoadOrStore(int64(i), v)
			}
			// Prime the map to get it into a steady state.
			for i := 0; i < hits*2; i++ {
				m.Load(int64(i % hits))
			}
		},

		perG: func(b *testing.B, pb *testing.PB, i int, m intMapInterface) {
			for ; pb.Next(); i++ {
				m.Load(int64(i % (hits + misses)))
			}
		},
	})
}

func BenchmarkLoadMostlyMisses(b *testing.B) {
	const hits, misses = 1, 1023
	v := new(int64)

	benchMap(b, bench{
		setup: func(_ *testing.B, m intMapInterface) {
			for i := 0; i < hits; i++ {
				m.LoadOrStore(int64(i), v)
			}
			// Prime the map to get it into a steady state.
			for i := 0; i < hits*2; i++ {
				m.Load(int64(i % hits))
			}
		},

		perG: func(b *testing.B, pb *testing.PB, i int, m intMapInterface) {
			for ; pb.Next(); i++ {
				m.Load(int64(i % (hits + misses)))
			}
		},
	})
}

func BenchmarkLoadOrStoreBalanced(b *testing.B) {
	const hits, misses = 128, 128
	v := new(int64)

	benchMap(b, bench{
		setup: func(b *testing.B, m intMapInterface) {
			if _, ok := m.(*DeepCopyMap[int64, int64]); ok {
				b.Skip("DeepCopyMap has quadratic running time.")
			}
			for i := 0; i < hits; i++ {
				m.LoadOrStore(int64(i), v)
			}
			// Prime the map to get it into a steady state.
			for i := 0; i < hits*2; i++ {
				m.Load(int64(i % hits))
			}
		},

		perG: func(b *testing.B, pb *testing.PB, i int, m intMapInterface) {
			for ; pb.Next(); i++ {
				j := i % (hits + misses)
				if j < hits {
					if _, ok := m.LoadOrStore(int64(j), v); !ok {
						b.Fatalf("unexpected miss for %v", j)
					}
				} else {
					if e, loaded := m.LoadOrStore(int64(i), v); loaded {
						b.Fatalf("failed to store %v: existing value %v", i, e)
					}
				}
			}
		},
	})
}

func BenchmarkLoadOrStoreUnique(b *testing.B) {
	v := new(int64)

	benchMap(b, bench{
		setup: func(b *testing.B, m intMapInterface) {
			if _, ok := m.(*DeepCopyMap[int64, int64]); ok {
				b.Skip("DeepCopyMap has quadratic running time.")
			}
		},

		perG: func(b *testing.B, pb *testing.PB, i int, m intMapInterface) {
			for ; pb.Next(); i++ {
				m.LoadOrStore(int64(i), v)
			}
		},
	})
}

func BenchmarkLoadOrStoreCollision(b *testing.B) {
	v := new(int64)

	benchMap(b, bench{
		setup: func(_ *testing.B, m intMapInterface) {
			m.LoadOrStore(int64(0), v)
		},

		perG: func(b *testing.B, pb *testing.PB, i int, m intMapInterface) {
			for ; pb.Next(); i++ {
				m.LoadOrStore(int64(0), v)
			}
		},
	})
}

func BenchmarkLoadAndDeleteBalanced(b *testing.B) {
	const hits, misses = 128, 128
	v := new(int64)

	benchMap(b, bench{
		setup: func(b *testing.B, m intMapInterface) {
			if _, ok := m.(*DeepCopyMap[int64, int64]); ok {
				b.Skip("DeepCopyMap has quadratic running time.")
			}
			for i := 0; i < hits; i++ {
				m.LoadOrStore(int64(i), v)
			}
			// Prime the map to get it into a steady state.
			for i := 0; i < hits*2; i++ {
				m.Load(int64(i % hits))
			}
		},

		perG: func(b *testing.B, pb *testing.PB, i int, m intMapInterface) {
			for ; pb.Next(); i++ {
				j := i % (hits + misses)
				if j < hits {
					m.LoadAndDelete(int64(j))
				} else {
					m.LoadAndDelete(int64(i))
				}
			}
		},
	})
}

func BenchmarkLoadAndDeleteUnique(b *testing.B) {
	benchMap(b, bench{
		setup: func(b *testing.B, m intMapInterface) {
			if _, ok := m.(*DeepCopyMap[int64, int64]); ok {
				b.Skip("DeepCopyMap has quadratic running time.")
			}
		},

		perG: func(b *testing.B, pb *testing.PB, i int, m intMapInterface) {
			for ; pb.Next(); i++ {
				m.LoadAndDelete(int64(i))
			}
		},
	})
}

func BenchmarkLoadAndDeleteCollision(b *testing.B) {
	v := new(int64)

	benchMap(b, bench{
		setup: func(_ *testing.B, m intMapInterface) {
			m.LoadOrStore(0, v)
		},

		perG: func(b *testing.B, pb *testing.PB, i int, m intMapInterface) {
			for ; pb.Next(); i++ {
				m.LoadAndDelete(0)
			}
		},
	})
}

func BenchmarkRange(b *testing.B) {
	const mapSize = 1 << 10
	v := new(int64)

	benchMap(b, bench{
		setup: func(_ *testing.B, m intMapInterface) {
			for i := 0; i < mapSize; i++ {
				m.Store(int64(i), v)
			}
		},

		perG: func(b *testing.B, pb *testing.PB, i int, m intMapInterface) {
			for ; pb.Next(); i++ {
				m.Range(func(_ int64, _ *int64) bool { return true })
			}
		},
	})
}

// BenchmarkAdversarialAlloc tests performance when we store a new value
// immediately whenever the map is promoted to clean and otherwise load a
// unique, missing key.
//
// This forces the Load calls to always acquire the map's mutex.
func BenchmarkAdversarialAlloc(b *testing.B) {
	v := new(int64)

	benchMap(b, bench{
		perG: func(b *testing.B, pb *testing.PB, i int, m intMapInterface) {
			var stores, loadsSinceStore int64
			for ; pb.Next(); i++ {
				m.Load(int64(i))
				if loadsSinceStore++; loadsSinceStore > stores {
					m.LoadOrStore(int64(i), v)
					loadsSinceStore = 0
					stores++
				}
			}
		},
	})
}

// BenchmarkAdversarialDelete tests performance when we periodically delete
// one key and add a different one in a large map.
//
// This forces the Load calls to always acquire the map's mutex and periodically
// makes a full copy of the map despite changing only one entry.
func BenchmarkAdversarialDelete(b *testing.B) {
	const mapSize = 1 << 10
	v := new(int64)

	benchMap(b, bench{
		setup: func(_ *testing.B, m intMapInterface) {
			for i := 0; i < mapSize; i++ {
				m.Store(int64(i), v)
			}
		},

		perG: func(b *testing.B, pb *testing.PB, i int, m intMapInterface) {
			for ; pb.Next(); i++ {
				m.Load(int64(i))

				if i%mapSize == 0 {
					m.Range(func(k int64, _ *int64) bool {
						m.Delete(k)
						return false
					})
					m.Store(int64(i), v)
				}
			}
		},
	})
}

func BenchmarkDeleteCollision(b *testing.B) {
	v := new(int64)

	benchMap(b, bench{
		setup: func(_ *testing.B, m intMapInterface) {
			m.LoadOrStore(0, v)
		},

		perG: func(b *testing.B, pb *testing.PB, i int, m intMapInterface) {
			for ; pb.Next(); i++ {
				m.Delete(0)
			}
		},
	})
}
