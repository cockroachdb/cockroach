// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package shuffle

import (
	"math/rand"
	"sync"
	"sync/atomic"
)

// Interface for shuffle. When it is satisfied, a collection can be shuffled by
// the routines in this package. The methods require that the elements of the
// collection be enumerable by an integer index. This interface is similar to
// sort.Interface.
type Interface interface {
	// Len is the number of elements in the collection.
	Len() int
	// Swap swaps the elements with indexes i and j.
	Swap(i, j int)
}

var seedSource int64
var randSyncPool = sync.Pool{
	New: func() interface{} {
		return rand.New(rand.NewSource(atomic.AddInt64(&seedSource, 1)))
	},
}

// Shuffle randomizes the order of the array.
func Shuffle(data Interface) {
	r := randSyncPool.Get().(*rand.Rand)
	defer randSyncPool.Put(r)
	r.Shuffle(data.Len(), data.Swap)
}
