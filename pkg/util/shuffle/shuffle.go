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

package shuffle

import "math/rand"

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

// Shuffle randomizes the order of the array.
func Shuffle(data Interface) {
	n := data.Len()
	for i := 1; i < n; i++ {
		data.Swap(i, rand.Intn(i+1))
	}
}
