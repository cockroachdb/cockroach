// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
