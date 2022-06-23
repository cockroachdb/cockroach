// Copyright 2015-2018 trivago N.V.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tcontainer

import "sort"

// Int64Slice is a typedef to allow sortable int64 slices
type Int64Slice []int64

func (s Int64Slice) Len() int {
	return len(s)
}

func (s Int64Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s Int64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Sort is a shortcut for sort.Sort(s)
func (s Int64Slice) Sort() {
	sort.Sort(s)
}

// IsSorted is a shortcut for sort.IsSorted(s)
func (s Int64Slice) IsSorted() bool {
	return sort.IsSorted(s)
}

// Set sets all values in this slice to the given value
func (s Int64Slice) Set(v int64) {
	for i := range s {
		s[i] = v
	}
}

// Uint64Slice is a typedef to allow sortable uint64 slices
type Uint64Slice []uint64

func (s Uint64Slice) Len() int {
	return len(s)
}

func (s Uint64Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s Uint64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Sort is a shortcut for sort.Sort(s)
func (s Uint64Slice) Sort() {
	sort.Sort(s)
}

// IsSorted is a shortcut for sort.IsSorted(s)
func (s Uint64Slice) IsSorted() bool {
	return sort.IsSorted(s)
}

// Set sets all values in this slice to the given value
func (s Uint64Slice) Set(v uint64) {
	for i := range s {
		s[i] = v
	}
}

// Float32Slice is a typedef to allow sortable float32 slices
type Float32Slice []float32

func (s Float32Slice) Len() int {
	return len(s)
}

func (s Float32Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s Float32Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Sort is a shortcut for sort.Sort(s)
func (s Float32Slice) Sort() {
	sort.Sort(s)
}

// IsSorted is a shortcut for sort.IsSorted(s)
func (s Float32Slice) IsSorted() bool {
	return sort.IsSorted(s)
}

// Set sets all values in this slice to the given value
func (s Float32Slice) Set(v float32) {
	for i := range s {
		s[i] = v
	}
}
