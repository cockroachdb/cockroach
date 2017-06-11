// Copyright 2017 The Cockroach Authors.
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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package util

import (
	"bytes"
	"fmt"
)

// FastIntSet keeps track of a set of integers. It is very fast when the values
// are small.
type FastIntSet struct {
	smallVals uint64
	largeVals map[uint32]struct{}
}

// We store bits for values smaller than this cutoff.
const smallValCutoff = 64

// Add adds a value to the set. No-op if the value is already in the set.
func (s *FastIntSet) Add(i uint32) {
	if i < smallValCutoff {
		s.smallVals |= (1 << uint64(i))
	} else {
		if s.largeVals == nil {
			s.largeVals = make(map[uint32]struct{})
		}
		s.largeVals[i] = struct{}{}
	}
}

// Remove removes a value from the set. No-op if the value is not in the set.
func (s *FastIntSet) Remove(i uint32) {
	if i < smallValCutoff {
		s.smallVals &= ^(1 << uint64(i))
	} else {
		delete(s.largeVals, i)
	}
}

// Contains returns true if the set contains the value.
func (s *FastIntSet) Contains(i uint32) bool {
	if i < smallValCutoff {
		return (s.smallVals & (1 << uint64(i))) != 0
	}
	_, ok := s.largeVals[i]
	return ok
}

// Empty returns true if the set is empty.
func (s *FastIntSet) Empty() bool {
	return s.smallVals == 0 && len(s.largeVals) == 0
}

// ForEach calls a function for each value in the set (in arbitrary order).
func (s *FastIntSet) ForEach(f func(i uint32)) {
	if s.smallVals != 0 {
		for i := uint32(0); i < smallValCutoff; i++ {
			if (s.smallVals & (1 << uint64(i))) != 0 {
				f(i)
			} else {
				// See if we can skip 8 bytes at a time
				if (s.smallVals & (0xFF << uint64(i))) == 0 {
					i += 7
				}
			}
		}
	}
	for v := range s.largeVals {
		f(v)
	}
}

func (s *FastIntSet) String() string {
	var buf bytes.Buffer
	buf.WriteByte('(')
	first := true
	s.ForEach(func(i uint32) {
		if first {
			first = false
		} else {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, "%d", i)
	})
	buf.WriteByte(')')
	return buf.String()
}
