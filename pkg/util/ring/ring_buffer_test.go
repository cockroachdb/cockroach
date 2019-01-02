// Copyright 2018 The Cockroach Authors.
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

package ring

import (
	"fmt"
	"math/rand"
	"testing"
)

const maxCount = 1000

func testRingBuffer(t *testing.T, count int) {
	var buffer Buffer
	naiveBuffer := make([]interface{}, 0, count)
	for elementIdx := 0; elementIdx < count; elementIdx++ {
		if buffer.Len() != len(naiveBuffer) {
			t.Errorf("Ring buffer returned incorrect Len: expected %v, found %v", len(naiveBuffer), buffer.Len())
			panic("")
		}

		op := rand.Float64()
		if op < 0.35 {
			buffer.AddFirst(elementIdx)
			naiveBuffer = append([]interface{}{elementIdx}, naiveBuffer...)
		} else if op < 0.70 {
			buffer.AddLast(elementIdx)
			naiveBuffer = append(naiveBuffer, elementIdx)
		} else if op < 0.85 {
			if len(naiveBuffer) > 0 {
				buffer.RemoveFirst()
				naiveBuffer = naiveBuffer[1:]
			}
		} else {
			if len(naiveBuffer) > 0 {
				buffer.RemoveLast()
				naiveBuffer = naiveBuffer[:len(naiveBuffer)-1]
			}
		}

		for pos, el := range naiveBuffer {
			res := buffer.Get(pos)
			if res != el {
				panic(fmt.Sprintf("Ring buffer returned incorrect value in position %v: expected %+v, found %+v", pos, el, res))
			}
		}
		if len(naiveBuffer) > 0 {
			if buffer.GetFirst() != naiveBuffer[0] {
				panic(fmt.Sprintf("Ring buffer returned incorrect value of the first element: expected %+v, found %+v", naiveBuffer[0], buffer.GetFirst()))
			}
			if buffer.GetLast() != naiveBuffer[len(naiveBuffer)-1] {
				panic(fmt.Sprintf("Ring buffer returned incorrect value of the last element: expected %+v, found %+v", naiveBuffer[len(naiveBuffer)-1], buffer.GetLast()))
			}
		}
	}
}

func TestRingBuffer(t *testing.T) {
	for count := 1; count <= maxCount; count++ {
		testRingBuffer(t, count)
	}
}
