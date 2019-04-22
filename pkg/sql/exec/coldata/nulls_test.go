// Copyright 2019 The Cockroach Authors.
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

package coldata

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNullsOr(t *testing.T) {
	length1, length2 := uint16(300), uint16(400)
	n1, n2 := NewNulls(int(length1)), NewNulls(int(length2))
	for i := uint16(0); i < length1; i++ {
		if i%3 == 0 {
			n1.SetNull(i)
		}
	}
	for i := uint16(0); i < length2; i++ {
		if i%5 == 0 {
			n2.SetNull(i)
		}
	}
	or := n1.Or(&n2)
	require.True(t, or.hasNulls)
	for i := uint16(0); i < length2; i++ {
		if i < length1 && n1.NullAt(i) || i < length2 && n2.NullAt(i) {
			require.True(t, or.NullAt(i), "or.NullAt(%d) should be true", i)
		} else {
			require.False(t, or.NullAt(i), "or.NullAt(%d) should be false", i)
		}
	}
}
