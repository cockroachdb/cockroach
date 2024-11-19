// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keys

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

func FuzzPrettyPrint(f *testing.F) {
	f.Add([]byte(nil), []byte(nil))

	byteSliceToValDirs := func(s []byte) (dir []encoding.Direction) {
		for _, b := range s {
			dir = append(dir, encoding.Direction(b))
			if len(dir) > 5 { // Arbitrary limit on values direction as fuzzer can generate huge arrays.
				break
			}
		}
		return dir
	}

	f.Fuzz(func(t *testing.T, valsDirs, key []byte) {
		s := PrettyPrint(byteSliceToValDirs(valsDirs), key)
		// Arbitrary limit.  Some inputs generate bit arrays (and those tend to be long
		// in string form), but we want to make
		// sure that pretty printer doesn't do something super silly and allocate
		// excessive amount of memory when the key is small.
		if len(s) > 1<<19 {
			t.Fatalf("pretty string is %d bytes, while key is only %d with valDirs len %d", len(s), len(key), len(valsDirs))
		}
	})
}
