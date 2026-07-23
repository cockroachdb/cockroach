// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package raftlog

import (
	"fmt"
	"testing"
)

func BenchmarkNewEntry(b *testing.B) {
	ent, metaB := mkBenchEnt(b)
	b.ResetTimer()
	for _, fromRawValue := range []bool{false, true} {
		for _, release := range []bool{false, true} {
			b.Run(fmt.Sprintf("fromRawValue=%t,release=%t", fromRawValue, release), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var e *Entry
					var err error
					if fromRawValue {
						e, err = NewEntryFromRawValue(metaB)
					} else {
						e, err = NewEntry(ent)
					}
					if err != nil {
						b.Fatal(err)
					}
					if release {
						e.Release()
					}
				}
			})
		}
	}
}
