// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util"
)

func BenchmarkColSet(b *testing.B) {
	// Verify that the wrapper doesn't add overhead (as was the case with earlier
	// go versions which couldn't do mid-stack inlining).
	const n = 50
	b.Run("fastintset", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var c util.FastIntSet
			for j := 0; j < n; j++ {
				c.Add(j)
			}
		}
	})
	b.Run("tablecolset", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var c TableColSet
			for j := 0; j < n; j++ {
				c.Add(descpb.ColumnID(j))
			}
		}
	})
}

func TestColSet_Ordered(t *testing.T) {
	testData := []struct {
		set      TableColSet
		expected []descpb.ColumnID
	}{
		{MakeTableColSet(1, 2, 3), []descpb.ColumnID{1, 2, 3}},
		{MakeTableColSet(3, 5, 6, 17), []descpb.ColumnID{3, 5, 6, 17}},
		{MakeTableColSet(9, 4, 6, 1), []descpb.ColumnID{1, 4, 6, 9}},
	}

	for _, d := range testData {
		t.Run(d.set.String(), func(t *testing.T) {
			res := d.set.Ordered()

			if len(res) != len(d.expected) {
				t.Fatalf("%s: expected %v, got %v", d.set, d.expected, res)
			}

			for i := 0; i < len(res); i++ {
				if res[i] != d.expected[i] {
					t.Errorf("%s: expected %v, got %v", d.set, d.expected, res)
				}
			}
		})
	}
}
