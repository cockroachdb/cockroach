// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tsearch

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRank(t *testing.T) {
	tests := []struct {
		weights  []float32
		v        string
		q        string
		method   int
		expected float32
	}{
		{v: "a:1 s:2C d g", q: "a | s", expected: 0.091189064},
		{v: "a:1 sa:2C d g", q: "a | s", expected: 0.030396355},
		{v: "a:1 sa:2C d g", q: "a | s:*", expected: 0.091189064},
		{v: "a:1 sa:2C d g", q: "a | sa:*", expected: 0.091189064},
		{v: "a:1 s:2B d g", q: "a | s", expected: 0.15198177},
		{v: "a:1 s:2 d g", q: "a | s", expected: 0.06079271},
		{v: "a:1 s:2C d g", q: "a & s", expected: 0.14015312},
		{v: "a:1 s:2B d g", q: "a & s", expected: 0.19820644},
		{v: "a:1 s:2 d g", q: "a & s", expected: 0.09910322},
	}
	for _, tt := range tests {
		v, err := ParseTSVector(tt.v)
		assert.NoError(t, err)
		q, err := ParseTSQuery(tt.q)
		assert.NoError(t, err)
		actual, err := Rank(tt.weights, v, q, tt.method)
		assert.NoError(t, err)
		assert.Equalf(t, tt.expected, actual, "Rank(%v, %v, %v, %v)", tt.weights, tt.v, tt.q, tt.method)
	}
}
