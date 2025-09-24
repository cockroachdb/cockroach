// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package yamlutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarshal(t *testing.T) {
	type T2 struct {
		C string
	}
	type T1 struct {
		A int
		B T2
	}
	v := T1{A: 1, B: T2{C: "foo"}}
	out, err := Marshal(v)
	require.NoError(t, err)
	require.Equal(t, `a: 1
b:
  c: foo
`, string(out))
}

func TestUnmarshalStrict(t *testing.T) {
	type T2 struct {
		A int
		B string
	}
	err := UnmarshalStrict([]byte(`A: 1
B: foo
C: bar
`), &T2{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "field C not found in type yamlutil.T2")
}
