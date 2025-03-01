// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package version

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateSeries(t *testing.T) {
	cases := []struct {
		in  string
		err bool
	}{
		{"v24.1", false},
		{"v24.2", false},
		{"v24.999", false},
		{"v0.1", false},
		{"v24.1.0", true},
		{"v24.0", true},
		{"bob", true},
		{"", true},
	}

	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			_, err := ParseMajorVersion(tc.in)
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
