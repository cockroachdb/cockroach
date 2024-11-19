// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlclient

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseBool(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testcases := []struct {
		input     string
		expect    bool
		expectErr bool
	}{
		{"true", true, false},
		{"on", true, false},
		{"yes", true, false},
		{"1", true, false},
		{" TrUe	", true, false},

		{"false", false, false},
		{"off", false, false},
		{"no", false, false},
		{"0", false, false},
		{"	FaLsE ", false, false},

		{"", false, true},
		{"foo", false, true},
	}

	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			b, err := ParseBool(tc.input)
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expect, b)
			}
		})
	}
}
