// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestSplitIdentifierList(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		in       string
		expected []string
	}{
		{`abc`, []string{"abc"}},
		{`abc.dEf  `, []string{"abc", "def"}},
		{` "aBc"  . d  ."HeLLo"""`, []string{"aBc", "d", `HeLLo"`}},
	}

	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			out, err := splitIdentifierList(tc.in)
			require.NoError(t, err)
			require.Equal(t, tc.expected, out)
		})
	}

	errorTestCases := []struct {
		in            string
		expectedError string
	}{
		{`"unclosed`, `invalid name: unclosed ": "unclosed`},
		{`"unclosed""`, `invalid name: unclosed ": "unclosed""`},
		{`hello !`, `invalid name: expected separator .: hello !`},
	}

	for _, tc := range errorTestCases {
		t.Run(tc.in, func(t *testing.T) {
			_, err := splitIdentifierList(tc.in)
			require.EqualError(t, err, tc.expectedError)
		})
	}
}
