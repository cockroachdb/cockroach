// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
			out, err := SplitIdentifierList(tc.in)
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
			_, err := SplitIdentifierList(tc.in)
			require.EqualError(t, err, tc.expectedError)
		})
	}
}
