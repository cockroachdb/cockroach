// Copyright 2020 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCastStringToRegClassTableName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		in       string
		expected tree.TableName
	}{
		{"a", tree.MakeUnqualifiedTableName("a")},
		{`a"`, tree.MakeUnqualifiedTableName(`a"`)},
		{`"a""".bB."cD" `, tree.MakeTableNameWithSchema(`a"`, "bb", "cD")},
	}

	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			out, err := castStringToRegClassTableName(tc.in)
			require.NoError(t, err)
			require.Equal(t, tc.expected, out)
		})
	}

	errorTestCases := []struct {
		in            string
		expectedError string
	}{
		{"a.b.c.d", "too many components: a.b.c.d"},
		{"", `invalid table name: `},
	}

	for _, tc := range errorTestCases {
		t.Run(tc.in, func(t *testing.T) {
			_, err := castStringToRegClassTableName(tc.in)
			require.EqualError(t, err, tc.expectedError)
		})
	}

}
