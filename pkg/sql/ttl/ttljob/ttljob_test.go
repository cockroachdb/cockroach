// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ttljob

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestMakeColumnNamesSQL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		cols     []string
		expected string
	}{
		{[]string{"a"}, "a"},
		{[]string{"index"}, `"index"`},
		{[]string{"a", "b"}, "a, b"},
		{[]string{"escape-me", "index", "c"}, `"escape-me", "index", c`},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			require.Equal(t, tc.expected, makeColumnNamesSQL(tc.cols))
		})
	}
}
