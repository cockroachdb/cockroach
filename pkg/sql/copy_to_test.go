// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestEncodeCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		in        string
		expected  string
		delimiter byte
	}{
		{in: `ab|c`, expected: `ab|c`, delimiter: '\t'},
		{in: `ab|c`, expected: `ab\|c`, delimiter: '|'},
		{in: `ab|c` + string("\t\r\n\t\b\t\f\\") + "|d", expected: `ab\|c\t\r\n\t\b\t\f\\\|d`, delimiter: '|'},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s, delimiter %c", tc.in, tc.delimiter), func(t *testing.T) {
			var b bytes.Buffer
			require.NoError(t, EncodeCopy(&b, []byte(tc.in), tc.delimiter))
			require.Equal(t, tc.expected, b.String())

			// Check decode is the same.
			require.Equal(t, tc.in, DecodeCopy(tc.expected))
		})
	}
}
