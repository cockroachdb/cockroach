// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverutils

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestBenignNotifyFn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var logFn func(string, ...interface{})
	var buf strings.Builder
	logFn = func(format string, args ...interface{}) { fmt.Fprintf(&buf, format, args...) }

	defer func(old bool) { reportCalls = old }(reportCalls)

	testutils.RunTrueAndFalse(t, "reportCalls", func(t *testing.T, tb bool) {
		reportCalls = tb

		testutils.RunTrueAndFalse(t, "showTip", func(t *testing.T, showTip bool) {
			buf.Reset()
			fn := makeBenignNotifyFn(&logFn, "IN", "ACC", showTip)
			fn("foo")
			fn("foo")
			result := buf.String()
			if reportCalls {
				require.Contains(t, result, "NOTICE: .foo() called via implicit interface IN")
				require.Contains(t, result, "HINT: consider using .ACC().foo() instead")
				if showTip {
					require.Contains(t, result, "TIP:")
				} else {
					require.NotContains(t, result, "TIP:")
				}
			}

			if reportCalls {
				require.Equal(t, 2, strings.Count(result, "NOTICE:"))
			} else {
				require.Equal(t, 0, strings.Count(result, "NOTICE:"))
			}
		})
	})
}
