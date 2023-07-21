// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metric

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
)

// TestHistogramBuckets is used to generate additional prometheus buckets to be
// used with Histogram. Please include obs-inf in the review process of new
// buckets.
func TestHistogramBuckets(t *testing.T) {
	verifyAndPrint := func(t *testing.T, exp []float64, category string) string {
		t.Helper()
		var buf strings.Builder
		for idx, f := range exp {
			if idx == 0 {
				fmt.Fprintf(&buf, "%s", category)
			}
			fmt.Fprintf(&buf, "\n%f", f)
		}
		return buf.String()
	}

	for _, config := range StaticBucketConfigs {
		exp := config.GetBucketsFromBucketConfig()
		buf := verifyAndPrint(t, exp, config.category)

		echotest.Require(t, buf, datapathutils.TestDataPath(t, config.category))
	}

}
