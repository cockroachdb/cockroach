// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"fmt"
	"os"
	"runtime"
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

	arch := runtime.GOARCH
	for _, config := range StaticBucketConfigs {
		exp := config.GetBucketsFromBucketConfig()
		buf := verifyAndPrint(t, exp, config.category)

		category := config.category
		if arch == "s390x" {
			s390xPath := datapathutils.TestDataPath(t, fmt.Sprintf("%s_%s", category, arch))
			if _, err := os.Stat(s390xPath); err == nil {
				category = fmt.Sprintf("%s_%s", category, arch)
			}
		}

		echotest.Require(t, buf, datapathutils.TestDataPath(t, category))
	}

}
