// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestMain(m *testing.M) {
	fmt.Fprintf(log.OrigStderr, "initial logging configuration:\n  %s\n",
		strings.TrimSpace(strings.ReplaceAll(log.DescribeAppliedConfig(), "\n", "\n  ")))

	randutil.SeedForTests()

	os.Exit(m.Run())
}
