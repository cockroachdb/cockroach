// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
