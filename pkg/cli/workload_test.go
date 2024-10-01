// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	// register some workloads for TestWorkload
	_ "github.com/cockroachdb/cockroach/pkg/workload/examples"
)

func TestWorkload(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := NewCLITest(TestCLIParams{NoServer: true})
	defer c.Cleanup()

	out, err := c.RunWithCapture("workload init --help")
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(out, `startrek`) {
		t.Fatalf(`startrek workload failed to register got: %s`, out)
	}
}
