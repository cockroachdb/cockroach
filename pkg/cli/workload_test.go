// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
