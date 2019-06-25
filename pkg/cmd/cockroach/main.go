// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This is the default entry point for a CockroachDB binary.
//
// The ccl hook import below means building this will produce CCL'ed binaries.
// This file itself remains Apache2 to preserve the organization of ccl code
// under the /pkg/ccl subtree, but is unused for pure FLOSS builds.
package main

import (
	_ "github.com/cockroachdb/cockroach/pkg/ccl"        // ccl init hooks
	_ "github.com/cockroachdb/cockroach/pkg/ccl/cliccl" // cliccl init hooks
	"github.com/cockroachdb/cockroach/pkg/cli"
	_ "github.com/cockroachdb/cockroach/pkg/ui/distccl" // ccl web UI init hook
)

func main() {
	cli.Main()
}
