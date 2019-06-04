// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
