// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// cockroach-oss is an entry point for a CockroachDB binary that excludes all
// CCL-licensed code.
package main

import (
	"github.com/cockroachdb/cockroach/pkg/cli"
	_ "github.com/cockroachdb/cockroach/pkg/ui/distoss" // web UI init hooks
)

func main() {
	cli.Main()
}
