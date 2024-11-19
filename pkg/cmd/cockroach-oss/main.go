// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
