// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/drtprod/cli"
)

func main() {
	// Initialize and register all commands
	cli.Initialize(context.Background())
}
