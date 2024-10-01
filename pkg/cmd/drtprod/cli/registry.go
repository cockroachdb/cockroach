// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/drtprod/cli/commands"
	"github.com/spf13/cobra"
)

// register registers all drtprod subcommands.
// Add your commands here
func register(ctx context.Context) []*cobra.Command {
	return []*cobra.Command{
		commands.GetYamlProcessor(ctx),
	}
}
