// Copyright 2024 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/drtprod/cli/commands"
	"github.com/spf13/cobra"
)

// register registers all drtprod subcommands.
// Add your commands here
func register(ctx context.Context) []*cobra.Command {
	return []*cobra.Command{
		commands.GetCreateDrtClusterCmd(ctx),
		commands.GetDnsCmd(ctx),
	}
}
