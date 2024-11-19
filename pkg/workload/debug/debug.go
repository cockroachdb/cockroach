// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package debug provides a workload subcommand under which useful workload
// utilities live.
package debug

import (
	"github.com/cockroachdb/cockroach/pkg/workload/cli"
	"github.com/spf13/cobra"
)

var debugCmd = &cobra.Command{
	Use:   `debug`,
	Short: `debug subcommands`,
	Args:  cobra.NoArgs,
}

func init() {
	debugCmd.AddCommand(tpccMergeResultsCmd)
	debugCmd.AddCommand(webhookServerCmd)
	cli.AddSubCmd(func(userFacing bool) *cobra.Command {
		return debugCmd
	})
}
