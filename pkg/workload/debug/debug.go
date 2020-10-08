// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	cli.AddSubCmd(func(userFacing bool) *cobra.Command {
		return debugCmd
	})
}
