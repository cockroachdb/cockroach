// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmd

import (
	provcmd "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/cmd/provisionings"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "roachprod-centralized",
	Short: "Centralized API for managing roachprod instances and tasks",
	Long: `The roachprod-centralized project provides a centralized API for managing
roachprod instances and their associated tasks. It offers a structured way to
interact with roachprod functionalities programmatically.`,
	// SilenceErrors prevents Cobra from printing errors itself; main()
	// handles error printing so errors appear exactly once.
	SilenceErrors: true,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	// Add the config flag to the root command so it's available to all subcommands
	rootCmd.PersistentFlags().String("config", "", "Path to configuration file")

	rootCmd.AddCommand(provcmd.Cmd)
}
