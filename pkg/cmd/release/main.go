// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import "github.com/spf13/cobra"

var rootCmd = &cobra.Command{Use: "release"}

func main() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}

func init() {
	rootCmd.AddCommand(pickSHACmd)
	rootCmd.AddCommand(postReleaseSeriesBlockersCmd)
	rootCmd.AddCommand(setOrchestrationVersionCmd)
}
