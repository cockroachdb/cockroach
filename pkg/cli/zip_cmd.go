// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import "github.com/spf13/cobra"

var debugZipCmd = &cobra.Command{
	Use:   "zip <file>",
	Short: "gather cluster debug data into a zip file",
	Long: `

Gather cluster debug data into a zip file. Data includes cluster events, node
liveness, node status, range status, node stack traces, node engine stats, log
files, and SQL schema.

Retrieval of per-node details (status, stack traces, range status, engine stats)
requires the node to be live and operating properly. Retrieval of SQL data
requires the cluster to be live.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runDebugZip),
}
