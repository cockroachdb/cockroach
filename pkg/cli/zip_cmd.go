// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/spf13/cobra"
)

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
	RunE: clierrorplus.MaybeDecorateError(runDebugZip),
}

// debugZipUploadCmd is a hidden command that uploads the generated debug.zip
// to datadog. This will not apprear in the help text of the zip command.
var debugZipUploadCmd = &cobra.Command{
	Use:    "upload <path to debug dir>",
	Short:  "upload the contents of the debug.zip to an observability platform",
	Args:   cobra.ExactArgs(1),
	Hidden: true,
	RunE:   clierrorplus.MaybeDecorateError(runDebugZipUpload),
}
