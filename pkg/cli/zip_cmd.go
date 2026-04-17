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

// debugZipUploadCmd is a hidden command that uploads the generated
// debug.zip to an observability platform or CRL's upload server. This
// will not appear in the help text of the zip command.
var debugZipUploadCmd = &cobra.Command{
	Use:   "upload <path to debug zip file or dir>",
	Short: "upload debug zip artifacts to an observability platform or CRL upload server",
	Long: `Upload debug zip data to a supported destination.

Use --destination=datadog to upload extracted debug zip artifacts to Datadog.
This requires a path to an extracted debug directory.

Use --destination=upload-server to upload the raw debug.zip file to CRL's
upload server. This requires a path to a .zip file and authentication via
--upload-server-api-key and --upload-server-url.
`,
	Args:   cobra.ExactArgs(1),
	Hidden: true,
	RunE:   clierrorplus.MaybeDecorateError(runDebugZipUpload),
}
