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
// debug.zip to datadog or to CRL support. It will
// not appear in the help text of the zip command.
var debugZipUploadCmd = &cobra.Command{
	Use:   "upload <path to debug zip file or dir>",
	Short: "upload debug zip artifacts to datadog or CRL support",
	Long: `Upload debug zip data to a supported destination.

Use --destination=datadog (the default) to upload extracted debug zip
artifacts to Datadog. This requires a path to an extracted debug
directory.

Use --destination=crl-support to upload the raw debug.zip file to CRL
support. This requires a path to a .zip file and authentication via
--crl-support-api-key and --crl-support-url. Optionally pass
--crl-support-ticket-id=<id> to associate the upload with a support
ticket. To resume an interrupted upload, pass --resume-session=<session_id>.

Use --proxy=<url> (or HTTPS_PROXY) when CRL support must be reached
through a forward proxy. The proxy applies to both the crl-support
API calls and the blob storage PUT.
`,
	Args:   cobra.ExactArgs(1),
	Hidden: true,
	RunE:   clierrorplus.MaybeDecorateError(runDebugZipUpload),
}
