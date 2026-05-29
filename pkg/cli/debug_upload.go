// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"fmt"
	"io"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// errMsgUploadArtifact is the user-facing wrap prefix attached to any
// error surfaced by `debug upload`.
const errMsgUploadArtifact = "error in uploading artifact"

var debugUploadOpts = struct {
	crlSupportAPIKey   string
	crlSupportURL      string
	crlSupportTicketID string
	resumeSession      string
	proxy              string
}{}

var debugUploadCmd = &cobra.Command{
	Use:   "upload <path>",
	Short: "upload an artifact (e.g. debug.zip) to Cockroach Labs support",
	Long: `Upload an artifact to Cockroach Labs support.

Currently debug.zip is supported; the file's contents are detected
automatically. Authenticate with --crl-support-api-key (or the
COCKROACH_CRL_SUPPORT_API_KEY environment variable) and pass the
upload server URL via --crl-support-url.

Optionally pass --crl-support-ticket-id=<id> to associate the upload
with a support ticket. To resume an interrupted upload, pass
--resume-session=<session_id> printed by the previous attempt.

Use --proxy=<url> (or the HTTPS_PROXY environment variable) when the
upload server must be reached through a forward proxy.
`,
	Args: cobra.ExactArgs(1),
	RunE: clierrorplus.MaybeDecorateError(runDebugUpload),
}

func runDebugUpload(cmd *cobra.Command, args []string) error {
	inputPath := args[0]
	if err := detectArtifact(inputPath); err != nil {
		return errors.Wrap(err, errMsgUploadArtifact)
	}
	if err := validateUploadConfig(); err != nil {
		return errors.Wrap(err, errMsgUploadArtifact)
	}
	fmt.Fprintf(os.Stderr, "\n=== uploading to Cockroach Labs support\n\n")
	if err := uploadToCRLSupport(cmd.Context(), inputPath); err != nil {
		return errors.Wrap(err, errMsgUploadArtifact)
	}
	return nil
}

func detectArtifact(inputPath string) error {
	f, err := os.Open(inputPath)
	if err != nil {
		return errors.Wrapf(err, "cannot access %q", inputPath)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return errors.Wrapf(err, "cannot stat %q", inputPath)
	}
	if fi.IsDir() {
		return errors.Newf("%q is a directory, expected a file", inputPath)
	}
	if fi.Size() == 0 {
		return errors.Newf("file %q is empty", inputPath)
	}

	var magic [4]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return errors.Wrapf(err, "reading %q", inputPath)
	}
	if magic != [4]byte{0x50, 0x4b, 0x03, 0x04} {
		return errors.Newf(
			"%q is not a supported artifact (only debug.zip is accepted)",
			inputPath,
		)
	}
	return nil
}
