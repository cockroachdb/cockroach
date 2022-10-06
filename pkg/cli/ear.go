// Copyright 2022 The Cockroach Authors.
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
	"io"
	"os"

	"github.com/cockroachdb/cockroach/pkg/ccl/baseccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// Sub-commands for encryption-at-rest command.
var earCmds = []*cobra.Command{
	earDecryptCmd,
}

var earCmd = &cobra.Command{
	Use:   "encryption-at-rest [command]",
	Short: "tools for working with encrypted stores",
}

func init() {
	earCmd.AddCommand(earCmds...)
}

var earDecryptCmd = &cobra.Command{
	Use:   "decrypt <encryption-spec> <in-file> [out-file]",
	Short: "decrypt a file from an encrypted store",
	Long: `Decrypts an file from an encrypted store, and outputs it to the
specified path.

If out-file is not specified, the command will output the decrypted contents to
stdout.
`,
	Args: cobra.MinimumNArgs(2),
	RunE: clierrorplus.MaybeDecorateError(runDecrypt),
}

func runDecrypt(cmd *cobra.Command, args []string) (returnErr error) {
	encSpecStr, inPath := args[0], args[1]
	var outPath string
	if len(args) > 2 {
		outPath = args[2]
	}

	encSpec, err := baseccl.NewStoreEncryptionSpec(encSpecStr)
	if err != nil {
		return errors.Wrap(err, "could not parse encryption spec")
	}
	storePath := encSpec.Path
	encOpts, err := encSpec.ToEncryptionOptions()
	if err != nil {
		return errors.Wrap(err, "could not generate encryption opts")
	}

	fs := &absoluteFS{pebbleToolFS}
	fr := &storage.PebbleFileRegistry{
		FS:       fs,
		DBDir:    storePath,
		ReadOnly: true,
	}
	if err := fr.Load(); err != nil {
		return errors.Wrap(err, "could not load file registry")
	}

	env, err := engineccl.NewEncryptedEnv(fs, fr, storePath, true, encOpts)
	if err != nil {
		return errors.Wrap(err, "could not construct encryption env")
	}

	// Open the specified file through the FS, decrypting it.
	f, err := env.FS.Open(inPath)
	if err != nil {
		return errors.Wrapf(err, "could not open input file %s", inPath)
	}
	defer f.Close()

	// Copy the raw bytes into the destination file.
	outFile := cmd.OutOrStdout()
	if outPath != "" {
		f, err := os.OpenFile(outPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		if err != nil {
			return errors.Wrapf(err, "could not open output file %s", outPath)
		}
		defer f.Close()
		outFile = f
	}
	if _, err = io.Copy(outFile, f); err != nil {
		return errors.Wrap(err, "could not write to output file")
	}

	return nil
}
