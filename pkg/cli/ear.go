// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"cmp"
	"fmt"
	"io"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

func runDecrypt(cmd *cobra.Command, args []string) (returnErr error) {
	dir, inPath := args[0], args[1]
	var outPath string
	if len(args) > 2 {
		outPath = args[2]
	}

	env, err := OpenFilesystemEnv(dir, fs.ReadOnly)
	if err != nil {
		return errors.Wrap(err, "could not open store")
	}
	defer env.Close()

	// Open the specified file through the FS, decrypting it.
	f, err := env.Open(inPath)
	if err != nil {
		return errors.Wrapf(err, "could not open input file %s", inPath)
	}
	defer f.Close()

	// Copy the raw bytes into the destination file.
	out := cmd.OutOrStdout()
	if outPath != "" {
		outFile, err := env.UnencryptedFS.Create(outPath, fs.UnspecifiedWriteCategory)
		if err != nil {
			return errors.Wrapf(err, "could not open output file %s", outPath)
		}
		defer outFile.Close()
		out = outFile
	}
	if _, err = io.Copy(out, f); err != nil {
		return errors.Wrapf(err, "could not write to output file")
	}
	return nil
}

type fileEntry struct {
	name     string
	envType  enginepb.EnvType
	settings enginepb.EncryptionSettings
}

func (f fileEntry) String() string {
	var b bytes.Buffer
	_, _ = fmt.Fprintf(
		&b, "%s:\n  env type: %s, %s\n",
		f.name, f.envType, f.settings.EncryptionType,
	)
	if f.settings.EncryptionType != enginepb.EncryptionType_Plaintext {
		_, _ = fmt.Fprintf(
			&b, "  keyID: %s\n  nonce: % x\n  counter: %d",
			f.settings.KeyId, f.settings.Nonce, f.settings.Counter,
		)
	}
	return b.String()
}

func runList(cmd *cobra.Command, args []string) (returnErr error) {
	dir := args[0]

	env, err := OpenFilesystemEnv(dir, fs.ReadOnly)
	if err != nil {
		return errors.Wrap(err, "could not open store")
	}
	defer env.Close()

	if env.Registry == nil {
		return errors.Newf("encryption-at-rest not enabled")
	}

	// List files and print to stdout.
	var entries []fileEntry
	for name, entry := range env.Registry.List() {
		var encSettings enginepb.EncryptionSettings
		settings := entry.EncryptionSettings
		if err := protoutil.Unmarshal(settings, &encSettings); err != nil {
			return errors.Wrapf(err, "could not unmarshal encryption settings for %s", name)
		}
		entries = append(entries, fileEntry{
			name:     name,
			envType:  entry.EnvType,
			settings: encSettings,
		})
	}
	slices.SortFunc(entries, func(a, b fileEntry) int { return cmp.Compare(a.name, b.name) })
	for _, e := range entries {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s\n", e)
	}
	return nil
}
