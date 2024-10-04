// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cliccl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl/enginepbccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/spf13/cobra"
)

func runDecrypt(_ *cobra.Command, args []string) (returnErr error) {
	dir, inPath := args[0], args[1]
	var outPath string
	if len(args) > 2 {
		outPath = args[2]
	}

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	db, err := cli.OpenEngine(dir, stopper, storage.MustExist, storage.ReadOnly)
	if err != nil {
		return errors.Wrap(err, "could not open store")
	}

	// Open the specified file through the FS, decrypting it.
	f, err := db.Open(inPath)
	if err != nil {
		return errors.Wrapf(err, "could not open input file %s", inPath)
	}
	defer f.Close()

	// Copy the raw bytes into the destination file.
	outFile := os.Stdout
	if outPath != "" {
		outFile, err = os.OpenFile(outPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		if err != nil {
			return errors.Wrapf(err, "could not open output file %s", outPath)
		}
		defer outFile.Close()
	}
	if _, err = io.Copy(outFile, f); err != nil {
		return errors.Wrapf(err, "could not write to output file")
	}

	return nil
}

type fileEntry struct {
	name     string
	envType  enginepb.EnvType
	settings enginepbccl.EncryptionSettings
}

func (f fileEntry) String() string {
	var b bytes.Buffer
	_, _ = fmt.Fprintf(
		&b, "%s:\n  env type: %s, %s\n",
		f.name, f.envType, f.settings.EncryptionType,
	)
	if f.settings.EncryptionType != enginepbccl.EncryptionType_Plaintext {
		_, _ = fmt.Fprintf(
			&b, "  keyID: %s\n  nonce: % x\n  counter: %d",
			f.settings.KeyId, f.settings.Nonce, f.settings.Counter,
		)
	}
	return b.String()
}

func runList(cmd *cobra.Command, args []string) error {
	dir := args[0]

	fr := &storage.PebbleFileRegistry{
		FS:                  vfs.Default,
		DBDir:               dir,
		ReadOnly:            true,
		NumOldRegistryFiles: storage.DefaultNumOldFileRegistryFiles,
	}
	if err := fr.Load(cmd.Context()); err != nil {
		return errors.Wrapf(err, "could not load file registry")
	}
	defer func() { _ = fr.Close() }()

	// List files and print to stdout.
	var entries []fileEntry
	for name, entry := range fr.List() {
		var encSettings enginepbccl.EncryptionSettings
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
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].name < entries[j].name
	})
	for _, e := range entries {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s\n", e)
	}

	return nil
}
