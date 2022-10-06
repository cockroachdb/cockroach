// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl

import (
	"context"
	"io"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
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
