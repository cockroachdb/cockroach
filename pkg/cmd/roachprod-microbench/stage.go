// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"os"
	"path"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

// stage copies the specified archive to the remote machine and extracts it to
// the specified directory (creating it if it does not exist).
func stage(cluster, archivePath, remoteDest string) (err error) {
	ctx := context.Background()

	InitRoachprod()
	loggerCfg := logger.Config{Stdout: os.Stdout, Stderr: os.Stderr}
	l, err := loggerCfg.NewLogger("")
	if err != nil {
		return
	}

	archiveName := path.Base(archivePath)
	archiveRemotePath := path.Join("/tmp", archiveName)

	defer func() {
		// Remove the remote archive after we're done.
		cleanUpErr := RoachprodRun(cluster, l, []string{"rm", "-rf", archiveRemotePath})
		err = errors.CombineErrors(err, errors.Wrapf(cleanUpErr, "removing remote archive: %s", archiveRemotePath))
	}()

	// Remove the remote archive and destination directory if they exist.
	if err = RoachprodRun(cluster, l, []string{"rm", "-rf", archiveRemotePath}); err != nil {
		return errors.Wrapf(err, "removing remote archive: %s", archiveRemotePath)
	}
	if err = RoachprodRun(cluster, l, []string{"rm", "-rf", remoteDest}); err != nil {
		return errors.Wrapf(err, "removing remote destination: %s", remoteDest)
	}

	// Copy the archive to the remote machine.
	copyFromGCS := strings.HasPrefix(archivePath, "gs://")
	if copyFromGCS {
		if err = RoachprodRun(cluster, l, []string{"gsutil", "-q", "-m", "cp", archivePath, archiveRemotePath}); err != nil {
			return errors.Wrapf(err, "copying archive from GCS: %s", archivePath)
		}
	} else {
		if err = roachprod.Put(ctx, l, cluster, archivePath, archiveRemotePath, true); err != nil {
			return errors.Wrapf(err, "copying archive: %s", archivePath)
		}
	}

	// Extract the archive on the remote machine.
	if err = RoachprodRun(cluster, l, []string{"mkdir", "-p", remoteDest}); err != nil {
		return errors.Wrapf(err, "creating remote destination: %s", remoteDest)
	}
	if err = RoachprodRun(cluster, l, []string{"tar", "-C", remoteDest, "-xzf", archiveRemotePath}); err != nil {
		return errors.Wrapf(err, "extracting archive: %s", archiveRemotePath)
	}

	return
}
