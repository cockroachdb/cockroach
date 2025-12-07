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
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// stage copies the specified archive to the remote machine and extracts it to
// the specified directory (creating it if it does not exist).
func stage(cluster, archivePath, remoteDest string, longRetries bool) (err error) {
	ctx := context.Background()

	InitRoachprod()
	loggerCfg := logger.Config{Stdout: os.Stdout, Stderr: os.Stderr}
	l, err := loggerCfg.NewLogger("")
	if err != nil {
		return
	}

	runOptions := install.DefaultRunOptions()
	if longRetries {
		// For VMs that have started failing with transient errors (usually due to
		// preemption), we want to introduce a longer retry period. These VMs may
		// still be recoverable since they're part of a managed instance group
		// that attempts to recover failed VMs.
		runOptions = runOptions.WithRetryOpts(retry.Options{
			InitialBackoff: 1 * time.Minute,
			MaxBackoff:     5 * time.Minute,
			Multiplier:     2,
			MaxRetries:     10,
		})
	}

	archiveName := path.Base(archivePath)
	archiveRemotePath := path.Join("/tmp", archiveName)

	defer func() {
		// Remove the remote archive after we're done.
		cleanUpErr := RoachprodRun(cluster, l, []string{"rm", "-rf", archiveRemotePath}, runOptions)
		err = errors.CombineErrors(err, errors.Wrapf(cleanUpErr, "removing remote archive: %s", archiveRemotePath))
	}()

	// Remove the remote archive and destination directory if they exist.
	if err = RoachprodRun(cluster, l, []string{"rm", "-rf", archiveRemotePath}, runOptions); err != nil {
		return errors.Wrapf(err, "removing remote archive: %s", archiveRemotePath)
	}
	if err = RoachprodRun(cluster, l, []string{"rm", "-rf", remoteDest}, runOptions); err != nil {
		return errors.Wrapf(err, "removing remote destination: %s", remoteDest)
	}

	// Copy the archive to the remote machine.
	copyFromGCS := strings.HasPrefix(archivePath, "gs://")
	if copyFromGCS {
		if err = RoachprodRun(cluster, l, []string{"gsutil", "-q", "-m", "cp", archivePath, archiveRemotePath}, runOptions); err != nil {
			return errors.Wrapf(err, "copying archive from GCS: %s", archivePath)
		}
	} else {
		if err = roachprod.Put(ctx, l, cluster, archivePath, archiveRemotePath, true); err != nil {
			return errors.Wrapf(err, "copying archive: %s", archivePath)
		}
	}

	// Extract the archive on the remote machine.
	if err = RoachprodRun(cluster, l, []string{"mkdir", "-p", remoteDest}, runOptions); err != nil {
		return errors.Wrapf(err, "creating remote destination: %s", remoteDest)
	}
	if err = RoachprodRun(cluster, l, []string{"tar", "-C", remoteDest, "-xzf", archiveRemotePath}, runOptions); err != nil {
		return errors.Wrapf(err, "extracting archive: %s", archiveRemotePath)
	}

	return
}
