// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"os"
	"path"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
)

// stage copies the specified archive to the remote machine and extracts it to
// the specified directory (creating it if it does not exist).
func stage(cluster, archivePath, remoteDest string) error {
	ctx := context.Background()
	_ = roachprod.InitProviders()
	config.Quiet = true

	loggerCfg := logger.Config{Stdout: os.Stdout, Stderr: os.Stderr}
	l, err := loggerCfg.NewLogger("")
	if err != nil {
		return err
	}
	if _, err = roachprod.Sync(l, vm.ListOptions{}); err != nil {
		return err
	}

	archiveName := path.Base(archivePath)
	archiveRemotePath := path.Join("/tmp", archiveName)

	// Remove the remote archive and destination directory if they exist.
	if err = roachprodRun(cluster, l, []string{"rm", "-rf", archiveRemotePath}); err != nil {
		return err
	}
	if err = roachprodRun(cluster, l, []string{"rm", "-rf", remoteDest}); err != nil {
		return err
	}

	// Copy the archive to the remote machine.
	copyFromGCS := strings.HasPrefix(archivePath, "gs://")
	if copyFromGCS {
		if err = roachprodRun(cluster, l, []string{"gsutil", "-m", "cp", archivePath, archiveRemotePath}); err != nil {
			return err
		}
	} else {
		if err = roachprod.Put(ctx, l, cluster, archivePath, archiveRemotePath, true); err != nil {
			return err
		}
	}

	// Extract the archive on the remote machine.
	if err = roachprodRun(cluster, l, []string{"mkdir", "-p", remoteDest}); err != nil {
		return err
	}
	if err = roachprodRun(cluster, l, []string{"tar", "-C", remoteDest, "-xzf", archiveRemotePath}); err != nil {
		return err
	}

	// Remove the remote archive now that it has been extracted.
	if err = roachprodRun(cluster, l, []string{"rm", "-rf", archiveRemotePath}); err != nil {
		return err
	}
	return nil
}
