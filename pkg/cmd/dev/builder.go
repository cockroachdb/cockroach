// Copyright 2020 The Cockroach Authors.
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
	"log"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

const volumeFlag = "volume"

// MakeBuilderCmd constructs the subcommand used to run
func makeBuilderCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	builderCmd := &cobra.Command{
		Use:     "builder",
		Short:   "Run the Bazel builder image.",
		Long:    "Run the Bazel builder image.",
		Example: `dev builder`,
		Args:    cobra.ExactArgs(0),
		RunE:    runE,
	}
	builderCmd.Flags().String(volumeFlag, "bzlcache", "the Docker volume to use as the Bazel cache")
	return builderCmd
}

func (d *dev) builder(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()

	volume := mustGetFlagString(cmd, volumeFlag)
	if !isTesting {
		if _, err := exec.LookPath("docker"); err != nil {
			return errors.New("Could not find docker in PATH")
		}
	}

	// Ensure the volume to use exists.
	_, err := d.exec.CommandContextSilent(ctx, "docker", "volume", "inspect", volume)
	if err != nil {
		log.Printf("Creating volume %s with Docker...", volume)
		_, err := d.exec.CommandContextSilent(ctx, "docker", "volume", "create", volume)
		if err != nil {
			return err
		}
	}

	var args []string
	args = append(args, "run", "--rm", "-it")
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	args = append(args, "-v", workspace+":/cockroach:ro")
	args = append(args, "--workdir=/cockroach")
	// Create the artifacts directory.
	artifacts := filepath.Join(workspace, "artifacts")
	if err = d.os.MkdirAll(artifacts); err != nil {
		return err
	}
	args = append(args, "-v", artifacts+":/artifacts")
	// The `delegated` switch ensures that the container's view of the cache
	// is authoritative. This can result in writes to the actual underlying
	// filesystem to be lost, but it's a cache so we don't care about that.
	args = append(args, "-v", volume+":/root/.cache/bazel:delegated")
	// Read the Docker image from build/teamcity-bazel-support.sh.
	buf, err := d.os.ReadFile(filepath.Join(workspace, "build/teamcity-bazel-support.sh"))
	if err != nil {
		return err
	}
	var bazelImage string
	for _, line := range strings.Split(buf, "\n") {
		if strings.HasPrefix(line, "BAZEL_IMAGE=") {
			bazelImage = strings.Trim(strings.TrimPrefix(line, "BAZEL_IMAGE="), "\n ")
		}
	}
	if bazelImage == "" {
		return errors.New("Could not find BAZEL_IMAGE in build/teamcity-bazel-support.sh")
	}
	args = append(args, bazelImage)

	return d.exec.CommandContextNoRecord(ctx, "docker", args...)
}
