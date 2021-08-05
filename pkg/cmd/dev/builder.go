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
	"context"
	"log"
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
	args, err := d.getDockerRunArgs(ctx, volume, true)
	if err != nil {
		return err
	}
	return d.exec.CommandContextInheritingStdStreams(ctx, "docker", args...)
}

func (d *dev) ensureDockerVolume(ctx context.Context, volume string) error {
	_, err := d.exec.CommandContextSilent(ctx, "docker", "volume", "inspect", volume)
	if err != nil {
		log.Printf("Creating volume %s with Docker...", volume)
		_, err := d.exec.CommandContextSilent(ctx, "docker", "volume", "create", volume)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *dev) getDockerRunArgs(
	ctx context.Context, volume string, tty bool,
) (args []string, err error) {
	err = d.ensureBinaryInPath("docker")
	if err != nil {
		return
	}
	err = d.ensureDockerVolume(ctx, volume)
	if err != nil {
		return
	}

	args = append(args, "run", "--rm")
	if tty {
		args = append(args, "-it")
	} else {
		args = append(args, "-i")
	}
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return
	}
	args = append(args, "-v", workspace+":/cockroach:ro")
	args = append(args, "--workdir=/cockroach")
	// Create the artifacts directory.
	artifacts := filepath.Join(workspace, "artifacts")
	err = d.os.MkdirAll(artifacts)
	if err != nil {
		return
	}
	args = append(args, "-v", artifacts+":/artifacts")
	// The `delegated` switch ensures that the container's view of the cache
	// is authoritative. This can result in writes to the actual underlying
	// filesystem to be lost, but it's a cache so we don't care about that.
	args = append(args, "-v", volume+":/root/.cache/bazel:delegated")
	// Read the Docker image from build/teamcity-bazel-support.sh.
	buf, err := d.os.ReadFile(filepath.Join(workspace, "build/teamcity-bazel-support.sh"))
	if err != nil {
		return
	}
	var bazelImage string
	for _, line := range strings.Split(buf, "\n") {
		if strings.HasPrefix(line, "BAZEL_IMAGE=") {
			bazelImage = strings.Trim(strings.TrimPrefix(line, "BAZEL_IMAGE="), "\n ")
		}
	}
	if bazelImage == "" {
		err = errors.New("Could not find BAZEL_IMAGE in build/teamcity-bazel-support.sh")
		return
	}
	args = append(args, bazelImage)
	return
}
