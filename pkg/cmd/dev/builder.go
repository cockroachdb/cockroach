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
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

const volumeFlag = "volume"

// MakeBuilderCmd constructs the subcommand used to run
func makeBuilderCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	builderCmd := &cobra.Command{
		Use:     "builder",
		Short:   "Run the Bazel builder image",
		Long:    "Run the Bazel builder image.",
		Example: `dev builder`,
		Args:    cobra.MinimumNArgs(0),
		RunE:    runE,
	}
	builderCmd.Flags().String(volumeFlag, "bzlhome", "the Docker volume to use as the container home directory")
	return builderCmd
}

func (d *dev) builder(cmd *cobra.Command, extraArgs []string) error {
	ctx := cmd.Context()
	volume := mustGetFlagString(cmd, volumeFlag)
	var tty bool
	if len(extraArgs) == 0 {
		tty = true
	}
	args, err := d.getDockerRunArgs(ctx, volume, tty)
	args = append(args, extraArgs...)
	if err != nil {
		return err
	}
	if tty {
		logCommand("docker", args...)
	}
	return d.exec.CommandContextInheritingStdStreams(ctx, "docker", args...)
}

func (d *dev) getDockerRunArgs(
	ctx context.Context, volume string, tty bool,
) (args []string, err error) {
	err = d.ensureBinaryInPath("docker")
	if err != nil {
		return
	}

	// Apply the same munging for the UID/GID that we do in build/builder.sh.
	// Quoth a comment from there:
	// Attempt to run in the container with the same UID/GID as we have on the host,
	// as this results in the correct permissions on files created in the shared
	// volumes. This isn't always possible, however, as IDs less than 100 are
	// reserved by Debian, and IDs in the low 100s are dynamically assigned to
	// various system users and groups. To be safe, if we see a UID/GID less than
	// 500, promote it to 501. This is notably necessary on macOS Lion and later,
	// where administrator accounts are created with a GID of 20. This solution is
	// not foolproof, but it works well in practice.
	uid, gid, err := d.os.CurrentUserAndGroup()
	if err != nil {
		return
	}
	uidInt, err := strconv.Atoi(uid)
	if err == nil && uidInt < 500 {
		uid = "501"
	}
	gidInt, err := strconv.Atoi(gid)
	if err == nil && gidInt < 500 {
		gid = uid
	}

	// Read the Docker image from build/teamcity-bazel-support.sh.
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return
	}
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
		err = errors.New("could not find BAZEL_IMAGE in build/teamcity-bazel-support.sh")
		return
	}

	// Ensure the Docker volume exists.
	_, err = d.exec.CommandContextSilent(ctx, "docker", "volume", "inspect", volume)
	if err != nil {
		log.Printf("Creating volume %s with Docker...", volume)
		_, err := d.exec.CommandContextSilent(ctx, "docker", "volume", "create", volume)
		if err != nil {
			return nil, err
		}
		// When Docker creates the volume, the owner will be `root`.
		// Fix that with a quick chown.
		_, err = d.exec.CommandContextSilent(
			ctx, "docker", "run", "--rm", "-v", volume+":/vol",
			"--entrypoint=/usr/bin/chown", bazelImage,
			"-R", fmt.Sprintf("%s:%s", uid, gid), "/vol")
		if err != nil {
			return nil, err
		}
	}

	args = append(args, "run", "--rm")
	if tty {
		args = append(args, "-it")
	} else {
		args = append(args, "-i")
	}
	args = append(args, "-v", workspace+":/cockroach")
	args = append(args, "--workdir=/cockroach")
	args = append(args, "-v", filepath.Join(workspace, "build", "bazelutil", "empty.bazelrc")+":/cockroach/.bazelrc.user")
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
	args = append(args, "-v", volume+":/home/roach:delegated")
	args = append(args, "-u", fmt.Sprintf("%s:%s", uid, gid))
	args = append(args, bazelImage)
	return
}
