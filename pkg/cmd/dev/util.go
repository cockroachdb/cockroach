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
	"fmt"
	"log"
	"net"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// To be turned on for tests. Turns off some deeper checks for reproducibility.
var isTesting bool

func mustGetFlagString(cmd *cobra.Command, name string) string {
	val, err := cmd.Flags().GetString(name)
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}
	return val
}

func mustGetFlagBool(cmd *cobra.Command, name string) bool {
	val, err := cmd.Flags().GetBool(name)
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}
	return val
}

func mustGetFlagDuration(cmd *cobra.Command, name string) time.Duration {
	val, err := cmd.Flags().GetDuration(name)
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}
	return val
}

func mustGetRemoteCacheArgs(cacheAddr string) []string {
	if cacheAddr == "" {
		return nil
	}
	cAddr, err := parseAddr(cacheAddr)
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}
	var args []string
	args = append(args, "--remote_local_fallback")
	args = append(args, fmt.Sprintf("--remote_cache=grpc://%s", cAddr))
	args = append(args, fmt.Sprintf("--experimental_remote_downloader=grpc://%s", cAddr))
	return args
}

func parseAddr(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return "", errors.Newf("invalid address %s", addr)
	}

	return fmt.Sprintf("%s:%s", ip, port), nil
}

func (d *dev) getWorkspace(ctx context.Context) (string, error) {
	args := []string{"info", "workspace", "--color=no"}
	args = append(args, getConfigFlags()...)
	out, err := d.exec.CommandContextSilent(ctx, "bazel", args...)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func getConfigFlags() []string {
	if skipDevConfig {
		return []string{}
	}
	if !isTesting && runtime.GOOS == "darwin" && runtime.GOARCH == "amd64" {
		return []string{"--config=devdarwinx86_64"}
	}
	return []string{"--config=dev"}
}

func addCommonTestFlags(cmd *cobra.Command) {
	cmd.Flags().StringP(filterFlag, "f", "", "run unit tests matching this regex")
	cmd.Flags().Duration(timeoutFlag, 0*time.Minute, "timeout for test")
}

func ensureBinaryInPath(bin string) error {
	if !isTesting {
		if _, err := exec.LookPath(bin); err != nil {
			return errors.Newf("Could not find %s in PATH", bin)
		}
	}
	return nil
}
