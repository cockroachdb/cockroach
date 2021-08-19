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
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// Common testing flags.
const (
	filterFlag  = "filter"
	timeoutFlag = "timeout"
	shortFlag   = "short"
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

func (d *dev) getBazelInfo(ctx context.Context, key string) (string, error) {
	args := []string{"info", key, "--color=no"}
	args = append(args, getConfigFlags()...)
	out, err := d.exec.CommandContextSilent(ctx, "bazel", args...)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil

}

func (d *dev) getWorkspace(ctx context.Context) (string, error) {
	return d.getBazelInfo(ctx, "workspace")
}

func (d *dev) getBazelBin(ctx context.Context) (string, error) {
	return d.getBazelInfo(ctx, "bazel-bin")
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
	cmd.Flags().Bool(shortFlag, false, "run only short tests")
}

func (d *dev) ensureBinaryInPath(bin string) error {
	if !isTesting {
		if _, err := d.exec.LookPath(bin); err != nil {
			return errors.Newf("Could not find %s in PATH", bin)
		}
	}
	return nil
}

// setupPath removes the ccache directory from PATH to prevent it writing files
// outside of the bazel sandbox. This function is called only once to prevent
// multiple unnecessary calls.
func setupPath(dev *dev) error {
	var once sync.Once
	var err error
	once.Do(func() {
		err = setupPathReal(dev)
	})
	return err
}

// setupPathReal uses a list of known compiler names to check if they are
// symlinks to `ccache`.
func setupPathReal(dev *dev) error {
	knownCompilers := []string{"cc", "gcc", "c++", "g++", "clang", "clang++"}
	// datadriven uses Fprintln() to construct commands, adding '\n' to the end.
	// Trim the output here and in other places that don't expect the extra new line
	// at the end.
	origPath := strings.TrimSuffix(dev.os.Getenv("PATH"), "\n")
	pathEntries := strings.Split(origPath, string(os.PathListSeparator))
	for i, entry := range pathEntries {
		pathEntries[i] = filepath.Clean(entry)
	}
	for _, compiler := range knownCompilers {
		compilerPath, err := dev.exec.LookPath(compiler)
		if err != nil {
			continue
		}
		compilerPath = strings.TrimSuffix(compilerPath, "\n")
		compilerDir, _ := filepath.Split(compilerPath)
		compilerDir = filepath.Clean(compilerDir)
		compilerResolvedPath, err := dev.os.Readlink(compilerPath)
		if err != nil {
			// Skip broken symlinks and real binaries
			continue
		}
		compilerResolvedPath = strings.TrimSuffix(compilerResolvedPath, "\n")
		_, file := filepath.Split(filepath.Clean(compilerResolvedPath))
		if file != "ccache" {
			continue
		}
		// The compiler points to ccache, remove it from PATH
		var newPathEntries []string
		for _, dir := range pathEntries {
			if compilerDir != dir {
				newPathEntries = append(newPathEntries, dir)
			}
		}
		newPath := strings.Join(newPathEntries, string(os.PathListSeparator))
		if origPath == newPath {
			log.Printf("WARNING: PATH did not change: %s", origPath)
		}
		if err := dev.os.Setenv("PATH", newPath); err != nil {
			return fmt.Errorf("failed to set PATH to %s, %w", newPath, err)
		}
		// All done, return early without trying other known compilers
		return nil
	}
	return nil
}
