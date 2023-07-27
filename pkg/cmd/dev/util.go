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
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/alessio/shellescape"
	"github.com/spf13/cobra"
)

// Common testing flags.
const (
	filterFlag  = "filter"
	timeoutFlag = "timeout"
	shortFlag   = "short"
)

var (
	// Shared flags.
	numCPUs int
)

var archivedCdepConfigurations = []configuration{
	{"linux", "amd64"},
	{"linux", "arm64"},
	{"darwin", "amd64"},
	{"darwin", "arm64"},
	{"windows", "amd64"},
}

func mustGetFlagString(cmd *cobra.Command, name string) string {
	val, err := cmd.Flags().GetString(name)
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}
	return val
}

func mustGetFlagStringArray(cmd *cobra.Command, name string) []string {
	val, err := cmd.Flags().GetStringArray(name)
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}
	return val
}

func mustGetFlagStringSlice(cmd *cobra.Command, name string) []string {
	val, err := cmd.Flags().GetStringSlice(name)
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

func mustGetFlagInt(cmd *cobra.Command, name string) int {
	val, err := cmd.Flags().GetInt(name)
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}
	return val
}

func mustGetConstrainedFlagInt(cmd *cobra.Command, name string, validate func(int) error) int {
	val := mustGetFlagInt(cmd, name)
	if err := validate(val); err != nil {
		log.Fatalf("invalid argument: %v", err)
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

func (d *dev) getBazelInfo(ctx context.Context, key string) (string, error) {
	args := []string{"info", key, "--color=no"}
	out, err := d.exec.CommandContextSilent(ctx, "bazel", args...)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil

}

func (d *dev) getWorkspace(ctx context.Context) (string, error) {
	if _, err := os.Stat("WORKSPACE"); err == nil {
		return os.Getwd()
	}

	return d.getBazelInfo(ctx, "workspace")
}

func (d *dev) getBazelBin(ctx context.Context) (string, error) {
	return d.getBazelInfo(ctx, "bazel-bin")
}

func (d *dev) getExecutionRoot(ctx context.Context) (string, error) {
	return d.getBazelInfo(ctx, "execution_root")
}

// getDevBin returns the path to the running dev executable.
func (d *dev) getDevBin() string {
	if d.knobs.devBinOverride != "" {
		return d.knobs.devBinOverride
	}
	return os.Args[0]
}

// getArchivedCdepString returns a non-empty string iff the force_build_cdeps
// config is not being used. This string is the name of the cross config used to
// build the pre-built c-deps, minus the "cross" prefix. This can be used to
// locate the pre-built c-dep in
// $EXECUTION_ROOT/external/archived_cdep_{LIB}_{ARCHIVED_CDEP_STRING}.
// If the returned string is empty then force_build_cdeps is set in which case
// the (non-pre-built) libraries can be found in $BAZEL_BIN/c-deps/{LIB}_foreign.
//
// You MUST build //build/bazelutil:test_force_build_cdeps before calling this
// function.
func (d *dev) getArchivedCdepString(bazelBin string) (string, error) {
	var ret string
	// If force_build_cdeps is set then the prebuilt libraries won't be in
	// the archived location anyway.
	forceBuildCdeps, err := d.os.ReadFile(filepath.Join(bazelBin, "build", "bazelutil", "test_force_build_cdeps.txt"))
	if err != nil {
		return "", err
	}
	// force_build_cdeps is activated if the length of this file is not 0.
	if len(forceBuildCdeps) == 0 {
		for _, config := range archivedCdepConfigurations {
			if config.Os == runtime.GOOS && config.Arch == runtime.GOARCH {
				ret = config.Os
				if ret == "darwin" {
					ret = "macos"
				}
				if config.Arch == "arm64" {
					ret += "arm"
				}
				break
			}
		}
	}
	return ret, nil
}

func addCommonBuildFlags(cmd *cobra.Command) {
	cmd.Flags().IntVar(&numCPUs, "cpus", 0, "cap the number of cpu cores used")
}

func addCommonTestFlags(cmd *cobra.Command) {
	cmd.Flags().StringP(filterFlag, "f", "", "run unit tests matching this regex")
	cmd.Flags().Duration(timeoutFlag, 0*time.Minute, "timeout for test")
	cmd.Flags().Bool(shortFlag, false, "run only short tests")
}

func (d *dev) ensureBinaryInPath(bin string) error {
	if _, err := d.exec.LookPath(bin); err != nil {
		return fmt.Errorf("could not find %s in PATH", bin)
	}
	return nil
}

func splitArgsAtDash(cmd *cobra.Command, args []string) (before, after []string) {
	argsLenAtDash := cmd.ArgsLenAtDash()
	if argsLenAtDash < 0 {
		// If there's no dash, the value of this is -1.
		before = args[:len(args):len(args)]
	} else {
		// NB: Have to do this verbose slicing to force Go to copy the
		// memory. Otherwise later `append`s will break stuff.
		before = args[0:argsLenAtDash:argsLenAtDash]
		after = args[argsLenAtDash:len(args):len(args)]
	}
	return
}

func logCommand(cmd string, args ...string) {
	var fullArgs []string
	fullArgs = append(fullArgs, cmd)
	fullArgs = append(fullArgs, args...)
	log.Printf("$ %s", shellescape.QuoteCommand(fullArgs))
}

func sendBepDataToBeaverHubIfNeeded(bepFilepath string) error {
	// Check if a BEP file exists.
	if _, err := os.Stat(bepFilepath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	file, err := os.Open(bepFilepath)
	if err != nil {
		return err
	}
	defer file.Close()
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", beaverHubServerEndpoint, file)
	req.Header.Add("Run-Env", "dev")
	req.Header.Add("Content-Type", "application/octet-stream")
	if _, err := httpClient.Do(req); err != nil {
		return err
	}
	_ = os.Remove(bepFilepath)
	return nil
}
