// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	numCPUs    int
	pgoEnabled bool
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

func (d *dev) getBazelInfo(ctx context.Context, key string, extraArgs []string) (string, error) {
	args := []string{"info", key, "--color=no"}
	args = append(args, extraArgs...)
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

	return d.getBazelInfo(ctx, "workspace", []string{})
}

// The second argument should be the relevant "config args", namely Bazel arguments
// that are --config or --compilation_mode arguments (see getConfigArgs()).
func (d *dev) getBazelBin(ctx context.Context, configArgs []string) (string, error) {
	return d.getBazelInfo(ctx, "bazel-bin", configArgs)
}

func (d *dev) getExecutionRoot(ctx context.Context, configArgs []string) (string, error) {
	return d.getBazelInfo(ctx, "execution_root", configArgs)
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
	cmd.Flags().IntVar(&numCPUs, "cpus", 0, "cap the number of CPU cores used for building and testing at the Bazel level (note that this has no impact on GOMAXPROCS or the functionality of any build or test action under the Bazel level)")
	cmd.Flags().BoolVar(&pgoEnabled, "pgo", false, "build with profile-guided optimization (PGO)")
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

	// TODO(dt): Ideally this would be <100ms since it is blocking the process
	// from exiting and returning to the user to the their prompt, and any delay
	// longer than that is noticeable. However empirical testing suggests we need
	// ~300ms to upload successfully. Ideally we would move the invocation of this
	// function to its own subcommand, and then post-build we would just exec that
	// command forked in a new process, but not wait for it before exiting.
	const timeout = 400 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "POST", beaverHubServerEndpoint, file)
	req.Header.Add("Run-Env", "dev")
	req.Header.Add("Content-Type", "application/octet-stream")
	if _, err := httpClient.Do(req); err != nil {
		return err
	}
	_ = os.Remove(bepFilepath)
	return nil
}

func (d *dev) warnAboutChangeInStressBehavior(timeout time.Duration) {
	if e := d.os.Getenv("DEV_I_UNDERSTAND_ABOUT_STRESS"); e == "" {
		log.Printf("NOTE: The behavior of `dev test --stress` has changed. The new default behavior is to run the test 1,000 times in parallel (500 for logictests), stopping if any of the tests fail. The number of runs can be tweaked with the `--count` parameter to `dev`.")
		if timeout > 0 {
			log.Printf("WARNING: The behavior of --timeout under --stress has changed. --timeout controls the timeout of the test, not the entire `stress` invocation.")
		}
		log.Printf("Set DEV_I_UNDERSTAND_ABOUT_STRESS=1 to squelch this message")
	}
}

// This function retrieves the merge-base hash between the current branch and master
func (d *dev) getMergeBaseHash(ctx context.Context) (string, error) {
	// List files changed against `master`
	remotes, err := d.exec.CommandContextSilent(ctx, "git", "remote", "-v")
	if err != nil {
		return "", err
	}
	var upstream string
	for _, remote := range strings.Split(strings.TrimSpace(string(remotes)), "\n") {
		if (strings.Contains(remote, "github.com/cockroachdb/cockroach") || strings.Contains(remote, "github.com:cockroachdb/cockroach")) && strings.HasSuffix(remote, "(fetch)") {
			upstream = strings.Fields(remote)[0]
			break
		}
	}
	if upstream == "" {
		return "", fmt.Errorf("could not find git upstream, run `git remote add upstream git@github.com:cockroachdb/cockroach.git`")
	}
	baseBytes, err := d.exec.CommandContextSilent(ctx, "git", "merge-base", fmt.Sprintf("%s/master", upstream), "HEAD")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(baseBytes)), nil
}

func addCommonBazelArguments(args *[]string) {
	if numCPUs != 0 {
		*args = append(*args, fmt.Sprintf("--local_cpu_resources=%d", numCPUs))
	}
	if pgoEnabled {
		*args = append(*args, "--config=pgo")
	}
}
