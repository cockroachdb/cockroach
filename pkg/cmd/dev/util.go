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
	"os"
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

func mustGetFlagInt(cmd *cobra.Command, name string) int {
	val, err := cmd.Flags().GetInt(name)
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

// getDevBin returns the path to the running dev executable.
func (d *dev) getDevBin() string {
	if d.knobs.devBinOverride != "" {
		return d.knobs.devBinOverride
	}
	return os.Args[0]
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
		after = args[argsLenAtDash : len(args) : len(args)-argsLenAtDash+1]
	}
	return
}

func logCommand(cmd string, args ...string) {
	var fullArgs []string
	fullArgs = append(fullArgs, cmd)
	fullArgs = append(fullArgs, args...)
	log.Printf("$ %s", shellescape.QuoteCommand(fullArgs))
}
