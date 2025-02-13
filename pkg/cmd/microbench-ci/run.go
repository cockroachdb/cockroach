// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/parser"
	"github.com/cockroachdb/errors"
	"github.com/google/pprof/profile"
)

// execFunc is a function that executes a command and returns its output. This
// is used to allow for mocking out the exec package in tests.
var execFunc = func(cmd *exec.Cmd) ([]byte, error) {
	return cmd.CombinedOutput()
}

// command returns the command to run the benchmark binary for a given revision
// and iteration type.
func (b *Benchmark) command(revision Revision, profileSuffix string) *exec.Cmd {
	cmd := exec.Command(
		path.Join(suite.binDir(revision), b.binaryName()),
		b.args(suite.artifactsDir(revision), profileSuffix)...,
	)
	cmd.Env = append(os.Environ(), "COCKROACH_RANDOM_SEED=1")
	return cmd
}

// runIteration runs a single iteration of the benchmark for the given revision.
func (b *Benchmark) runIteration(revision Revision, profileSuffix string) error {
	cmd := b.command(revision, profileSuffix)
	output, err := execFunc(cmd)
	if err != nil {
		return errors.Wrapf(err, "benchmark %q, command %q failed to run:\n%s",
			b.DisplayName, cmd.String(), string(output))
	}

	results := parser.ExtractBenchmarkResults(string(output))
	if results.Errors {
		return errors.Newf("benchmark results contained errors:\n%s", string(output))
	}
	if results.Skipped {
		return errors.Newf("benchmark invalid due to being skipped:\n%s", string(output))
	}

	var cleanedOutput strings.Builder
	for _, result := range results.Results {
		cleanedOutput.WriteString(strings.Join(result, " "))
		cleanedOutput.WriteString("\n")
	}
	err = appendToFile(path.Join(suite.artifactsDir(revision), b.cleanLog()), cleanedOutput.String())
	if err != nil {
		return err
	}
	err = appendToFile(path.Join(suite.artifactsDir(revision), b.rawLog()), string(output))
	if err != nil {
		return err
	}
	return nil
}

// run runs the benchmark as configured in the Benchmark struct.
func (b *Benchmark) run() error {
	for _, revision := range []Revision{New, Old} {
		err := os.MkdirAll(suite.artifactsDir(revision), 0755)
		if err != nil {
			return err
		}
	}

	log.Printf("Running benchmark %q for %d iterations", b.Name, b.Count)
	for i := 0; i < b.Count; i++ {
		for _, revision := range []Revision{New, Old} {
			log.Printf("%s binary iteration (%d out of %d)",
				revision, i+1, b.Count,
			)
			err := b.runIteration(revision, fmt.Sprintf("%d", i+1))
			if err != nil {
				return err
			}
		}
	}

	// Write failure marker file if the benchmark regressed.
	compareResult, err := b.compare()
	if err != nil {
		return err
	}
	if compareResult.regressed() {
		// Mark the revision as failed.
		for _, revision := range []Revision{New, Old} {
			err = os.WriteFile(path.Join(suite.artifactsDir(revision), ".FAILED"), nil, 0644)
			if err != nil {
				return err
			}
		}
	}

	// Concat profiles.
	for _, profileType := range []ProfileType{ProfileCPU, ProfileMemory, ProfileMutex} {
		err = b.concatProfile(profileType)
		if err != nil {
			return errors.Wrapf(err, "failed to concat %s profiles", profileType)
		}
	}
	return nil
}

func (b *Benchmark) concatProfile(profileType ProfileType) error {
	for _, revision := range []Revision{New, Old} {
		profiles := make([]*profile.Profile, 0, b.Count)
		deleteProfiles := make([]func() error, 0)
		for i := 0; i < b.Count; i++ {
			profilePath := path.Join(suite.artifactsDir(revision),
				b.profile(profileType, fmt.Sprintf("%d", i+1)))
			profileData, err := os.ReadFile(profilePath)
			if err != nil {
				return err
			}
			p, err := profile.Parse(bytes.NewReader(profileData))
			if err != nil {
				return err
			}
			profiles = append(profiles, p)
			deleteProfiles = append(deleteProfiles, func() error {
				return os.Remove(profilePath)
			})
		}
		merged, err := profile.Merge(profiles)
		if err != nil {
			return err
		}
		mergedPath := path.Join(suite.artifactsDir(revision), b.profile(profileType, "merged"))
		f, err := os.Create(mergedPath)
		if err != nil {
			return err
		}
		if err = merged.Write(f); err != nil {
			_ = f.Close()
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
		for _, deleteProfile := range deleteProfiles {
			if err = deleteProfile(); err != nil {
				return err
			}
		}
	}
	return nil
}

func appendToFile(filename, data string) error {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.WriteString(data); err != nil {
		return err
	}
	return nil
}

// run runs all benchmarks in the Benchmarks suite for the active runner group.
// The runner groups help split the benchmarks up between multiple VMs to reduce
// overall CI runtime.
func (b Benchmarks) run() error {
	for _, benchmark := range b {
		if config.Group != 0 && benchmark.RunnerGroup != config.Group {
			continue
		}
		if err := benchmark.run(); err != nil {
			return err
		}
	}
	return nil
}
