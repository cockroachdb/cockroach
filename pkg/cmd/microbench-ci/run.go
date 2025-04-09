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
	status := NoChange
	tries := 0
	for retries := 0; retries < b.Retries; retries++ {
		log.Printf("Running benchmark %q for %d iterations (attempt %d)", b.Name, b.Count, retries+1)
		for i := 1; i <= b.Count; i++ {
			for _, revision := range []Revision{New, Old} {
				log.Printf("%s binary iteration (%d out of %d)",
					revision, i, b.Count,
				)
				err := b.runIteration(revision, fmt.Sprintf("%d", (tries*b.Count)+i))
				if err != nil {
					return err
				}
			}
		}
		tries++

		compareResult, err := b.compare(b.Count)
		if err != nil {
			return err
		}
		currentStatus := compareResult.top()
		// If the benchmark shows no change, we can stop running additional retry
		// iterations. Retries are run once a benchmark has regressed or improved,
		// on the first try, to ensure that the change is not a fluke.
		if currentStatus == NoChange {
			status = currentStatus
			break
		}

		// If this is the first run, we set the status to the current status.
		// Otherwise, we check if the regression or improvement is persistent and
		// continue running until all retries are exhausted. If the status flips
		// from a regression to an improvement or vice versa, we set the status to
		// NoChange and stop running, as the results are inconclusive.
		if status == NoChange {
			status = currentStatus
		} else if status != currentStatus {
			// If the benchmark shows a different change, the results are inconclusive.
			status = NoChange
			break
		}
	}

	// Write change marker file if the benchmark changed.
	if status != NoChange {
		marker := strings.ToUpper(status.String())
		err := os.WriteFile(path.Join(suite.artifactsDir(New), b.sanitizedName()+"."+marker), nil, 0644)
		if err != nil {
			return err
		}
	}

	// Concat profiles.
	for _, profileType := range []ProfileType{ProfileCPU, ProfileMemory, ProfileMutex} {
		err := b.concatProfile(profileType, tries*b.Count)
		if err != nil {
			return errors.Wrapf(err, "failed to concat %s profiles", profileType)
		}
	}
	return nil
}

func (b *Benchmark) concatProfile(profileType ProfileType, count int) error {
	for _, revision := range []Revision{New, Old} {
		profiles := make([]*profile.Profile, 0, b.Count)
		deleteProfiles := make([]func() error, 0)
		for i := 1; i <= count; i++ {
			profilePath := path.Join(suite.artifactsDir(revision),
				b.profile(profileType, fmt.Sprintf("%d", i)))
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
