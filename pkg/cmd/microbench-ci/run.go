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

	status := make(map[string]Status)
	for _, metric := range b.Metrics {
		status[metric.Name] = NoChange
	}

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

		// Loop through all metrics and check if they have a consistent status.
		// As long as one metric maintains a consistent status (other than
		// NoChange), we continue running additional retry iterations.
		keepRunning := false
		for _, metric := range b.Metrics {
			currentStatus := compareResult.status(metric.Name)
			// If this is the first run, we set the status to the current
			// status. Only continue running if a metric has regressed or
			// improved.
			if retries == 0 {
				status[metric.Name] = currentStatus
				if currentStatus != NoChange {
					keepRunning = true
				}
				continue
			}

			// On a retry, if any metric continues having a consistent state (other
			// than NoChange), we run additional retries.
			if status[metric.Name] != NoChange {
				if status[metric.Name] == currentStatus {
					keepRunning = true
				} else {
					// The metric previously indicated a change, but now
					// indicates a change in the opposite direction. Reset the
					// status to NoChange to indicate that the benchmark did not
					// change.
					status[metric.Name] = NoChange
				}
			}
		}

		if !keepRunning {
			// Reset all metrics to NoChange to indicate that the benchmark did
			// not change, and we can stop running additional retries.
			for _, metric := range b.Metrics {
				status[metric.Name] = NoChange
			}
			break
		}
	}

	// Write change marker file (per metric) if the benchmark changed. It's
	// possible that both a regression and an improvement occurred, for
	// different metrics of the same benchmark, in which case both markers will
	// be written.
	for _, metric := range b.Metrics {
		if status[metric.Name] != NoChange {
			err := os.WriteFile(path.Join(suite.artifactsDir(New), b.markerName(status[metric.Name])), nil, 0644)
			if err != nil {
				return err
			}
		}
	}

	// Concat profiles.
	for _, profileType := range []ProfileType{ProfileCPU, ProfileMemory, ProfileMutex} {
		for try := range tries {
			err := b.concatProfile(profileType, try, b.Count)
			if err != nil {
				return errors.Wrapf(err, "failed to concat %s profiles", profileType)
			}
		}
	}
	return nil
}

// concatProfile concatenates the profiles for the given profile type and
// revision. Start is the try number, and count is the number of iterations
// per try.
func (b *Benchmark) concatProfile(profileType ProfileType, start, count int) error {
	offset := start * b.Count
	for _, revision := range []Revision{New, Old} {
		profiles := make([]*profile.Profile, 0, b.Count)
		deleteProfiles := make([]string, 0)
		for i := 1; i <= count; i++ {
			profilePath := path.Join(suite.artifactsDir(revision),
				b.profile(profileType, fmt.Sprintf("%d", offset+i)))
			profileData, err := os.ReadFile(profilePath)
			if err != nil {
				return err
			}
			p, err := profile.Parse(bytes.NewReader(profileData))
			if err != nil {
				return err
			}
			profiles = append(profiles, p)
			deleteProfiles = append(deleteProfiles, profilePath)
		}
		merged, err := profile.Merge(profiles)
		if err != nil {
			return err
		}
		mergedPath := path.Join(suite.artifactsDir(revision), b.profile(profileType, fmt.Sprintf("merged_%d", start+1)))
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
		for _, path := range deleteProfiles {
			if err = os.Remove(path); err != nil {
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
