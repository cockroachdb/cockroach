// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"hash/fnv"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	yaml "gopkg.in/yaml.v2"
)

type (
	Benchmark struct {
		DisplayName string   `yaml:"display_name"`
		Package     string   `yaml:"package"`
		Labels      []string `yaml:"labels"`
		Name        string   `yaml:"name"`
		RunnerGroup int      `yaml:"runner_group"`
		Count       int      `yaml:"count"`
		Iterations  int      `yaml:"iterations"`

		Thresholds map[string]float64 `yaml:"thresholds"`
	}
	Benchmarks  []Benchmark
	ProfileType string
)

const (
	ProfileCPU    ProfileType = "cpu"
	ProfileMemory ProfileType = "memory"
	ProfileMutex  ProfileType = "mutex"
)

var sanitizeRe = regexp.MustCompile(`\W+`)

func (b *Benchmark) sanitizedPackageName() string {
	return strings.ReplaceAll(b.Package, "/", "_")
}

func (b *Benchmark) sanitizedName() string {
	return sanitizeRe.ReplaceAllString(strings.TrimPrefix(b.Name, "Benchmark"), "_")
}

func (b *Benchmark) binaryName() string {
	return b.sanitizedPackageName()
}

func (b *Benchmark) rawLog() string {
	return fmt.Sprintf("raw_%s.log", b.sanitizedName())
}

func (b *Benchmark) cleanLog() string {
	return fmt.Sprintf("cleaned_%s.log", b.sanitizedName())
}

func (b *Benchmark) profile(profileType ProfileType, suffix string) string {
	return fmt.Sprintf("%s_%s_%s.prof", profileType, b.sanitizedName(), suffix)
}

func (b *Benchmark) packageHash() string {
	h := fnv.New32a()
	_, _ = h.Write([]byte("./" + b.Package))
	u := h.Sum32()
	return strconv.Itoa(int(u))
}

// args returns the arguments to pass to the test binary for the given iteration
// type.
func (b *Benchmark) args(outputDir string, profileSuffix string) []string {
	args := []string{
		"-test.run", "^$",
		"-test.bench", b.Name,
		"-test.benchmem",
		"-test.count", "1",
		"-test.outputdir", outputDir,
		"-test.benchtime", fmt.Sprintf("%dx", b.Iterations),
		"-test.cpuprofile", b.profile(ProfileCPU, profileSuffix),
		"-test.memprofile", b.profile(ProfileMemory, profileSuffix),
		"-test.mutexprofile", b.profile(ProfileMutex, profileSuffix),
	}
	return args
}

// loadBenchmarkConfig loads the benchmark configurations from the given file.
func loadBenchmarkConfig(path string) ([]Benchmark, error) {
	var c = struct {
		Benchmarks []Benchmark `yaml:"benchmarks"`
	}{}
	fileContent, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read config file %s", path)
	}
	err = yaml.UnmarshalStrict(fileContent, &c)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal config file %s", path)
	}
	return c.Benchmarks, nil
}
