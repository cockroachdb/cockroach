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
		DisplayName  string             `yaml:"display_name"`
		Package      string             `yaml:"package"`
		Labels       []string           `yaml:"labels"`
		Name         string             `yaml:"name"`
		RunnerGroup  int                `yaml:"runner_group"`
		MeasureCount int                `yaml:"measure_count"`
		Measure      Sample             `yaml:"measure"`
		CPUProfile   Sample             `yaml:"cpu_profile"`
		MemProfile   Sample             `yaml:"mem_profile"`
		MutexProfile Sample             `yaml:"mutex_profile"`
		Thresholds   map[string]float64 `yaml:"thresholds"`
	}
	BenchmarkIterationType uint8
	Benchmarks             []Benchmark

	Sample struct {
		Iterations int `yaml:"iterations"`
	}
)

const (
	Measure BenchmarkIterationType = iota
	ProfileCPU
	ProfileMemory
	ProfileMutex
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

func (b *Benchmark) cpuProfile() string {
	return fmt.Sprintf("cpu_%s.out", b.sanitizedName())
}

func (b *Benchmark) memProfile() string {
	return fmt.Sprintf("mem_%s.out", b.sanitizedName())
}

func (b *Benchmark) mutexProfile() string {
	return fmt.Sprintf("mutex_%s.out", b.sanitizedName())
}

func (b *Benchmark) packageHash() string {
	h := fnv.New32a()
	_, _ = h.Write([]byte("./" + b.Package))
	u := h.Sum32()
	return strconv.Itoa(int(u))
}

// args returns the arguments to pass to the test binary for the given iteration
// type.
func (b *Benchmark) args(outputDir string, iterType BenchmarkIterationType) []string {
	args := []string{
		"-test.run", "^$",
		"-test.bench", b.Name,
		"-test.benchmem",
		"-test.count", "1",
		"-test.outputdir", outputDir,
	}

	var sample Sample
	switch iterType {
	case Measure:
		sample = b.Measure
	case ProfileCPU:
		sample = b.CPUProfile
		args = append(args, "-test.cpuprofile", b.cpuProfile())
	case ProfileMemory:
		sample = b.MemProfile
		args = append(args, "-test.memprofile", b.memProfile())
	case ProfileMutex:
		sample = b.MemProfile
		args = append(args, "-test.mutexprofile", b.mutexProfile())
	default:
		panic("unknown iteration type")
	}
	// If the sample does not specify iterations, use the default iterations.
	if sample.Iterations == 0 {
		sample.Iterations = b.Measure.Iterations
	}
	args = append(args, "-test.benchtime", fmt.Sprintf("%dx", sample.Iterations))
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
