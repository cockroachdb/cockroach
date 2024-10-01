// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
)

type workloadStep struct {
	Command string
	Args    []string
	Timeout time.Duration `yaml:"timeout,omitempty"`
}

type workloadConfig struct {
	Name, Kind string
	Steps      []workloadStep
}

type config struct {
	Cluster         spec.ClusterSpec
	Cloud           string `yaml:"cloud"`
	CertsDir        string `yaml:"certs"`
	ClusterName     string `yaml:"cluster_name"`
	CockroachBinary string `yaml:"cockroach_binary"`
	WorkloadBinary  string `yaml:"workload_binary"`
	RoachtestBinary string `yaml:"roachtest_binary"`
	Workloads       []workloadConfig

	Operations struct {
		Parallelism int
		Sets        []struct {
			Cadence time.Duration
			Filter  string
		}
	}
}
