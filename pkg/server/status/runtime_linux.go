// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build linux

package status

func getDefaultIgnoredDevices() string {
	// Excludes disks that have likely been counted elsewhere already, eg.
	// sda1 gets excluded because sda would count it instead, and nvme1n1p1 is
	// excluded as nvme1n1 is counted.
	//
	// This default regex is taken from Prometheus:
	// https://github.com/prometheus/node_exporter/blob/690efa61e86acefdf05bb4334a3d68128ded49c9/collector/diskstats_linux.go#L39
	return "^(ram|loop|fd|(h|s|v|xv)d[a-z]|nvme\\d+n\\d+p)\\d+$"
}
