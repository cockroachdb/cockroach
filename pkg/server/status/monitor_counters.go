// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !linux
// +build !linux

package status

import "github.com/cockroachdb/cockroach/pkg/storage/disk"

// GetMonitorCounters returns DiskStats for all monitored disks.
// TODO(cheranm): Retrieve disk counters and filter by the monitored disk path for Darwin builds.
func GetMonitorCounters(monitors map[string]disk.Monitor) (map[string]DiskStats, error) {
	return map[string]DiskStats{}, nil
}
