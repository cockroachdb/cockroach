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

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/storage/disk"
)

// GetMonitorCounters returns DiskStats for all monitored disks.
// TODO(cheranm): Filter disk counters by the monitored disk path for Darwin builds.
func GetMonitorCounters(monitors map[string]disk.Monitor) (map[string]DiskStats, error) {
	diskCounters, err := GetDiskCounters(context.Background())
	if err != nil {
		return map[string]DiskStats{}, err
	}
	output := make(map[string]DiskStats, len(diskCounters))
	for _, stats := range diskCounters {
		output[stats.Name] = stats
	}
	return output, nil
}
