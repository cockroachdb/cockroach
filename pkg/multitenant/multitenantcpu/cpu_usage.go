// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multitenantcpu

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// GetCPUSeconds returns the total CPU usage of the current process in seconds.
// It is used for measuring tenant RU consumption.
func GetCPUSeconds(ctx context.Context) (cpuSecs float64) {
	userTimeMillis, sysTimeMillis, err := status.GetProcCPUTime(ctx)
	if err != nil {
		log.Ops.Errorf(ctx, "unable to get cpu usage: %v", err)
		return 0
	}
	return float64(userTimeMillis+sysTimeMillis) * 1e-3
}
