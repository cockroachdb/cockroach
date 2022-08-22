// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admission

import (
	"runtime"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
)

var _ elasticCPUUtilizationAdjuster = &elasticCPUGranter{}

// setTargetUtilization is part of the elasticCPUUtilizationAdjuster interface.
func (e *elasticCPUGranter) setTargetUtilization(targetUtilization float64) {
	e.mu.Lock()
	e.mu.targetUtilization = targetUtilization
	e.mu.Unlock()
	e.metrics.TargetUtilization.Update(targetUtilization)

	rate := targetUtilization * float64(int64(runtime.GOMAXPROCS(0))*time.Second.Nanoseconds())
	e.rl.UpdateLimit(quotapool.Limit(rate), int64(rate))
	if log.V(1) {
		log.Infof(e.ctx, "elastic cpu granter refill rate = %0.4f cpu seconds per second (utilization across %d procs = %0.2f%%)",
			time.Duration(rate).Seconds(), runtime.GOMAXPROCS(0), targetUtilization*100)
	}
}

// getTargetUtilization is part of the elasticCPUUtilizationAdjuster interface.
func (e *elasticCPUGranter) getTargetUtilization() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mu.targetUtilization
}

// getObservedUtilization is part of the elasticCPUUtilizationAdjuster interface.
func (e *elasticCPUGranter) getObservedUtilization() float64 {
	availableElasticCPUTime := time.Duration(e.rl.Available())
	totalElasticCPUTime := e.getTargetUtilization() *
		float64(int64(runtime.GOMAXPROCS(0))*time.Second.Nanoseconds())
	return e.getTargetUtilization() *
		float64(int64(totalElasticCPUTime)-availableElasticCPUTime.Nanoseconds()) / totalElasticCPUTime
}
