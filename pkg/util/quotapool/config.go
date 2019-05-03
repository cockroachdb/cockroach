// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package quotapool

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Option is used to configure a QuotaPool.
type Option interface {
	apply(*config)
}

// OnSlowAcquisition creates an Option to configure a callback upon slow
// acquisitions. Only one OnSlowAcquisition may be used. If multiple are
// specified only the last will be used.
func OnSlowAcquisition(threshold time.Duration, f SlowAcquisitionFunc) Option {
	return optionFunc(func(cfg *config) {
		cfg.slowAcquisitionThreshold = threshold
		cfg.onSlowAcquisition = f
	})
}

// LogSlowAcquisition is an Option to log slow acquisitions.
var LogSlowAcquisition = OnSlowAcquisition(base.SlowRequestThreshold, logSlowAcquire)

func logSlowAcquire(ctx context.Context, poolName string, r Request, start time.Time) func() {
	log.Warningf(ctx, "have been waiting %s attempting to acquire %s quota",
		timeutil.Since(start), poolName)
	return func() {
		log.Infof(ctx, "acquired %s quota after %s",
			poolName, timeutil.Since(start))
	}
}

// SlowAcquisitionFunc is used to configure a quotapool to call a function when
// quota acquisition is slow. The returned callback is called when the
// acquisition occurs.
type SlowAcquisitionFunc func(
	ctx context.Context, poolName string, r Request, start time.Time,
) (onAcquire func())

type optionFunc func(cfg *config)

func (f optionFunc) apply(cfg *config) { f(cfg) }

type config struct {
	name                     string
	onSlowAcquisition        SlowAcquisitionFunc
	slowAcquisitionThreshold time.Duration
}
