// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package quotapool

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Option is used to configure a QuotaPool.
type Option interface {
	apply(*config)
}

// AcquisitionFunc is used to configure a quotapool to call a function after
// an acquisition has occurred.
type AcquisitionFunc func(
	ctx context.Context, poolName string, r Request, start time.Time,
)

// OnAcquisition creates an Option to configure a callback upon acquisition.
// It is often useful for recording metrics.
func OnAcquisition(f AcquisitionFunc) Option {
	return optionFunc(func(cfg *config) {
		cfg.onAcquisition = f
	})
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

// LogSlowAcquisition is a SlowAcquisitionFunc.
func LogSlowAcquisition(ctx context.Context, poolName string, r Request, start time.Time) func() {
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

// WithTimeSource is used to configure a quotapool to use the provided
// TimeSource.
func WithTimeSource(ts TimeSource) Option {
	return optionFunc(func(cfg *config) {
		cfg.timeSource = ts
	})
}

type config struct {
	onAcquisition            AcquisitionFunc
	onSlowAcquisition        SlowAcquisitionFunc
	slowAcquisitionThreshold time.Duration
	timeSource               TimeSource
}

var defaultConfig = config{
	timeSource: defaultTimeSource{},
}

func initializeConfig(cfg *config, options ...Option) {
	*cfg = defaultConfig
	for _, opt := range options {
		opt.apply(cfg)
	}
}
