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
	"github.com/cockroachdb/redact"
)

// Option is used to configure a quotapool.
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

// OnWaitStartFunc is the prototype for functions called to notify the start or
// finish of a waiting period when a request is blocked.
type OnWaitStartFunc func(
	ctx context.Context, poolName string, r Request,
)

// OnWaitStart creates an Option to configure a callback which is called when a
// request blocks and has to wait for quota.
func OnWaitStart(onStart OnWaitStartFunc) Option {
	return optionFunc(func(cfg *config) {
		cfg.onWaitStart = onStart
	})
}

// OnWaitFinish creates an Option to configure a callback which is called when a
// previously blocked request acquires resources.
func OnWaitFinish(onFinish AcquisitionFunc) Option {
	return optionFunc(func(cfg *config) {
		cfg.onWaitFinish = onFinish
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
		timeutil.Since(start), redact.Safe(poolName))
	return func() {
		log.Infof(ctx, "acquired %s quota after %s",
			redact.Safe(poolName), timeutil.Since(start))
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
func WithTimeSource(ts timeutil.TimeSource) Option {
	return optionFunc(func(cfg *config) {
		cfg.timeSource = ts
	})
}

// WithCloser allows the client to provide a channel which will lead to the
// AbstractPool being closed.
func WithCloser(closer <-chan struct{}) Option {
	return optionFunc(func(cfg *config) {
		cfg.closer = closer
	})
}

// WithMinimumWait is used with the RateLimiter to control the minimum duration
// which a goroutine will sleep waiting for quota to accumulate. This
// can help avoid expensive spinning when the workload consists of many
// small acquisitions. If used with a regular (not rate limiting) quotapool,
// this option has no effect.
func WithMinimumWait(duration time.Duration) Option {
	return optionFunc(func(cfg *config) {
		cfg.minimumWait = duration
	})
}

type config struct {
	onAcquisition            AcquisitionFunc
	onSlowAcquisition        SlowAcquisitionFunc
	onWaitStart              OnWaitStartFunc
	onWaitFinish             AcquisitionFunc
	slowAcquisitionThreshold time.Duration
	timeSource               timeutil.TimeSource
	closer                   <-chan struct{}
	minimumWait              time.Duration
}

var defaultConfig = config{
	timeSource: timeutil.DefaultTimeSource{},
}

func initializeConfig(cfg *config, options ...Option) {
	*cfg = defaultConfig
	for _, opt := range options {
		opt.apply(cfg)
	}
}
