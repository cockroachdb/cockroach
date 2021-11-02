// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// Option configures a RangeFeed.
type Option interface {
	set(*config)
}

type config struct {
	retryOptions           retry.Options
	onInitialScanDone      OnInitialScanDone
	withInitialScan        bool
	initialScanParallelism func() int
	withDiff               bool
	onInitialScanError     OnInitialScanError
	onUnrecoverableError   OnUnrecoverableError
	onCheckpoint           OnCheckpoint
	onFrontierAdvance      OnFrontierAdvance
}

type optionFunc func(*config)

func (o optionFunc) set(c *config) { o(c) }

// OnInitialScanDone is called when an initial scan is finished before any rows
// from the rangefeed are supplied.
type OnInitialScanDone func(ctx context.Context)

// OnInitialScanError is called when an initial scan encounters an error. It
// allows the caller to tell the RangeFeed to stop as opposed to retrying
// endlessly.
type OnInitialScanError func(ctx context.Context, err error) (shouldFail bool)

// OnUnrecoverableError is called when the rangefeed exits with an unrecoverable
// error (preventing internal retries). One example is when the rangefeed falls
// behind to a point where the frontier timestamp precedes the GC threshold, and
// thus will never work. The callback lets callers find out about such errors
// (possibly, in our example, to start a new rangefeed with an initial scan).
type OnUnrecoverableError func(ctx context.Context, err error)

// WithInitialScan enables an initial scan of the data in the span. The rows of
// an initial scan will be passed to the value function used to construct the
// RangeFeed. Upon completion of the initial scan, the passed function (if
// non-nil) will be called. The initial scan may be restarted and thus rows
// may be observed multiple times. The caller cannot rely on rows being returned
// in order.
func WithInitialScan(f OnInitialScanDone) Option {
	return optionFunc(func(c *config) {
		c.withInitialScan = true
		c.onInitialScanDone = f
	})
}

// WithOnInitialScanError sets up a callback to report errors during the initial
// scan to the caller. The caller may instruct the RangeFeed to halt rather than
// retrying endlessly. This option will not enable an initial scan; it must be
// used in conjunction with WithInitialScan to have any effect.
func WithOnInitialScanError(f OnInitialScanError) Option {
	return optionFunc(func(c *config) {
		c.onInitialScanError = f
	})
}

// WithOnInternalError sets up a callback to report unrecoverable errors during
// operation.
func WithOnInternalError(f OnUnrecoverableError) Option {
	return optionFunc(func(c *config) {
		c.onUnrecoverableError = f
	})
}

// WithDiff makes an option to enable an initial scan which defaults to
// false.
func WithDiff() Option {
	return optionFunc(func(c *config) {
		c.withDiff = true
	})
}

// WithRetry configures the retry options for the rangefeed.
func WithRetry(options retry.Options) Option {
	return optionFunc(func(c *config) {
		c.retryOptions = options
	})
}

// OnCheckpoint is called when a rangefeed checkpoint occurs.
type OnCheckpoint func(ctx context.Context, checkpoint *roachpb.RangeFeedCheckpoint)

// WithOnCheckpoint sets up a callback that's invoked whenever a check point
// event is emitted.
func WithOnCheckpoint(f OnCheckpoint) Option {
	return optionFunc(func(c *config) {
		c.onCheckpoint = f
	})
}

// OnFrontierAdvance is called when the rangefeed frontier is advanced with the
// new frontier timestamp.
type OnFrontierAdvance func(ctx context.Context, timestamp hlc.Timestamp)

// WithOnFrontierAdvance sets up a callback that's invoked whenever the
// rangefeed frontier is advanced.
func WithOnFrontierAdvance(f OnFrontierAdvance) Option {
	return optionFunc(func(c *config) {
		c.onFrontierAdvance = f
	})
}

func initConfig(c *config, options []Option) {
	*c = config{} // the default config is its zero value
	for _, o := range options {
		o.set(c)
	}
}

// WithInitialScanParallelismFn configures rangefeed to issue up to specified number
// of concurrent initial scan requests.
func WithInitialScanParallelismFn(parallelismFn func() int) Option {
	return optionFunc(func(c *config) {
		c.initialScanParallelism = parallelismFn
	})
}

// DefaultInitialScanParallelismFn configures rangefeed to issue 1 scan request
// at a time.
var DefaultInitialScanParallelismFn func() int
