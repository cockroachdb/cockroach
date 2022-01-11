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
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// Option configures a RangeFeed.
type Option interface {
	set(*config)
}

type config struct {
	scanConfig
	retryOptions         retry.Options
	onInitialScanDone    OnInitialScanDone
	withInitialScan      bool
	withDiff             bool
	onInitialScanError   OnInitialScanError
	onUnrecoverableError OnUnrecoverableError
	onCheckpoint         OnCheckpoint
	onFrontierAdvance    OnFrontierAdvance
	extraPProfLabels     []string
}

type scanConfig struct {
	// scanParallelism controls the number of concurrent scan requests
	// that can be issued.  If unspecified, only 1 scan request at a time is issued.
	scanParallelism func() int

	// targetScanBytes requests that many bytes to be returned per scan request.
	// adjusting this setting should almost always be used together with the setting
	// to configure memory monitor.
	targetScanBytes int64

	// mon is the memory monitor to while scanning.
	mon *mon.BytesMonitor

	// callback to invoke when initial scan of a span completed.
	onSpanDone OnScanCompleted

	// configures retry behavior
	retryBehavior ScanRetryBehavior
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

// WithDiff makes an option to request that rangefeed events carry the previous
// value in addition to the new value. The option defaults to false. If set,
// initial scan events will carry the same value for both Value and PrevValue.
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

	if c.targetScanBytes == 0 {
		c.targetScanBytes = 1 << 19 // 512 KiB
	}
}

// WithInitialScanParallelismFn configures rangefeed to issue up to specified number
// of concurrent initial scan requests.
func WithInitialScanParallelismFn(parallelismFn func() int) Option {
	return optionFunc(func(c *config) {
		c.scanParallelism = parallelismFn
	})
}

// WithTargetScanBytes configures rangefeed to request specified number of bytes per scan request.
// This option should be used together with the option to configure memory monitor.
func WithTargetScanBytes(target int64) Option {
	return optionFunc(func(c *config) {
		c.targetScanBytes = target
	})
}

// WithMemoryMonitor configures rangefeed to use memory monitor when issuing scan requests.
func WithMemoryMonitor(mon *mon.BytesMonitor) Option {
	return optionFunc(func(c *config) {
		c.mon = mon
	})
}

// OnScanCompleted is called when the rangefeed initial scan completes scanning
// the span.  An error returned from this function is handled based on the WithOnInitialScanError
// option.  If the error handler is not set, the scan is retried based on the
// WithSAcanRetryBehavior option.
type OnScanCompleted func(ctx context.Context, sp roachpb.Span) error

// WithOnScanCompleted sets up a callback which is invoked when a span (or part of the span)
// have been completed when performing an initial scan.
func WithOnScanCompleted(fn OnScanCompleted) Option {
	return optionFunc(func(c *config) {
		c.onSpanDone = fn
	})
}

// ScanRetryBehavior specifies how rangefeed should handle errors during initial scan.
type ScanRetryBehavior int

const (
	// ScanRetryAll will retry all spans if any error occurred during initial scan.
	ScanRetryAll ScanRetryBehavior = iota
	// ScanRetryRemaining will retry remaining spans, including the one that failed.
	ScanRetryRemaining
)

// WithScanRetryBehavior configures range feed to retry initial scan as per specified behavior.
func WithScanRetryBehavior(b ScanRetryBehavior) Option {
	return optionFunc(func(c *config) {
		c.retryBehavior = b
	})
}

// WithPProfLabel configures rangefeed to annotate go routines started by range feed
// with the specified key=value label.
func WithPProfLabel(key, value string) Option {
	return optionFunc(func(c *config) {
		c.extraPProfLabels = append(c.extraPProfLabels, key, value)
	})
}
