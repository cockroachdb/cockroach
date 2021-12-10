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
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// Option configures a RangeFeed.
type Option interface {
	set(*config)
}

type config struct {
	retryOptions       retry.Options
	withInitialScan    bool
	initialScanHandler InitialScanHandler
	withDiff           bool
}

type optionFunc func(*config)

func (o optionFunc) set(c *config) { o(c) }

// WithInitialScan enables an initial scan of the data in the span. The rows of
// an initial scan will be passed to the value function used to construct the
// RangeFeed. Upon completion of the initial scan, the passed function (if
// non-nil) will be called. The initial scan may be restarted and thus rows
// may be observed multiple times. The caller cannot rely on rows being returned
// in order.
func WithInitialScan(initialScanHandler InitialScanHandler) Option {
	return optionFunc(func(c *config) {
		c.withInitialScan = true
		c.initialScanHandler = initialScanHandler
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

func initConfig(c *config, options []Option) {
	*c = config{} // the default config is its zero value
	for _, o := range options {
		o.set(c)
	}
}
