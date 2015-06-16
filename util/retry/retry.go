// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package retry

//go:generate stringer -type=Status

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// retryJitter specifies random jitter to add to backoff
// durations. Specified as a percentage of the backoff.
const retryJitter = 0.15

// Status is an enum describing the possible statuses of a
// backoff / retry worker function.
type Status int32

// MaxAttemptsError indicates max attempts were exceeded.
type MaxAttemptsError struct {
	MaxAttempts int
}

// Error implements error interface.
func (re *MaxAttemptsError) Error() string {
	return fmt.Sprintf("maximum number of attempts exceeded %d", re.MaxAttempts)
}

const (
	// Succeed indicates the retry loop has succeeded and should return nil.
	Succeed Status = iota
	// Abort indicates the retry loop has failed and should return the error
	// return from the retry worker function.
	Abort
	// Reset indicates that the retry loop should be reset with
	// no backoff for an immediate retry.
	Reset
	// Continue indicates that the retry loop should continue with
	// another iteration of backoff / retry.
	Continue
)

// Options provides control of retry loop logic via the
// WithBackoffOptions method.
type Options struct {
	Tag         string        // Tag for helpful logging of backoffs
	Backoff     time.Duration // Default retry backoff interval
	MaxBackoff  time.Duration // Maximum retry backoff interval
	Constant    float64       // Default backoff constant
	MaxAttempts int           // Maximum number of attempts (0 for infinite)
	UseV1Info   bool          // Use verbose V(1) level for log messages
	Stopper     *util.Stopper // Optionally end retry loop on stopper signal
}

// WithBackoff implements retry with exponential backoff using the supplied
// options as parameters. When fn returns Continue and the number of retry
// attempts haven't been exhausted, fn is retried. When fn returns Abort or
// Succeed, retry ends. As a special case, if fn returns Reset, the backoff
// and retry count are reset to starting values and the next retry occurs
// immediately. Returns an error if the maximum number of retries is exceeded
// or if the fn returns an error.
func WithBackoff(opts Options, fn func() (Status, error)) error {
	backoff := opts.Backoff
	tag := opts.Tag
	if tag == "" {
		tag = "invocation"
	}
	for count := 1; true; count++ {
		status, err := fn()
		if status == Succeed {
			if err == nil {
				return nil
			}
			panic(fmt.Sprintf("%s passed with a non-nil error: %s", status, err))
		} else {
			if err == nil {
				panic(fmt.Sprintf("%s passed with a nil error", status))
			}
		}

		if status == Abort {
			return err
		}

		if err != nil && (!opts.UseV1Info || log.V(1) == true) {
			log.InfoDepth(1, tag, " failed an iteration: ", err)
		}
		var wait time.Duration
		if status == Reset {
			backoff = opts.Backoff
			wait = 0
			count = 0
			if !opts.UseV1Info || log.V(1) == true {
				log.InfoDepth(1, tag, " failed; retrying immediately")
			}
		} else {
			if opts.MaxAttempts > 0 && count >= opts.MaxAttempts {
				return &MaxAttemptsError{opts.MaxAttempts}
			}
			if !opts.UseV1Info || log.V(1) == true {
				log.InfoDepth(1, tag, " failed; retrying in ", backoff)
			}
			wait = backoff + time.Duration(rand.Float64()*float64(backoff.Nanoseconds())*retryJitter)
			// Increase backoff for next iteration.
			backoff = time.Duration(float64(backoff) * opts.Constant)
			if backoff > opts.MaxBackoff {
				backoff = opts.MaxBackoff
			}
		}
		// Wait before retry.
		select {
		case <-time.After(wait):
			// Continue retrying.
		case <-opts.Stopper.ShouldStop():
			return util.Errorf("%s retry loop stopped", tag)
		}
	}
	return nil
}
