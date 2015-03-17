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

package util

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/util/log"
)

// retryJitter specifies random jitter to add to backoff
// durations. Specified as a percentage of the backoff.
const retryJitter = 0.15

// RetryStatus is an enum describing the possible statuses of a
// backoff / retry worker function.
type RetryStatus int32

// RetryMaxAttemptsError indicates max attempts were exceeded.
type RetryMaxAttemptsError struct {
	MaxAttempts int
}

// Error implements error interface.
func (re *RetryMaxAttemptsError) Error() string {
	return fmt.Sprintf("maximum number of attempts exceeded %d", re.MaxAttempts)
}

const (
	// RetryBreak indicates the retry loop is finished and should return
	// the result of the retry worker function.
	RetryBreak RetryStatus = iota
	// RetryReset indicates that the retry loop should be reset with
	// no backoff for an immediate retry.
	RetryReset
	// RetryContinue indicates that the retry loop should continue with
	// another iteration of backoff / retry.
	RetryContinue
)

// RetryOptions provides control of retry loop logic via the
// RetryWithBackoffOptions method.
type RetryOptions struct {
	Tag         string        // Tag for helpful logging of backoffs
	Backoff     time.Duration // Default retry backoff interval
	MaxBackoff  time.Duration // Maximum retry backoff interval
	Constant    float64       // Default backoff constant
	MaxAttempts int           // Maximum number of attempts (0 for infinite)
	UseV1Info   bool          // Use verbose V(1) level for log messages
}

// RetryWithBackoff implements retry with exponential backoff using
// the supplied options as parameters. When fn returns RetryContinue
// and the number of retry attempts haven't been exhausted, fn is
// retried. When fn returns RetryBreak, retry ends. As a special case,
// if fn returns RetryReset, the backoff and retry count are reset to
// starting values and the next retry occurs immediately. Returns an
// error if the maximum number of retries is exceeded or if the fn
// returns an error.
func RetryWithBackoff(opts RetryOptions, fn func() (RetryStatus, error)) error {
	backoff := opts.Backoff
	for count := 1; true; count++ {
		status, err := fn()
		if status == RetryBreak {
			return err
		}
		if err != nil && (!opts.UseV1Info || log.V(1) == true) {
			log.Infof("%s failed an iteration: %s", opts.Tag, err)
		}
		var wait time.Duration
		if status == RetryReset {
			backoff = opts.Backoff
			wait = 0
			count = 0
			if !opts.UseV1Info || log.V(1) == true {
				log.Infof("%s failed; retrying immediately", opts.Tag)
			}
		} else {
			if opts.MaxAttempts > 0 && count >= opts.MaxAttempts {
				return &RetryMaxAttemptsError{opts.MaxAttempts}
			}
			if !opts.UseV1Info || log.V(1) == true {
				log.Infof("%s failed; retrying in %s", opts.Tag, backoff)
			}
			wait = backoff + time.Duration(rand.Float64()*float64(backoff.Nanoseconds())*retryJitter)
			// Increase backoff for next iteration.
			backoff = time.Duration(float64(backoff) * opts.Constant)
			if backoff > opts.MaxBackoff {
				backoff = opts.MaxBackoff
			}
		}
		// Wait before retry.
		time.Sleep(wait)
	}
	return nil
}
