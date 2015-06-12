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

import (
	"fmt"
	"time"

	"github.com/cenkalti/backoff"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// MaxAttemptsError indicates max attempts were exceeded.
type MaxAttemptsError struct {
	MaxAttempts uint
}

// Error implements error interface.
func (re *MaxAttemptsError) Error() string {
	return fmt.Sprintf("maximum number of attempts exceeded %d", re.MaxAttempts)
}

// Retry implements the methods which control execution of retry loops. See
// Reset, Stop below.
type Retry struct {
	// TODO(tamird): use backoff.BackOff here (it's an interface)
	backOff backoff.ExponentialBackOff
	stopped bool // Used for explicit stopping
	count   uint
}

// Reset resets the exponential backoff and causes an immediate retry.
func (r *Retry) Reset() {
	r.count = 0
	r.backOff.Reset()
}

// Stop causes the retry loop to stop on the next iteration.
func (r *Retry) Stop() {
	r.stopped = true
}

// Options provides control of retry loop logic via the
// WithBackoffOptions method.
type Options struct {
	// TODO(tamird): use backoff.BackOff here (it's an interface)
	BackOff     backoff.ExponentialBackOff
	Tag         string        // Tag for helpful logging of backoffs
	MaxAttempts uint          // Maximum number of attempts (0 for infinite)
	UseV1Info   bool          // Use verbose V(1) level for log messages
	Stopper     *util.Stopper // Optionally end retry loop on stopper signal
}

// WithBackoff implements retry with exponential backoff using the supplied
// options as parameters. When fn returns non-nil and the number of retry
// attempts haven't been exhausted, fn is retried. When fn returns nil or
// Stop() is called, retry ends. As a special case, if Reset is called, the
// backoff and retry count are reset to starting values and the next retry
// occurs immediately. Returns an error if the maximum number of retries is
// exceeded or if fn returns an error on the last attempt.
func WithBackoff(opts Options, fn func(*Retry) error) error {
	r := Retry{opts.BackOff, false, 0}
	tag := opts.Tag
	if tag == "" {
		tag = "invocation"
	}

	var err error
	var next time.Duration

	r.Reset()
	for r.count = 1; ; r.count++ {
		if err = fn(&r); err == nil {
			return nil
		}

		if r.stopped {
			return err
		}

		if !opts.UseV1Info || log.V(1) {
			log.InfoDepth(1, tag, " failed an iteration: ", err)
		}

		// We've been Reset, retry immediately
		if r.count == 0 {
			if !opts.UseV1Info || log.V(1) {
				log.InfoDepth(1, tag, " failed; retrying immediately")
			}
			continue
		}

		if opts.MaxAttempts > 0 && r.count >= opts.MaxAttempts {
			return &MaxAttemptsError{opts.MaxAttempts}
		}

		if next = r.backOff.NextBackOff(); next == backoff.Stop {
			return err
		}

		if !opts.UseV1Info || log.V(1) {
			log.InfoDepth(1, tag, " failed; retrying in ", next)
		}

		// Wait before retry.
		select {
		case <-time.After(next):
			// Continue retrying.
		case <-opts.Stopper.ShouldStop():
			return util.Errorf("%s retry loop stopped", tag)
		}
	}
}
