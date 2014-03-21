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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package util

import "time"

// Options provides control of retry loop logic via the
// RetryWithBackoffOptions method.
type Options struct {
	Backoff     time.Duration // Default retry backoff interval
	MaxBackoff  time.Duration // Maximum retry backoff interval
	Constant    float64       // Default backoff constant
	MaxAttempts int           // Maximum number of attempts (0 for infinite)
}

// RetryWithBackoff is uses default retry constants to implement an
// exponential backoff. When fn returns true, retry ends. Returns
// an error if the maximum number of retries is exceeded.
func RetryWithBackoff(fn func() bool) error {
	defaultOpts := Options{
		Backoff:     time.Millisecond * 10,
		MaxBackoff:  time.Minute,
		Constant:    2,
		MaxAttempts: 10,
	}
	return RetryWithBackoffOptions(defaultOpts, fn)
}

// RetryWithBackoffOptions implements retry with exponential backoff
// using the supplied options as parameters.
func RetryWithBackoffOptions(opts Options, fn func() bool) error {
	backoff := opts.Backoff
	for count := 1; true; count++ {
		if fn() {
			return nil
		}
		if opts.MaxAttempts > 0 && count >= opts.MaxAttempts {
			return Errorf("exceeded maximum retry attempts: %d", opts.MaxAttempts)
		}
		select {
		case <-time.After(backoff):
			// Increase backoff.
			backoff = time.Duration(float64(backoff) * opts.Constant)
			if backoff > opts.MaxBackoff {
				backoff = opts.MaxBackoff
			}
		}
	}
	return nil
}
