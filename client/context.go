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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package client

import (
	"time"

	"github.com/cockroachdb/cockroach/util"
)

// A SystemClock is an implementation of the Clock interface that
// returns the node's wall time.
// It is used by default when creating a new Context.
type SystemClock struct{}

// Now implements the Clock interface, returning the node's wall time.
func (*SystemClock) Now() int64 {
	return time.Now().UnixNano()
}

// Context defaults.
var (
	// DefaultTxnRetryOptions are the standard retry options used
	// for transactions.
	// This is exported for testing purposes only.
	DefaultTxnRetryOptions = util.RetryOptions{
		Backoff:     50 * time.Millisecond,
		MaxBackoff:  5 * time.Second,
		Constant:    2,
		MaxAttempts: 0, // retry indefinitely
	}
	defaultClock = Clock(&SystemClock{})
)

// A Context stores configuration to be used when creating a KV object.
type Context struct {
	User            string
	UserPriority    int32
	TxnRetryOptions util.RetryOptions
	Clock           Clock
}

// NewContext creates a new context with default values.
func NewContext() *Context {
	return &Context{
		Clock:           defaultClock,
		TxnRetryOptions: DefaultTxnRetryOptions,
	}
}
