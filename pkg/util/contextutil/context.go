// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License.

package contextutil

import (
	"runtime/debug"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// WithCancel adds an info log to context.WithCancel's CancelFunc.
func WithCancel(parent context.Context) (context.Context, context.CancelFunc) {
	return wrap(context.WithCancel(parent))
}

// WithDeadline adds an info log to context.WithDeadline's CancelFunc.
func WithDeadline(
	parent context.Context, deadline time.Time,
) (context.Context, context.CancelFunc) {
	return wrap(context.WithDeadline(parent, deadline))
}

// WithTimeout adds an info log to context.WithTimeout's CancelFunc.
func WithTimeout(
	parent context.Context, timeout time.Duration,
) (context.Context, context.CancelFunc) {
	return wrap(context.WithTimeout(parent, timeout))
}

func wrap(ctx context.Context, cancel context.CancelFunc) (context.Context, context.CancelFunc) {
	return ctx, func() {
		log.InfofDepth(ctx, 1, "canceling context")
		if log.V(1) {
			debug.PrintStack()
		}
		cancel()
	}
}
