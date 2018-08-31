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
	"context"
	"runtime/debug"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

type reasonCancelCtx struct {
	context.Context

	mu struct {
		syncutil.Mutex
		reason error
	}
}

func (c *reasonCancelCtx) Err() error {
	var reason error
	c.mu.Lock()
	reason = c.mu.reason
	c.mu.Unlock()
	return errors.Wrap(reason, c.Context.Err().Error())
}

// WithCancel adds an info log to context.WithCancel's CancelFunc.
func WithCancel(parent context.Context) (context.Context, context.CancelFunc) {
	return wrap(context.WithCancel(parent))
}

// CancelWithReason cancels a context as normal, annotating it with a reason for
// cancellation given in err if the context was created with
// contextutil.WithCancel.
func CancelWithReason(ctx context.Context, cancelFunc context.CancelFunc, err error) {
	if reasonCancelCtx, ok := ctx.(*reasonCancelCtx); ok {
		reasonCancelCtx.mu.Lock()
		reasonCancelCtx.mu.reason = err
		reasonCancelCtx.mu.Unlock()
	}
	cancelFunc()
}

func wrap(ctx context.Context, cancel context.CancelFunc) (context.Context, context.CancelFunc) {
	ctx = &reasonCancelCtx{Context: ctx}
	if !log.V(1) {
		return ctx, cancel
	}
	return ctx, func() {
		if log.V(2) {
			log.InfofDepth(ctx, 1, "canceling context:\n%s", debug.Stack())
		} else if log.V(1) {
			log.InfofDepth(ctx, 1, "canceling context")
		}
		cancel()
	}
}
