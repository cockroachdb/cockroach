// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package contextutil

import (
	"context"
	"time"
)

// uncanceledContext is an implementation of context.Context that ignores the
// cancellation of the parent.
type uncanceledContext struct {
	context.Context
}

var _ context.Context = &uncanceledContext{}

// Done overrides the Context method, clearing any cancellation channel. Always return nil.
func (*uncanceledContext) Done() <-chan struct{} {
	return nil
}

// Err overrides the Context method. Always returns nil
func (*uncanceledContext) Err() error {
	return nil
}

// Deadline overrides the Context method. Always returns false.
func (*uncanceledContext) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}

// WithoutCancel returns a context that doesn't inherit the cancellation of its
// parent, and so it can never be canceled.
func WithoutCancel(ctx context.Context) context.Context {
	return &uncanceledContext{Context: ctx}
}
