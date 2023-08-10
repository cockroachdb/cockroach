// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build go1.20

package ctxutil

import (
	"context"
	_ "unsafe" // Must import unsafe to enable gross hack below.
)

// WhenDoneCause is the same as WhenDone, but accepts WhenDoneCauseFunc as a callback.
func WhenDoneCause(parent context.Context, done WhenDoneCauseFunc) bool {
	if parent.Done() == nil {
		return false
	}
	c := &whenDone{Context: parent, notify: done}
	context_propagateCancel(parent, c)
	return true
}

// Gross hack to access internal context function.  Sometimes, go makes things
// SO much more difficult than it needs to.

// A canceler is a context type that can be canceled directly. The
// implementations are *cancelCtx and *timerCtx.
type canceler interface {
	cancel(removeFromParent bool, err, cause error)
	Done() <-chan struct{}
}

//go:linkname context_propagateCancel context.propagateCancel
func context_propagateCancel(parent context.Context, child canceler)

//go:linkname context_removeChild context.removeChild
func context_removeChild(parent context.Context, child canceler)

func (c *whenDone) cancel(removeFromParent bool, err, cause error) {
	c.cancelWithCause(removeFromParent, err, cause)
}
