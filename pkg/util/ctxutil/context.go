// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ctxutil

import (
	"context"
	_ "unsafe" // Must import unsafe to enable linkname.

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// WhenDoneFunc is the callback invoked by context when it becomes done.
// The callback is passed the error from the parent context.
type WhenDoneFunc func(err error)

// WhenDoneCauseFunc accepts context error (context.Err()) as well
// as the cause for cancellation (cause is nil prior to go1.20).
type WhenDoneCauseFunc func(err, cause error)

// WhenDone arranges for the specified function to be invoked when
// parent context becomes done and returns true.
// If the context is non-cancellable (i.e. `Done() == nil`), returns false and
// never calls the function.
// If the parent context is derived from context.WithCancel or
// context.WithTimeout/Deadline, then no additional goroutines are created.
// Otherwise, a goroutine is spun up by context.Context to detect
// parent cancellation.
func WhenDone(parent context.Context, done WhenDoneFunc) bool {
	if parent.Done() == nil {
		return false
	}

	// All contexts that complete (ctx.Done() != nil) used in cockroach should
	// support direct cancellation detection, since they should be derived from
	// one of the standard context.Context.
	// But, be safe and loudly fail tests in case somebody introduces strange
	// context implementation.
	if buildutil.CrdbTestBuild && !CanDirectlyDetectCancellation(parent) {
		log.Fatalf(parent, "expected context that supports direct cancellation detection, found %T", parent)
	}

	c := &whenDone{Context: parent, notify: func(err, cause error) { done(err) }}
	context_propagateCancel(parent, c)
	return true
}

// CanDirectlyDetectCancellation checks to make sure that the parent
// context can be used to detect parent cancellation without the need
// to spin up goroutine.
// That would mean that the parent context is derived from
// context.WithCancel or context.WithTimeout/Deadline.
// Even if parent is not derived from one of the above contexts (i.e. it
// is a custom implementation), WhenDone function can still be used; it just
// means that there will be an additional goroutine spun up.  As such,
// this function is meant to be used in test environment only.
func CanDirectlyDetectCancellation(parent context.Context) bool {
	// context.parentCancelCtx would have been preferred mechanism to check
	// if the cancellation can be propagated; alas, this function returns
	// an unexported *cancelCtx, which we do not have access to.
	// So, instead try to do what that method essentially does by
	// getting access to internal cancelCtxKey.
	cancellable, ok := parent.Value(&context_cancelCtxKey).(context.Context)
	return ok && cancellable.Done() == parent.Done()
}

type whenDone struct {
	context.Context
	notify WhenDoneCauseFunc
}

func (c *whenDone) cancelWithCause(removeFromParent bool, err, cause error) {
	c.notify(err, cause)
	if removeFromParent {
		context_removeChild(c.Context, c)
	}
}

//go:linkname context_cancelCtxKey context.cancelCtxKey
var context_cancelCtxKey int
