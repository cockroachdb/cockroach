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

	"github.com/cockroachdb/errors"
)

// WhenDoneFunc is the callback invoked by context when it becomes done.
// The callback is passed the error from the parent context.
type WhenDoneFunc func(err error)

// WhenDoneCauseFunc accepts context error (context.Err()) as well
// as the cause for cancellation (cause is nil prior to go1.20).
type WhenDoneCauseFunc func(err, cause error)

// ErrNeverCompletes is an error indicating that the context never completes.
var ErrNeverCompletes = errors.New("context never completes")

// WhenDone arranges for the specified function to be invoked when
// parent context becomes done.
// If the context never becomes done, returns ErrNeverCompletes error.
func WhenDone(parent context.Context, done WhenDoneFunc) error {
	if parent.Done() == nil {
		return ErrNeverCompletes
	}
	c := &whenDone{Context: parent, notify: func(err, cause error) { done(err) }}
	context_propagateCancel(parent, c)
	return nil
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
