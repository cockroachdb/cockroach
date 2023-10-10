// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build go1.21 && bazel

package ctxutil

import "context"

// A canceler is a context type that can be canceled directly. The
// implementations are *cancelCtx and *timerCtx.
// This interface definition is applicable to go1.20 or higher
type canceler interface {
	cancel(removeFromParent bool, err, cause error)
	Done() <-chan struct{}
}

func (c *whenDone) cancel(removeFromParent bool, err, cause error) {
	cx := (&c.ExportedCancelCtx).(canceler)
	cx.cancel(removeFromParent, err, cause)
	c.notify()
}

type whenDone struct {
	context.ExportedCancelCtx
	notify WhenDoneFunc
}

func makeWhenDone(parent context.Context, done WhenDoneFunc) {
	c := &whenDone{notify: done}
	c.ExportedPropagateCancel(parent, c)
}
