// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build go1.20 && !go1.21

package ctxutil

import (
	"context"
	// Must import unsafe to enable linkname below.
	_ "unsafe"
)

// A canceler is a context type that can be canceled directly. The
// implementations are *cancelCtx and *timerCtx.
// This interface definition is applicable to go1.20 or higher
type canceler interface {
	cancel(removeFromParent bool, err, cause error)
	Done() <-chan struct{}
}

func (c *whenDone) cancel(removeFromParent bool, err, cause error) {
	if removeFromParent {
		context_removeChild(c.Context, c)
	}
	c.notify()
}

type whenDone struct {
	context.Context
	notify WhenDoneFunc
}

func makeWhenDone(parent context.Context, done WhenDoneFunc) {
	c := &whenDone{Context: parent, notify: done}
	context_propagateCancel(parent, c)
}

//go:linkname context_removeChild context.removeChild
func context_removeChild(parent context.Context, child canceler)

//go:linkname context_propagateCancel context.propagateCancel
func context_propagateCancel(parent context.Context, child canceler)
