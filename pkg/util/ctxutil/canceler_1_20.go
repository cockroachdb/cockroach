// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build go1.20 && !go1.21

package ctxutil

import (
	"context"
	_ "unsafe"
)

// propagateCancel arranges for f to be invoked when parent completes.
func propagateCancel(parent context.Context, f WhenDoneFunc) {
	child := whenDone{Context: parent, notify: f}
	context_propagateCancel(parent, &child)
}

// A canceler is a context type that can be canceled directly. The
// implementations are *cancelCtx and *timerCtx.
// This interface definition is applicable to go1.20 or higher
type canceler interface {
	cancel(removeFromParent bool, err, cause error)
	Done() <-chan struct{}
}

// whenDone is an adopter that implements canceler interface.
type whenDone struct {
	context.Context
	notify WhenDoneFunc
}

func (c *whenDone) cancel(removeFromParent bool, err, cause error) {
	if removeFromParent {
		context_removeChild(c.Context, c)
	}
	c.notify()
}

//go:linkname context_removeChild context.removeChild
func context_removeChild(parent context.Context, child canceler)

//go:linkname context_propagateCancel context.propagateCancel
func context_propagateCancel(parent context.Context, child canceler)
