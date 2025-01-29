// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !race

package tracing

import "runtime"

// setFinalizer registers a finalizer to run when the Span is garbage collected.
// Passing nil clears any previously registered finalizer.
func (sp *Span) setFinalizer(fn func(sp *Span)) {
	if fn == nil {
		// Avoid typed nil.
		runtime.SetFinalizer(sp, nil)
		return
	}
	runtime.SetFinalizer(sp, fn)
}
