// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !race
// +build !race

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
