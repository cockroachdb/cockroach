// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build go1.21 && !bazel

package ctxutil

import "context"

// Linkage definition for go1.21 or higher built outside ./dev toolchain --
// that is, a toolchain that did not apply cockroach runtime patches.

// Since this code was built outside ./dev (i.e. "!bazel" tag defined),
// we cannot use patched context implementation.
// Instead, fallback to spinning up goroutine to detect parent cancellation.
func propagateCancel(parent context.Context, notify WhenDoneFunc) {
	done := parent.Done()
	if done == nil {
		panic("unexpected non-cancelable context")
	}
	go func() {
		<-done
		notify()
	}()
}
