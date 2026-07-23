// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build go1.21 && bazel

package ctxutil

import (
	"context"

	"github.com/cockroachdb/errors"
)

// Linkage definition for go1.21 or higher built with ./dev toolchain --
// that is, a toolchain that applies cockroach runtime patches.

// propagateCancel invokes notify when parent context completes.
// Since this code was built with ./dev, use patched context.Context to access
// needed functionality.
func propagateCancel(parent context.Context, notify WhenDoneFunc) {
	if !context.PropagateCancel(parent, notify) {
		// This shouldn't happen since WhenDone checks to make sure parent is cancellable.
		panic(errors.Newf("parent context expected to be cancellable, found %T", parent))
	}
}
