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

import (
	"context"
	_ "unsafe" // Must import unsafe to enable gross hack below.

	"github.com/cockroachdb/errors"
)

// Linkage definition for go1.21 or higher built with ./dev toolchain --
// that is, a toolchain that applies cockroach runtime patches.

// Gross hack to access internal context function.  Sometimes, go makes things
// SO much more difficult than it needs to.

// context_propagateCancel propagates cancellation from parent to child.
// Since this code was built with ./dev, use patched context.Context to access
// needed functionality.
func context_propagateCancel(parent context.Context, child canceler) {
	if !context.PropagateCancel(parent, child) {
		// This shouldn't happen since WhenDone checks to make sure
		// parent is cancellable.
		panic(errors.Newf("parent context expected to be cancellable, found %T", parent))
	}
}
