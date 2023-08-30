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
	_ "unsafe" // Must import unsafe to enable gross hack below.
)

// Gross hack to access internal context function.  Sometimes, go makes things
// SO much more difficult than it needs to.

//go:linkname context_removeChild context.removeChild
func context_removeChild(parent context.Context, child canceler)
