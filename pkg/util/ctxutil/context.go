// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ctxutil

// WhenDoneFunc is the callback invoked by context when it becomes done.
type WhenDoneFunc func()
