// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package task

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

type Func func(context.Context, *logger.Logger) error

// Tasker is an interface for executing tasks (goroutines). It is intended for
// use in tests, enabling the test framework to manage panics and errors.
type Tasker interface {
	// Go runs the given function in a goroutine.
	Go(fn Func, opts ...Option)
	// GoWithCancel runs the given function in a goroutine and returns a
	// CancelFunc that can be used to cancel the function.
	GoWithCancel(fn Func, opts ...Option) context.CancelFunc
}
