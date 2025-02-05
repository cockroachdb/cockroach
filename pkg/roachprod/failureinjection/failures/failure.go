// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// FailureArgs describes the args passed to a failure mode.
//
// For now, this interface is not necessarily needed. However, it sets up for
// future failure injection work when we want a failure controller to be able
// to parse args from a YAML file and pass them to a failure controller.
type FailureArgs interface {
}

// FailureMode describes a failure that can be injected into a system.
//
// For now, this interface is not necessarily needed, however it sets up for
// future failure injection work when we want a failure controller to be
// able to inject multiple different types of failures.
type FailureMode interface {
	Description() string

	// Setup any dependencies required for the failure to be injected.
	Setup(ctx context.Context, l *logger.Logger, args FailureArgs) error

	// Inject a failure into the system.
	Inject(ctx context.Context, l *logger.Logger, args FailureArgs) error

	// Restore reverses the effects of Inject.
	Restore(ctx context.Context, l *logger.Logger, args FailureArgs) error

	// Cleanup uninstalls any dependencies that were installed by Setup.
	Cleanup(ctx context.Context, l *logger.Logger, args FailureArgs) error

	// WaitForFailureToPropagate waits until the failure is at full effect.
	WaitForFailureToPropagate(ctx context.Context, l *logger.Logger, args FailureArgs) error

	// WaitForFailureToRestore waits until the failure was restored completely along with any side effects.
	WaitForFailureToRestore(ctx context.Context, l *logger.Logger, args FailureArgs) error
}
