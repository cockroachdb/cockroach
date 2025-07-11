// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

const NoopFailureName = "noop"

func registerNoopFailure(r *FailureRegistry) {
	r.add(NoopFailureName, NoopFailureArgs{}, MakeNoopFailure)
}

func MakeNoopFailure(
	clusterName string, l *logger.Logger, clusterOpts ClusterOptions,
) (FailureMode, error) {
	return &NoopFailureMode{}, nil
}

// NoopFailureMode is a failure mode that does nothing to the CRDB cluster. It
// is intended for testing the failure injection framework itself and not for
// real failure injection purposes.
type NoopFailureMode struct{}

type NoopFailureArgs struct {
	InjectedError error
}

func (n NoopFailureMode) Description() string {
	return ""
}

func (n NoopFailureMode) Setup(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	return args.(NoopFailureArgs).InjectedError
}

func (n NoopFailureMode) Inject(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	return args.(NoopFailureArgs).InjectedError
}

func (n NoopFailureMode) Recover(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	return args.(NoopFailureArgs).InjectedError
}

func (n NoopFailureMode) Cleanup(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	return args.(NoopFailureArgs).InjectedError
}

func (n NoopFailureMode) WaitForFailureToPropagate(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	return args.(NoopFailureArgs).InjectedError
}

func (n NoopFailureMode) WaitForFailureToRecover(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	return args.(NoopFailureArgs).InjectedError
}
