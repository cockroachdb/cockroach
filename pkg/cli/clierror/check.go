// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clierror

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
)

// CheckAndMaybeLog reports the error, if non-nil, to the given
// logger.
func CheckAndMaybeLog(
	err error, logger func(context.Context, logpb.Severity, string, ...interface{}),
) error {
	if err == nil {
		return nil
	}
	severity := logpb.Severity_ERROR
	cause := err
	var ec *Error
	if errors.As(err, &ec) {
		severity = ec.GetSeverity()
		cause = ec.Unwrap()
	}
	logger(context.Background(), severity, "%v", cause)
	return err
}
