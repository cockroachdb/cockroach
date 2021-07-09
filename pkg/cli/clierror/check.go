// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clierror

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/errors"
)

// CheckAndMaybeShout shouts the error, if non-nil to the OPS logging
// channel.
func CheckAndMaybeShout(err error) error {
	return checkAndMaybeShoutTo(err, log.Ops.Shoutf)
}

func checkAndMaybeShoutTo(
	err error, logger func(context.Context, log.Severity, string, ...interface{}),
) error {
	if err == nil {
		return nil
	}
	severity := severity.ERROR
	cause := err
	var ec *Error
	if errors.As(err, &ec) {
		severity = ec.GetSeverity()
		cause = ec.Unwrap()
	}
	logger(context.Background(), severity, "%v", cause)
	return err
}
