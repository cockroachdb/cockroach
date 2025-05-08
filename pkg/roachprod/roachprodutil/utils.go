// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprodutil

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/debugutil"
)

// PanicAsError logs the panic stack trace and returns an error with the
// panic message.
func PanicAsError(
	ctx context.Context, l *logger.Logger, f func(context.Context) error,
) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = logPanicToErr(l, r)
		}
	}()
	return f(ctx)
}

// logPanicToErr logs the panic stack trace and returns an error with the
// panic message.
func logPanicToErr(l *logger.Logger, r interface{}) error {
	l.Printf("panic stack trace:\n%s", debugutil.Stack())
	return fmt.Errorf("panic (stack trace above): %v", r)
}
