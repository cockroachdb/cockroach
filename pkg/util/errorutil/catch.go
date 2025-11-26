// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package errorutil

import (
	"context"
	"runtime"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// shouldCatch is used for catching errors thrown as panics. Its argument is the
// object returned by recover(); it succeeds if the object is an error. If the
// error is a runtime.Error, it is converted to an internal error (see
// errors.AssertionFailedf).
func shouldCatch(obj interface{}) (ok bool, err error) {
	err, ok = obj.(error)
	if ok {
		if errors.HasInterface(err, (*runtime.Error)(nil)) {
			// Convert runtime errors to internal errors, which display the stack and
			// get reported to Sentry.
			err = errors.HandleAsAssertionFailure(err)
		}
	}
	return ok, err
}

// MaybeCatchPanic is a helper function that can be deferred to catch errors
// thrown as panics (retErr is updated with the caught error). If an error is
// caught and doLog is true, then the error is logged with VEvent with verbosity
// level 1. If doLog is false, then the context parameter doesn't matter.
//
// NB: it can only be used when the code does not update shared state and does
// not manipulate locks.
func MaybeCatchPanic(ctx context.Context, retErr *error, doLog bool) {
	if r := recover(); r != nil {
		if ok, e := shouldCatch(r); ok {
			*retErr = e
			if doLog {
				log.VEventf(ctx, 1, "%v", e)
			}
		} else {
			panic(r)
		}
	}
}
