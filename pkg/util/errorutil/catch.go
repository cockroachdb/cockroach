// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package errorutil

import (
	"runtime"

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
// thrown as panics (retErr is updated with the caught error). The callback
// function, if non-nil, will be called with the error if it is caught.
//
// NB: it can only be used when the code does not update shared state and does
// not manipulate locks.
func MaybeCatchPanic(retErr *error, errCallback func(caughtErr error)) {
	if r := recover(); r != nil {
		if ok, e := shouldCatch(r); ok {
			*retErr = errors.CombineErrors(*retErr, e)
			if errCallback != nil {
				errCallback(e)
			}
		} else {
			panic(r)
		}
	}
}
