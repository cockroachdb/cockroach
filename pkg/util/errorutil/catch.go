// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package errorutil

import (
	"runtime"

	"github.com/cockroachdb/errors"
)

// ShouldCatch is used for catching errors thrown as panics. Its argument is the
// object returned by recover(); it succeeds if the object is an error. If the
// error is a runtime.Error, it is converted to an internal error (see
// errors.AssertionFailedf).
func ShouldCatch(obj interface{}) (ok bool, err error) {
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
