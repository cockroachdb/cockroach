// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package causer

// A Causer is an error that wraps a causing error (which in turn could be
// another Causer). A Causer is usually constructed via errors.Wrap or Wrapf.
type Causer interface {
	error
	Cause() error
}

// Visit walks along the chain of errors until it encounters the first one that
// does not implement Causer. The visitor is invoked with each error visited
// until there are no more errors to visit or the visitor returns true (which is
// then the return value of Visit as well). Returns false when the visitor never
// returns true or if the initial error is nil.
// Calling this method on a cyclic error chain results in an infinite loop.
func Visit(err error, f func(error) bool) bool {
	for err != nil {
		if f(err) {
			return true
		}
		cause, ok := err.(Causer)
		if !ok {
			return false
		}
		err = cause.Cause()
	}
	return false
}
