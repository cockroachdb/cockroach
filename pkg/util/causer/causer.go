// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
