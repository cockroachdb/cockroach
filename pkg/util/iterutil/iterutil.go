// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package iterutil

import "github.com/cockroachdb/errors"

var errStopIteration = errors.New("stop iteration")

// StopIteration returns a sentinel error that indicates stopping the iteration.
//
// This error should not be propagated further, i.e., if a closure returns
// this error, the loop should break returning nil error. For example:
//
//	f := func(i int) error {
//		if i == 10 {
//			return iterutil.StopIteration()
//		}
//		return nil
//	}
//
//	for i := range slice {
//		if err := f(i); err != nil {
//			return iterutil.Map(err)
//		}
//		// continue when nil error
//	}
func StopIteration() error { return errStopIteration }

// Map the nil if it is StopIteration, or keep the error otherwise
func Map(err error) error {
	if errors.Is(err, errStopIteration) {
		return nil
	}
	return err
}
