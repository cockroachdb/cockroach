// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package iterutil

import "github.com/cockroachdb/errors"

var errStopIteration = errors.New("stop iteration")

// StopIteration returns a sentinel error that indicates stopping the iteration.
//
// This error should not be propagated further, i.e., if a closure returns
// this error, the loop should break returning nil error. For example:
//
// 	f := func(i int) error {
// 		if i == 10 {
// 			return iterutil.StopIteration()
// 		}
// 		return nil
// 	}
//
// 	for i := range slice {
// 		if err := f(i); err != nil {
// 			if iterutil.Done() {
// 				return nil
// 			}
// 			return err
// 		}
// 		// continue when nil error
// 	}
//
func StopIteration() error { return errStopIteration }

// Done tells if the error is ErrStopIteration, i.e., should the iteration stop.
func Done(err error) bool { return errors.Is(err, errStopIteration) }
