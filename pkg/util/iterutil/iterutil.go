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

// ErrStopIteration is a sentinel error that indicates stopping the iteration.
//
// This error should not be propogated further, i.e., if a closure returns
// this error, the loop should break returning nil error. For example:
//
// 	f := func(i int) error {
// 		if i == 10 {
// 			return iterutil.ErrStopIteration
// 		}
// 		return nil
// 	}
//
// 	for i := range slice {
// 		if err := f(i); err != nil {
// 			if errors.Is(err, iterutil.ErrStopIteration) {
// 				return nil
// 			}
// 			return err
// 		}
// 		// continue when nil error
// 	}
//
var ErrStopIteration = errors.New("stop iteration")

// Done tells if the error is ErrStopIteration, i.e., should the iteration stop.
func Done(err error) bool { return errors.Is(err, ErrStopIteration) }
