// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package math

import "golang.org/x/exp/constraints"

// Min returns the minimal value of the given two.
func Min[T constraints.Ordered](a, b T) T {
	if a <= b {
		return a
	}
	return b
}

// Max returns the maximal value of the given two.
func Max[T constraints.Ordered](a, b T) T {
	if a >= b {
		return a
	}
	return b
}
