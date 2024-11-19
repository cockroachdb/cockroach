// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

func boolVecToSel(vec []bool, sel []int) []int {
	l := len(vec)
	for i := 0; i < l; i++ {
		if vec[i] {
			sel = append(sel, i)
		}
	}
	return sel
}
