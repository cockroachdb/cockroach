// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

func boolVecToSel64(vec []bool, sel []int) []int {
	l := len(vec)
	for i := 0; i < l; i++ {
		if vec[i] {
			sel = append(sel, i)
		}
	}
	return sel
}
