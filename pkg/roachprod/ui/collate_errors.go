// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ui

// ErrorsByError compares error objects by their Error() value.
type ErrorsByError []error

func (l ErrorsByError) Len() int {
	return len(l)
}
func (l ErrorsByError) Less(i, j int) bool {
	return l[i].Error() < l[j].Error()
}
func (l ErrorsByError) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
