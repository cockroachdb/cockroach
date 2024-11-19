// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
