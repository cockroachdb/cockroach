// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package enginepb

// IsEmpty returns true if the header is empty.
// gcassert:inline
func (h MVCCValueHeader) IsEmpty() bool {
	return h == MVCCValueHeader{}
}
