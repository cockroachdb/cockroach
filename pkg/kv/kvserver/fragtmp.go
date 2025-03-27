// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

type FragSpan struct {
	// Start and End encode the user key range of all the contained items, with
	// an inclusive start key and exclusive end key. Both Start and End must be
	// non-nil, or both nil if representing an invalid Span.
	Start, End []byte
}

type Fragmenter struct {
	FragmenterI
	Emit func(FragSpan)
}

type FragmenterI interface {
	Add(s FragSpan)
	Empty() bool
	Start() []byte
	Truncate(key []byte)
	Finish()
}
