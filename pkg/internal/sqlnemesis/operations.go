// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlnemesis

type Step struct {
	// is Step just an Operation?
	Operation
}

type Operation struct {
}

func (op Operation) String() string {
}
