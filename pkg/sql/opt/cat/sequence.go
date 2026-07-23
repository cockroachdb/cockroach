// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cat

import "github.com/cockroachdb/cockroach/pkg/util/treeprinter"

// Sequence is an interface to a database sequence.
type Sequence interface {
	DataSource

	// SequenceMarker is a dummy method, included as a safety measure so that
	// every DataSource does not trivially implement Sequence.
	SequenceMarker()
}

// FormatSequence nicely formats a catalog sequence using a treeprinter for
// debugging and testing.
func FormatSequence(cat Catalog, seq Sequence, tp treeprinter.Node) {
	tp.Childf("SEQUENCE %s", seq.Name())
}
