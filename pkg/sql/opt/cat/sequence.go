// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package cat

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// Sequence is an interface to a database sequence.
type Sequence interface {
	DataSource

	// SequenceName returns the name of the sequence. This method should always
	// return the same value as DataSource.Name(), but is included here as a
	// safety measure so that every DataSource does not trivially implement
	// Sequence.
	SequenceName() *tree.TableName
}

// FormatSequence nicely formats a catalog sequence using a treeprinter for
// debugging and testing.
func FormatSequence(cat Catalog, seq Sequence, tp treeprinter.Node) {
	tp.Childf("SEQUENCE %s", seq.Name())
}
