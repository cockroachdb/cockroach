// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
