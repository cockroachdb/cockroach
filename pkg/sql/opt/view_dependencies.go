// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// ViewDeps contains information about the dependencies of a view.
type ViewDeps []ViewDep

// ViewDep contains information about a view dependency.
type ViewDep struct {
	DataSource cat.DataSource

	// ColumnOrdinals is the set of column ordinals that are referenced by the
	// view for this table. In most cases, this consists of all "public" columns
	// of the table; the only exception is when a table is referenced by table ID
	// with a specific list of column IDs.
	ColumnOrdinals util.FastIntSet

	// If an index is referenced specifically (via an index hint), SpecificIndex
	// is true and Index is the ordinal of that index.
	SpecificIndex bool
	Index         cat.IndexOrdinal
}
