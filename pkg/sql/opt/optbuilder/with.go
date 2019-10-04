// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// getCTECols renames a presentation for the scope, renaming the columns to
// those provided in the AliasClause (if any). Throws an error if there is a
// mistmatch in the number of columns.
func (b *Builder) getCTECols(cteScope *scope, name tree.AliasClause) physical.Presentation {
	presentation := cteScope.makePresentation()

	if len(presentation) == 0 {
		err := pgerror.Newf(
			pgcode.FeatureNotSupported,
			"WITH clause %q does not return any columns",
			tree.ErrString(&name),
		)
		panic(errors.WithHint(err, "missing RETURNING clause?"))
	}

	if name.Cols == nil {
		return presentation
	}

	if len(presentation) != len(name.Cols) {
		panic(pgerror.Newf(
			pgcode.InvalidColumnReference,
			"source %q has %d columns available but %d columns specified",
			name.Alias, len(presentation), len(name.Cols),
		))
	}
	for i := range presentation {
		presentation[i].Alias = string(name.Cols[i])
	}
	return presentation
}
