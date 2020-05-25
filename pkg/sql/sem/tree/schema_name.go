// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// UnresolvedSchemaName is an unresolved qualified name for a schema.
type UnresolvedSchemaName struct {
	NumParts int
	Parts    [2]string
	AnnotatedNode
}

// NewUnresolvedSchemaName creates an unresolved schema name, verifying that it
// is well-formed.
func NewUnresolvedSchemaName(
	numParts int, parts [2]string, annotationIdx AnnotationIdx,
) (*UnresolvedSchemaName, error) {
	u := &UnresolvedSchemaName{
		NumParts:      numParts,
		Parts:         parts,
		AnnotatedNode: AnnotatedNode{annotationIdx},
	}
	if numParts < 1 || parts[0] == "" {
		return nil, pgerror.Newf(pgcode.InvalidName, "invalid schema name: %s", u)
	}
	return u, nil
}

// Format implements the NodeFormatter interface.
func (u *UnresolvedSchemaName) Format(ctx *FmtCtx) {
	// TODO (lucy): Deal with annotating the resolved name.

	for i := u.NumParts; i > 0; i-- {
		// The first part to print is the last item in u.Parts. It is also
		// a potentially restricted name to disambiguate from keywords in
		// the grammar, so print it out as a "Name". Every part after that is
		// necessarily an unrestricted name.
		if i == u.NumParts {
			ctx.FormatNode((*Name)(&u.Parts[i-1]))
		} else {
			ctx.WriteByte('.')
			ctx.FormatNode((*UnrestrictedName)(&u.Parts[i-1]))
		}
	}
}

func (u *UnresolvedSchemaName) String() string { return AsString(u) }

func (u *UnresolvedSchemaName) Schema() string {
	return u.Parts[0]
}

func (u *UnresolvedSchemaName) Catalog() string {
	return u.Parts[1]
}
func (u *UnresolvedSchemaName) HasExplicitCatalog() bool {
	return u.NumParts > 1
}
