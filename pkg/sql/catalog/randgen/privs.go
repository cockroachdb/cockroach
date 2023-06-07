// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package randgen

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

// checkCanCreateOnSchema is a variant of (*planner).checkCanCreateOnSchema that
// avoids looking up descriptors -- we already have them.
func (g *testSchemaGenerator) checkCanCreateOnSchema(
	ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor,
) {
	switch sc.SchemaKind() {
	case catalog.SchemaPublic:
		// The public schema is valid to create in if the parent database is.
		if err := g.ext.cat.CheckPrivilegeForUser(ctx, db, privilege.CREATE, g.cfg.user); err != nil {
			panic(genError{err})
		}

	case catalog.SchemaTemporary:
		// The temp schema can always be written to after it's been
		// created.

	case catalog.SchemaVirtual:
		panic(genError{sqlerrors.NewCannotModifyVirtualSchemaError(sc.GetName())})

	case catalog.SchemaUserDefined:
		if err := g.ext.cat.CheckPrivilegeForUser(ctx, sc, privilege.CREATE, g.cfg.user); err != nil {
			panic(genError{err})
		}

	default:
		panic(genError{errors.AssertionFailedf("unknown schema kind %d", sc.SchemaKind())})
	}
}

func (g *testSchemaGenerator) checkCanCreateOnDatabase(
	ctx context.Context, db catalog.DatabaseDescriptor,
) {
	if err := g.ext.cat.CheckPrivilegeForUser(ctx, db, privilege.CREATE, g.cfg.user); err != nil {
		panic(genError{err})
	}
}

func (g *testSchemaGenerator) checkCanCreateDatabase(ctx context.Context) {
	if err := g.ext.cat.CanCreateDatabase(ctx); err != nil {
		panic(genError{err})
	}
}
