// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randgen

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
)

// maybeGenDatabases either creates some test databases or recurses to
// create schemas/tables in an existing database.
func (g *testSchemaGenerator) maybeGenDatabases(ctx context.Context) {
	if g.gencfg.createDatabases {
		g.checkCanCreateDatabase(ctx)
		g.genDatabases(ctx)
		return
	}

	db := g.target.db
	if g.gencfg.createSchemas {
		g.checkCanCreateOnDatabase(ctx, db)
	}
	g.maybeGenSchemas(ctx, db)
}

// maybeGenSchema either creates some test schemas or recurses
// to create test tables.
// The caller is responsible for checking the user has privilege
// to create schemas on the target database.
func (g *testSchemaGenerator) maybeGenSchemas(ctx context.Context, db *dbdesc.Mutable) {
	if g.gencfg.createSchemas {
		g.genSchemas(ctx, db)
		return
	}

	sc := g.target.sc
	if g.gencfg.createTables && !g.gencfg.useGeneratedPublicSchema {
		g.checkCanCreateOnSchema(ctx, db, sc)
	}
	g.genMultipleTables(ctx, db, sc)
}
