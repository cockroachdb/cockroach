// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randgen

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/randident"
	"github.com/cockroachdb/errors"
)

func (g *testSchemaGenerator) genDatabases(ctx context.Context) {
	if g.gencfg.numDatabases == 0 {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(genError); ok {
				e.err = errors.Wrapf(e.err,
					"generating %d databases with name pattern %q",
					g.gencfg.numDatabases, g.gencfg.dbNamePat)
				panic(e)
			}
			panic(r)
		}
	}()

	// Compute the names ahead of time; this also takes care of
	// avoiding duplicates.
	ng := randident.NewNameGenerator(&g.cfg.NameGen, g.rand, g.gencfg.dbNamePat)
	conflictNames := g.dbNames(ctx)
	dbNames, err := ng.GenerateMultiple(ctx, g.gencfg.numDatabases, conflictNames)
	if err != nil {
		panic(genError{err})
	}

	// Actually generate the databases.
	firstID := g.makeIDs(ctx, g.gencfg.numDatabases*2)
	for i := 0; i < g.gencfg.numDatabases; i++ {
		dbName := dbNames[i]
		id := firstID + catid.DescID(i*2)
		publicSchemaID := firstID + catid.DescID(i*2+1)

		g.models.db.ID = id
		g.models.db.Name = dbName
		g.models.db.Schemas[catconstants.PublicSchemaName] = descpb.DatabaseDescriptor_SchemaInfo{ID: publicSchemaID}
		db := dbdesc.NewBuilder(&g.models.db).BuildCreatedMutableDatabase()
		g.cfg.GeneratedCounts.Databases++
		g.newDesc(ctx, db)

		g.models.publicSc.ParentID = id
		g.models.publicSc.ID = publicSchemaID
		publicSchema := schemadesc.NewBuilder(&g.models.publicSc).BuildCreatedMutableSchema()
		g.cfg.GeneratedCounts.Schemas++
		g.newDesc(ctx, publicSchema)

		g.target.sc = publicSchema
		g.maybeGenSchemas(ctx, db)
	}
}
