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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/randident"
	"github.com/cockroachdb/errors"
)

func (g *testSchemaGenerator) genDatabases(ctx context.Context) {
	if g.numDatabases == 0 {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(genError); ok {
				e.err = errors.Wrapf(e.err,
					"generating %d databases with name pattern %q",
					g.numDatabases, g.dbNamePat)
				panic(e)
			}
			panic(r)
		}
	}()

	// Compute the names ahead of time; this also takes care of
	// avoiding duplicates.
	ng := randident.NewNameGenerator(&g.cfg.NameGen, g.rand, g.dbNamePat)
	conflictNames := g.dbNames(ctx)
	dbNames, err := ng.GenerateMultiple(ctx, g.numDatabases, conflictNames)
	if err != nil {
		panic(genError{err})
	}

	// Actually generate the databases.
	firstID := g.makeIDs(ctx, g.numDatabases*2)
	for i := 0; i < g.numDatabases; i++ {
		dbName := dbNames[i]
		id := firstID + catid.DescID(i*2)
		publicSchemaID := firstID + catid.DescID(i*2+1)

		g.dbTemplate.ID = id
		g.dbTemplate.Name = dbName
		g.dbTemplate.Schemas[tree.PublicSchema] = descpb.DatabaseDescriptor_SchemaInfo{ID: publicSchemaID}
		db := dbdesc.NewBuilder(&g.dbTemplate).BuildCreatedMutableDatabase()
		g.cfg.GeneratedCounts.Databases++
		g.newDesc(ctx, db)

		g.publicScTemplate.ParentID = id
		g.publicScTemplate.ID = publicSchemaID
		publicSchema := schemadesc.NewBuilder(&g.publicScTemplate).BuildCreatedMutableSchema()
		g.cfg.GeneratedCounts.Schemas++
		g.newDesc(ctx, publicSchema)

		g.baseSchema = publicSchema
		g.maybeGenSchemas(ctx, db)
	}
}
