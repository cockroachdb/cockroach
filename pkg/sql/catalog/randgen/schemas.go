// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randgen

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/randident"
	"github.com/cockroachdb/errors"
)

func (g *testSchemaGenerator) genSchemas(ctx context.Context, db *dbdesc.Mutable) {
	if g.gencfg.numSchemasPerDatabase == 0 {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(genError); ok {
				e.err = errors.Wrapf(e.err,
					"generating %d schemas with name pattern %q under database %s",
					g.gencfg.numSchemasPerDatabase,
					g.gencfg.scNamePat, tree.NameString(db.Name))
				panic(e)
			}
			panic(r)
		}
	}()

	// Compute the shared schema privileges just once.
	//
	// Note: we can't precompute the schema privileges in the general
	// case because we are not always creating fresh databases. When
	// reusing a pre-existing database, we must properly reuse its
	// default privileges.
	privs, err := catprivilege.CreatePrivilegesFromDefaultPrivileges(
		db.GetDefaultPrivilegeDescriptor(),
		nil, /* schemaDefaultPrivilegeDescriptor */
		db.GetID(),
		g.cfg.user,
		privilege.Schemas,
	)
	if err != nil {
		panic(genError{err})
	}
	privs.SetOwner(g.cfg.user)

	// Reject creation of schemas with a name like pg_xxx.
	if err := schemadesc.IsSchemaNameValid(g.gencfg.scNamePat); err != nil {
		panic(genError{err})
	}

	// Compute the names ahead of time; this also takes care of
	// avoiding duplicates.
	ng := randident.NewNameGenerator(&g.cfg.NameGen, g.rand, g.gencfg.scNamePat)
	conflictNames := g.scNamesInDb(ctx, db)
	scNamesInDb, err := ng.GenerateMultiple(ctx, g.gencfg.numSchemasPerDatabase, conflictNames)
	if err != nil {
		panic(genError{err})
	}

	// Actually generate the schemas.
	firstID := g.makeIDs(ctx, g.gencfg.numSchemasPerDatabase)
	for i := 0; i < g.gencfg.numSchemasPerDatabase; i++ {
		scName := scNamesInDb[i]
		id := firstID + catid.DescID(i)

		g.models.sc.ParentID = db.GetID()
		g.models.sc.ID = id
		g.models.sc.Name = scName
		g.models.sc.Privileges = privs

		// Note: we do not call RunPostDeserializationChanges here. This is
		// intentional: it incurs a large hit in performance. Also the call
		// is not needed, because we 100% control the structure of the
		// template and this is validated by tests.
		sc := schemadesc.NewBuilder(&g.models.sc).BuildCreatedMutableSchema()

		db.AddSchemaToDatabase(sc.Name, descpb.DatabaseDescriptor_SchemaInfo{ID: id})

		g.cfg.GeneratedCounts.Schemas++
		g.newDesc(ctx, sc)

		// Note: no need to call checkSchemaCreatePriv here, since we've
		// just created the schema we have privilege to create in it.
		g.genMultipleTables(ctx, db, sc)
	}

	// The db was modified (adding new schemas), queue a write.
	g.queueDescMut(ctx, db)
}
