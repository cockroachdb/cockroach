// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randgen

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/errors"
)

// dbNames retrieves the list of database names in the schema.
func (g *testSchemaGenerator) dbNames(ctx context.Context) map[string]struct{} {
	dbs, err := g.ext.coll.GetAllDatabases(ctx, g.ext.txn)
	if err != nil {
		panic(genError{err})
	}
	res := make(map[string]struct{})
	_ = dbs.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
		res[e.GetName()] = struct{}{}
		return nil
	})
	return res
}

// scNamesInDb retrieves the list of schema names in the given
// database.
func (g *testSchemaGenerator) scNamesInDb(
	ctx context.Context, db catalog.DatabaseDescriptor,
) map[string]struct{} {
	res := make(map[string]struct{})
	_ = db.ForEachSchema(func(_ descpb.ID, name string) error {
		res[name] = struct{}{}
		return nil
	})
	return res
}

// objNamesInSchema retrieves the list of objects in the given
// database and schema.
func (g *testSchemaGenerator) objNamesInSchema(
	ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor,
) map[string]struct{} {
	objs, err := g.ext.coll.GetAllObjectsInSchema(ctx, g.ext.txn, db, sc)
	if err != nil {
		panic(genError{err})
	}
	res := make(map[string]struct{})
	_ = objs.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
		res[e.GetName()] = struct{}{}
		return nil
	})
	return res
}

// lookupSchema looks up the descriptor for the given schema.
func (g *testSchemaGenerator) lookupSchema(
	ctx context.Context, db catalog.DatabaseDescriptor, scName string, required bool,
) catalog.SchemaDescriptor {
	b := g.ext.coll.ByName(g.ext.txn)
	var bng descs.ByNameGetter
	if required {
		bng = b.Get()
	} else {
		bng = b.MaybeGet()
	}
	sc, err := bng.Schema(ctx, db, scName)
	if err != nil {
		panic(genError{err})
	}
	return sc
}

// lookupSchema looks up the descriptor for the given database.
func (g *testSchemaGenerator) lookupDatabase(ctx context.Context, dbName string) *dbdesc.Mutable {
	db, err := g.ext.coll.MutableByName(g.ext.txn).Database(ctx, dbName)
	if err != nil {
		panic(genError{err})
	}
	return db
}

// makeIDs allocates multiple fresh descriptor IDs.
func (g *testSchemaGenerator) makeIDs(ctx context.Context, num int) catid.DescID {
	if num <= 0 {
		panic(genError{errors.AssertionFailedf("programming error: %d", num)})
	}
	firstID, err := g.ext.idGen.IncrementDescID(ctx, int64(num))
	if err != nil {
		panic(genError{err})
	}
	return firstID
}
