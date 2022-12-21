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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// isAdmin returns true if the current user has the admin role.
func (g *testSchemaGenerator) isAdmin(ctx context.Context) bool {
	isAdmin, err := g.hasAdminRole(ctx)
	if err != nil {
		panic(genError{err})
	}
	return isAdmin
}

// dbNames retrieves the list of database names in the schema.
func (g *testSchemaGenerator) dbNames(ctx context.Context) map[string]struct{} {
	dbs, err := g.coll.GetAllDatabases(ctx, g.txn)
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
	objs, err := g.coll.GetAllObjectsInSchema(ctx, g.txn, db, sc)
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
	sc, err := g.coll.GetImmutableSchemaByName(ctx,
		g.txn, db, scName, tree.SchemaLookupFlags{Required: required})
	if err != nil {
		panic(genError{err})
	}
	return sc
}

// lookupSchema looks up the descriptor for the given database.
func (g *testSchemaGenerator) lookupDatabase(ctx context.Context, dbName string) *dbdesc.Mutable {
	db, err := g.coll.GetMutableDatabaseByName(ctx,
		g.txn, dbName, tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		panic(genError{err})
	}
	return db
}

// makeID allocates one fresh descriptor ID.
func (g *testSchemaGenerator) makeID(ctx context.Context) catid.DescID {
	id, err := g.idGen.GenerateUniqueDescID(ctx)
	if err != nil {
		panic(genError{err})
	}
	return id
}

// makeIDs allocates multiple fresh descriptor IDs.
func (g *testSchemaGenerator) makeIDs(ctx context.Context, num int) catid.DescID {
	if num <= 0 {
		panic(genError{errors.AssertionFailedf("programming error: count %d", num)})
	}
	firstID, err := g.idGen.IncrementDescID(ctx, int64(num))
	if err != nil {
		panic(genError{err})
	}
	return firstID
}
