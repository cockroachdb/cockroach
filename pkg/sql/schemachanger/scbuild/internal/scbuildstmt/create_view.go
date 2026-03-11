// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdecomp"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func CreateView(b BuildCtx, n *tree.CreateView) {
	// Reject unsupported variants.
	if n.Materialized {
		panic(scerrors.NotImplementedErrorf(n, "CREATE MATERIALIZED VIEW is not supported"))
	}
	if n.Persistence.IsTemporary() {
		panic(scerrors.NotImplementedErrorf(n, "CREATE TEMPORARY VIEW is not supported"))
	}
	if n.Replace {
		panic(scerrors.NotImplementedErrorf(n, "CREATE OR REPLACE VIEW is not supported"))
	}

	b.IncrementSchemaChangeCreateCounter("view")

	dbElts, scElts := b.ResolveTargetObject(n.Name.ToUnresolvedObjectName(), privilege.CREATE)
	_, _, sc := scpb.FindSchema(scElts)
	_, _, db := scpb.FindDatabase(dbElts)
	_, _, scName := scpb.FindNamespace(scElts)
	_, _, dbname := scpb.FindNamespace(dbElts)
	n.Name.SchemaName = tree.Name(scName.Name)
	n.Name.CatalogName = tree.Name(dbname.Name)
	n.Name.ExplicitCatalog = true
	n.Name.ExplicitSchema = true

	// Check for name collision.
	ers := b.ResolveRelation(n.Name.ToUnresolvedObjectName(), ResolveParams{
		IsExistenceOptional: true,
		RequiredPrivilege:   0,
		WithOffline:         true,
		ResolveTypes:        true,
	})
	if ers != nil && !ers.IsEmpty() {
		if n.IfNotExists {
			return
		}
		panic(sqlerrors.NewRelationAlreadyExistsError(n.Name.FQString()))
	}

	// Build the reference provider via the optimizer. This parses and analyzes
	// the view query, extracts the fully-qualified query text, result columns,
	// and all dependencies (tables, views, sequences, types, routines).
	refProvider := b.BuildReferenceProvider(n)

	// Validate cross-database references.
	validateTypeReferences(b, refProvider, db.DatabaseID)
	validateFunctionRelationReferences(b, refProvider, db.DatabaseID)
	validateFunctionToFunctionReferences(b, refProvider, db.DatabaseID)

	viewID := b.GenerateUniqueDescID()

	// Collect referenced relation and type IDs for the View element.
	usesRelationIDs := refProvider.ReferencedRelationIDs().Ordered()
	usesTypeIDs := refProvider.ReferencedTypes().Ordered()
	usesRoutineIDs := refProvider.ReferencedRoutines().Ordered()

	// Build forward references from the reference provider.
	var forwardRefs []*scpb.View_Reference
	if err := refProvider.ForEachTableReference(func(
		tblID descpb.ID, idxID descpb.IndexID, colIDs descpb.ColumnIDs,
	) error {
		forwardRefs = append(forwardRefs, &scpb.View_Reference{
			ToID:      tblID,
			IndexID:   idxID,
			ColumnIDs: colIDs,
		})
		return nil
	}); err != nil {
		panic(err)
	}
	if err := refProvider.ForEachViewReference(func(
		viewRefID descpb.ID, colIDs descpb.ColumnIDs,
	) error {
		forwardRefs = append(forwardRefs, &scpb.View_Reference{
			ToID:      viewRefID,
			ColumnIDs: colIDs,
		})
		return nil
	}); err != nil {
		panic(err)
	}

	viewElem := &scpb.View{
		ViewID:            viewID,
		UsesTypeIDs:       usesTypeIDs,
		UsesRelationIDs:   usesRelationIDs,
		UsesRoutineIDs:    usesRoutineIDs,
		ForwardReferences: forwardRefs,
		IsTemporary:       false,
		IsMaterialized:    false,
	}
	b.Add(viewElem)

	// Namespace and schema child.
	b.Add(&scpb.Namespace{
		DatabaseID:   db.DatabaseID,
		SchemaID:     sc.SchemaID,
		DescriptorID: viewID,
		Name:         string(n.Name.ObjectName),
	})
	b.Add(&scpb.SchemaChild{
		ChildObjectID: viewID,
		SchemaID:      sc.SchemaID,
	})

	// Columns from the view query result.
	viewColumns := refProvider.ViewColumns()
	for i, col := range viewColumns {
		colID := catid.ColumnID(i + 1)
		colTyp := col.Typ
		if colTyp.Family() == types.UnknownFamily {
			colTyp = types.String
		}
		b.Add(&scpb.Column{
			TableID:  viewID,
			ColumnID: colID,
		})
		b.Add(&scpb.ColumnName{
			TableID:  viewID,
			ColumnID: colID,
			Name:     col.Name,
		})
		b.Add(&scpb.ColumnType{
			TableID:                 viewID,
			ColumnID:                colID,
			TypeT:                   newTypeT(colTyp),
			ElementCreationMetadata: scdecomp.NewElementCreationMetadata(b.EvalCtx().Settings.Version.ActiveVersion(b)),
		})
	}

	// ViewQuery element with the query text and forward references.
	b.Add(b.WrapViewQuery(viewID, refProvider.ViewQuery(), refProvider))

	// Owner and privileges.
	owner, ups := b.BuildUserPrivilegesFromDefaultPrivileges(
		db, sc, viewID, privilege.Tables, b.CurrentUser(),
	)
	b.Add(owner)
	for _, up := range ups {
		b.Add(up)
	}

	b.LogEventForExistingTarget(viewElem)
}
