// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// CommentOnDatabase implements COMMENT ON DATABASE xxx IS xxx statement.
func CommentOnDatabase(b BuildCtx, n *tree.CommentOnDatabase) {
	dc := &scpb.DatabaseComment{}
	if n.Comment != nil {
		dc.Comment = *n.Comment
	}

	dbElements := b.ResolveDatabase(n.Name, ResolveParams{
		IsExistenceOptional: false,
		RequiredPrivilege:   privilege.CREATE,
	})
	dbElements.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.Database:
			dc.DatabaseID = t.DatabaseID
		case *scpb.DatabaseComment:
			if len(dc.Comment) == 0 {
				b.Drop(e)
			}
		}
	})

	if len(dc.Comment) > 0 {
		b.Add(dc)
	}
}

// CommentOnSchema implements COMMENT ON SCHEMA xxx IS xxx statement;
func CommentOnSchema(b BuildCtx, n *tree.CommentOnSchema) {
	sc := &scpb.SchemaComment{}
	if n.Comment != nil {
		sc.Comment = *n.Comment
	}

	schemaElements := b.ResolveSchema(
		n.Name,
		ResolveParams{
			IsExistenceOptional: false,
			RequiredPrivilege:   privilege.CREATE,
		},
	)

	schemaElements.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.Schema:
			sc.SchemaID = t.SchemaID
		case *scpb.SchemaComment:
			if len(sc.Comment) == 0 {
				b.Drop(e)
			}
		}
	})

	if len(sc.Comment) > 0 {
		b.Add(sc)
	}
}

// CommentOnTable implements COMMENT ON TABLE xxx IS xxx statement.
func CommentOnTable(b BuildCtx, n *tree.CommentOnTable) {
	tc := &scpb.TableComment{}
	if n.Comment != nil {
		tc.Comment = *n.Comment
	}

	tableElements := b.ResolveTable(n.Table, ResolveParams{
		IsExistenceOptional: false,
		RequiredPrivilege:   privilege.CREATE,
	})

	tableElements.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.Table:
			tc.TableID = t.TableID
		case *scpb.TableComment:
			if len(tc.Comment) == 0 {
				b.Drop(e)
			}
		}
	})

	if len(tc.Comment) > 0 {
		b.Add(tc)
	}
}

// CommentOnColumn implements COMMENT ON COLUMN xxx IS xxx statement.
func CommentOnColumn(b BuildCtx, n *tree.CommentOnColumn) {
	cc := &scpb.ColumnComment{}
	if n.Comment != nil {
		cc.Comment = *n.Comment
	}

	params := ResolveParams{
		IsExistenceOptional: false,
		RequiredPrivilege:   privilege.CREATE,
	}
	tableElements := b.ResolveTable(n.ColumnItem.TableName, params)
	tableElements.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.Table:
			cc.TableID = t.TableID
		}
	})

	columnElements := b.ResolveColumn(cc.TableID, n.ColumnItem.ColumnName, params)

	columnElements.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.Column:
			cc.ColumnID = t.ColumnID
		case *scpb.ColumnComment:
			if len(cc.Comment) == 0 {
				b.Drop(e)
			}
		}
	})

	if len(cc.Comment) > 0 {
		b.Add(cc)
	}
}

// CommentOnIndex implements COMMENT ON INDEX xxx iS xxx statement.
func CommentOnIndex(b BuildCtx, n *tree.CommentOnIndex) {
	ic := &scpb.IndexComment{}
	if n.Comment != nil {
		ic.Comment = *n.Comment
	}

	params := ResolveParams{
		IsExistenceOptional: false,
		RequiredPrivilege:   privilege.CREATE,
	}

	var tableElements ElementResultSet
	if len(n.Index.Table.ObjectName) > 0 {
		tableElements = b.ResolveTable(n.Index.Table.ToUnresolvedObjectName(), params)
	} else {
		tableElements = b.ResolveTableWithIndexBestEffort(tree.Name(n.Index.Index), n.Index.Table.ObjectNamePrefix, params)
	}

	tableElements.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.Table:
			// Mutate the AST to have the fully resolved name from above, which will be
			// used for both event logging and errors. This is enforced by validation.
			n.Index.Table.ObjectNamePrefix = b.NamePrefix(t)
			ic.TableID = t.TableID
		}
	})

	indexElements := b.ResolveIndex(ic.TableID, tree.Name(n.Index.Index), params)

	indexElements.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.SecondaryIndex:
			ic.IndexID = t.IndexID
		case *scpb.PrimaryIndex:
			ic.IndexID = t.IndexID
		case *scpb.IndexComment:
			if len(ic.Comment) == 0 {
				b.Drop(e)
			}
		}
	})

	if len(ic.Comment) > 0 {
		b.Add(ic)
	}
}

// CommentOnConstraint implements COMMENT ON CONSTRAINT xxx ON table_name IS xxx
// statement.
func CommentOnConstraint(b BuildCtx, n *tree.CommentOnConstraint) {
	params := ResolveParams{
		IsExistenceOptional: false,
		RequiredPrivilege:   privilege.CREATE,
	}
	cc := &scpb.ConstraintComment{}
	if n.Comment != nil {
		cc.Comment = *n.Comment
	}

	tableElements := b.ResolveTable(n.Table, params)
	tableElements.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.Table:
			cc.TableID = t.TableID
		}
	})

	constraintElements := b.ResolveConstraint(cc.TableID, n.Constraint, params)
	constraintElements.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.CheckConstraint:
			cc.ConstraintID = t.ConstraintID
		case *scpb.ForeignKeyConstraint:
			cc.ConstraintID = t.ConstraintID
		case *scpb.UniqueWithoutIndexConstraint:
			cc.ConstraintID = t.ConstraintID
		case *scpb.ConstraintComment:
			if len(cc.Comment) == 0 {
				b.Drop(e)
			}
		}
	})

	if len(cc.Comment) > 0 {
		b.Add(cc)
	}
}
