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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var commentResolveParams = ResolveParams{
	IsExistenceOptional: false,
	RequiredPrivilege:   privilege.CREATE,
}

// CommentOnDatabase implements COMMENT ON DATABASE xxx IS xxx statement.
func CommentOnDatabase(b BuildCtx, n *tree.CommentOnDatabase) {
	dbElements := b.ResolveDatabase(n.Name, commentResolveParams)

	if n.Comment == nil {
		dbElements.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
			switch e.(type) {
			case *scpb.DatabaseComment:
				b.Drop(e)
			}
		})
	} else {
		dc := &scpb.DatabaseComment{
			Comment: *n.Comment,
		}
		dbElements.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
			switch t := e.(type) {
			case *scpb.Database:
				dc.DatabaseID = t.DatabaseID
			}
		})
		b.Add(dc)
	}
}

// CommentOnSchema implements COMMENT ON SCHEMA xxx IS xxx statement;
func CommentOnSchema(b BuildCtx, n *tree.CommentOnSchema) {
	schemaElements := b.ResolveSchema(n.Name, commentResolveParams)

	if n.Comment == nil {
		schemaElements.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
			switch e.(type) {
			case *scpb.SchemaComment:
				b.Drop(e)
			}
		})
	} else {
		sc := &scpb.SchemaComment{
			Comment: *n.Comment,
		}
		schemaElements.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
			switch t := e.(type) {
			case *scpb.Schema:
				sc.SchemaID = t.SchemaID
			}
		})
		b.Add(sc)
	}
}

// CommentOnTable implements COMMENT ON TABLE xxx IS xxx statement.
func CommentOnTable(b BuildCtx, n *tree.CommentOnTable) {
	tableElements := b.ResolveTable(n.Table, commentResolveParams)

	if n.Comment == nil {
		tableElements.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
			switch e.(type) {
			case *scpb.TableComment:
				b.Drop(e)
			}
		})
	} else {
		tc := &scpb.TableComment{
			Comment: *n.Comment,
		}
		tableElements.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
			switch t := e.(type) {
			case *scpb.Table:
				tc.TableID = t.TableID
			}
		})
		b.Add(tc)
	}
}

// CommentOnColumn implements COMMENT ON COLUMN xxx IS xxx statement.
func CommentOnColumn(b BuildCtx, n *tree.CommentOnColumn) {
	var tableID catid.DescID
	tableElements := b.ResolveTable(n.ColumnItem.TableName, commentResolveParams)
	tableElements.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.Table:
			tableID = t.TableID
		}
	})
	columnElements := b.ResolveColumn(tableID, n.ColumnItem.ColumnName, commentResolveParams)

	if n.Comment == nil {
		columnElements.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
			switch e.(type) {
			case *scpb.ColumnComment:
				b.Drop(e)
			}
		})
	} else {
		cc := &scpb.ColumnComment{
			TableID: tableID,
			Comment: *n.Comment,
		}
		columnElements.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
			switch t := e.(type) {
			case *scpb.Column:
				cc.ColumnID = t.ColumnID
				cc.PgAttributeNum = t.PgAttributeNum
			}
		})
		b.Add(cc)
	}
}

// CommentOnIndex implements COMMENT ON INDEX xxx iS xxx statement.
func CommentOnIndex(b BuildCtx, n *tree.CommentOnIndex) {
	var tableID catid.DescID
	indexElements := b.ResolveTableIndexBestEffort(&n.Index, commentResolveParams, true)
	indexElements.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch e.(type) {
		case *scpb.PrimaryIndex, *scpb.SecondaryIndex:
			tableID = screl.GetDescID(e)
		}
	})

	if n.Comment == nil {
		indexElements.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
			switch e.(type) {
			case *scpb.IndexComment:
				b.Drop(e)
			}
		})
	} else {
		ic := &scpb.IndexComment{
			TableID: tableID,
			Comment: *n.Comment,
		}
		indexElements.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
			switch e.(type) {
			case *scpb.PrimaryIndex, *scpb.SecondaryIndex:
				id, _ := screl.Schema.GetAttribute(screl.IndexID, e)
				ic.IndexID = id.(catid.IndexID)
			}
		})
		b.Add(ic)
	}
}

// CommentOnConstraint implements COMMENT ON CONSTRAINT xxx ON table_name IS xxx
// statement.
func CommentOnConstraint(b BuildCtx, n *tree.CommentOnConstraint) {
	var tableID catid.DescID
	tableElements := b.ResolveTable(n.Table, commentResolveParams)
	tableElements.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.Table:
			tableID = t.TableID
		}
	})
	constraintElements := b.ResolveConstraint(tableID, n.Constraint, commentResolveParams)

	if n.Comment == nil {
		constraintElements.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
			switch e.(type) {
			case *scpb.ConstraintComment:
				b.Drop(e)
			}
		})
	} else {
		cc := &scpb.ConstraintComment{
			Comment: *n.Comment,
			TableID: tableID,
		}
		constraintElements.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
			switch e.(type) {
			case *scpb.CheckConstraint, *scpb.ForeignKeyConstraint, *scpb.UniqueWithoutIndexConstraint,
				*scpb.PrimaryIndex, *scpb.SecondaryIndex:
				id, _ := screl.Schema.GetAttribute(screl.ConstraintID, e)
				cc.ConstraintID = id.(catid.ConstraintID)
			}
		})
		b.Add(cc)
	}
}
