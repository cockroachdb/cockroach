// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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
		dbElements.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
			switch e.(type) {
			case *scpb.DatabaseComment:
				b.Drop(e)
				b.LogEventForExistingTarget(e)
			}
		})
	} else {
		dc := &scpb.DatabaseComment{
			Comment: *n.Comment,
		}
		dbElements.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
			switch t := e.(type) {
			case *scpb.Database:
				dc.DatabaseID = t.DatabaseID
			}
		})
		b.Add(dc)
		b.LogEventForExistingTarget(dc)
	}
}

// CommentOnSchema implements COMMENT ON SCHEMA xxx IS xxx statement;
func CommentOnSchema(b BuildCtx, n *tree.CommentOnSchema) {
	schemaElements := b.ResolveSchema(n.Name, commentResolveParams)

	if n.Comment == nil {
		schemaElements.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
			switch e.(type) {
			case *scpb.SchemaComment:
				b.Drop(e)
				b.LogEventForExistingTarget(e)
			}
		})
	} else {
		sc := &scpb.SchemaComment{
			Comment: *n.Comment,
		}
		schemaElements.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
			switch t := e.(type) {
			case *scpb.Schema:
				sc.SchemaID = t.SchemaID
			}
		})
		b.Add(sc)
		b.LogEventForExistingTarget(sc)
	}
}

// CommentOnTable implements COMMENT ON TABLE xxx IS xxx statement.
func CommentOnTable(b BuildCtx, n *tree.CommentOnTable) {
	tableElements := b.ResolveTable(n.Table, commentResolveParams)
	_, _, tbl := scpb.FindTable(tableElements)
	tbn := n.Table.ToTableName()
	if tbl == nil {
		b.MarkNameAsNonExistent(&tbn)
		return
	}
	tbn.ObjectNamePrefix = b.NamePrefix(tbl)
	n.Table = tbn.ToUnresolvedObjectName()

	if n.Comment == nil {
		_, _, tableComment := scpb.FindTableComment(tableElements)
		if tableComment != nil {
			b.Drop(tableComment)
			b.LogEventForExistingTarget(tableComment)
		}
	} else {
		_, _, table := scpb.FindTable(tableElements)
		tc := &scpb.TableComment{
			Comment: *n.Comment,
			TableID: table.TableID,
		}
		b.Add(tc)
		b.LogEventForExistingTarget(tc)
	}
}

// CommentOnColumn implements COMMENT ON COLUMN xxx IS xxx statement.
func CommentOnColumn(b BuildCtx, n *tree.CommentOnColumn) {
	if n.ColumnItem.TableName == nil {
		panic(pgerror.New(pgcode.Syntax, "column name must be qualified"))
	}
	tableElements := b.ResolveTable(n.ColumnItem.TableName, commentResolveParams)
	_, _, table := scpb.FindTable(tableElements)
	columnElements := b.ResolveColumn(table.TableID, n.ColumnItem.ColumnName, commentResolveParams)

	if n.Comment == nil {
		columnElements.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
			switch e.(type) {
			case *scpb.ColumnComment:
				b.Drop(e)
				b.LogEventForExistingTarget(e)
			}
		})
	} else {
		cc := &scpb.ColumnComment{
			TableID: table.TableID,
			Comment: *n.Comment,
		}
		columnElements.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
			switch t := e.(type) {
			case *scpb.Column:
				cc.ColumnID = t.ColumnID
				cc.PgAttributeNum = t.PgAttributeNum
			}
		})
		b.Add(cc)
		b.LogEventForExistingTarget(cc)
	}
}

// CommentOnIndex implements COMMENT ON INDEX xxx iS xxx statement.
func CommentOnIndex(b BuildCtx, n *tree.CommentOnIndex) {
	var tableID catid.DescID
	indexElements := b.ResolveIndexByName(&n.Index, commentResolveParams)
	indexElements.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch e.(type) {
		case *scpb.PrimaryIndex, *scpb.SecondaryIndex:
			tableID = screl.GetDescID(e)
		}
	})

	if n.Comment == nil {
		indexElements.ForEach(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
			switch e.(type) {
			case *scpb.IndexComment:
				b.Drop(e)
				b.LogEventForExistingTarget(e)
			}
		})
	} else {
		ic := &scpb.IndexComment{
			TableID: tableID,
			Comment: *n.Comment,
		}
		indexElements.ForEach(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
			switch e.(type) {
			case *scpb.PrimaryIndex, *scpb.SecondaryIndex:
				id, _ := screl.Schema.GetAttribute(screl.IndexID, e)
				ic.IndexID = id.(catid.IndexID)
			}
		})
		b.Add(ic)
		b.LogEventForExistingTarget(ic)
	}
}

// CommentOnConstraint implements COMMENT ON CONSTRAINT xxx ON table_name IS xxx
// statement.
func CommentOnConstraint(b BuildCtx, n *tree.CommentOnConstraint) {
	tableElements := b.ResolveTable(n.Table, commentResolveParams)
	_, _, table := scpb.FindTable(tableElements)
	constraintElements := b.ResolveConstraint(table.TableID, n.Constraint, commentResolveParams)

	if n.Comment == nil {
		constraintElements.ForEach(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
			switch e.(type) {
			case *scpb.ConstraintComment:
				b.Drop(e)
				b.LogEventForExistingTarget(e)
			}
		})
	} else {
		cc := &scpb.ConstraintComment{
			Comment: *n.Comment,
			TableID: table.TableID,
		}
		constraintElements.ForEach(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
			switch e.(type) {
			case *scpb.CheckConstraint, *scpb.ForeignKeyConstraint, *scpb.UniqueWithoutIndexConstraint,
				*scpb.PrimaryIndex, *scpb.SecondaryIndex:
				id, _ := screl.Schema.GetAttribute(screl.ConstraintID, e)
				cc.ConstraintID = id.(catid.ConstraintID)
			}
		})
		b.Add(cc)
		b.LogEventForExistingTarget(cc)
	}
}
