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
	"reflect"

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
func CommentOnDatabase(b BuildCtx, statement *tree.CommentOnDatabase) {
	elements := b.ResolveDatabase(statement.Name, commentResolveParams)

	if statement.Comment == nil {
		dropComment(b, elements)
	} else {
		addComment(b, elements, &scpb.DatabaseComment{Comment: *statement.Comment})
	}
}

// CommentOnSchema implements COMMENT ON SCHEMA xxx IS xxx statement;
func CommentOnSchema(b BuildCtx, statement *tree.CommentOnSchema) {
	elements := b.ResolveSchema(statement.Name, commentResolveParams)

	if statement.Comment == nil {
		dropComment(b, elements)
	} else {
		addComment(b, elements, &scpb.SchemaComment{Comment: *statement.Comment})
	}
}

// CommentOnTable implements COMMENT ON TABLE xxx IS xxx statement.
func CommentOnTable(b BuildCtx, statement *tree.CommentOnTable) {
	tableElements := b.ResolveTable(statement.Table, commentResolveParams)
	_, _, table := scpb.FindTable(tableElements)
	name := statement.Table.ToTableName()

	if table == nil {
		b.MarkNameAsNonExistent(&name)
		return
	}

	name.ObjectNamePrefix = b.NamePrefix(table)
	statement.Table = name.ToUnresolvedObjectName()

	if statement.Comment == nil {
		dropComment(b, tableElements)
	} else {
		addComment(b, tableElements, &scpb.TableComment{Comment: *statement.Comment})
	}
}

// CommentOnColumn implements COMMENT ON COLUMN xxx IS xxx statement.
func CommentOnColumn(b BuildCtx, statement *tree.CommentOnColumn) {
	if statement.ColumnItem.TableName == nil {
		panic(pgerror.New(pgcode.Syntax, "column name must be qualified"))
	}
	tableElements := b.ResolveTable(statement.ColumnItem.TableName, commentResolveParams)
	_, _, table := scpb.FindTable(tableElements)
	columnElements := b.ResolveColumn(table.TableID, statement.ColumnItem.ColumnName, commentResolveParams)

	if statement.Comment == nil {
		dropComment(b, columnElements)
	} else {
		addComment(b, columnElements, &scpb.ColumnComment{TableID: table.TableID, Comment: *statement.Comment})
	}
}

// CommentOnIndex implements COMMENT ON INDEX xxx iS xxx statement.
func CommentOnIndex(b BuildCtx, statement *tree.CommentOnIndex) {
	var tableID catid.DescID
	indexElements := b.ResolveIndexByName(&statement.Index, commentResolveParams)
	indexElements.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch e.(type) {
		case *scpb.PrimaryIndex, *scpb.SecondaryIndex:
			tableID = screl.GetDescID(e)
		}
	})

	if statement.Comment == nil {
		dropComment(b, indexElements)
	} else {
		addComment(b, indexElements, &scpb.IndexComment{TableID: tableID, Comment: *statement.Comment})
	}
}

// CommentOnConstraint implements COMMENT ON CONSTRAINT xxx ON table_name IS xxx
// statement.
func CommentOnConstraint(b BuildCtx, statement *tree.CommentOnConstraint) {
	tableElements := b.ResolveTable(statement.Table, commentResolveParams)
	_, _, table := scpb.FindTable(tableElements)
	elements := b.ResolveConstraint(table.TableID, statement.Constraint, commentResolveParams)

	if statement.Comment == nil {
		dropComment(b, elements)
	} else {
		addComment(b, elements, &scpb.ConstraintComment{TableID: table.TableID, Comment: *statement.Comment})
	}
}

func dropComment(b BuildCtx, elements ElementResultSet) {
	elements.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch e.(type) {
		case *scpb.DatabaseComment, *scpb.SchemaComment, *scpb.IndexComment,
			*scpb.ConstraintComment, *scpb.ColumnComment, *scpb.TableComment:
			b.Drop(e)
			b.LogEventForExistingTarget(e)
		}
	})
}

func addComment(b BuildCtx, elements ElementResultSet, comment scpb.Element) {
	elements.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch object := e.(type) {
		case *scpb.Database:
			comment.(*scpb.DatabaseComment).DatabaseID = object.DatabaseID
		case *scpb.Schema:
			comment.(*scpb.SchemaComment).SchemaID = object.SchemaID
		case *scpb.PrimaryIndex, *scpb.SecondaryIndex:
			setIndexID(comment, object)
			setConstraintID(comment, object)
		case *scpb.CheckConstraint, *scpb.ForeignKeyConstraint, *scpb.UniqueWithoutIndexConstraint:
			setConstraintID(comment, object)
		case *scpb.Table:
			comment.(*scpb.TableComment).TableID = object.TableID
		case *scpb.Column:
			if reflect.TypeOf(comment) == reflect.TypeOf(&scpb.ColumnComment{}) {
				comment.(*scpb.ColumnComment).ColumnID = object.ColumnID
			}
		}
	})

	b.Add(comment)
	b.LogEventForExistingTarget(comment)
}

func setIndexID(comment scpb.Element, e scpb.Element) {
	if reflect.TypeOf(comment) == reflect.TypeOf(&scpb.IndexComment{}) {
		switch object := e.(type) {
		case *scpb.PrimaryIndex:
			comment.(*scpb.IndexComment).IndexID = object.IndexID
		case *scpb.SecondaryIndex:
			comment.(*scpb.IndexComment).IndexID = object.IndexID
		}
	}
}

func setConstraintID(comment scpb.Element, e scpb.Element) {
	if reflect.TypeOf(comment) == reflect.TypeOf(&scpb.ConstraintComment{}) {
		switch object := e.(type) {
		case *scpb.PrimaryIndex:
			comment.(*scpb.ConstraintComment).ConstraintID = object.ConstraintID
		case *scpb.SecondaryIndex:
			comment.(*scpb.ConstraintComment).ConstraintID = object.ConstraintID
		case *scpb.CheckConstraint:
			comment.(*scpb.ConstraintComment).ConstraintID = object.ConstraintID
		case *scpb.ForeignKeyConstraint:
			comment.(*scpb.ConstraintComment).ConstraintID = object.ConstraintID
		case *scpb.UniqueWithoutIndexConstraint:
			comment.(*scpb.ConstraintComment).ConstraintID = object.ConstraintID
		}
	}
}
