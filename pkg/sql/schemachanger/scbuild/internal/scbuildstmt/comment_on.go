// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
		if _, _, tableComment := scpb.FindTableComment(tableElements); tableComment != nil {
			b.Drop(tableComment)
			b.LogEventForExistingTarget(tableComment)
		}
	} else {
		addComment(b, tableElements, &scpb.TableComment{Comment: *statement.Comment})
	}
}

// CommentOnType implements COMMENT ON TYPE xxx IS xxx statement.
func CommentOnType(b BuildCtx, statement *tree.CommentOnType) {
	typeElements := b.ResolveUserDefinedTypeType(statement.Name, commentResolveParams)
	name := statement.Name.ToTypeName()

	if _, _, enumType := scpb.FindEnumType(typeElements); enumType != nil {
		name.ObjectNamePrefix = b.NamePrefix(enumType)
	} else if _, _, compositeType := scpb.FindCompositeType(typeElements); compositeType != nil {
		name.ObjectNamePrefix = b.NamePrefix(compositeType)
	} else {
		panic(pgerror.New(pgcode.Syntax, "did not find composite type or enumerated type"))
	}

	statement.Name = name.ToUnresolvedObjectName()

	if statement.Comment == nil {
		dropComment(b, typeElements)
	} else {
		addComment(b, typeElements, &scpb.TypeComment{Comment: *statement.Comment})
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
	indexElements := b.ResolveIndexByName(&statement.Index, commentResolveParams)

	if statement.Comment == nil {
		dropComment(b, indexElements)
	} else {
		addComment(b, indexElements, &scpb.IndexComment{Comment: *statement.Comment})
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

func dropComment(b BuildCtx, elements *scpb.ElementCollection[scpb.Element]) {
	elements.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch e.(type) {
		case *scpb.DatabaseComment, *scpb.SchemaComment, *scpb.IndexComment,
			*scpb.ConstraintComment, *scpb.ColumnComment, *scpb.TypeComment:
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
		case *scpb.EnumType:
			comment.(*scpb.TypeComment).TypeID = object.TypeID
		case *scpb.CompositeType:
			comment.(*scpb.TypeComment).TypeID = object.TypeID
		case *scpb.Table:
			comment.(*scpb.TableComment).TableID = object.TableID
		case *scpb.Column:
			if reflect.TypeOf(comment) == reflect.TypeOf(&scpb.ColumnComment{}) {
				comment.(*scpb.ColumnComment).ColumnID = object.ColumnID
				comment.(*scpb.ColumnComment).PgAttributeNum = object.PgAttributeNum
			}
		}
	})

	b.Add(comment)
	b.LogEventForExistingTarget(comment)
}

func setIndexID(comment scpb.Element, e scpb.Element) {
	if reflect.TypeOf(comment) == reflect.TypeOf(&scpb.IndexComment{}) {
		id, _ := screl.Schema.GetAttribute(screl.IndexID, e)
		comment.(*scpb.IndexComment).IndexID = id.(catid.IndexID)
		comment.(*scpb.IndexComment).TableID = screl.GetDescID(e)
	}
}

func setConstraintID(comment scpb.Element, e scpb.Element) {
	if reflect.TypeOf(comment) == reflect.TypeOf(&scpb.ConstraintComment{}) {
		switch e.(type) {
		case *scpb.PrimaryIndex, *scpb.SecondaryIndex, *scpb.CheckConstraint,
			*scpb.ForeignKeyConstraint, *scpb.UniqueWithoutIndexConstraint:
			id, _ := screl.Schema.GetAttribute(screl.ConstraintID, e)
			comment.(*scpb.ConstraintComment).ConstraintID = id.(catid.ConstraintID)
		}
	}
}
