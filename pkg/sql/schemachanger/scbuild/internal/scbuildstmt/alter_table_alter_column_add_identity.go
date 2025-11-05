// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func alterTableAddIdentity(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableAddIdentity,
) {
	alterColumnPreChecks(b, tn, tbl, t.Column)
	colElems := b.ResolveColumn(tbl.TableID, t.Column, ResolveParams{
		RequiredPrivilege: privilege.CREATE,
	})
	colElem := colElems.FilterColumn().MustGetOneElement()
	columnID := colElem.ColumnID
	// Block alters on system columns.
	panicIfSystemColumn(colElem, t.Column)
	colTypeElem := mustRetrieveColumnTypeElem(b, tbl.TableID, columnID)
	// Ensure that column is an integer
	if colTypeElem.Type == nil || colTypeElem.Type.InternalType.Family != types.IntFamily {
		panic(pgerror.Newf(
			pgcode.InvalidParameterValue,
			"column %q of relation %q type must be an integer type", t.Column, tn.ObjectName))
	}
	// Ensure that column is not already an identity column
	if isColumnGeneratedAsIdentity(b, tbl.TableID, columnID) {
		panic(pgerror.Newf(
			pgcode.InvalidParameterValue,
			"column %q of relation %q is already an identity column", t.Column, tn.ObjectName))
	}
	// Ensure that column does not have a default expression
	defaultExpr := retrieveColumnDefaultExpressionElem(b, tbl.TableID, columnID)
	if defaultExpr != nil {
		panic(pgerror.Newf(
			pgcode.ObjectNotInPrerequisiteState,
			"column %q of relation %q already has a default value", t.Column, tn.ObjectName))
	}
	// Ensure that column does not have a compute expression
	computeExpr := retrieveColumnComputeExpression(b, tbl.TableID, columnID)
	if computeExpr != nil {
		panic(pgerror.Newf(
			pgcode.ObjectNotInPrerequisiteState,
			"column %q of relation %q already has a computed value", t.Column, tn.ObjectName))
	}
	// Ensure that column is declared as not null
	columNotNull := b.QueryByID(tbl.TableID).FilterColumnNotNull().Filter(
		func(current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnNotNull) bool {
			return e.ColumnID == columnID
		}).MustGetZeroOrOneElement()
	if columNotNull == nil {
		panic(pgerror.Newf(
			pgcode.ObjectNotInPrerequisiteState,
			"column %q of relation %q must be declared NOT NULL before identity can be added",
			t.Column, tn.ObjectName))
	}
	// Ensure that column does not have a on update expression
	onUpdate := retrieveColumnOnUpdateExpressionElem(b, tbl.TableID, columnID)
	if onUpdate != nil {
		panic(pgerror.Newf(
			pgcode.ObjectNotInPrerequisiteState,
			"column %q of relation %q already has an update expression", t.Column, tn.ObjectName))
	}
	// Create column definition for identity column
	q := []tree.NamedColumnQualification{{Qualification: t.Qualification}}
	colDef, err := tree.NewColumnTableDef(t.Column, colTypeElem.Type, false /* isSerial */, q)
	if err != nil {
		// panic internally
		return
	}
	// 0. Get type and seqopts
	colDef.GeneratedIdentity.IsGeneratedAsIdentity = true
	var identityType catpb.GeneratedAsIdentityType
	switch q := (t.Qualification).(type) {
	case *tree.GeneratedAlwaysAsIdentity:
		identityType = catpb.GeneratedAsIdentityType_GENERATED_ALWAYS
		colDef.GeneratedIdentity.GeneratedAsIdentityType = tree.GeneratedAlways
		colDef.GeneratedIdentity.SeqOptions = q.SeqOptions
	case *tree.GeneratedByDefAsIdentity:
		identityType = catpb.GeneratedAsIdentityType_GENERATED_BY_DEFAULT
		colDef.GeneratedIdentity.GeneratedAsIdentityType = tree.GeneratedByDefault
		colDef.GeneratedIdentity.SeqOptions = q.SeqOptions
	}

	// 1. create a sequence and default exression
	colDef, expr := alterTableAddColumnSerialOrGeneratedIdentity(b, colDef, tn)
	// 2. add the sequence owner
	b.Add(&scpb.SequenceOwner{
		SequenceID: expr.UsesSequenceIDs[0],
		TableID:    tbl.TableID,
		ColumnID:   columnID,
	})
	// 3. add the default expression
	b.Add(&scpb.ColumnDefaultExpression{
		TableID:    tbl.TableID,
		ColumnID:   columnID,
		Expression: *expr,
	})
	// ?? b.IncrementSchemaChangeAddColumnQualificationCounter("default_expr")
	// 4. add the GeneratedAsidentity element
	b.Add(&scpb.ColumnGeneratedAsIdentity{
		TableID:        tbl.TableID,
		ColumnID:       columnID,
		Type:           identityType,
		SequenceOption: tree.Serialize(&colDef.GeneratedIdentity.SeqOptions),
	})
}
