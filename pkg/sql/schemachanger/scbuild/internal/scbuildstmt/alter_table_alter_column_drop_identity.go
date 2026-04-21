// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func alterTableDropIdentity(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableDropIdentity,
) {
	alterColumnPreChecks(b, tn, tbl, t.Column)

	columnID := getColumnIDFromColumnName(b, tbl.TableID, t.Column, true /* required */)
	column := mustRetrieveColumnElem(b, tbl.TableID, columnID)
	panicIfSystemColumn(column, t.Column)

	identityType := retrieveColumnGeneratedAsIdentityType(b, tbl.TableID, columnID)
	if identityType == catpb.GeneratedAsIdentityType_NOT_IDENTITY_COLUMN {
		if t.IfExists {
			b.EvalCtx().ClientNoticeSender.BufferClientNotice(b,
				pgnotice.Newf("column %q of relation %q is not an identity column, skipping",
					tree.ErrString(&t.Column), tree.ErrString(&tn.ObjectName)),
			)
			return
		}
		panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"column %q of relation %q is not an identity column",
			tree.ErrString(&t.Column), tree.ErrString(&tn.ObjectName),
		))
	}

	// Find the sequence that backs this identity column. Identity columns
	// must be backed by exactly one sequence; SERIAL columns created with
	// serial_normalization=rowid don't have one and cannot have their
	// identity dropped.
	sequenceOwners := b.QueryByID(tbl.TableID).FilterSequenceOwner().Filter(
		func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.SequenceOwner) bool {
			return e.ColumnID == columnID
		})
	switch sequenceOwners.Size() {
	case 0:
		panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"identity column %q of relation %q is not backed by a sequence",
			tree.ErrString(&t.Column), tree.ErrString(&tn.ObjectName),
		))
	case 1:
	default:
		panic(errors.AssertionFailedf(
			"identity column %q of relation %q has %d sequences instead of 1",
			tree.ErrString(&t.Column), tree.ErrString(&tn.ObjectName), sequenceOwners.Size(),
		))
	}
	sequenceOwner := sequenceOwners.MustGetOneElement()

	// Verify nothing outside this column depends on the owned sequence.
	// The sequence's only legitimate backrefs are the ColumnDefaultExpression
	// and SequenceOwner of the column we're dropping identity from. Anything
	// else means another column references nextval(<seq>), and the legacy
	// canRemoveAllColumnOwnedSequences check would have rejected the drop.
	checkOwnedSequenceHasNoExternalDeps(b, tn, t.Column, columnID, sequenceOwner.SequenceID)

	// Drop the identity element. This may be nil for descriptors created
	// before V26_1, where the identity property was stored on the Column
	// element itself; fall back to mutating the Column element in that case.
	if identityElem := retrieveColumnGeneratedAsIdentityElem(b, tbl.TableID, columnID); identityElem != nil {
		b.Drop(identityElem)
	} else {
		// Pre-V26_1 descriptor: clear the identity fields by re-adding the
		// Column element with the identity bits stripped.
		b.Drop(column)
		newColumn := *column
		newColumn.GeneratedAsIdentityType = catpb.GeneratedAsIdentityType_NOT_IDENTITY_COLUMN
		newColumn.GeneratedAsIdentitySequenceOption = ""
		b.Add(&newColumn)
	}

	// Drop the implicit nextval default expression. This may be absent if
	// the column never had one wired up; tolerate that defensively.
	if defaultExpr := retrieveColumnDefaultExpressionElem(b, tbl.TableID, columnID); defaultExpr != nil {
		b.Drop(defaultExpr)
	}

	// Drop the sequence ownership link, then drop the sequence descriptor
	// itself. RESTRICT is correct because checkOwnedSequenceHasNoExternalDeps
	// already proved no other dependents exist.
	b.Drop(sequenceOwner)
	dropRestrictDescriptor(b, sequenceOwner.SequenceID)
}

// checkOwnedSequenceHasNoExternalDeps panics with a dependency error if the
// sequence is referenced by anything other than the SequenceOwner and
// ColumnDefaultExpression elements of the given column. This mirrors the
// behavior of canRemoveAllColumnOwnedSequences with DropDefault behavior in
// the legacy schema changer.
func checkOwnedSequenceHasNoExternalDeps(
	b BuildCtx,
	tn *tree.TableName,
	columnName tree.Name,
	columnID catid.ColumnID,
	sequenceID catid.DescID,
) {
	undroppedBackrefs(b, sequenceID).ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch elem := e.(type) {
		case *scpb.SequenceOwner:
			if elem.ColumnID == columnID {
				return
			}
		case *scpb.ColumnDefaultExpression:
			if elem.ColumnID == columnID {
				return
			}
		}
		seqName := b.QueryByID(sequenceID).FilterNamespace().MustGetOneElement().Name
		seqIdent := tree.Name(seqName)
		panic(errors.WithDetailf(
			pgerror.Newf(pgcode.DependentObjectsStillExist,
				"cannot drop sequence %s because other objects depend on it",
				tree.ErrString(&seqIdent),
			),
			"sequence is owned by column %s of relation %s",
			tree.ErrString(&columnName), tree.ErrString(&tn.ObjectName),
		))
	})
}
