// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func alterTableAlterColumnIdentity(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, stmt tree.Statement, t *tree.AlterTableIdentity,
) {
	alterColumnPreChecks(b, tn, tbl, t.Column)

	columnID := getColumnIDFromColumnName(b, tbl.TableID, t.Column, true /* required */)
	// Block alters on system columns.
	panicIfSystemColumn(mustRetrieveColumnElem(b, tbl.TableID, columnID), t.Column)

	sequenceOwner := b.QueryByID(tbl.TableID).FilterSequenceOwner().Filter(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.SequenceOwner) bool {
		return e.TableID == tbl.TableID && e.ColumnID == columnID
	}).MustGetZeroOrOneElement()

	if sequenceOwner == nil {
		panic(pgerror.Newf(
			pgcode.FeatureNotSupported,
			"cannot alter identity of a non-sequence column %q", tree.ErrString(&t.Column)))
	}

	newOpts := descpb.TableDescriptor_SequenceOpts{
		Increment: 1,
	}
	if err := schemaexpr.AssignSequenceOptions(&newOpts,
		t.SeqOptions,
		64,
		true,
		nil,
	); err != nil {
		panic(err)
	}

	defaultOpts := descpb.TableDescriptor_SequenceOpts{
		Increment: 1,
	}
	if err := schemaexpr.AssignSequenceOptions(&defaultOpts,
		nil,
		64,
		true,
		nil,
	); err != nil {
		panic(err)
	}

	updateSequenceOption := func(key string, defaultValue, value interface{}) {
		newSeqOption := scpb.SequenceOption{
			SequenceID: sequenceOwner.SequenceID,
			Key:        key,
			Value:      fmt.Sprintf("%v", value),
		}

		oldSeqOption := b.QueryByID(sequenceOwner.SequenceID).FilterSequenceOption().Filter(func(current scpb.Status, target scpb.TargetStatus, e *scpb.SequenceOption) bool {
			return e.Key == key
		}).MustGetZeroOrOneElement()
		if oldSeqOption != nil {
			// Skip a noop update.
			if oldSeqOption.Value == newSeqOption.Value {
				return
			}
			b.Drop(oldSeqOption)
		}

		// Skip setting to default values.
		if reflect.DeepEqual(defaultValue, value) {
			return
		}

		b.Add(&newSeqOption)
	}

	updateSequenceOption(tree.SeqOptIncrement, defaultOpts.Increment, newOpts.Increment)
	// updateSequenceOption(tree.SeqOptMinValue, defaultOpts.MinValue, newOpts.MinValue)
	// updateSequenceOption(tree.SeqOptMaxValue, defaultOpts.MaxValue, newOpts.MaxValue)
	// updateSequenceOption(tree.SeqOptStart, defaultOpts.Start, newOpts.Start)
	// updateSequenceOption(tree.SeqOptVirtual, defaultOpts.Virtual, newOpts.Virtual)
	// updateSequenceOption(tree.SeqOptCacheSession, defaultOpts.SessionCacheSize, newOpts.SessionCacheSize)
	// updateSequenceOption(tree.SeqOptCacheNode, defaultOpts.NodeCacheSize, newOpts.NodeCacheSize)
	// updateSequenceOption(tree.SeqOptAs, defaultOpts.AsIntegerType, newOpts.AsIntegerType)
}
