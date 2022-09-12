// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package evalcatalog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/redact"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// UpdatableCommand matches update operations in postgres.
type UpdatableCommand tree.DInt

// The following constants are the values for UpdatableCommand enumeration.
const (
	UpdateCommand UpdatableCommand = 2 + iota
	InsertCommand
	DeleteCommand
)

var (
	nonUpdatableEvents = tree.NewDInt(0)
	allUpdatableEvents = tree.NewDInt((1 << UpdateCommand) | (1 << InsertCommand) | (1 << DeleteCommand))
)

// RedactDescriptor takes an encoded protobuf descriptor and returns the
// encoded protobuf after redaction.
func (b *Builtins) RedactDescriptor(ctx context.Context, encodedDescriptor []byte) ([]byte, error) {
	var desc descpb.Descriptor
	if err := protoutil.Unmarshal(encodedDescriptor, &desc); err != nil {
		return nil, err
	}
	// The descpb package has some strict rules about the timestamps in the
	// descriptors. The strictness was an attempt to make it difficult to read the
	// descriptor without setting the MVCC timestamp from the descriptor. In order
	// to make it safe to use the descriptor, we preempt the assertions by setting
	// a non-empty timestamp if there is no timestamp set. This is always safe.
	//
	// A downside of setting this timestamp is that it'll be set in the returned
	// descriptor. There is no existing uniform mechanism to reset the
	// ModificationTime across the protobuf descriptor oneof members, and this
	// seems like it's probably a good thing. At some point we may want to
	// consider replacing all of this policy with something more flexible. For
	// now, the silly sentinel timestamp should serve to make these descriptors
	// harder to use, which seems good.
	if mt := descpb.GetDescriptorModificationTime(&desc); mt.IsEmpty() {
		descpb.MaybeSetDescriptorModificationTimeFromMVCCTimestamp(
			&desc, hlc.Timestamp{Logical: 1},
		)
	}
	builder := descbuilder.NewBuilder(&desc)
	mut := builder.BuildCreatedMutable()
	if errs := redact.Redact(mut); len(errs) != 0 {
		var ret error
		for _, err := range errs {
			ret = errors.CombineErrors(ret, err)
		}
		return nil, ret
	}
	return protoutil.Marshal(mut.DescriptorProto())
}

// PGRelationIsUpdatable is part of the eval.CatalogBuiltins interface.
func (b *Builtins) PGRelationIsUpdatable(
	ctx context.Context, oidArg *tree.DOid,
) (*tree.DInt, error) {
	tableDesc, err := b.dc.GetImmutableTableByID(
		ctx, b.txn, descpb.ID(oidArg.Oid), tree.ObjectLookupFlagsWithRequired(),
	)
	if err != nil {
		// For postgres compatibility, it is expected that rather returning
		// an error this return nonUpdatableEvents (Zero) because there could
		// be oid references on deleted tables.
		if sqlerrors.IsUndefinedRelationError(err) {
			return nonUpdatableEvents, nil
		}
		return nonUpdatableEvents, err
	}
	if !tableDesc.IsTable() || tableDesc.IsVirtualTable() {
		return nonUpdatableEvents, nil
	}

	// pg_relation_is_updatable was created for compatibility. This
	// should return the update events the relation supports, but as crdb
	// does not support updatable views or foreign tables, right now this
	// basically return allEvents or none.
	return allUpdatableEvents, nil
}

// PGColumnIsUpdatable is part of the eval.CatalogBuiltins interface.
func (b *Builtins) PGColumnIsUpdatable(
	ctx context.Context, oidArg *tree.DOid, attNumArg tree.DInt,
) (*tree.DBool, error) {
	if attNumArg < 0 {
		// System columns are not updatable.
		return tree.DBoolFalse, nil
	}
	attNum := descpb.PGAttributeNum(attNumArg)
	tableDesc, err := b.dc.GetImmutableTableByID(ctx, b.txn, descpb.ID(oidArg.Oid), tree.ObjectLookupFlagsWithRequired())
	if err != nil {
		if sqlerrors.IsUndefinedRelationError(err) {
			// For postgres compatibility, it is expected that rather returning
			// an error this return nonUpdatableEvents (Zero) because there could
			// be oid references on deleted tables.
			return tree.DBoolFalse, nil
		}
		return nil, err
	}
	if !tableDesc.IsTable() || tableDesc.IsVirtualTable() {
		return tree.DBoolFalse, nil
	}

	column, err := tableDesc.FindColumnWithPGAttributeNum(attNum)
	if err != nil {
		if sqlerrors.IsUndefinedColumnError(err) {
			// When column does not exist postgres returns true.
			return tree.DBoolTrue, nil
		}
		return nil, err
	}

	// pg_column_is_updatable was created for compatibility. This
	// will return true if is a table (not virtual) and column is not
	// a computed column.
	return tree.MakeDBool(tree.DBool(!column.IsComputed())), nil
}
