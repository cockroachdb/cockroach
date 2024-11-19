// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package evalcatalog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/redact"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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
func (b *Builtins) RedactDescriptor(_ context.Context, encodedDescriptor []byte) ([]byte, error) {
	var descProto descpb.Descriptor
	if err := protoutil.Unmarshal(encodedDescriptor, &descProto); err != nil {
		return nil, err
	}
	if errs := redact.Redact(&descProto); len(errs) != 0 {
		var ret error
		for _, err := range errs {
			ret = errors.CombineErrors(ret, err)
		}
		return nil, ret
	}
	return protoutil.Marshal(&descProto)
}

func (b *Builtins) RepairedDescriptor(
	_ context.Context,
	encodedDescriptor []byte,
	descIDMightExist func(id descpb.ID) bool,
	nonTerminalJobIDMightExist func(id jobspb.JobID) bool,
	roleExists func(username username.SQLUsername) bool,
) ([]byte, error) {
	// Use the largest-possible timestamp as a sentinel value when decoding the
	// descriptor bytes. This will be used to set the modification time field in
	// the built descriptor without triggering any errors. We need to remove it
	// before re-encoding the descriptor, because it's nonsensical and it's what
	// the descriptor collection does anyway when persisting a descriptor to
	// storage.
	var mvccTimestampSentinel = hlc.MaxTimestamp
	db, err := descbuilder.FromBytesAndMVCCTimestamp(encodedDescriptor, mvccTimestampSentinel)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, pgerror.New(pgcode.InvalidBinaryRepresentation, "empty descriptor")
	}
	if err := db.StripDanglingBackReferences(descIDMightExist, nonTerminalJobIDMightExist); err != nil {
		return nil, err
	}
	if err := db.StripNonExistentRoles(roleExists); err != nil {
		return nil, err
	}
	mut := db.BuildCreatedMutable()
	// Strip the sentinel value from the descriptor.
	if mvccTimestampSentinel.Equal(mut.GetModificationTime()) {
		mut.ResetModificationTime()
	}
	return protoutil.Marshal(mut.DescriptorProto())
}

// PGRelationIsUpdatable is part of the eval.CatalogBuiltins interface.
func (b *Builtins) PGRelationIsUpdatable(
	ctx context.Context, oidArg *tree.DOid,
) (*tree.DInt, error) {
	tableDesc, err := b.dc.ByIDWithLeased(b.txn).WithoutNonPublic().Get().Table(ctx, descpb.ID(oidArg.Oid))
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
	tableDesc, err := b.dc.ByIDWithLeased(b.txn).WithoutNonPublic().Get().Table(ctx, descpb.ID(oidArg.Oid))
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

	column := catalog.FindColumnByPGAttributeNum(tableDesc, attNum)
	if column == nil {
		// When column does not exist postgres returns true.
		return tree.DBoolTrue, nil
	}
	// pg_column_is_updatable was created for compatibility. This
	// will return true if is a table (not virtual) and column is not
	// a computed column.
	return tree.MakeDBool(tree.DBool(!column.IsComputed())), nil
}
