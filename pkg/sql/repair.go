// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"encoding/hex"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// UnsafeUpsertDescriptor powers the repair builtin of the same name. The idea
// is that it should be used only by an administrator in the most dire of
// circumstances. It exists for two practical but perhaps unfortunate reasons.
// Firstly, the sql schema of the descriptor table does not match the way the
// table is actually modified. Specifically, only one column family is every
// populated and code elsewhere assumes that this is the case. Secondly, we
// don't generally want users writing to that table.
//
// This method will perform *some* validation of the descriptor. Namely, it
// will ensure that if a version currently exists, that the upserted descriptor
// is of the subsequent version. This method may only be used to update any
// individual descriptor one time per transaction and may not be used to
// interact with a descriptor that has already been modified in the transaction.
// Note however that it may be used any number of times in a transaction to
// write to different descriptors. It will also validate the structure of the
// descriptor but not its references.
//
// TODO(ajwerner): It is critical that we not validate all of the relevant
// descriptors during statement execution as it may be the case that more than
// one descriptor is corrupt. Instead, we should validate all of the relevant
// descriptors just prior to committing the transaction. This would bring the
// requirement that if a descriptor is upserted, that it leave the database in
// a valid state, at least in terms of that descriptor and its references.
// Perhaps transactions which do end up using this should also end up validating
// all descriptors at the end of the transaction to ensure that this operation
// didn't break a reference to this descriptor.
func (p *planner) UnsafeUpsertDescriptor(
	ctx context.Context, descID int64, encodedDesc []byte, force bool,
) error {
	const method = "crdb_internal.unsafe_upsert_descriptor()"
	if err := checkPlannerStateForRepairFunctions(ctx, p, method); err != nil {
		return err
	}

	id := descpb.ID(descID)
	var desc descpb.Descriptor
	if err := protoutil.Unmarshal(encodedDesc, &desc); err != nil {
		return pgerror.Wrapf(err, pgcode.InvalidTableDefinition, "failed to decode descriptor")
	}

	// Fetch the existing descriptor.

	existing, err := p.Descriptors().GetMutableDescriptorByID(ctx, id, p.txn)
	var forceNoticeString string // for the event
	if !errors.Is(err, catalog.ErrDescriptorNotFound) && err != nil {
		if force {
			notice := pgnotice.NewWithSeverityf("WARNING",
				"failed to retrieve existing descriptor, continuing with force flag: %v", err)
			p.SendClientNotice(ctx, notice)
			forceNoticeString = notice.Error()
		} else {
			return err
		}
	}

	// Validate that existing is sane and store its hex serialization into
	// existingStr to be written to the event log.
	var existingStr string
	if existing != nil {
		if existing.IsUncommittedVersion() {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"cannot modify a modified descriptor (%d) with UnsafeUpsertDescriptor", id)
		}
		version := descpb.GetDescriptorVersion(&desc)
		if version != existing.GetVersion() && version != existing.GetVersion()+1 {
			return pgerror.Newf(pgcode.InvalidTableDefinition, "mismatched descriptor version, expected %v or %v, got %v",
				version, version+1, existing.GetVersion())
		}
		marshaled, err := protoutil.Marshal(existing.DescriptorProto())
		if err != nil {
			return errors.AssertionFailedf("failed to marshal existing descriptor %v: %v", existing, err)
		}
		existingStr = hex.EncodeToString(marshaled)
	}

	switch md := existing.(type) {
	case *tabledesc.Mutable:
		md.TableDescriptor = *desc.GetTable() // nolint:descriptormarshal
	case *schemadesc.Mutable:
		md.SchemaDescriptor = *desc.GetSchema()
	case *dbdesc.Mutable:
		md.DatabaseDescriptor = *desc.GetDatabase()
	case *typedesc.Mutable:
		md.TypeDescriptor = *desc.GetType()
	case nil:
		// nolint:descriptormarshal
		if tableDesc := desc.GetTable(); tableDesc != nil {
			existing = tabledesc.NewCreatedMutable(*tableDesc)
		} else if schemaDesc := desc.GetSchema(); schemaDesc != nil {
			existing = schemadesc.NewCreatedMutable(*schemaDesc)
		} else if dbDesc := desc.GetDatabase(); dbDesc != nil {
			existing = dbdesc.NewCreatedMutable(*dbDesc)
		} else if typeDesc := desc.GetType(); typeDesc != nil {
			existing = typedesc.NewCreatedMutable(*typeDesc)
		} else {
			return pgerror.New(pgcode.InvalidTableDefinition, "invalid ")
		}
	default:
		return errors.AssertionFailedf("unknown descriptor type %T for id %d", existing, id)
	}
	{
		b := p.txn.NewBatch()
		if err := p.Descriptors().WriteDescToBatch(
			ctx, p.extendedEvalCtx.Tracing.KVTracingEnabled(), existing, b,
		); err != nil {
			return err
		}
		if err := p.txn.Run(ctx, b); err != nil {
			return err
		}
	}

	return MakeEventLogger(p.execCfg).InsertEventRecord(
		ctx,
		p.txn,
		EventLogUnsafeUpsertDescriptor,
		int32(id),
		int32(p.EvalContext().NodeID.SQLInstanceID()),
		&struct {
			ID                 descpb.ID `json:"id"`
			ExistingDescriptor string    `json:"existing_descriptor,omitempty"`
			Descriptor         string    `json:"descriptor,omitempty"`
			Force              bool      `json:"force,omitempty"`
			ValidationErrors   string    `json:"validation_errors,omitempty"`
		}{
			ID:                 id,
			ExistingDescriptor: existingStr,
			Descriptor:         hex.EncodeToString(encodedDesc),
			Force:              force,
			ValidationErrors:   forceNoticeString,
		})
}

// UnsafeUpsertNamespaceEntry powers the repair builtin of the same name. The
// idea is that it should be used only by an administrator in the most dire of
// circumstances. It exists for two practical but perhaps unfortunate reasons.
// Firstly, the sql schema of the namespace table does not match the way the
// table is actually modified. Specifically, only one column family is every
// populated and code elsewhere assumes that this is the case. Secondly, we
// don't generally want users writing to that table.
//
// If force is true, most validation will be attached to the event log entry but
// will not lead to an error.
//
// This method will perform *some* validation of the namespace entry. Namely, it
// will ensure that the new entry corresponds to a non-dropped descriptor and
// that the parents exist appropriately for the type of descriptor.
func (p *planner) UnsafeUpsertNamespaceEntry(
	ctx context.Context,
	parentIDInt, parentSchemaIDInt int64,
	name string,
	descIDInt int64,
	force bool,
) error {
	const method = "crdb_internal.unsafe_upsert_namespace_entry()"
	if err := checkPlannerStateForRepairFunctions(ctx, p, method); err != nil {
		return err
	}
	parentID, parentSchemaID, descID := descpb.ID(parentIDInt), descpb.ID(parentSchemaIDInt), descpb.ID(descIDInt)
	key := catalogkeys.MakeNameMetadataKey(p.execCfg.Codec, parentID, parentSchemaID, name)
	val, err := p.txn.Get(ctx, key)
	if err != nil {
		return errors.Wrapf(err, "failed to read namespace entry (%d, %d, %s)",
			parentID, parentSchemaID, name)
	}

	// TODO(ajwerner): Validate properties of the existing entry if its descriptor
	// exists.
	var existingID descpb.ID
	if val.Value != nil {
		existingID = descpb.ID(val.ValueInt())
	}
	validateDescriptor := func() error {
		desc, err := p.Descriptors().GetMutableDescriptorByID(ctx, descID, p.txn)
		if err != nil && descID != keys.PublicSchemaID {
			return errors.Wrapf(err, "failed to retrieve descriptor %d", descID)
		}
		switch desc.(type) {
		case nil:
			return nil
		case *tabledesc.Mutable, *typedesc.Mutable:
			if parentID == 0 || parentSchemaID == 0 {
				return pgerror.Newf(pgcode.InvalidCatalogName,
					"invalid prefix (%d, %d) for object %d",
					parentID, parentSchemaID, descID)
			}
		case *schemadesc.Mutable:
			if parentID == 0 || parentSchemaID != 0 {
				return pgerror.Newf(pgcode.InvalidCatalogName,
					"invalid prefix (%d, %d) for schema %d",
					parentID, parentSchemaID, descID)
			}
		case *dbdesc.Mutable:
			if parentID != 0 || parentSchemaID != 0 {
				return pgerror.Newf(pgcode.InvalidCatalogName,
					"invalid prefix (%d, %d) for database %d",
					parentID, parentSchemaID, descID)
			}
		default:
			// The public schema does not have a descriptor.
			if descID == keys.PublicSchemaID {
				return nil
			}
			return errors.AssertionFailedf(
				"unexpected descriptor type %T for descriptor %d", desc, descID)
		}
		return nil
	}
	validateParentDescriptor := func() error {
		if parentID == 0 {
			return nil
		}
		parent, err := p.Descriptors().GetMutableDescriptorByID(
			ctx, parentID, p.txn,
		)
		if err != nil {
			return errors.Wrapf(err, "failed to look up parent %d", parentID)
		}
		if _, isDatabase := parent.(catalog.DatabaseDescriptor); !isDatabase {
			return pgerror.Newf(pgcode.InvalidCatalogName,
				"parentID %d is a %T, not a database", parentID, parent)
		}
		return nil
	}
	validateParentSchemaDescriptor := func() error {
		if parentSchemaID == 0 || parentSchemaID == keys.PublicSchemaID {
			return nil
		}
		schema, err := p.Descriptors().GetMutableDescriptorByID(
			ctx, parentSchemaID, p.txn,
		)
		if err != nil {
			return err
		}
		if _, isSchema := schema.(catalog.SchemaDescriptor); !isSchema {
			return pgerror.Newf(pgcode.InvalidCatalogName,
				"parentSchemaID %d is a %T, not a schema", parentSchemaID, schema)
		}
		return nil
	}

	// validationErr will hold combined errors if force is true.
	var validationErr error
	for _, f := range []func() error{
		validateDescriptor,
		validateParentDescriptor,
		validateParentSchemaDescriptor,
	} {
		if err := f(); err != nil && force {
			validationErr = errors.CombineErrors(validationErr, err)
		} else if err != nil {
			return err
		}
	}
	if err := p.txn.Put(ctx, key, descID); err != nil {
		return err
	}
	var validationErrStr string
	if validationErr != nil {
		validationErrStr = validationErr.Error()
	}
	return MakeEventLogger(p.execCfg).InsertEventRecord(
		ctx,
		p.txn,
		EventLogUnsafeUpsertNamespaceEntry,
		int32(descID),
		int32(p.EvalContext().NodeID.SQLInstanceID()),
		&struct {
			ParentID         descpb.ID `json:"parent_id,omitempty"`
			ParentSchemaID   descpb.ID `json:"parent_schema_id,omitempty"`
			Name             string    `json:"name"`
			ID               descpb.ID `json:"id"`
			ExistingID       descpb.ID `json:"existing_id,omitempty"`
			Force            bool      `json:"force,omitempty"`
			ValidationErrors string    `json:"validation_errors,omitempty"`
		}{
			ParentID:         parentID,
			ParentSchemaID:   parentSchemaID,
			ID:               descID,
			Name:             name,
			ExistingID:       existingID,
			Force:            force,
			ValidationErrors: validationErrStr,
		})
}

// UnsafeDeleteNamespaceEntry powers the repair builtin of the same name. The
// idea is that it should be used only by an administrator in the most dire of
// circumstances. It exists to empower administrators to perform repair.
//
// This method will perform *some* validation of the namespace entry. Namely, it
// will ensure that the entry does not correspond to a non-dropped descriptor
// and that the entry exists with the provided ID.
func (p *planner) UnsafeDeleteNamespaceEntry(
	ctx context.Context,
	parentIDInt, parentSchemaIDInt int64,
	name string,
	descIDInt int64,
	force bool,
) error {
	const method = "crdb_internal.unsafe_delete_namespace_entry()"
	if err := checkPlannerStateForRepairFunctions(ctx, p, method); err != nil {
		return err
	}
	parentID, parentSchemaID, descID := descpb.ID(parentIDInt), descpb.ID(parentSchemaIDInt), descpb.ID(descIDInt)
	key := catalogkeys.MakeNameMetadataKey(p.execCfg.Codec, parentID, parentSchemaID, name)
	val, err := p.txn.Get(ctx, key)
	if err != nil {
		return errors.Wrapf(err, "failed to read namespace entry (%d, %d, %s)",
			parentID, parentSchemaID, name)
	}
	if val.Value == nil {
		// Perhaps this is not the best pgcode but it's something.
		return pgerror.Newf(pgcode.InvalidCatalogName,
			"no namespace entry exists for (%d, %d, %s)",
			parentID, parentSchemaID, name)
	}
	if val.Value != nil {
		existingID := descpb.ID(val.ValueInt())
		if existingID != descID {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"namespace entry for (%d, %d, %s) has id %d, not %d",
				parentID, parentSchemaID, name, existingID, descID)
		}
	}
	desc, err := p.Descriptors().GetMutableDescriptorByID(ctx, descID, p.txn)
	var forceNoticeString string // for the event
	if err != nil && !errors.Is(err, catalog.ErrDescriptorNotFound) {
		if force {
			notice := pgnotice.NewWithSeverityf("WARNING",
				"failed to retrieve existing descriptor, continuing with force flag: %v", err)
			p.SendClientNotice(ctx, notice)
			forceNoticeString = notice.Error()
		} else {
			return errors.Wrapf(err, "failed to retrieve descriptor %d", descID)
		}
	}
	if err == nil && !desc.Dropped() {
		return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"refusing to delete namespace entry for non-dropped descriptor")
	}
	if err := p.txn.Del(ctx, key); err != nil {
		return errors.Wrap(err, "failed to delete entry")
	}
	return MakeEventLogger(p.execCfg).InsertEventRecord(
		ctx,
		p.txn,
		EventLogUnsafeDeleteNamespaceEntry,
		int32(descID),
		int32(p.EvalContext().NodeID.SQLInstanceID()),
		&struct {
			ParentID         descpb.ID `json:"parent_id,omitempty"`
			ParentSchemaID   descpb.ID `json:"parent_schema_id,omitempty"`
			Name             string    `json:"name"`
			ID               descpb.ID `json:"id"`
			ExistingID       descpb.ID `json:"existing_id,omitempty"`
			Force            bool      `json:"force,omitempty"`
			ValidationErrors string    `json:"validation_errors,omitempty"`
		}{
			ParentID:         parentID,
			ParentSchemaID:   parentSchemaID,
			ID:               descID,
			Name:             name,
			Force:            force,
			ValidationErrors: forceNoticeString,
		})
}

// UnsafeDeleteDescriptor powers the repair builtin of the same name. The
// idea is that it should be used only by an administrator in the most dire of
// circumstances. It exists to empower administrators to perform repair.
//
// This method will perform very minimal validation. An error will be returned
// if no such descriptor exists. This method can very easily introduce
// corruption, beware.
func (p *planner) UnsafeDeleteDescriptor(ctx context.Context, descID int64, force bool) error {
	const method = "crdb_internal.unsafe_delete_descriptor()"
	if err := checkPlannerStateForRepairFunctions(ctx, p, method); err != nil {
		return err
	}
	id := descpb.ID(descID)
	mut, err := p.Descriptors().GetMutableDescriptorByID(ctx, id, p.txn)
	var forceNoticeString string // for the event
	if err != nil {
		if !errors.Is(err, catalog.ErrDescriptorNotFound) && force {
			notice := pgnotice.NewWithSeverityf("WARNING",
				"failed to retrieve existing descriptor, continuing with force flag: %v", err)
			p.SendClientNotice(ctx, notice)
			forceNoticeString = notice.Error()
		} else {
			return err
		}
	}
	descKey := catalogkeys.MakeDescMetadataKey(p.execCfg.Codec, id)
	if err := p.txn.Del(ctx, descKey); err != nil {
		return err
	}
	ev := struct {
		ParentID         descpb.ID `json:"parent_id,omitempty"`
		ParentSchemaID   descpb.ID `json:"parent_schema_id,omitempty"`
		Name             string    `json:"name"`
		ID               descpb.ID `json:"id"`
		Force            bool      `json:"force,omitempty"`
		ValidationErrors string    `json:"validation_errors,omitempty"`
	}{
		ID:               id,
		Force:            force,
		ValidationErrors: forceNoticeString,
	}
	if mut != nil {
		ev.ParentID = mut.GetParentID()
		ev.ParentSchemaID = mut.GetParentSchemaID()
		ev.Name = mut.GetName()
	}
	return MakeEventLogger(p.execCfg).InsertEventRecord(
		ctx,
		p.txn,
		EventLogUnsafeDeleteDescriptor,
		int32(descID),
		int32(p.EvalContext().NodeID.SQLInstanceID()),
		ev,
	)
}

func checkPlannerStateForRepairFunctions(ctx context.Context, p *planner, method string) error {
	if p.extendedEvalCtx.TxnReadOnly {
		return readOnlyError(method)
	}
	hasAdmin, err := p.UserHasAdminRole(ctx, p.User())
	if err != nil {
		return err
	}
	if !hasAdmin {
		return pgerror.Newf(pgcode.InsufficientPrivilege, "admin role required for %s", method)
	}
	return nil
}
