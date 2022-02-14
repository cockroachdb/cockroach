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
	"bytes"
	"context"
	"encoding/hex"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
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
// It is critical that we not validate all of the relevant descriptors during
// statement execution as it may be the case that more than one descriptor is
// corrupt. Instead, we rely on ValidateTxnCommit which runs just prior to
// committing any transaction. This brings the requirement that if a descriptor
// is to be upserted, it must leave the database in a valid state, at least in
// terms of that descriptor and its references. This validation can be disabled
// via the `sql.catalog.descs.validate_on_write.enabled` cluster setting if need
// be, even though such a need is rather not obvious to foresee.
func (p *planner) UnsafeUpsertDescriptor(
	ctx context.Context, descID int64, encodedDesc []byte, force bool,
) error {
	const method = "crdb_internal.unsafe_upsert_descriptor()"
	ev := eventpb.UnsafeUpsertDescriptor{Force: force}
	if err := checkPlannerStateForRepairFunctions(ctx, p, method); err != nil {
		return err
	}

	id := descpb.ID(descID)
	var desc descpb.Descriptor
	if err := protoutil.Unmarshal(encodedDesc, &desc); err != nil {
		return pgerror.Wrapf(err, pgcode.InvalidObjectDefinition, "failed to decode descriptor")
	}
	newID, newVersion, _, _, newModTime, err := descpb.GetDescriptorMetadata(&desc)
	if err != nil {
		return pgerror.Wrapf(err, pgcode.InvalidObjectDefinition, "invalid descriptor")
	}
	if newID != id {
		if !force {
			return pgerror.Newf(pgcode.InvalidObjectDefinition, "invalid descriptor ID %d, expected %d", newID, id)
		}
		newID = id
	}

	// Fetch the existing descriptor, if it exists.
	mut, notice, err := unsafeReadDescriptor(ctx, p, id, force)
	if err != nil {
		return err
	}
	if notice != nil {
		ev.ForceNotice = notice.Error()
	}

	// Validate that existing is sane and store its hex serialization into
	// existingStr to be written to the event log.
	var existingProto *descpb.Descriptor
	var existingVersion descpb.DescriptorVersion
	var existingModTime hlc.Timestamp
	var previousOwner string
	var previousUserPrivileges []catpb.UserPrivileges
	if mut != nil {
		if mut.IsUncommittedVersion() {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"cannot modify a modified descriptor (%d) with UnsafeUpsertDescriptor", id)
		}
		existingProto = protoutil.Clone(mut.DescriptorProto()).(*descpb.Descriptor)
		existingVersion = mut.GetVersion()
		existingModTime = mut.GetModificationTime()
		previousOwner = mut.GetPrivileges().Owner().Normalized()
		previousUserPrivileges = mut.GetPrivileges().Users
	}

	// Check version validity.
	if newVersion != existingVersion+1 {
		if !force {
			return pgerror.Newf(pgcode.InvalidObjectDefinition, "invalid new descriptor version %d, expected %v",
				newVersion, existingVersion+1)
		}
		newVersion = existingVersion + 1
	}
	if newModTime.IsEmpty() {
		if newVersion > 1 && existingModTime.IsEmpty() {
			return pgerror.Newf(pgcode.InvalidObjectDefinition, "missing modification time in updated descriptor with version %d",
				newVersion)
		}
		// Override the modification time in all cases to prevent panics.
		// It will be reset to the empty value by the descs.Collection's
		// WriteDescToBatch method.
		newModTime = existingModTime
	}

	// Overwrite corrected version and ID values and fetch new descriptor type.
	objectType := privilege.Any
	{
		//nolint:descriptormarshal
		if tbl := desc.GetTable(); tbl != nil {
			tbl.ID = newID
			tbl.Version = newVersion
			objectType = privilege.Table
		}
		//nolint:descriptormarshal
		if db := desc.GetDatabase(); db != nil {
			db.ID = newID
			db.Version = newVersion
			objectType = privilege.Database
		}
		//nolint:descriptormarshal
		if typ := desc.GetType(); typ != nil {
			typ.ID = newID
			typ.Version = newVersion
			objectType = privilege.Type
		}
		//nolint:descriptormarshal
		if sc := desc.GetSchema(); sc != nil {
			sc.ID = newID
			sc.Version = newVersion
			objectType = privilege.Schema
		}
	}
	if objectType == privilege.Any {
		return pgerror.Newf(pgcode.InvalidObjectDefinition, "invalid new descriptor %+v", desc)
	}

	// Update the mutable descriptor with the new proto.
	tbl, db, typ, schema := descpb.FromDescriptorWithMVCCTimestamp(&desc, newModTime)
	switch md := mut.(type) {
	case *tabledesc.Mutable:
		if objectType != privilege.Table {
			return pgerror.Newf(pgcode.InvalidObjectDefinition, "cannot replace table descriptor with %s", objectType)
		}
		md.TableDescriptor = *tbl
	case *schemadesc.Mutable:
		if objectType != privilege.Schema {
			return pgerror.Newf(pgcode.InvalidObjectDefinition, "cannot replace schema descriptor with %s", objectType)
		}
		md.SchemaDescriptor = *schema
	case *dbdesc.Mutable:
		if objectType != privilege.Database {
			return pgerror.Newf(pgcode.InvalidObjectDefinition, "cannot replace database descriptor with %s", objectType)
		}
		md.DatabaseDescriptor = *db
	case *typedesc.Mutable:
		if objectType != privilege.Type {
			return pgerror.Newf(pgcode.InvalidObjectDefinition, "cannot replace type descriptor with %s", objectType)
		}
		md.TypeDescriptor = *typ
	case nil:
		b := descbuilder.NewBuilderWithMVCCTimestamp(&desc, newModTime)
		if b == nil {
			return pgerror.Newf(pgcode.InvalidObjectDefinition, "invalid new descriptor %+v", desc)
		}
		mut = b.BuildCreatedMutable()
	default:
		return errors.AssertionFailedf("unknown descriptor type %T for id %d", mut, id)
	}

	// Marshal the hex encoding of the existing protobuf for the event log.
	if existingProto != nil {
		marshaled, err := protoutil.Marshal(existingProto)
		if err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err, "failed to marshal existing descriptor %+v", existingProto)
		}
		ev.PreviousDescriptor = hex.EncodeToString(marshaled)
	}

	// Marshal the hex encoding of the new protobuf for the event log.
	{
		marshaled, err := protoutil.Marshal(mut.DescriptorProto())
		if err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err, "failed to marshal new descriptor %+v", mut.DescriptorProto())
		}
		ev.NewDescriptor = hex.EncodeToString(marshaled)
	}

	// Check that the descriptor ID is less than the counter used for creating new
	// descriptor IDs. If not, and if the force flag is set, increment it.
	maxDescIDKeyVal, err := p.extendedEvalCtx.DB.Get(context.Background(), p.extendedEvalCtx.Codec.DescIDSequenceKey())
	if err != nil {
		return err
	}
	maxDescID, err := maxDescIDKeyVal.Value.GetInt()
	if err != nil {
		return err
	}
	if maxDescID <= descID {
		if !force {
			return pgerror.Newf(pgcode.InvalidObjectDefinition,
				"descriptor ID %d must be less than the descriptor ID sequence value %d", descID, maxDescID)
		}
		inc := descID - maxDescID + 1
		_, err = kv.IncrementValRetryable(ctx, p.extendedEvalCtx.DB, p.extendedEvalCtx.Codec.DescIDSequenceKey(), inc)
		if err != nil {
			return err
		}
	}

	if force {
		p.Descriptors().SkipValidationOnWrite()
	}

	{
		b := p.txn.NewBatch()
		if err := p.Descriptors().WriteDescToBatch(
			ctx, p.extendedEvalCtx.Tracing.KVTracingEnabled(), mut, b,
		); err != nil {
			return err
		}
		if err := p.txn.Run(ctx, b); err != nil {
			return err
		}
	}

	// Log any ownership changes.
	newOwner := mut.GetPrivileges().Owner().Normalized()
	if previousOwner != newOwner {
		if err := logOwnerEvents(ctx, p, newOwner, mut); err != nil {
			return err
		}
	}

	// Log any privilege changes.
	if err := comparePrivileges(
		ctx, p, mut, previousUserPrivileges, objectType,
	); err != nil {
		return err
	}

	return p.logEvent(ctx, id, &ev)
}

// comparePrivileges iterates through all users and for each user, compares
// their old privileges to their new privileges.
// It then logs the granted and/or revoked privileges for that user.
func comparePrivileges(
	ctx context.Context,
	p *planner,
	existing catalog.MutableDescriptor,
	prevUserPrivileges []catpb.UserPrivileges,
	objectType privilege.ObjectType,
) error {
	computePrivilegeChanges := func(prev, cur *catpb.UserPrivileges) (granted, revoked []string) {
		// User has no privileges anymore after upsert, all privileges revoked.
		if cur == nil {
			revoked = privilege.ListFromBitField(prev.Privileges, objectType).SortedNames()
			return nil, revoked
		}

		// User privileges have not changed.
		if prev.Privileges == cur.Privileges {
			return nil, nil
		}

		// Construct a set of this user's old privileges (before upsert).
		prevPrivilegeSet := make(map[string]struct{})
		for _, priv := range privilege.ListFromBitField(prev.Privileges, objectType).SortedNames() {
			prevPrivilegeSet[priv] = struct{}{}
		}

		// Compare with this user's new privileges.
		for _, priv := range privilege.ListFromBitField(cur.Privileges, objectType).SortedNames() {
			if _, ok := prevPrivilegeSet[priv]; !ok {
				// New privileges that do not exist in the old privileges set imply that they have been granted.
				granted = append(granted, priv)
			} else {
				// Old privilege still exists, remove it from set.
				delete(prevPrivilegeSet, priv)
			}
		}

		// Any remaining old privileges imply they do not exist in
		// the new privilege set, so they have been revoked.
		for priv := range prevPrivilegeSet {
			revoked = append(revoked, priv)
		}
		sort.Strings(revoked)

		return granted, revoked
	}

	curUserPrivileges := existing.GetPrivileges().Users
	curUserMap := make(map[string]*catpb.UserPrivileges)
	for i := range curUserPrivileges {
		curUser := &curUserPrivileges[i]
		curUserMap[curUser.User().Normalized()] = curUser
	}

	for i := range prevUserPrivileges {
		prev := &prevUserPrivileges[i]
		username := prev.User().Normalized()
		cur := curUserMap[username]
		granted, revoked := computePrivilegeChanges(prev, cur)
		delete(curUserMap, username)
		if granted == nil && revoked == nil {
			continue
		}
		// Log events.
		if err := logPrivilegeEvents(
			ctx, p, existing, granted, revoked, username,
		); err != nil {
			return err
		}
	}

	// Any leftovers in the new users map indicate privileges for a new user.
	for i := range curUserPrivileges {
		username := curUserPrivileges[i].User().Normalized()
		if _, ok := curUserMap[username]; ok {
			granted := privilege.ListFromBitField(curUserPrivileges[i].Privileges, objectType).SortedNames()
			if granted == nil {
				continue
			}
			if err := logPrivilegeEvents(
				ctx, p, existing, granted, nil, username,
			); err != nil {
				return err
			}
		}
	}

	return nil
}

// logPrivilegeEvents logs the privilege event for each user.
func logPrivilegeEvents(
	ctx context.Context,
	p *planner,
	existing catalog.MutableDescriptor,
	grantedPrivileges []string,
	revokedPrivileges []string,
	grantee string,
) error {

	eventDetails := eventpb.CommonSQLPrivilegeEventDetails{
		Grantee:           grantee,
		GrantedPrivileges: grantedPrivileges,
		RevokedPrivileges: revokedPrivileges,
	}

	switch md := existing.(type) {
	case *tabledesc.Mutable:
		return p.logEvent(ctx, existing.GetID(), &eventpb.ChangeTablePrivilege{
			CommonSQLPrivilegeEventDetails: eventDetails,
			TableName:                      md.GetName(),
		})
	case *schemadesc.Mutable:
		return p.logEvent(ctx, existing.GetID(), &eventpb.ChangeSchemaPrivilege{
			CommonSQLPrivilegeEventDetails: eventDetails,
			SchemaName:                     md.GetName(),
		})
	case *dbdesc.Mutable:
		return p.logEvent(ctx, existing.GetID(), &eventpb.ChangeDatabasePrivilege{
			CommonSQLPrivilegeEventDetails: eventDetails,
			DatabaseName:                   md.GetName(),
		})
	case *typedesc.Mutable:
		return p.logEvent(ctx, existing.GetID(), &eventpb.ChangeTypePrivilege{
			CommonSQLPrivilegeEventDetails: eventDetails,
			TypeName:                       md.GetName(),
		})
	}
	return nil
}

// logPrivilegeEvents logs the owner event for a descriptor.
func logOwnerEvents(
	ctx context.Context, p *planner, newOwner string, existing catalog.MutableDescriptor,
) error {
	switch md := existing.(type) {
	case *tabledesc.Mutable:
		return p.logEvent(ctx, md.GetID(), &eventpb.AlterTableOwner{
			TableName: md.GetName(),
			Owner:     newOwner,
		})
	case *schemadesc.Mutable:
		return p.logEvent(ctx, md.GetID(), &eventpb.AlterSchemaOwner{
			SchemaName: md.GetName(),
			Owner:      newOwner,
		})
	case *dbdesc.Mutable:
		return p.logEvent(ctx, md.GetID(), &eventpb.AlterDatabaseOwner{
			DatabaseName: md.GetName(),
			Owner:        newOwner,
		})
	case *typedesc.Mutable:
		return p.logEvent(ctx, md.GetID(), &eventpb.AlterTypeOwner{
			TypeName: md.GetName(),
			Owner:    newOwner,
		})
	}
	return nil
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
	key := catalogkeys.MakeObjectNameKey(p.execCfg.Codec, parentID, parentSchemaID, name)
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
	flags := p.CommonLookupFlags(true /* required */)
	flags.IncludeDropped = true
	flags.IncludeOffline = true
	validateDescriptor := func() error {
		desc, err := p.Descriptors().GetImmutableDescriptorByID(ctx, p.Txn(), descID, flags)
		if err != nil && descID != keys.PublicSchemaID {
			return errors.Wrapf(err, "failed to retrieve descriptor %d", descID)
		}
		invalid := false
		switch desc.(type) {
		case nil:
			return nil
		case catalog.TableDescriptor, catalog.TypeDescriptor:
			invalid = parentID == descpb.InvalidID || parentSchemaID == descpb.InvalidID
		case catalog.SchemaDescriptor:
			invalid = parentID == descpb.InvalidID || parentSchemaID != descpb.InvalidID
		case catalog.DatabaseDescriptor:
			invalid = parentID != descpb.InvalidID || parentSchemaID != descpb.InvalidID
		default:
			// The public schema does not have a descriptor.
			if descID == keys.PublicSchemaID {
				return nil
			}
			return errors.AssertionFailedf(
				"unexpected descriptor type %T for descriptor %d", desc, descID)
		}

		if invalid {
			return pgerror.Newf(pgcode.InvalidCatalogName,
				"invalid prefix (%d, %d) for %s %d",
				parentID, parentSchemaID, desc.DescriptorType(), descID)
		}
		return nil
	}
	validateParentDescriptor := func() error {
		if parentID == descpb.InvalidID {
			return nil
		}
		parent, err := p.Descriptors().GetImmutableDescriptorByID(ctx, p.Txn(), parentID, flags)
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
		if parentSchemaID == descpb.InvalidID || parentSchemaID == keys.PublicSchemaID {
			return nil
		}
		schema, err := p.Descriptors().GetImmutableDescriptorByID(ctx, p.Txn(), parentSchemaID, flags)
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
	return p.logEvent(ctx, descID,
		&eventpb.UnsafeUpsertNamespaceEntry{
			ParentID:         uint32(parentID),
			ParentSchemaID:   uint32(parentSchemaID),
			Name:             name,
			PreviousID:       uint32(existingID),
			Force:            force,
			FailedValidation: validationErr != nil,
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
	key := catalogkeys.MakeObjectNameKey(p.execCfg.Codec, parentID, parentSchemaID, name)
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
	desc, notice, err := unsafeReadDescriptor(ctx, p, descID, force)
	if err != nil {
		return errors.Wrapf(err, "failed to retrieve descriptor %d", descID)
	}
	if desc != nil && !desc.Dropped() && !force {
		return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"refusing to delete namespace entry for non-dropped descriptor")
	}
	if err := p.txn.Del(ctx, key); err != nil {
		return errors.Wrap(err, "failed to delete entry")
	}

	ev := eventpb.UnsafeDeleteNamespaceEntry{
		ParentID:       uint32(parentID),
		ParentSchemaID: uint32(parentSchemaID),
		Name:           name,
		Force:          force,
	}
	if notice != nil {
		ev.ForceNotice = notice.Error()
	}
	return p.logEvent(ctx, descID, &ev)
}

// UnsafeDeleteDescriptor powers the repair builtin of the same name. The
// idea is that it should be used only by an administrator in the most dire of
// circumstances. It exists to empower administrators to perform repair.
//
// This method will perform very minimal validation. An error will be returned
// if no such descriptor exists. This method can very easily introduce
// corruption, beware.
//
// See UnsafeUpsertDescriptor for additional details, and warnings.
func (p *planner) UnsafeDeleteDescriptor(ctx context.Context, descID int64, force bool) error {
	const method = "crdb_internal.unsafe_delete_descriptor()"
	if err := checkPlannerStateForRepairFunctions(ctx, p, method); err != nil {
		return err
	}
	id := descpb.ID(descID)
	mut, notice, err := unsafeReadDescriptor(ctx, p, id, force)
	if err != nil {
		return err
	}

	// Set the descriptor to dropped so that subsequent attempts to use it in
	// the transaction fail and deleting its namespace entry becomes permitted.
	if mut != nil {
		mut.MaybeIncrementVersion()
		mut.SetDropped()
		if err := p.Descriptors().AddUncommittedDescriptor(mut); err != nil {
			return errors.WithAssertionFailure(err)
		}
		if force {
			p.Descriptors().SkipValidationOnWrite()
		}
	}
	descKey := catalogkeys.MakeDescMetadataKey(p.execCfg.Codec, id)
	if err := p.txn.Del(ctx, descKey); err != nil {
		return err
	}

	ev := eventpb.UnsafeDeleteDescriptor{
		Force: force,
	}
	if mut != nil {
		ev.ParentID = uint32(mut.GetParentID())
		ev.ParentSchemaID = uint32(mut.GetParentSchemaID())
		ev.Name = mut.GetName()
	}
	if notice != nil {
		ev.ForceNotice = notice.Error()
	}
	return p.logEvent(ctx, id, &ev)
}

// unsafeReadDescriptor reads a descriptor by id. It first tries to go through
// the descs.Collection, but this can fail if the descriptor exists but has been
// corrupted and fails validation. In this case, if the force flag is set, we
// bypass the collection and the validation checks and read the descriptor proto
// straight from KV and issue a notice. Otherwise, we return an error.
func unsafeReadDescriptor(
	ctx context.Context, p *planner, id descpb.ID, force bool,
) (mut catalog.MutableDescriptor, notice error, err error) {
	mut, err = p.Descriptors().GetMutableDescriptorByID(ctx, p.txn, id)
	if mut != nil {
		return mut, nil, nil
	}
	if errors.Is(err, catalog.ErrDescriptorNotFound) {
		return nil, nil, nil
	}
	if !force {
		return nil, nil, err
	}
	notice = pgnotice.NewWithSeverityf("WARNING",
		"failed to retrieve existing descriptor, continuing with force flag: %v", err)
	p.BufferClientNotice(ctx, notice)
	// Fall back to low-level descriptor read which bypasses validation.
	descKey := catalogkeys.MakeDescMetadataKey(p.execCfg.Codec, id)
	descRow, err := p.txn.Get(ctx, descKey)
	if err != nil {
		return nil, notice, err
	}
	var descProto descpb.Descriptor
	if err := descRow.ValueProto(&descProto); err != nil {
		return nil, notice, err
	}
	if b := descbuilder.NewBuilderWithMVCCTimestamp(&descProto, descRow.Value.Timestamp); b != nil {
		mut = b.BuildExistingMutable()
	}
	return mut, notice, nil
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

// ForceDeleteTableData only clears the spans backing this table, and does
// not clean up any descriptors or metadata.
func (p *planner) ForceDeleteTableData(ctx context.Context, descID int64) error {
	const method = "crdb_internal.force_delete_table_data()"
	err := checkPlannerStateForRepairFunctions(ctx, p, method)
	if err != nil {
		return err
	}

	// Validate no descriptor exists for this table
	id := descpb.ID(descID)
	desc, err := p.Descriptors().GetImmutableTableByID(ctx, p.txn, id,
		tree.ObjectLookupFlags{
			CommonLookupFlags: tree.CommonLookupFlags{
				Required:    true,
				AvoidLeased: true,
			},
			DesiredTableDescKind: tree.ResolveRequireTableDesc,
		})
	if err != nil && pgerror.GetPGCode(err) != pgcode.UndefinedTable {
		return err
	}
	if desc != nil {
		return errors.New("descriptor still exists force deletion is blocked")
	}
	// Validate the descriptor ID could have been used
	maxDescID, err := p.extendedEvalCtx.DB.Get(context.Background(), p.extendedEvalCtx.Codec.DescIDSequenceKey())
	if err != nil {
		return err
	}
	if maxDescID.ValueInt() <= descID {
		return errors.Newf("descriptor id was never used (descID: %d exceeds maxDescID: %d)",
			descID, maxDescID)
	}

	prefix := p.extendedEvalCtx.Codec.TablePrefix(uint32(id))
	tableSpans := roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
	b := &kv.Batch{}
	b.AddRawRequest(&roachpb.ClearRangeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    tableSpans.Key,
			EndKey: tableSpans.EndKey,
		},
	})

	err = p.txn.DB().Run(ctx, b)
	if err != nil {
		return err
	}

	return p.logEvent(ctx, id,
		&eventpb.ForceDeleteTableDataEntry{
			DescriptorID: uint32(descID),
		})
}

func (p *planner) ExternalReadFile(ctx context.Context, uri string) ([]byte, error) {
	if err := p.RequireAdminRole(ctx, "network I/O"); err != nil {
		return nil, err
	}

	conn, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, uri, p.User())
	if err != nil {
		return nil, err
	}

	file, err := conn.ReadFile(ctx, "")
	if err != nil {
		return nil, err
	}
	return ioctx.ReadAll(ctx, file)
}

func (p *planner) ExternalWriteFile(ctx context.Context, uri string, content []byte) error {
	if err := p.RequireAdminRole(ctx, "network I/O"); err != nil {
		return err
	}

	conn, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, uri, p.User())
	if err != nil {
		return err
	}
	return cloud.WriteFile(ctx, conn, "", bytes.NewReader(content))
}
