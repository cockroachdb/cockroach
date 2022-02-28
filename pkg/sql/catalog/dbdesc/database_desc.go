// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package dbdesc contains the concrete implementations of
// catalog.DatabaseDescriptor.
package dbdesc

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var _ catalog.DatabaseDescriptor = (*immutable)(nil)
var _ catalog.DatabaseDescriptor = (*Mutable)(nil)
var _ catalog.MutableDescriptor = (*Mutable)(nil)

// immutable wraps a database descriptor and provides methods
// on it.
type immutable struct {
	descpb.DatabaseDescriptor

	// isUncommittedVersion is set to true if this descriptor was created from
	// a copy of a Mutable with an uncommitted version.
	isUncommittedVersion bool

	// changed represents whether or not the descriptor was changed
	// after RunPostDeserializationChanges.
	changes catalog.PostDeserializationChanges
}

// Mutable wraps a database descriptor and provides methods
// on it. It can be mutated and generally has not been committed.
type Mutable struct {
	immutable
	ClusterVersion *immutable
}

// SafeMessage makes immutable a SafeMessager.
func (desc *immutable) SafeMessage() string {
	return formatSafeMessage("dbdesc.immutable", desc)
}

// SafeMessage makes Mutable a SafeMessager.
func (desc *Mutable) SafeMessage() string {
	return formatSafeMessage("dbdesc.Mutable", desc)
}

func formatSafeMessage(typeName string, desc catalog.DatabaseDescriptor) string {
	var buf redact.StringBuilder
	buf.Print(typeName + ": {")
	catalog.FormatSafeDescriptorProperties(&buf, desc)
	buf.Print("}")
	return buf.String()
}

// DescriptorType returns the plain type of this descriptor.
func (desc *immutable) DescriptorType() catalog.DescriptorType {
	return catalog.Database
}

// DatabaseDesc implements the Descriptor interface.
func (desc *immutable) DatabaseDesc() *descpb.DatabaseDescriptor {
	return &desc.DatabaseDescriptor
}

// SetDrainingNames implements the MutableDescriptor interface.
//
// Deprecated: Do not use.
func (desc *Mutable) SetDrainingNames(names []descpb.NameInfo) {
	desc.DrainingNames = names
}

// GetParentID implements the Descriptor interface.
func (desc *immutable) GetParentID() descpb.ID {
	return keys.RootNamespaceID
}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *immutable) IsUncommittedVersion() bool {
	return desc.isUncommittedVersion
}

// GetParentSchemaID implements the Descriptor interface.
func (desc *immutable) GetParentSchemaID() descpb.ID {
	return keys.RootNamespaceID
}

// GetAuditMode is part of the DescriptorProto interface.
// This is a stub until per-database auditing is enabled.
func (desc *immutable) GetAuditMode() descpb.TableDescriptor_AuditMode {
	return descpb.TableDescriptor_DISABLED
}

// Public implements the Descriptor interface.
func (desc *immutable) Public() bool {
	return desc.State == descpb.DescriptorState_PUBLIC
}

// Adding implements the Descriptor interface.
func (desc *immutable) Adding() bool {
	return false
}

// Offline implements the Descriptor interface.
func (desc *immutable) Offline() bool {
	return desc.State == descpb.DescriptorState_OFFLINE
}

// Dropped implements the Descriptor interface.
func (desc *immutable) Dropped() bool {
	return desc.State == descpb.DescriptorState_DROP
}

// DescriptorProto wraps a DatabaseDescriptor in a Descriptor.
func (desc *immutable) DescriptorProto() *descpb.Descriptor {
	return &descpb.Descriptor{
		Union: &descpb.Descriptor_Database{
			Database: &desc.DatabaseDescriptor,
		},
	}
}

// ByteSize implements the Descriptor interface.
func (desc *immutable) ByteSize() int64 {
	return int64(desc.Size())
}

// NewBuilder implements the catalog.Descriptor interface.
func (desc *immutable) NewBuilder() catalog.DescriptorBuilder {
	return newBuilder(desc.DatabaseDesc(), desc.isUncommittedVersion, desc.changes)
}

// NewBuilder implements the catalog.Descriptor interface.
//
// It overrides the wrapper's implementation to deal with the fact that
// mutable has overridden the definition of IsUncommittedVersion.
func (desc *Mutable) NewBuilder() catalog.DescriptorBuilder {
	return newBuilder(desc.DatabaseDesc(), desc.IsUncommittedVersion(), desc.changes)
}

// IsMultiRegion implements the DatabaseDescriptor interface.
func (desc *immutable) IsMultiRegion() bool {
	return desc.RegionConfig != nil
}

// PrimaryRegionName implements the DatabaseDescriptor interface.
func (desc *immutable) PrimaryRegionName() (catpb.RegionName, error) {
	if !desc.IsMultiRegion() {
		return "", errors.AssertionFailedf(
			"can not get the primary region of a non multi-region database")
	}
	return desc.RegionConfig.PrimaryRegion, nil
}

// MultiRegionEnumID implements the DatabaseDescriptor interface.
func (desc *immutable) MultiRegionEnumID() (descpb.ID, error) {
	if !desc.IsMultiRegion() {
		return descpb.InvalidID, errors.AssertionFailedf(
			"can not get multi-region enum ID of a non multi-region database")
	}
	return desc.RegionConfig.RegionEnumID, nil
}

// SetName sets the name on the descriptor.
func (desc *Mutable) SetName(name string) {
	desc.Name = name
}

// ForEachSchemaInfo implements the DatabaseDescriptor interface.
func (desc *immutable) ForEachSchemaInfo(
	f func(id descpb.ID, name string, isDropped bool) error,
) error {
	for name, info := range desc.Schemas {
		if err := f(info.ID, name, info.Dropped); err != nil {
			if iterutil.Done(err) {
				return nil
			}
			return err
		}
	}
	return nil
}

// ForEachNonDroppedSchema implements the DatabaseDescriptor interface.
func (desc *immutable) ForEachNonDroppedSchema(f func(id descpb.ID, name string) error) error {
	for name, info := range desc.Schemas {
		if info.Dropped {
			continue
		}
		if err := f(info.ID, name); err != nil {
			if iterutil.Done(err) {
				return nil
			}
			return err
		}
	}
	return nil
}

// GetSchemaID implements the DatabaseDescriptor interface.
func (desc *immutable) GetSchemaID(name string) descpb.ID {
	info := desc.Schemas[name]
	if info.Dropped {
		return descpb.InvalidID
	}
	return info.ID
}

// HasPublicSchemaWithDescriptor returns if the database has a public schema
// with a descriptor.
// If descs.Schemas has an explicit entry for "public", then it has a descriptor
// otherwise it is an implicit public schema.
func (desc *immutable) HasPublicSchemaWithDescriptor() bool {
	// The system database does not have a public schema backed by a descriptor.
	if desc.ID == keys.SystemDatabaseID {
		return false
	}
	_, found := desc.Schemas[tree.PublicSchema]
	return found
}

// GetNonDroppedSchemaName returns the name in the schema mapping entry for the
// given ID, if it's not marked as dropped, empty string otherwise.
func (desc *immutable) GetNonDroppedSchemaName(schemaID descpb.ID) string {
	for name, info := range desc.Schemas {
		if !info.Dropped && info.ID == schemaID {
			return name
		}
	}
	return ""
}

// ValidateSelf validates that the database descriptor is well formed.
// Checks include validate the database name, and verifying that there
// is at least one read and write user.
func (desc *immutable) ValidateSelf(vea catalog.ValidationErrorAccumulator) {
	// Validate local properties of the descriptor.
	vea.Report(catalog.ValidateName(desc.GetName(), "descriptor"))
	if desc.GetID() == descpb.InvalidID {
		vea.Report(fmt.Errorf("invalid database ID %d", desc.GetID()))
	}

	// Validate the privilege descriptor.
	if desc.Privileges == nil {
		vea.Report(errors.AssertionFailedf("privileges not set"))
	} else {
		vea.Report(catprivilege.Validate(*desc.Privileges, desc, privilege.Database))
	}

	// The DefaultPrivilegeDescriptor may be nil.
	if desc.GetDefaultPrivileges() != nil {
		// Validate the default privilege descriptor.
		vea.Report(catprivilege.ValidateDefaultPrivileges(*desc.GetDefaultPrivileges()))
	}

	if desc.IsMultiRegion() {
		desc.validateMultiRegion(vea)
	}
}

// validateMultiRegion performs checks specific to multi-region DBs.
func (desc *immutable) validateMultiRegion(vea catalog.ValidationErrorAccumulator) {
	if desc.RegionConfig.PrimaryRegion == "" {
		vea.Report(errors.AssertionFailedf(
			"primary region unset on a multi-region db %d", desc.GetID()))
	}
}

// GetReferencedDescIDs returns the IDs of all descriptors referenced by
// this descriptor, including itself.
func (desc *immutable) GetReferencedDescIDs() (catalog.DescriptorIDSet, error) {
	ids := catalog.MakeDescriptorIDSet(desc.GetID())
	if desc.IsMultiRegion() {
		id, err := desc.MultiRegionEnumID()
		if err != nil {
			return catalog.DescriptorIDSet{}, err
		}
		ids.Add(id)
	}
	for _, schema := range desc.Schemas {
		ids.Add(schema.ID)
	}
	return ids, nil
}

// ValidateCrossReferences implements the catalog.Descriptor interface.
func (desc *immutable) ValidateCrossReferences(
	vea catalog.ValidationErrorAccumulator, vdg catalog.ValidationDescGetter,
) {
	// Check multi-region enum type.
	if enumID, err := desc.MultiRegionEnumID(); err == nil {
		report := func(err error) {
			vea.Report(errors.Wrap(err, "multi-region enum"))
		}
		typ, err := vdg.GetTypeDescriptor(enumID)
		if err != nil {
			report(err)
			return
		}
		if typ.Dropped() {
			report(errors.Errorf("type descriptor is dropped"))
		}
		if typ.GetParentID() != desc.GetID() {
			report(errors.Errorf("parentID is actually %d", typ.GetParentID()))
		}
		// Further validation should be handled by the type descriptor itself.
	}
}

// ValidateTxnCommit implements the catalog.Descriptor interface.
func (desc *immutable) ValidateTxnCommit(
	vea catalog.ValidationErrorAccumulator, vdg catalog.ValidationDescGetter,
) {
	// Check schema references.
	// This could be done in ValidateCrossReferences but it can be quite expensive
	// so we do it here instead.
	for schemaName, schemaInfo := range desc.Schemas {
		if schemaInfo.Dropped {
			continue
		}
		report := func(err error) {
			vea.Report(errors.Wrapf(err, "schema mapping entry %q (%d)",
				errors.Safe(schemaName), schemaInfo.ID))
		}
		schemaDesc, err := vdg.GetSchemaDescriptor(schemaInfo.ID)
		if err != nil {
			report(err)
			continue
		}
		if schemaDesc.GetName() != schemaName {
			report(errors.Errorf("schema name is actually %q", errors.Safe(schemaDesc.GetName())))
		}
		if schemaDesc.GetParentID() != desc.GetID() {
			report(errors.Errorf("schema parentID is actually %d", schemaDesc.GetParentID()))
		}
		if schemaDesc.Dropped() {
			report(errors.Errorf("back-referenced schema %q (%d) is dropped",
				schemaDesc.GetName(), schemaDesc.GetID()))
		}
	}
}

// MaybeIncrementVersion implements the MutableDescriptor interface.
func (desc *Mutable) MaybeIncrementVersion() {
	// Already incremented, no-op.
	if desc.ClusterVersion == nil || desc.Version == desc.ClusterVersion.Version+1 {
		return
	}
	desc.Version++
	desc.ModificationTime = hlc.Timestamp{}
}

// OriginalName implements the MutableDescriptor interface.
func (desc *Mutable) OriginalName() string {
	if desc.ClusterVersion == nil {
		return ""
	}
	return desc.ClusterVersion.Name
}

// OriginalID implements the MutableDescriptor interface.
func (desc *Mutable) OriginalID() descpb.ID {
	if desc.ClusterVersion == nil {
		return descpb.InvalidID
	}
	return desc.ClusterVersion.ID
}

// OriginalVersion implements the MutableDescriptor interface.
func (desc *Mutable) OriginalVersion() descpb.DescriptorVersion {
	if desc.ClusterVersion == nil {
		return 0
	}
	return desc.ClusterVersion.Version
}

// ImmutableCopy implements the MutableDescriptor interface.
func (desc *Mutable) ImmutableCopy() catalog.Descriptor {
	return desc.NewBuilder().BuildImmutable()
}

// IsNew implements the MutableDescriptor interface.
func (desc *Mutable) IsNew() bool {
	return desc.ClusterVersion == nil
}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *Mutable) IsUncommittedVersion() bool {
	return desc.IsNew() || desc.GetVersion() != desc.ClusterVersion.GetVersion()
}

// SetPublic implements the MutableDescriptor interface.
func (desc *Mutable) SetPublic() {
	desc.State = descpb.DescriptorState_PUBLIC
	desc.OfflineReason = ""
}

// SetDropped implements the MutableDescriptor interface.
func (desc *Mutable) SetDropped() {
	desc.State = descpb.DescriptorState_DROP
	desc.OfflineReason = ""
}

// SetOffline implements the MutableDescriptor interface.
func (desc *Mutable) SetOffline(reason string) {
	desc.State = descpb.DescriptorState_OFFLINE
	desc.OfflineReason = reason
}

// AddDrainingName adds a draining name to the DatabaseDescriptor's slice of
// draining names.
//
// Deprecated: Do not use.
func (desc *Mutable) AddDrainingName(name descpb.NameInfo) {
	desc.DrainingNames = append(desc.DrainingNames, name)
}

// UnsetMultiRegionConfig removes the stored multi-region config from the
// database descriptor.
func (desc *Mutable) UnsetMultiRegionConfig() {
	desc.RegionConfig = nil
}

// SetInitialMultiRegionConfig initializes and sets a RegionConfig on a database
// descriptor. It returns an error if a RegionConfig already exists.
func (desc *Mutable) SetInitialMultiRegionConfig(config *multiregion.RegionConfig) error {
	// We only should be doing this for the initial multi-region configuration.
	if desc.RegionConfig != nil {
		return errors.AssertionFailedf(
			"expected no region config on database %q with ID %d",
			desc.GetName(),
			desc.GetID(),
		)
	}
	desc.RegionConfig = &descpb.DatabaseDescriptor_RegionConfig{
		SurvivalGoal:  config.SurvivalGoal(),
		PrimaryRegion: config.PrimaryRegion(),
		RegionEnumID:  config.RegionEnumID(),
	}
	return nil
}

// SetRegionConfig sets the region configuration of a database descriptor.
func (desc *Mutable) SetRegionConfig(cfg *descpb.DatabaseDescriptor_RegionConfig) {
	desc.RegionConfig = cfg
}

// SetPlacement sets the placement on the region config for a database
// descriptor.
func (desc *Mutable) SetPlacement(placement descpb.DataPlacement) {
	desc.RegionConfig.Placement = placement
}

// GetPostDeserializationChanges returns if the MutableDescriptor was changed after running
// RunPostDeserializationChanges.
func (desc *immutable) GetPostDeserializationChanges() catalog.PostDeserializationChanges {
	return desc.changes
}

// GetDefaultPrivilegeDescriptor returns a DefaultPrivilegeDescriptor.
func (desc *immutable) GetDefaultPrivilegeDescriptor() catalog.DefaultPrivilegeDescriptor {
	defaultPrivilegeDescriptor := desc.GetDefaultPrivileges()
	if defaultPrivilegeDescriptor == nil {
		defaultPrivilegeDescriptor = catprivilege.MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE)
	}
	return catprivilege.MakeDefaultPrivileges(defaultPrivilegeDescriptor)
}

// GetMutableDefaultPrivilegeDescriptor returns a catprivilege.Mutable.
func (desc *Mutable) GetMutableDefaultPrivilegeDescriptor() *catprivilege.Mutable {
	defaultPrivilegeDescriptor := desc.GetDefaultPrivileges()
	if defaultPrivilegeDescriptor == nil {
		defaultPrivilegeDescriptor = catprivilege.MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE)
	}
	return catprivilege.NewMutableDefaultPrivileges(defaultPrivilegeDescriptor)
}

// SetDefaultPrivilegeDescriptor sets the default privilege descriptor
// for the database.
func (desc *Mutable) SetDefaultPrivilegeDescriptor(
	defaultPrivilegeDescriptor *catpb.DefaultPrivilegeDescriptor,
) {
	desc.DefaultPrivileges = defaultPrivilegeDescriptor
}

// AddSchemaToDatabase adds a schemaName and schemaInfo entry into the
// database's Schemas map. If the map is nil, then we create a map before
// adding the entry.
// If there is an existing entry in the map with schemaName as the key,
// it will be overridden.
func (desc *Mutable) AddSchemaToDatabase(
	schemaName string, schemaInfo descpb.DatabaseDescriptor_SchemaInfo,
) {
	if desc.Schemas == nil {
		desc.Schemas = make(map[string]descpb.DatabaseDescriptor_SchemaInfo)
	}
	desc.Schemas[schemaName] = schemaInfo
}

// GetDeclarativeSchemaChangerState is part of the catalog.MutableDescriptor
// interface.
func (desc *immutable) GetDeclarativeSchemaChangerState() *scpb.DescriptorState {
	return desc.DeclarativeSchemaChangerState.Clone()
}

// SetDeclarativeSchemaChangerState is part of the catalog.MutableDescriptor
// interface.
func (desc *Mutable) SetDeclarativeSchemaChangerState(state *scpb.DescriptorState) {
	desc.DeclarativeSchemaChangerState = state
}

// maybeRemoveDroppedSelfEntryFromSchemas removes an entry in the Schemas map corresponding to the
// database itself which was added due to a bug in prior versions when dropping any user-defined schema.
// The bug inserted an entry for the database rather than the schema being dropped. This function fixes the
// problem by deleting the erroneous entry.
func maybeRemoveDroppedSelfEntryFromSchemas(dbDesc *descpb.DatabaseDescriptor) bool {
	if dbDesc == nil {
		return false
	}
	if sc, ok := dbDesc.Schemas[dbDesc.Name]; ok && sc.ID == dbDesc.ID {
		delete(dbDesc.Schemas, dbDesc.Name)
		return true
	}
	return false
}
