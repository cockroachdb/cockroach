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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
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
}

// Mutable wraps a database descriptor and provides methods
// on it. It can be mutated and generally has not been committed.
type Mutable struct {
	immutable
	ClusterVersion *immutable

	// changed represents whether or not the descriptor was changed
	// after RunPostDeserializationChanges.
	changed bool
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

// IsMultiRegion returns whether the database has multi-region properties
// configured. If so, desc.RegionConfig can be used.
func (desc *immutable) IsMultiRegion() bool {
	return desc.RegionConfig != nil
}

// MultiRegionEnumID returns the ID of the multi-region enum if the database
// is a multi-region database, and an error otherwise.
func (desc *immutable) MultiRegionEnumID() (descpb.ID, error) {
	if !desc.IsMultiRegion() {
		return descpb.InvalidID, errors.AssertionFailedf(
			"can not get multi-region enum ID of a non multi-region database")
	}
	return desc.RegionConfig.RegionEnumID, nil
}

// PrimaryRegionName returns the primary region for a multi-region database.
func (desc *immutable) PrimaryRegionName() (descpb.RegionName, error) {
	if !desc.IsMultiRegion() {
		return "", errors.AssertionFailedf(
			"can not get the primary region of a non multi-region database")
	}
	return desc.RegionConfig.PrimaryRegion, nil
}

// SetName sets the name on the descriptor.
func (desc *Mutable) SetName(name string) {
	desc.Name = name
}

// ForEachSchemaInfo iterates f over each schema info mapping in the descriptor.
// iterutil.StopIteration is supported.
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

// GetSchemaID returns the ID in the schema mapping entry for the
// given name, 0 otherwise.
func (desc *immutable) GetSchemaID(name string) descpb.ID {
	info := desc.Schemas[name]
	if info.Dropped {
		return descpb.InvalidID
	}
	return info.ID
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
	vea.Report(desc.Privileges.Validate(desc.GetID(), privilege.Database))

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
	imm := NewBuilder(desc.DatabaseDesc()).BuildImmutableDatabase()
	imm.(*immutable).isUncommittedVersion = desc.IsUncommittedVersion()
	return imm
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

// HasPostDeserializationChanges returns if the MutableDescriptor was changed after running
// RunPostDeserializationChanges.
func (desc *Mutable) HasPostDeserializationChanges() bool {
	return desc.changed
}
