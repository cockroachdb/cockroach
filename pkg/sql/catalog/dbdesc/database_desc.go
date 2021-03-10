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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var _ catalog.DatabaseDescriptor = (*Immutable)(nil)
var _ catalog.DatabaseDescriptor = (*Mutable)(nil)
var _ catalog.MutableDescriptor = (*Mutable)(nil)

// Immutable wraps a database descriptor and provides methods
// on it.
type Immutable struct {
	descpb.DatabaseDescriptor

	// isUncommittedVersion is set to true if this descriptor was created from
	// a copy of a Mutable with an uncommitted version.
	isUncommittedVersion bool
}

// Mutable wraps a database descriptor and provides methods
// on it. It can be mutated and generally has not been committed.
type Mutable struct {
	Immutable

	ClusterVersion *Immutable
}

// SafeMessage makes Immutable a SafeMessager.
func (desc *Immutable) SafeMessage() string {
	return formatSafeMessage("dbdesc.Immutable", desc)
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
func (desc *Immutable) DescriptorType() catalog.DescriptorType {
	return catalog.Database
}

// DatabaseDesc implements the Descriptor interface.
func (desc *Immutable) DatabaseDesc() *descpb.DatabaseDescriptor {
	return &desc.DatabaseDescriptor
}

// SetDrainingNames implements the MutableDescriptor interface.
func (desc *Mutable) SetDrainingNames(names []descpb.NameInfo) {
	desc.DrainingNames = names
}

// GetParentID implements the Descriptor interface.
func (desc *Immutable) GetParentID() descpb.ID {
	return keys.RootNamespaceID
}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *Immutable) IsUncommittedVersion() bool {
	return desc.isUncommittedVersion
}

// GetParentSchemaID implements the Descriptor interface.
func (desc *Immutable) GetParentSchemaID() descpb.ID {
	return keys.RootNamespaceID
}

// NameResolutionResult implements the Descriptor interface.
func (desc *Immutable) NameResolutionResult() {}

// GetAuditMode is part of the DescriptorProto interface.
// This is a stub until per-database auditing is enabled.
func (desc *Immutable) GetAuditMode() descpb.TableDescriptor_AuditMode {
	return descpb.TableDescriptor_DISABLED
}

// Public implements the Descriptor interface.
func (desc *Immutable) Public() bool {
	return desc.State == descpb.DescriptorState_PUBLIC
}

// Adding implements the Descriptor interface.
func (desc *Immutable) Adding() bool {
	return false
}

// Offline implements the Descriptor interface.
func (desc *Immutable) Offline() bool {
	return desc.State == descpb.DescriptorState_OFFLINE
}

// Dropped implements the Descriptor interface.
func (desc *Immutable) Dropped() bool {
	return desc.State == descpb.DescriptorState_DROP
}

// DescriptorProto wraps a DatabaseDescriptor in a Descriptor.
func (desc *Immutable) DescriptorProto() *descpb.Descriptor {
	return &descpb.Descriptor{
		Union: &descpb.Descriptor_Database{
			Database: &desc.DatabaseDescriptor,
		},
	}
}

// IsMultiRegion returns whether the database has multi-region properties
// configured. If so, desc.RegionConfig can be used.
func (desc *Immutable) IsMultiRegion() bool {
	return desc.RegionConfig != nil
}

// GetRegionConfig returns the region config for the given database.
func (desc *Immutable) GetRegionConfig() *descpb.DatabaseDescriptor_RegionConfig {
	return desc.RegionConfig
}

// RegionNames returns the multi-region regions that have been added to a
// database.
func (desc *Immutable) RegionNames() (descpb.RegionNames, error) {
	if !desc.IsMultiRegion() {
		return nil, errors.AssertionFailedf(
			"can not get regions of a non multi-region database")
	}
	regions := make(descpb.RegionNames, len(desc.RegionConfig.Regions))
	for i, region := range desc.RegionConfig.Regions {
		regions[i] = region.Name
	}
	return regions, nil
}

// MultiRegionEnumID returns the ID of the multi-region enum if the database
// is a multi-region database, and an error otherwise.
func (desc *Immutable) MultiRegionEnumID() (descpb.ID, error) {
	if !desc.IsMultiRegion() {
		return descpb.InvalidID, errors.AssertionFailedf(
			"can not get multi-region enum ID of a non multi-region database")
	}
	return desc.RegionConfig.RegionEnumID, nil
}

// PrimaryRegionName returns the primary region for a multi-region database.
func (desc *Immutable) PrimaryRegionName() (descpb.RegionName, error) {
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
func (desc *Immutable) ForEachSchemaInfo(
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

// ValidateSelf validates that the database descriptor is well formed.
// Checks include validate the database name, and verifying that there
// is at least one read and write user.
func (desc *Immutable) ValidateSelf(vea catalog.ValidationErrorAccumulator) {
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
func (desc *Immutable) validateMultiRegion(vea catalog.ValidationErrorAccumulator) {

	// Ensure no regions are duplicated.
	regions := make(map[descpb.RegionName]struct{})
	dbRegions, err := desc.RegionNames()
	if err != nil {
		vea.Report(err)
		return
	}

	for _, region := range dbRegions {
		if _, seen := regions[region]; seen {
			vea.Report(errors.AssertionFailedf(
				"region %q seen twice on db %d", region, desc.GetID()))
		}
		regions[region] = struct{}{}
	}

	if desc.RegionConfig.PrimaryRegion == "" {
		vea.Report(errors.AssertionFailedf(
			"primary region unset on a multi-region db %d", desc.GetID()))
	} else if _, found := regions[desc.RegionConfig.PrimaryRegion]; !found {
		vea.Report(errors.AssertionFailedf(
			"primary region not found in list of regions on db %d", desc.GetID()))
	}
}

// GetReferencedDescIDs returns the IDs of all descriptors referenced by
// this descriptor, including itself.
func (desc *Immutable) GetReferencedDescIDs() catalog.DescriptorIDSet {
	ids := catalog.MakeDescriptorIDSet(desc.GetID())
	if id, err := desc.MultiRegionEnumID(); err == nil {
		ids.Add(id)
	}
	for _, schema := range desc.Schemas {
		ids.Add(schema.ID)
	}
	return ids
}

// ValidateCrossReferences implements the catalog.Descriptor interface.
func (desc *Immutable) ValidateCrossReferences(
	vea catalog.ValidationErrorAccumulator, vdg catalog.ValidationDescGetter,
) {
	// Check schema references.
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
func (desc *Immutable) ValidateTxnCommit(
	_ catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
	// No-op.
}

// SchemaMeta implements the tree.SchemaMeta interface.
// TODO (rohany): I don't want to keep this here, but it seems to be used
//  by backup only for the fake resolution that occurs in backup. Is it possible
//  to have this implementation only visible there? Maybe by creating a type
//  alias for database descriptor in the backupccl package, and then defining
//  SchemaMeta on it?
func (desc *Immutable) SchemaMeta() {}

// LookupSchema returns a descpb.DatabaseDescriptor_SchemaInfo for the schema
// with the provided name, if a schema with that name belongs to the database.
func (desc *Immutable) LookupSchema(
	name string,
) (schemaInfo descpb.DatabaseDescriptor_SchemaInfo, found bool) {
	schemaInfo, found = desc.Schemas[name]
	return schemaInfo, found
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
	imm.isUncommittedVersion = desc.IsUncommittedVersion()
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
