// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemadesc

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var _ catalog.SchemaDescriptor = (*immutable)(nil)
var _ catalog.SchemaDescriptor = (*Mutable)(nil)
var _ catalog.MutableDescriptor = (*Mutable)(nil)

// immutable wraps a Schema descriptor and provides methods on it.
type immutable struct {
	descpb.SchemaDescriptor

	// isUncommittedVersion is set to true if this descriptor was created from
	// a copy of a Mutable with an uncommitted version.
	isUncommittedVersion bool
}

func (desc *immutable) SchemaKind() catalog.ResolvedSchemaKind {
	return catalog.SchemaUserDefined
}

// SafeMessage makes immutable a SafeMessager.
func (desc *immutable) SafeMessage() string {
	return formatSafeMessage("schemadesc.immutable", desc)
}

// SafeMessage makes Mutable a SafeMessager.
func (desc *Mutable) SafeMessage() string {
	return formatSafeMessage("schemadesc.Mutable", desc)
}

func formatSafeMessage(typeName string, desc catalog.SchemaDescriptor) string {
	var buf redact.StringBuilder
	buf.Printf(typeName + ": {")
	catalog.FormatSafeDescriptorProperties(&buf, desc)
	buf.Printf("}")
	return buf.String()
}

// Mutable is a mutable reference to a SchemaDescriptor.
//
// Note: Today this isn't actually ever mutated but rather exists for a future
// where we anticipate having a mutable copy of Schema descriptors. There's a
// large amount of space to question this `Mutable|ImmutableCopy` version of each
// descriptor type. Maybe it makes no sense but we're running with it for the
// moment. This is an intermediate state on the road to descriptors being
// handled outside of the catalog entirely as interfaces.
type Mutable struct {
	immutable

	ClusterVersion *immutable

	// changed represents whether or not the descriptor was changed
	// after RunPostDeserializationChanges.
	changed bool
}

var _ redact.SafeMessager = (*immutable)(nil)

// SetDrainingNames implements the MutableDescriptor interface.
func (desc *Mutable) SetDrainingNames(names []descpb.NameInfo) {
	desc.DrainingNames = names
}

// GetParentSchemaID implements the Descriptor interface.
func (desc *immutable) GetParentSchemaID() descpb.ID {
	return keys.RootNamespaceID
}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *immutable) IsUncommittedVersion() bool {
	return desc.isUncommittedVersion
}

// GetAuditMode implements the DescriptorProto interface.
func (desc *immutable) GetAuditMode() descpb.TableDescriptor_AuditMode {
	return descpb.TableDescriptor_DISABLED
}

// DescriptorType implements the DescriptorProto interface.
func (desc *immutable) DescriptorType() catalog.DescriptorType {
	return catalog.Schema
}

// SchemaDesc implements the Descriptor interface.
func (desc *immutable) SchemaDesc() *descpb.SchemaDescriptor {
	return &desc.SchemaDescriptor
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

// DescriptorProto wraps a SchemaDescriptor in a Descriptor.
func (desc *immutable) DescriptorProto() *descpb.Descriptor {
	return &descpb.Descriptor{
		Union: &descpb.Descriptor_Schema{
			Schema: &desc.SchemaDescriptor,
		},
	}
}

// ValidateSelf implements the catalog.Descriptor interface.
func (desc *immutable) ValidateSelf(vea catalog.ValidationErrorAccumulator) {
	// Validate local properties of the descriptor.
	vea.Report(catalog.ValidateName(desc.GetName(), "descriptor"))
	if desc.GetID() == descpb.InvalidID {
		vea.Report(fmt.Errorf("invalid schema ID %d", desc.GetID()))
	}

	// Validate the privilege descriptor.
	vea.Report(desc.Privileges.Validate(desc.GetID(), privilege.Schema))
}

// GetReferencedDescIDs returns the IDs of all descriptors referenced by
// this descriptor, including itself.
func (desc *immutable) GetReferencedDescIDs() (catalog.DescriptorIDSet, error) {
	return catalog.MakeDescriptorIDSet(desc.GetID(), desc.GetParentID()), nil
}

// ValidateCrossReferences implements the catalog.Descriptor interface.
func (desc *immutable) ValidateCrossReferences(
	vea catalog.ValidationErrorAccumulator, vdg catalog.ValidationDescGetter,
) {
	// Check schema parent reference.
	db, err := vdg.GetDatabaseDescriptor(desc.GetParentID())
	if err != nil {
		vea.Report(err)
		return
	}

	// Check that parent has correct entry in schemas mapping.
	isInDBSchemas := false
	_ = db.ForEachSchemaInfo(func(id descpb.ID, name string, isDropped bool) error {
		if id == desc.GetID() {
			if isDropped {
				if name == desc.GetName() {
					vea.Report(errors.AssertionFailedf("present in parent database [%d] schemas mapping but marked as dropped",
						desc.GetParentID()))
				}
				return nil
			}
			if name != desc.GetName() {
				vea.Report(errors.AssertionFailedf("present in parent database [%d] schemas mapping but under name %q",
					desc.GetParentID(), errors.Safe(name)))
				return nil
			}
			isInDBSchemas = true
			return nil
		}
		if name == desc.GetName() && !isDropped {
			vea.Report(errors.AssertionFailedf("present in parent database [%d] schemas mapping but name maps to other schema [%d]",
				desc.GetParentID(), id))
		}
		return nil
	})
	if !isInDBSchemas {
		vea.Report(errors.AssertionFailedf("not present in parent database [%d] schemas mapping",
			desc.GetParentID()))
	}
}

// ValidateTxnCommit implements the catalog.Descriptor interface.
func (desc *immutable) ValidateTxnCommit(
	_ catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
	// No-op.
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
	imm := NewBuilder(desc.SchemaDesc()).BuildImmutable()
	imm.(*immutable).isUncommittedVersion = desc.IsUncommittedVersion()
	return imm
}

// IsNew implements the MutableDescriptor interface.
func (desc *Mutable) IsNew() bool {
	return desc.ClusterVersion == nil
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

// SetName sets the name of the schema. It handles installing a draining name
// for the old name of the descriptor.
func (desc *Mutable) SetName(name string) {
	desc.DrainingNames = append(desc.DrainingNames, descpb.NameInfo{
		ParentID:       desc.ParentID,
		ParentSchemaID: keys.RootNamespaceID,
		Name:           desc.Name,
	})
	desc.Name = name
}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *Mutable) IsUncommittedVersion() bool {
	return desc.IsNew() || desc.GetVersion() != desc.ClusterVersion.GetVersion()
}

// HasPostDeserializationChanges returns if the MutableDescriptor was changed after running
// RunPostDeserializationChanges.
func (desc *Mutable) HasPostDeserializationChanges() bool {
	return desc.changed
}

// IsSchemaNameValid returns whether the input name is valid for a user defined
// schema.
func IsSchemaNameValid(name string) error {
	// Schemas starting with "pg_" are not allowed.
	if strings.HasPrefix(name, catconstants.PgSchemaPrefix) {
		err := pgerror.Newf(pgcode.ReservedName, "unacceptable schema name %q", name)
		err = errors.WithDetail(err, `The prefix "pg_" is reserved for system schemas.`)
		return err
	}
	return nil
}
