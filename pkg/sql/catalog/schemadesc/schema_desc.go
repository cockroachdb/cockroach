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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var _ catalog.SchemaDescriptor = (*Immutable)(nil)
var _ catalog.SchemaDescriptor = (*Mutable)(nil)
var _ catalog.MutableDescriptor = (*Mutable)(nil)

// Immutable wraps a Schema descriptor and provides methods on it.
type Immutable struct {
	descpb.SchemaDescriptor

	// isUncommittedVersion is set to true if this descriptor was created from
	// a copy of a Mutable with an uncommitted version.
	isUncommittedVersion bool
}

// SafeMessage makes Immutable a SafeMessager.
func (desc *Immutable) SafeMessage() string {
	return formatSafeMessage("schemadesc.Immutable", desc)
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
	Immutable

	ClusterVersion *Immutable
}

var _ redact.SafeMessager = (*Immutable)(nil)

// NewMutableExisting returns a Mutable from the
// given schema descriptor with the cluster version also set to the descriptor.
// This is for schemas that already exist.
func NewMutableExisting(desc descpb.SchemaDescriptor) *Mutable {
	return &Mutable{
		Immutable:      makeImmutable(*protoutil.Clone(&desc).(*descpb.SchemaDescriptor)),
		ClusterVersion: NewImmutable(desc),
	}
}

// NewImmutable makes a new Schema descriptor.
func NewImmutable(desc descpb.SchemaDescriptor) *Immutable {
	m := makeImmutable(desc)
	return &m
}

func makeImmutable(desc descpb.SchemaDescriptor) Immutable {
	return Immutable{SchemaDescriptor: desc}
}

// Reference these functions to defeat the linter.
var (
	_ = NewImmutable
)

// NewCreatedMutable returns a Mutable from the
// given SchemaDescriptor with the cluster version being the zero schema. This
// is for a schema that is created within the current transaction.
func NewCreatedMutable(desc descpb.SchemaDescriptor) *Mutable {
	return &Mutable{
		Immutable: makeImmutable(desc),
	}
}

// SetDrainingNames implements the MutableDescriptor interface.
func (desc *Mutable) SetDrainingNames(names []descpb.NameInfo) {
	desc.DrainingNames = names
}

// GetParentSchemaID implements the Descriptor interface.
func (desc *Immutable) GetParentSchemaID() descpb.ID {
	return keys.RootNamespaceID
}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *Immutable) IsUncommittedVersion() bool {
	return desc.isUncommittedVersion
}

// GetAuditMode implements the DescriptorProto interface.
func (desc *Immutable) GetAuditMode() descpb.TableDescriptor_AuditMode {
	return descpb.TableDescriptor_DISABLED
}

// TypeName implements the DescriptorProto interface.
func (desc *Immutable) TypeName() string {
	return "schema"
}

// SchemaDesc implements the Descriptor interface.
func (desc *Immutable) SchemaDesc() *descpb.SchemaDescriptor {
	return &desc.SchemaDescriptor
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

// DescriptorProto wraps a SchemaDescriptor in a Descriptor.
func (desc *Immutable) DescriptorProto() *descpb.Descriptor {
	return &descpb.Descriptor{
		Union: &descpb.Descriptor_Schema{
			Schema: &desc.SchemaDescriptor,
		},
	}
}

// NameResolutionResult implements the ObjectDescriptor interface.
func (desc *Immutable) NameResolutionResult() {}

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
	// TODO (lucy): Should the immutable descriptor constructors always make a
	// copy, so we don't have to do it here?
	imm := NewImmutable(*protoutil.Clone(desc.SchemaDesc()).(*descpb.SchemaDescriptor))
	imm.isUncommittedVersion = desc.IsUncommittedVersion()
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

// IsSchemaNameValid returns whether the input name is valid for a user defined
// schema.
func IsSchemaNameValid(name string) error {
	// Schemas starting with "pg_" are not allowed.
	if strings.HasPrefix(name, sessiondata.PgSchemaPrefix) {
		err := pgerror.Newf(pgcode.ReservedName, "unacceptable schema name %q", name)
		err = errors.WithDetail(err, `The prefix "pg_" is reserved for system schemas.`)
		return err
	}
	return nil
}
