// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

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
)

var _ catalog.SchemaDescriptor = (*ImmutableSchemaDescriptor)(nil)
var _ catalog.SchemaDescriptor = (*MutableSchemaDescriptor)(nil)

// ImmutableSchemaDescriptor wraps a Schema descriptor and provides methods
// on it.
type ImmutableSchemaDescriptor struct {
	descpb.SchemaDescriptor
}

// MutableSchemaDescriptor is a mutable reference to a SchemaDescriptor.
//
// Note: Today this isn't actually ever mutated but rather exists for a future
// where we anticipate having a mutable copy of Schema descriptors. There's a
// large amount of space to question this `Mutable|Immutable` version of each
// descriptor type. Maybe it makes no sense but we're running with it for the
// moment. This is an intermediate state on the road to descriptors being
// handled outside of the catalog entirely as interfaces.
type MutableSchemaDescriptor struct {
	ImmutableSchemaDescriptor

	ClusterVersion *ImmutableSchemaDescriptor
}

// NewMutableExistingSchemaDescriptor returns a MutableSchemaDescriptor from the
// given schema descriptor with the cluster version also set to the descriptor.
// This is for schemas that already exist.
func NewMutableExistingSchemaDescriptor(desc descpb.SchemaDescriptor) *MutableSchemaDescriptor {
	return &MutableSchemaDescriptor{
		ImmutableSchemaDescriptor: makeImmutableSchemaDescriptor(*protoutil.Clone(&desc).(*descpb.SchemaDescriptor)),
		ClusterVersion:            NewImmutableSchemaDescriptor(desc),
	}
}

// NewImmutableSchemaDescriptor makes a new Schema descriptor.
func NewImmutableSchemaDescriptor(desc descpb.SchemaDescriptor) *ImmutableSchemaDescriptor {
	m := makeImmutableSchemaDescriptor(desc)
	return &m
}

func makeImmutableSchemaDescriptor(desc descpb.SchemaDescriptor) ImmutableSchemaDescriptor {
	return ImmutableSchemaDescriptor{SchemaDescriptor: desc}
}

// Reference these functions to defeat the linter.
var (
	_ = NewImmutableSchemaDescriptor
)

// NewMutableCreatedSchemaDescriptor returns a MutableSchemaDescriptor from the
// given SchemaDescriptor with the cluster version being the zero schema. This
// is for a schema that is created within the current transaction.
func NewMutableCreatedSchemaDescriptor(desc descpb.SchemaDescriptor) *MutableSchemaDescriptor {
	return &MutableSchemaDescriptor{
		ImmutableSchemaDescriptor: makeImmutableSchemaDescriptor(desc),
	}
}

// SetDrainingNames implements the MutableDescriptor interface.
func (desc *MutableSchemaDescriptor) SetDrainingNames(names []descpb.NameInfo) {
	desc.DrainingNames = names
}

// GetParentSchemaID implements the Descriptor interface.
func (desc *ImmutableSchemaDescriptor) GetParentSchemaID() descpb.ID {
	return keys.RootNamespaceID
}

// GetAuditMode implements the DescriptorProto interface.
func (desc *ImmutableSchemaDescriptor) GetAuditMode() descpb.TableDescriptor_AuditMode {
	return descpb.TableDescriptor_DISABLED
}

// TypeName implements the DescriptorProto interface.
func (desc *ImmutableSchemaDescriptor) TypeName() string {
	return "schema"
}

// SchemaDesc implements the Descriptor interface.
func (desc *ImmutableSchemaDescriptor) SchemaDesc() *descpb.SchemaDescriptor {
	return &desc.SchemaDescriptor
}

// Adding implements the Descriptor interface.
func (desc *ImmutableSchemaDescriptor) Adding() bool {
	return false
}

// Offline implements the Descriptor interface.
func (desc *ImmutableSchemaDescriptor) Offline() bool {
	return false
}

// GetOfflineReason implements the Descriptor interface.
func (desc *ImmutableSchemaDescriptor) GetOfflineReason() string {
	return ""
}

// DescriptorProto wraps a SchemaDescriptor in a Descriptor.
func (desc *ImmutableSchemaDescriptor) DescriptorProto() *descpb.Descriptor {
	return &descpb.Descriptor{
		Union: &descpb.Descriptor_Schema{
			Schema: &desc.SchemaDescriptor,
		},
	}
}

// NameResolutionResult implements the ObjectDescriptor interface.
func (desc *ImmutableSchemaDescriptor) NameResolutionResult() {}

// MaybeIncrementVersion implements the MutableDescriptor interface.
func (desc *MutableSchemaDescriptor) MaybeIncrementVersion() {
	// Already incremented, no-op.
	if desc.ClusterVersion == nil || desc.Version == desc.ClusterVersion.Version+1 {
		return
	}
	desc.Version++
	desc.ModificationTime = hlc.Timestamp{}
}

// OriginalName implements the MutableDescriptor interface.
func (desc *MutableSchemaDescriptor) OriginalName() string {
	if desc.ClusterVersion == nil {
		return ""
	}
	return desc.ClusterVersion.Name
}

// OriginalID implements the MutableDescriptor interface.
func (desc *MutableSchemaDescriptor) OriginalID() descpb.ID {
	if desc.ClusterVersion == nil {
		return descpb.InvalidID
	}
	return desc.ClusterVersion.ID
}

// OriginalVersion implements the MutableDescriptor interface.
func (desc *MutableSchemaDescriptor) OriginalVersion() descpb.DescriptorVersion {
	if desc.ClusterVersion == nil {
		return 0
	}
	return desc.ClusterVersion.Version
}

// Immutable implements the MutableDescriptor interface.
func (desc *MutableSchemaDescriptor) Immutable() catalog.Descriptor {
	// TODO (lucy): Should the immutable descriptor constructors always make a
	// copy, so we don't have to do it here?
	return NewImmutableSchemaDescriptor(*protoutil.Clone(desc.SchemaDesc()).(*descpb.SchemaDescriptor))
}

// IsNew implements the MutableDescriptor interface.
func (desc *MutableSchemaDescriptor) IsNew() bool {
	return desc.ClusterVersion == nil
}

// SetName sets the name of the schema. It handles installing a draining name
// for the old name of the descriptor.
func (desc *MutableSchemaDescriptor) SetName(name string) {
	desc.DrainingNames = append(desc.DrainingNames, descpb.NameInfo{
		ParentID:       desc.ParentID,
		ParentSchemaID: keys.RootNamespaceID,
		Name:           desc.Name,
	})
	desc.Name = name
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
