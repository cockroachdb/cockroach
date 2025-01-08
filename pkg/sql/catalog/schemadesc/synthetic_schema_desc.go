// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemadesc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// synthetic implements many of the methods of catalog.SchemaDescriptor and
// is shared by the three "synthetic" implementations of that interface:
// virtual, temporary, and public.
type synthetic struct {
	syntheticBase
}

// syntheticBase is an interface to differentiate some of the
// behavior of synthetic.
type syntheticBase interface {
	GetPrivileges() *catpb.PrivilegeDescriptor
	kindName() string
	kind() catalog.ResolvedSchemaKind
}

func (p synthetic) GetParentSchemaID() descpb.ID {
	return descpb.InvalidID
}
func (p synthetic) IsUncommittedVersion() bool {
	return false
}
func (p synthetic) GetVersion() descpb.DescriptorVersion {
	return 1
}
func (p synthetic) GetModificationTime() hlc.Timestamp {
	return hlc.Timestamp{}
}
func (p synthetic) DescriptorType() catalog.DescriptorType {
	return catalog.Schema
}
func (p synthetic) GetAuditMode() descpb.TableDescriptor_AuditMode {
	return descpb.TableDescriptor_DISABLED
}
func (p synthetic) Public() bool {
	return true
}
func (p synthetic) Adding() bool {
	return false
}
func (p synthetic) Dropped() bool {
	return false
}
func (p synthetic) Offline() bool {
	return false
}
func (p synthetic) GetOfflineReason() string {
	return ""
}
func (p synthetic) ByteSize() int64 {
	return 0
}
func (p synthetic) NewBuilder() catalog.DescriptorBuilder {
	log.Fatalf(context.TODO(),
		"%s schema cannot create a builder", p.kindName())
	return nil // unreachable
}
func (p synthetic) GetReferencedDescIDs(catalog.ValidationLevel) (catalog.DescriptorIDSet, error) {
	return catalog.DescriptorIDSet{}, nil
}
func (p synthetic) ValidateSelf(_ catalog.ValidationErrorAccumulator) {
}
func (p synthetic) ValidateForwardReferences(
	_ catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
}
func (p synthetic) ValidateBackReferences(
	_ catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
}
func (p synthetic) ValidateTxnCommit(
	_ catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
}
func (p synthetic) SchemaKind() catalog.ResolvedSchemaKind { return p.kind() }
func (p synthetic) GetDeclarativeSchemaChangerState() *scpb.DescriptorState {
	return nil
}
func (p synthetic) GetPostDeserializationChanges() catalog.PostDeserializationChanges {
	return catalog.PostDeserializationChanges{}
}

// HasConcurrentSchemaChanges implements catalog.Descriptor.
func (p synthetic) HasConcurrentSchemaChanges() bool {
	return false
}

// ConcurrentSchemaChangeJobIDs implements catalog.Descriptor.
func (p synthetic) ConcurrentSchemaChangeJobIDs() []catpb.JobID {
	return nil
}

// SkipNamespace implements the descriptor interface.
// We never store synthetic descriptors.
func (p synthetic) SkipNamespace() bool {
	return true
}

// GetObjectType implements the Object interface.
func (p synthetic) GetObjectType() privilege.ObjectType {
	return privilege.Schema
}

// GetObjectTypeString implements the Object interface.
func (p synthetic) GetObjectTypeString() string {
	return string(privilege.Schema)
}

// GetDefaultPrivilegeDescriptor returns a DefaultPrivilegeDescriptor.
func (p synthetic) GetDefaultPrivilegeDescriptor() catalog.DefaultPrivilegeDescriptor {
	return catprivilege.MakeDefaultPrivileges(makeSyntheticDefaultPrivilegeDescriptor())
}

// GetFunction implements the SchemaDescriptor interface.
func (p synthetic) GetFunction(name string) (descpb.SchemaDescriptor_Function, bool) {
	return descpb.SchemaDescriptor_Function{}, false
}

// ForEachFunctionSignature implements the SchemaDescriptor interface.
func (p synthetic) ForEachFunctionSignature(
	fn func(sig descpb.SchemaDescriptor_FunctionSignature) error,
) error {
	return nil
}

// ForEachUDTDependentForHydration implements the catalog.Descriptor interface.
func (p synthetic) ForEachUDTDependentForHydration(fn func(t *types.T) error) error {
	return nil
}

// MaybeRequiresTypeHydration implements the catalog.Descriptor interface.
func (p synthetic) MaybeRequiresTypeHydration() bool { return false }

func (p synthetic) GetRawBytesInStorage() []byte {
	return nil
}

// GetResolvedFuncDefinition implements the SchemaDescriptor interface.
func (p synthetic) GetResolvedFuncDefinition(
	context.Context, string,
) (*tree.ResolvedFunctionDefinition, bool) {
	return nil, false
}

func makeSyntheticDefaultPrivilegeDescriptor() *catpb.DefaultPrivilegeDescriptor {
	return catprivilege.MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_SCHEMA)
}

func makeSyntheticSchemaDesc(sc catalog.SchemaDescriptor) *descpb.SchemaDescriptor {
	return &descpb.SchemaDescriptor{
		ID:                sc.GetID(),
		ParentID:          sc.GetParentID(),
		Version:           sc.GetVersion(),
		Name:              sc.GetName(),
		State:             descpb.DescriptorState_PUBLIC,
		Privileges:        sc.GetPrivileges(),
		DefaultPrivileges: makeSyntheticDefaultPrivilegeDescriptor(),
	}
}

func makeSyntheticDesc(sc catalog.SchemaDescriptor) *descpb.Descriptor {
	return &descpb.Descriptor{Union: &descpb.Descriptor_Schema{Schema: makeSyntheticSchemaDesc(sc)}}
}
