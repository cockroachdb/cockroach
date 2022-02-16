// Copyright 2021 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
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

// Deprecated: Do not use.
func (p synthetic) GetDrainingNames() []descpb.NameInfo {
	return nil
}
func (p synthetic) GetPrivileges() *catpb.PrivilegeDescriptor {
	log.Fatalf(context.TODO(), "cannot access privileges on a %s descriptor", p.kindName())
	return nil
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
func (p synthetic) DescriptorProto() *descpb.Descriptor {
	log.Fatalf(context.TODO(),
		"%s schema cannot be encoded", p.kindName())
	return nil // unreachable
}
func (p synthetic) ByteSize() int64 {
	return 0
}
func (p synthetic) NewBuilder() catalog.DescriptorBuilder {
	log.Fatalf(context.TODO(),
		"%s schema cannot create a builder", p.kindName())
	return nil // unreachable
}
func (p synthetic) GetReferencedDescIDs() (catalog.DescriptorIDSet, error) {
	return catalog.DescriptorIDSet{}, nil
}
func (p synthetic) ValidateSelf(_ catalog.ValidationErrorAccumulator) {}
func (p synthetic) ValidateCrossReferences(
	_ catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
}
func (p synthetic) ValidateTxnCommit(
	_ catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
}
func (p synthetic) SchemaKind() catalog.ResolvedSchemaKind { return p.kind() }
func (p synthetic) SchemaDesc() *descpb.SchemaDescriptor {
	log.Fatalf(context.TODO(),
		"synthetic %s cannot be encoded", p.kindName())
	return nil // unreachable
}
func (p synthetic) GetDeclarativeSchemaChangerState() *scpb.DescriptorState {
	return nil
}
func (p synthetic) GetPostDeserializationChanges() catalog.PostDeserializationChanges {
	return catalog.PostDeserializationChanges{}
}

// GetDefaultPrivilegeDescriptor returns a DefaultPrivilegeDescriptor.
func (p synthetic) GetDefaultPrivilegeDescriptor() catalog.DefaultPrivilegeDescriptor {
	return catprivilege.MakeDefaultPrivileges(catprivilege.MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_SCHEMA))
}
