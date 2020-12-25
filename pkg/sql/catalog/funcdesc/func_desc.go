// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package funcdesc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

var _ Descriptor = (*Immutable)(nil)
var _ Descriptor = (*Mutable)(nil)
var _ catalog.MutableDescriptor = (*Mutable)(nil)

// TypeDescriptor will eventually be called typedesc.Descriptor.
// It is implemented by (Imm|M)utableTypeDescriptor.
type Descriptor interface {
	catalog.Descriptor
	FuncDesc() *descpb.FuncDescriptor
	Validate(context.Context, catalog.DescGetter) error
}

// Immutable is a custom type for wrapping FuncDescriptors
// when used in a read only way.
type Immutable struct {
	descpb.FuncDescriptor

	// isUncommittedVersion is set to true if this descriptor was created from
	// a copy of a Mutable with an uncommitted version.
	isUncommittedVersion bool
}

// NewImmutable makes a new Schema descriptor.
func NewImmutable(desc descpb.FuncDescriptor) *Immutable {
	m := makeImmutable(desc)
	return &m
}

func makeImmutable(desc descpb.FuncDescriptor) Immutable {
	return Immutable{FuncDescriptor: desc}
}

// NewCreatedMutable returns a Mutable from the given func descriptor with the
// cluster version being the zero type. This is for a func that is created in
// the same transaction.
func NewCreatedMutable(desc descpb.FuncDescriptor) *Mutable {
	return &Mutable{
		Immutable: makeImmutable(desc),
	}
}

// NewExistingMutable returns a Mutable from the given func descriptor with the
// cluster version also set to the descriptor. This is for funcs that already
// exist.
func NewExistingMutable(desc descpb.FuncDescriptor) *Mutable {
	return &Mutable{
		Immutable:      makeImmutable(*protoutil.Clone(&desc).(*descpb.FuncDescriptor)),
		ClusterVersion: NewImmutable(desc),
	}
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

// IsUncommittedVersion implements the Descriptor interface.
func (desc *Immutable) IsUncommittedVersion() bool {
	return desc.isUncommittedVersion
}

// DescriptorProto returns a Descriptor for serialization.
func (desc *Immutable) DescriptorProto() *descpb.Descriptor {
	return &descpb.Descriptor{
		Union: &descpb.Descriptor_Func{
			Func: &desc.FuncDescriptor,
		},
	}
}

func (desc *Immutable) NameResolutionResult() {}

func (desc *Immutable) TypeName() string {
	return "func"
}

func (desc *Immutable) GetAuditMode() descpb.TableDescriptor_AuditMode {
	return descpb.TableDescriptor_DISABLED
}

func (desc *Immutable) GetOfflineReason() string {
	return desc.OfflineReason
}

func (desc *Immutable) FuncDesc() *descpb.FuncDescriptor {
	return &desc.FuncDescriptor
}

// Validate performs validation on the TypeDescriptor.
func (desc *Immutable) Validate(ctx context.Context, dg catalog.DescGetter) error {
	// Validate local properties of the descriptor.
	if err := catalog.ValidateName(desc.Name, "type"); err != nil {
		return err
	}
	if desc.ID == descpb.InvalidID {
		return errors.AssertionFailedf("invalid ID %d", errors.Safe(desc.ID))
	}
	if desc.ParentID == descpb.InvalidID {
		return errors.AssertionFailedf("invalid parentID %d", errors.Safe(desc.ParentID))
	}

	// TODO(jordan): validate more stuff.
	return nil
}

// MakeFuncDef returns a FunctionDefinition from this descriptor.
func (desc *Immutable) MakeFuncDef() (*tree.FunctionDefinition, error) {
	props := tree.FunctionProperties{
		DistsqlBlocklist: true,
		Class:            tree.UserDefinedClass,
	}
	typs := make(tree.ArgTypes, len(desc.ParamTypes))
	for i := range typs {
		typs[i].Typ = desc.ParamTypes[i]
		typs[i].Name = desc.ParamNames[i]
	}
	retType := desc.ReturnType
	expr, err := parser.ParseExpr(desc.Def)
	if err != nil {
		return nil, err
	}
	return tree.NewFunctionDefinition(desc.Name, &props, []tree.Overload{
		{
			Types:      typs,
			ReturnType: tree.FixedReturnType(&retType),
			Volatility: tree.VolatilityVolatile,
			UserDef:    expr,
		},
	}), nil
}

type Mutable struct {
	Immutable

	ClusterVersion *Immutable
}

func (desc *Mutable) MaybeIncrementVersion() {
	// Already incremented, no-op.
	if desc.ClusterVersion == nil || desc.Version == desc.ClusterVersion.Version+1 {
		return
	}
	desc.Version++
	desc.ModificationTime = hlc.Timestamp{}
}

func (desc *Mutable) SetDrainingNames(infos []descpb.NameInfo) {
	desc.DrainingNames = infos
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
	imm := NewImmutable(*protoutil.Clone(desc.FuncDesc()).(*descpb.FuncDescriptor))
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
