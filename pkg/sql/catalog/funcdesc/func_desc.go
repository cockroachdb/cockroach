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
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
	Name() string
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

// NewImmutableWithIsUncommittedVersion returns a Immutable from the given
// FuncDescriptor and allows the caller to mark the func as corresponding to
// an uncommitted version. This should be used when constructing a new copy of
// an Immutable from an existing descriptor which may have a new version.
func NewImmutableWithIsUncommittedVersion(
	tbl descpb.FuncDescriptor, isUncommittedVersion bool,
) *Immutable {
	desc := makeImmutable(tbl)
	desc.isUncommittedVersion = isUncommittedVersion
	return &desc
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

// Name returns the user-visible name of this function descriptor.
func (desc *Immutable) Name() string {
	// Unmangle the descriptor name.
	return strings.TrimPrefix(desc.FuncDescriptor.Name, tree.FuncPrefix)
}

// Validate performs validation on the FuncDescriptor.
func (desc *Immutable) Validate(ctx context.Context, dg catalog.DescGetter) error {
	// Validate local properties of the descriptor.
	if !strings.HasPrefix(desc.FuncDescriptor.Name, tree.FuncPrefix) {
		return errors.AssertionFailedf("invalid func name %s has no magic prefix", errors.Safe(desc.Name))
	}
	if err := catalog.ValidateName(desc.Name(), "func"); err != nil {
		return err
	}
	if desc.ID == descpb.InvalidID {
		return errors.AssertionFailedf("invalid ID %d", errors.Safe(desc.ID))
	}
	if desc.ParentID == descpb.InvalidID {
		return errors.AssertionFailedf("invalid parentID %d", errors.Safe(desc.ParentID))
	}

	for i := range desc.Overloads {
		o := &desc.Overloads[i]
		if _, err := parser.ParseExpr(o.Def); err != nil {
			return err
		}

		if len(o.ParamNames) != len(o.ParamTypes) {
			return errors.AssertionFailedf("len(paramNames) != len(paramTypes): %d %d",
				len(o.ParamNames), len(o.ParamTypes))
		}
	}

	return desc.Privileges.Validate(desc.ID, privilege.Func)
}

// MakeFuncDef returns a FunctionDefinition from this descriptor.
func (desc *Immutable) MakeFuncDef() (*tree.FunctionDefinition, error) {
	props := tree.FunctionProperties{
		DistsqlBlocklist: true,
		Class:            tree.UserDefinedClass,
	}
	name := desc.Name()
	overloads := make([]tree.Overload, len(desc.Overloads))
	for i := range desc.Overloads {
		o := &desc.Overloads[i]
		typs := make(tree.ArgTypes, len(o.ParamTypes))
		for j := range typs {
			typs[j].Typ = o.ParamTypes[j]
			typs[j].Name = o.ParamNames[j]
		}
		retType := o.ReturnType
		expr, err := parser.ParseExpr(o.Def)
		if err != nil {
			return nil, err
		}
		overloads[i] = tree.Overload{
			Types:      typs,
			ReturnType: tree.FixedReturnType(&retType),
			Volatility: tree.VolatilityVolatile,
			UserDef:    expr,
		}
	}
	return tree.NewFunctionDefinition(name, &props, overloads), nil
}

// ContainsUserDefinedTypes returns true if this descriptor contains
// user-defined types.
func (desc *Immutable) ContainsUserDefinedTypes() bool {
	for i := range desc.Overloads {
		for _, t := range desc.Overloads[i].ParamTypes {
			if t.UserDefined() {
				return true
			}
		}
		if desc.Overloads[i].ReturnType.UserDefined() {
			return true
		}
	}
	return false
}

func (desc *Immutable) GetAllReferencedTypeIDs() (descpb.IDs, error) {
	// For each of the collected type IDs in the table descriptor expressions,
	// collect the closure of ID's referenced.
	ids := make(map[descpb.ID]struct{})
	addIDsInType := func(t *types.T) {
		for id := range typedesc.GetTypeDescriptorClosure(t) {
			ids[id] = struct{}{}
		}
	}
	for i := range desc.Overloads {
		o := &desc.Overloads[i]
		for i := range o.ParamTypes {
			addIDsInType(o.ParamTypes[i])
		}
		addIDsInType(&o.ReturnType)
	}

	// Construct the output.
	result := make(descpb.IDs, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	// Sort the output so that the order is deterministic.
	sort.Sort(result)
	return result, nil
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
	return desc.ClusterVersion.FuncDescriptor.Name
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
