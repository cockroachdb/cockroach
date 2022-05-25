// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

var _ catalog.FunctionDescriptor = (*immutable)(nil)
var _ catalog.FunctionDescriptor = (*Mutable)(nil)
var _ catalog.MutableDescriptor = (*Mutable)(nil)

type immutable struct {
	descpb.FunctionDescriptor

	isUncommittedVersion bool

	changes catalog.PostDeserializationChanges
}

type Mutable struct {
	immutable
	ClusterVersion *immutable
}

func InitFunctionDescriptor(
	id descpb.ID,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
	argNum int,
	returnType *types.T,
	returnSet bool,
	privs *catpb.PrivilegeDescriptor,
) Mutable {
	return Mutable{
		immutable: immutable{
			FunctionDescriptor: descpb.FunctionDescriptor{
				Name:           name,
				ID:             id,
				ParentID:       parentID,
				ParentSchemaID: parentSchemaID,
				Args:           make([]descpb.FunctionDescriptor_Argument, 0, argNum),
				ReturnType: descpb.FunctionDescriptor_ReturnType{
					Type:  returnType,
					IsSet: returnSet,
				},
				Lang:              descpb.FunctionDescriptor_Sql,
				Volatility:        descpb.FunctionDescriptor_Immutable,
				LeakProof:         false,
				NullInputBehavior: descpb.FunctionDescriptor_CalledOnNullInput,
				Privileges:        privs,
				Version:           1,
				ModificationTime:  hlc.Timestamp{},
			},
		},
	}
}

func (desc *immutable) IsUncommittedVersion() bool {
	return desc.isUncommittedVersion
}

func (desc *immutable) GetDrainingNames() []descpb.NameInfo {
	return nil
}

func (desc *immutable) DescriptorType() catalog.DescriptorType {
	return catalog.Function
}

func (desc *immutable) GetAuditMode() descpb.TableDescriptor_AuditMode {
	return descpb.TableDescriptor_DISABLED
}

func (desc *immutable) Public() bool {
	return desc.State == descpb.DescriptorState_PUBLIC
}

func (desc *immutable) Adding() bool {
	return false
}

func (desc *immutable) Dropped() bool {
	return desc.State == descpb.DescriptorState_DROP
}

func (desc *immutable) Offline() bool {
	return desc.State == descpb.DescriptorState_OFFLINE
}

func (desc *immutable) DescriptorProto() *descpb.Descriptor {
	return &descpb.Descriptor{
		Union: &descpb.Descriptor_Function{
			Function: &desc.FunctionDescriptor,
		},
	}
}

func (desc *immutable) ByteSize() int64 {
	return int64(desc.Size())
}

func (desc *immutable) NewBuilder() catalog.DescriptorBuilder {
	return newBuilder(desc.FunctionDesc(), desc.isUncommittedVersion, desc.changes)
}

func (desc *immutable) GetReferencedDescIDs() (catalog.DescriptorIDSet, error) {
	var ret catalog.DescriptorIDSet
	for _, id := range desc.DependsOn {
		ret.Add(id)
	}
	for _, id := range desc.DependsOnTypes {
		ret.Add(id)
	}
	for _, dep := range desc.DependedOnBy {
		ret.Add(dep.ID)
	}

	return ret, nil
}

// TODO (Chengxiong): UDF validate self
func (desc *immutable) ValidateSelf(vea catalog.ValidationErrorAccumulator) {}

// TODO (Chengxiong): UDF validate cross references
func (desc *immutable) ValidateCrossReferences(
	vea catalog.ValidationErrorAccumulator, vdg catalog.ValidationDescGetter,
) {
}

// TODO (Chengxiong): UDF validate transaction commit
func (desc *immutable) ValidateTxnCommit(
	vea catalog.ValidationErrorAccumulator, vdg catalog.ValidationDescGetter,
) {
}

func (desc *immutable) GetPostDeserializationChanges() catalog.PostDeserializationChanges {
	return desc.changes
}

func (desc *immutable) HasConcurrentSchemaChanges() bool {
	return desc.DeclarativeSchemaChangerState != nil &&
		desc.DeclarativeSchemaChangerState.JobID != catpb.InvalidJobID
}

func (desc *immutable) SkipNamespace() bool {
	return true
}

func (desc *immutable) FunctionDesc() *descpb.FunctionDescriptor {
	return &desc.FunctionDescriptor
}

func (desc *Mutable) MaybeIncrementVersion() {
	// Already incremented, no-op.
	if desc.ClusterVersion == nil || desc.Version == desc.ClusterVersion.Version+1 {
		return
	}
	desc.Version++
	desc.ModificationTime = hlc.Timestamp{}
}

// Deprecated
func (desc *Mutable) SetDrainingNames([]descpb.NameInfo) {}

func (desc *Mutable) AddDrainingName(info descpb.NameInfo) {}

func (desc *Mutable) OriginalName() string {
	if desc.ClusterVersion == nil {
		return ""
	}
	return desc.ClusterVersion.Name
}

func (desc *Mutable) OriginalID() descpb.ID {
	if desc.ClusterVersion == nil {
		return descpb.InvalidID
	}
	return desc.ClusterVersion.ID
}

func (desc *Mutable) OriginalVersion() descpb.DescriptorVersion {
	if desc.ClusterVersion == nil {
		return 0
	}
	return desc.ClusterVersion.Version
}

func (desc *Mutable) ImmutableCopy() catalog.Descriptor {
	return desc.NewBuilder().BuildImmutable()
}

func (desc *Mutable) IsNew() bool {
	return desc.ClusterVersion == nil
}

func (desc *Mutable) SetPublic() {
	desc.State = descpb.DescriptorState_PUBLIC
	desc.OfflineReason = ""
}

func (desc *Mutable) SetDropped() {
	desc.State = descpb.DescriptorState_DROP
	desc.OfflineReason = ""
}

func (desc *Mutable) SetOffline(reason string) {
	desc.State = descpb.DescriptorState_OFFLINE
	desc.OfflineReason = reason
}

func (desc *Mutable) SetDeclarativeSchemaChangerState(state *scpb.DescriptorState) {
	desc.DeclarativeSchemaChangerState = state
}

func (desc *Mutable) AddArgument(arg descpb.FunctionDescriptor_Argument) {
	desc.Args = append(desc.Args, arg)
}

func (desc *Mutable) SetVolatility(v descpb.FunctionDescriptor_Volatility) {
	desc.Volatility = v
}

func (desc *Mutable) SetLeakProof(v bool) {
	desc.LeakProof = v
}

func (desc *Mutable) SetNullInputBehavior(v descpb.FunctionDescriptor_NullInputBehavior) {
	desc.NullInputBehavior = v
}

func (desc *Mutable) SetLang(v descpb.FunctionDescriptor_Language) {
	desc.Lang = v
}

func (desc *Mutable) SetFuncBody(v string) {
	desc.FunctionBody = v
}
