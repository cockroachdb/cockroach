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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

var _ catalog.Descriptor = (*immutable)(nil)
var _ catalog.FunctionDescriptor = (*immutable)(nil)
var _ catalog.FunctionDescriptor = (*Mutable)(nil)
var _ catalog.MutableDescriptor = (*Mutable)(nil)

// immutable represents immutable function descriptor.
type immutable struct {
	descpb.FunctionDescriptor

	// isUncommittedVersion is set to true if this descriptor was created from
	// a copy of a Mutable with an uncommitted version.
	isUncommittedVersion bool

	changes catalog.PostDeserializationChanges
}

// Mutable represents a mutable function descriptor.
type Mutable struct {
	immutable
	clusterVersion *immutable
}

// NewMutableFunctionDescriptor is a mutable function descriptor constructor
// used only with in the legacy schema changer.
func NewMutableFunctionDescriptor(
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
					Type:      returnType,
					ReturnSet: returnSet,
				},
				Lang:              catpb.Function_SQL,
				Volatility:        catpb.Function_VOLATILE,
				LeakProof:         false,
				NullInputBehavior: catpb.Function_CALLED_ON_NULL_INPUT,
				Privileges:        privs,
				Version:           1,
				ModificationTime:  hlc.Timestamp{},
			},
		},
	}
}

// IsUncommittedVersion implements the catalog.LeasableDescriptor interface.
func (desc *immutable) IsUncommittedVersion() bool {
	return desc.isUncommittedVersion
}

// DescriptorType implements the catalog.Descriptor interface.
func (desc *immutable) DescriptorType() catalog.DescriptorType {
	return catalog.Function
}

// GetAuditMode implements the catalog.Descriptor interface.
func (desc *immutable) GetAuditMode() descpb.TableDescriptor_AuditMode {
	return descpb.TableDescriptor_DISABLED
}

// Public implements the catalog.Descriptor interface.
func (desc *immutable) Public() bool {
	return desc.State == descpb.DescriptorState_PUBLIC
}

// Adding implements the catalog.Descriptor interface.
func (desc *immutable) Adding() bool {
	return false
}

// Dropped implements the catalog.Descriptor interface.
func (desc *immutable) Dropped() bool {
	return desc.State == descpb.DescriptorState_DROP
}

// Offline implements the catalog.Descriptor interface.
func (desc *immutable) Offline() bool {
	return desc.State == descpb.DescriptorState_OFFLINE
}

// DescriptorProto implements the catalog.Descriptor interface.
func (desc *immutable) DescriptorProto() *descpb.Descriptor {
	return &descpb.Descriptor{
		Union: &descpb.Descriptor_Function{
			Function: &desc.FunctionDescriptor,
		},
	}
}

// ByteSize implements the catalog.Descriptor interface.
func (desc *immutable) ByteSize() int64 {
	return int64(desc.Size())
}

// NewBuilder implements the catalog.Descriptor interface.
func (desc *Mutable) NewBuilder() catalog.DescriptorBuilder {
	return newBuilder(&desc.FunctionDescriptor, desc.IsUncommittedVersion(), desc.changes)
}

// NewBuilder implements the catalog.Descriptor interface.
func (desc *immutable) NewBuilder() catalog.DescriptorBuilder {
	return newBuilder(&desc.FunctionDescriptor, desc.IsUncommittedVersion(), desc.changes)
}

// GetReferencedDescIDs implements the catalog.Descriptor interface.
func (desc *immutable) GetReferencedDescIDs() (catalog.DescriptorIDSet, error) {
	ret := catalog.MakeDescriptorIDSet(desc.GetID(), desc.GetParentID(), desc.GetParentSchemaID())
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

// ValidateSelf implements the catalog.Descriptor interface.
func (desc *immutable) ValidateSelf(vea catalog.ValidationErrorAccumulator) {
	vea.Report(catalog.ValidateName(desc.Name, "function"))
	if desc.GetID() == descpb.InvalidID {
		vea.Report(errors.AssertionFailedf("invalid ID %d", desc.GetID()))
	}
	if desc.GetParentID() == descpb.InvalidID {
		vea.Report(errors.AssertionFailedf("invalid parentID %d", desc.GetParentID()))
	}
	if desc.GetParentSchemaID() == descpb.InvalidID {
		vea.Report(errors.AssertionFailedf("invalid parentSchemaID %d", desc.GetParentSchemaID()))
	}

	if desc.Privileges == nil {
		vea.Report(errors.AssertionFailedf("privileges not set"))
	} else {
		vea.Report(catprivilege.Validate(*desc.Privileges, desc, privilege.Function))
	}

	// Validate types are properly set.
	if desc.ReturnType.Type == nil {
		vea.Report(errors.AssertionFailedf("return type not set"))
	}
	for i, arg := range desc.Args {
		if arg.Type == nil {
			vea.Report(errors.AssertionFailedf("type not set for arg %d", i))
		}
	}

	if desc.LeakProof && desc.Volatility != catpb.Function_IMMUTABLE {
		vea.Report(errors.AssertionFailedf("leakproof is set for non-immutable function"))
	}

	for i, dep := range desc.DependedOnBy {
		if dep.ID == descpb.InvalidID {
			vea.Report(errors.AssertionFailedf("invalid relation id %d in depended-on-by references #%d", dep.ID, i))
		}
	}

	for i, depID := range desc.DependsOn {
		if depID == descpb.InvalidID {
			vea.Report(errors.AssertionFailedf("invalid relation id %d in depends-on references #%d", depID, i))
		}
	}

	for i, typeID := range desc.DependsOnTypes {
		if typeID == descpb.InvalidID {
			vea.Report(errors.AssertionFailedf("invalid type id %d in depends-on-types references #%d", typeID, i))
		}
	}
}

// ValidateCrossReferences implements the catalog.Descriptor interface.
func (desc *immutable) ValidateCrossReferences(
	vea catalog.ValidationErrorAccumulator, vdg catalog.ValidationDescGetter,
) {
	// Check that parent DB exists.
	dbDesc, err := vdg.GetDatabaseDescriptor(desc.GetParentID())
	if err != nil {
		vea.Report(err)
	} else if dbDesc.Dropped() {
		vea.Report(errors.AssertionFailedf("parent database %q (%d) is dropped", dbDesc.GetName(), dbDesc.GetID()))
	}

	// Check that parent Schema exists.
	scDesc, err := vdg.GetSchemaDescriptor(desc.GetParentSchemaID())
	if err != nil {
		vea.Report(err)
	} else if scDesc.Dropped() {
		vea.Report(errors.AssertionFailedf("parent schema %q (%d) is dropped", scDesc.GetName(), scDesc.GetID()))
	}
	vea.Report(desc.validateFuncExistsInSchema(scDesc))

	for _, depID := range desc.DependsOn {
		vea.Report(catalog.ValidateOutboundTableRef(desc.ID, depID, vdg))
	}

	for _, typeID := range desc.DependsOnTypes {
		vea.Report(catalog.ValidateOutboundTypeRef(desc.ID, typeID, vdg))
	}

	// Currently, we don't support cross function references yet.
	// So here we assume that all inbound references are from tables.
	for _, by := range desc.DependedOnBy {
		vea.Report(desc.validateInboundTableRef(by, vdg))
	}

}

func (desc *immutable) validateFuncExistsInSchema(scDesc catalog.SchemaDescriptor) error {
	// Check that parent Schema contains the matching function signature.
	if _, ok := scDesc.GetFunction(desc.GetName()); !ok {
		return errors.AssertionFailedf("function does not exist in schema %q (%d)",
			scDesc.GetName(), scDesc.GetID())
	}

	function, _ := scDesc.GetFunction(desc.GetName())
	for _, overload := range function.Overloads {
		// TODO (Chengxiong) maybe a overkill, but we could also validate function
		// signature matches.
		if overload.ID == desc.GetID() {
			return nil
		}
	}
	return errors.AssertionFailedf("function overload %q (%d) cannot be found in schema %q (%d)",
		desc.GetName(), desc.GetID(), scDesc.GetName(), scDesc.GetID())
}

func (desc *immutable) validateInboundTableRef(
	by descpb.FunctionDescriptor_Reference, vdg catalog.ValidationDescGetter,
) error {
	backRefTbl, err := vdg.GetTableDescriptor(by.ID)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "invalid depended-on-by relation back reference")
	}
	if backRefTbl.Dropped() {
		return errors.AssertionFailedf("depended-on-by relation %q (%d) is dropped",
			backRefTbl.GetName(), backRefTbl.GetID())
	}

	for _, colID := range by.ColumnIDs {
		_, err := backRefTbl.FindColumnWithID(colID)
		if err != nil {
			return errors.AssertionFailedf("depended-on-by relation %q (%d) does not have a column with ID %d",
				backRefTbl.GetName(), by.ID, colID)
		}
	}

	for _, idxID := range by.IndexIDs {
		_, err := backRefTbl.FindIndexWithID(idxID)
		if err != nil {
			return errors.AssertionFailedf("depended-on-by relation %q (%d) does not have an index with ID %d",
				backRefTbl.GetName(), by.ID, idxID)
		}
	}

	for _, cstID := range by.ConstraintIDs {
		_, err := backRefTbl.FindConstraintWithID(cstID)
		if err != nil {
			return errors.AssertionFailedf("depended-on-by relation %q (%d) does not have a constraint with ID %d",
				backRefTbl.GetName(), by.ID, cstID)
		}
	}

	for _, id := range backRefTbl.GetDependsOn() {
		if id == desc.GetID() {
			return nil
		}
	}
	return errors.AssertionFailedf("depended-on-by table %q (%d) has no corresponding depends-on forward reference",
		backRefTbl.GetName(), by.ID)
}

// ValidateTxnCommit implements the catalog.Descriptor interface.
func (desc *immutable) ValidateTxnCommit(
	vea catalog.ValidationErrorAccumulator, vdg catalog.ValidationDescGetter,
) {
	// No-op
}

// GetPostDeserializationChanges implements the catalog.Descriptor interface.
func (desc *immutable) GetPostDeserializationChanges() catalog.PostDeserializationChanges {
	return desc.changes
}

// HasConcurrentSchemaChanges implements the catalog.Descriptor interface.
func (desc *immutable) HasConcurrentSchemaChanges() bool {
	return desc.DeclarativeSchemaChangerState != nil &&
		desc.DeclarativeSchemaChangerState.JobID != catpb.InvalidJobID
}

// SkipNamespace implements the catalog.Descriptor interface.
func (desc *immutable) SkipNamespace() bool {
	return true
}

// IsUncommittedVersion implements the catalog.LeasableDescriptor interface.
func (desc *Mutable) IsUncommittedVersion() bool {
	return desc.IsNew() || desc.clusterVersion.GetVersion() != desc.GetVersion()
}

// MaybeIncrementVersion implements the catalog.MutableDescriptor interface.
func (desc *Mutable) MaybeIncrementVersion() {
	// Already incremented, no-op.
	if desc.clusterVersion == nil || desc.Version == desc.clusterVersion.Version+1 {
		return
	}
	desc.Version++
	desc.ModificationTime = hlc.Timestamp{}
}

// OriginalName implements the catalog.MutableDescriptor interface.
func (desc *Mutable) OriginalName() string {
	if desc.clusterVersion == nil {
		return ""
	}
	return desc.clusterVersion.Name
}

// OriginalID implements the catalog.MutableDescriptor interface.
func (desc *Mutable) OriginalID() descpb.ID {
	if desc.clusterVersion == nil {
		return descpb.InvalidID
	}
	return desc.clusterVersion.ID
}

// OriginalVersion implements the catalog.MutableDescriptor interface.
func (desc *Mutable) OriginalVersion() descpb.DescriptorVersion {
	if desc.clusterVersion == nil {
		return 0
	}
	return desc.clusterVersion.Version
}

// ImmutableCopy implements the catalog.MutableDescriptor interface.
func (desc *Mutable) ImmutableCopy() catalog.Descriptor {
	return desc.NewBuilder().BuildImmutable()
}

// IsNew implements the catalog.MutableDescriptor interface.
func (desc *Mutable) IsNew() bool {
	return desc.clusterVersion == nil
}

// SetPublic implements the catalog.MutableDescriptor interface.
func (desc *Mutable) SetPublic() {
	desc.State = descpb.DescriptorState_PUBLIC
	desc.OfflineReason = ""
}

// SetDropped implements the catalog.MutableDescriptor interface.
func (desc *Mutable) SetDropped() {
	desc.State = descpb.DescriptorState_DROP
	desc.OfflineReason = ""
}

// SetOffline implements the catalog.MutableDescriptor interface.
func (desc *Mutable) SetOffline(reason string) {
	desc.State = descpb.DescriptorState_OFFLINE
	desc.OfflineReason = reason
}

// SetDeclarativeSchemaChangerState implements the catalog.MutableDescriptor interface.
func (desc *Mutable) SetDeclarativeSchemaChangerState(state *scpb.DescriptorState) {
	desc.DeclarativeSchemaChangerState = state
}

// AddArgument adds a function argument to argument list.
func (desc *Mutable) AddArgument(arg descpb.FunctionDescriptor_Argument) {
	desc.Args = append(desc.Args, arg)
}

// SetVolatility sets the volatility attribute.
func (desc *Mutable) SetVolatility(v catpb.Function_Volatility) {
	desc.Volatility = v
}

// SetLeakProof sets the leakproof attribute.
func (desc *Mutable) SetLeakProof(v bool) {
	desc.LeakProof = v
}

// SetNullInputBehavior sets the NullInputBehavior attribute.
func (desc *Mutable) SetNullInputBehavior(v catpb.Function_NullInputBehavior) {
	desc.NullInputBehavior = v
}

// SetLang sets the function language.
func (desc *Mutable) SetLang(v catpb.Function_Language) {
	desc.Lang = v
}

// SetFuncBody sets the function body.
func (desc *Mutable) SetFuncBody(v string) {
	desc.FunctionBody = v
}

// GetObjectType implements the PrivilegeObject interface.
func (desc *immutable) GetObjectType() string {
	return string(desc.DescriptorType())
}

// GetPrivilegeDescriptor implements the PrivilegeObject interface.
func (desc *immutable) GetPrivilegeDescriptor(
	ctx context.Context, planner eval.Planner,
) (*catpb.PrivilegeDescriptor, error) {
	return desc.GetPrivileges(), nil
}

// FuncDesc implements the catalog.FunctionDescriptor interface.
func (desc *immutable) FuncDesc() *descpb.FunctionDescriptor {
	return &desc.FunctionDescriptor
}

// ContainsUserDefinedTypes implements the catalog.HydratableDescriptor interface.
func (desc *immutable) ContainsUserDefinedTypes() bool {
	for i := range desc.Args {
		if desc.Args[i].Type.UserDefined() {
			return true
		}
	}
	return desc.ReturnType.Type.UserDefined()
}
