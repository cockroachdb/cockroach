// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package funcdesc

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
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

	// This is the raw bytes (tag + data) of the function descriptor in storage.
	rawBytesInStorage []byte
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
	params []descpb.FunctionDescriptor_Parameter,
	returnType *types.T,
	returnSet bool,
	isProcedure bool,
	privs *catpb.PrivilegeDescriptor,
) Mutable {
	return Mutable{
		immutable: immutable{
			FunctionDescriptor: descpb.FunctionDescriptor{
				Name:           name,
				ID:             id,
				ParentID:       parentID,
				ParentSchemaID: parentSchemaID,
				Params:         params,
				ReturnType: descpb.FunctionDescriptor_ReturnType{
					Type:      returnType,
					ReturnSet: returnSet,
				},
				Lang:              catpb.Function_SQL,
				Volatility:        catpb.DefaultFunctionVolatility,
				LeakProof:         catpb.DefaultFunctionLeakProof,
				NullInputBehavior: catpb.Function_CALLED_ON_NULL_INPUT,
				IsProcedure:       isProcedure,
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
	return desc.State == descpb.DescriptorState_ADD
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

// GetDeclarativeSchemaChangerState is part of the catalog.MutableDescriptor
// interface.
func (desc *immutable) GetDeclarativeSchemaChangerState() *scpb.DescriptorState {
	return desc.DeclarativeSchemaChangerState.Clone()
}

// NewBuilder implements the catalog.Descriptor interface.
func (desc *Mutable) NewBuilder() catalog.DescriptorBuilder {
	b := newBuilder(&desc.FunctionDescriptor, hlc.Timestamp{}, desc.IsUncommittedVersion(), desc.changes)
	b.SetRawBytesInStorage(desc.GetRawBytesInStorage())
	return b
}

// NewBuilder implements the catalog.Descriptor interface.
func (desc *immutable) NewBuilder() catalog.DescriptorBuilder {
	b := newBuilder(&desc.FunctionDescriptor, hlc.Timestamp{}, desc.IsUncommittedVersion(), desc.changes)
	b.SetRawBytesInStorage(desc.GetRawBytesInStorage())
	return b
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
	vea.Report(catalog.ValidateName(desc))
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
		vea.Report(catprivilege.Validate(*desc.Privileges, desc, privilege.Routine))
	}

	// Validate types are properly set.
	if desc.ReturnType.Type == nil {
		vea.Report(errors.AssertionFailedf("return type not set"))
	}
	for i, param := range desc.Params {
		if param.Type == nil {
			vea.Report(errors.AssertionFailedf("type not set for arg %d", i))
		}
	}

	vp := funcinfo.MakeVolatilityProperties(desc.Volatility, desc.LeakProof)
	vea.Report(vp.Validate())

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

// ValidateForwardReferences implements the catalog.Descriptor interface.
func (desc *immutable) ValidateForwardReferences(
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

	for _, depID := range desc.DependsOn {
		vea.Report(catalog.ValidateOutboundTableRef(depID, vdg))
	}

	for _, typeID := range desc.DependsOnTypes {
		vea.Report(catalog.ValidateOutboundTypeRef(typeID, vdg))
	}
}

// ValidateBackReferences implements the catalog.Descriptor interface.
func (desc *immutable) ValidateBackReferences(
	vea catalog.ValidationErrorAccumulator, vdg catalog.ValidationDescGetter,
) {
	// Check that function exists in parent schema.
	if sc, err := vdg.GetSchemaDescriptor(desc.GetParentSchemaID()); err == nil {
		vea.Report(desc.validateFuncExistsInSchema(sc))
	}

	for _, depID := range desc.DependsOn {
		tbl, _ := vdg.GetTableDescriptor(depID)
		vea.Report(catalog.ValidateOutboundTableRefBackReference(desc.GetID(), tbl))
	}

	for _, typeID := range desc.DependsOnTypes {
		typ, _ := vdg.GetTypeDescriptor(typeID)
		vea.Report(catalog.ValidateOutboundTypeRefBackReference(desc.GetID(), typ))
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
	for _, sig := range function.Signatures {
		// TODO (Chengxiong) maybe a overkill, but we could also validate function
		// signature matches.
		if sig.ID == desc.GetID() {
			return nil
		}
	}
	return errors.AssertionFailedf("function sig %q (%d) cannot be found in schema %q (%d)",
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

	if backRefTbl.IsView() {
		for _, id := range backRefTbl.GetDependsOnFunctions() {
			if id == desc.GetID() {
				return nil
			}
		}
		return errors.AssertionFailedf("depended-on-by view %q (%d) has no corresponding depends-on forward reference",
			backRefTbl.GetName(), by.ID)
	}

	var foundInTable bool
	for _, colID := range by.ColumnIDs {
		col := catalog.FindColumnByID(backRefTbl, colID)
		if col == nil {
			return errors.AssertionFailedf("depended-on-by relation %q (%d) does not have a column with ID %d",
				backRefTbl.GetName(), by.ID, colID)
		}
		fnIDs := catalog.MakeDescriptorIDSet(col.ColumnDesc().UsesFunctionIds...)
		if fnIDs.Contains(desc.GetID()) {
			foundInTable = true
			continue
		}
		return errors.AssertionFailedf(
			"column %d in depended-on-by relation %q (%d) does not have reference to function %q (%d)",
			col.GetID(), backRefTbl.GetName(), backRefTbl.GetID(), desc.GetName(), desc.GetID(),
		)
	}

	for _, idxID := range by.IndexIDs {
		if catalog.FindIndexByID(backRefTbl, idxID) == nil {
			return errors.AssertionFailedf("depended-on-by relation %q (%d) does not have an index with ID %d",
				backRefTbl.GetName(), by.ID, idxID)
		}
		// TODO(chengxiong): add logic to validate reference in index expressions
		// when UDF usage is allowed in indexes.
	}

	for _, cstID := range by.ConstraintIDs {
		if catalog.FindConstraintByID(backRefTbl, cstID) == nil {
			return errors.AssertionFailedf("depended-on-by relation %q (%d) does not have a constraint with ID %d",
				backRefTbl.GetName(), by.ID, cstID)
		}
		fnIDs, err := backRefTbl.GetAllReferencedFunctionIDsInConstraint(cstID)
		if err != nil {
			return err
		}
		if fnIDs.Contains(desc.GetID()) {
			foundInTable = true
			continue
		}
		return errors.AssertionFailedf(
			"constraint %d in depended-on-by relation %q (%d) does not have reference to function %q (%d)",
			cstID, backRefTbl.GetName(), backRefTbl.GetID(), desc.GetName(), desc.GetID(),
		)
	}
	if foundInTable {
		return nil
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

// ConcurrentSchemaChangeJobIDs implements the catalog.Descriptor interface.
func (desc *immutable) ConcurrentSchemaChangeJobIDs() (ret []catpb.JobID) {
	if desc.DeclarativeSchemaChangerState != nil &&
		desc.DeclarativeSchemaChangerState.JobID != catpb.InvalidJobID {
		ret = append(ret, desc.DeclarativeSchemaChangerState.JobID)
	}
	return ret
}

// SkipNamespace implements the catalog.Descriptor interface.
func (desc *immutable) SkipNamespace() bool {
	return true
}

// GetRawBytesInStorage implements the catalog.Descriptor interface.
func (desc *immutable) GetRawBytesInStorage() []byte {
	return desc.rawBytesInStorage
}

// ForEachUDTDependentForHydration implements the catalog.Descriptor interface.
func (desc *immutable) ForEachUDTDependentForHydration(fn func(t *types.T) error) error {
	for _, p := range desc.Params {
		if !catid.IsOIDUserDefined(p.Type.Oid()) {
			continue
		}
		if err := fn(p.Type); err != nil {
			return iterutil.Map(err)
		}
	}
	if !catid.IsOIDUserDefined(desc.ReturnType.Type.Oid()) {
		return nil
	}
	return iterutil.Map(fn(desc.ReturnType.Type))
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
	desc.ResetModificationTime()
}

// ResetModificationTime implements the catalog.MutableDescriptor interface.
func (desc *Mutable) ResetModificationTime() {
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

// AddParams adds function parameters to the parameter list.
func (desc *Mutable) AddParams(params ...descpb.FunctionDescriptor_Parameter) {
	desc.Params = append(desc.Params, params...)
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

// SetName sets the function name.
func (desc *Mutable) SetName(n string) {
	desc.Name = n
}

// SetParentSchemaID sets function's parent schema id.
func (desc *Mutable) SetParentSchemaID(id descpb.ID) {
	desc.ParentSchemaID = id
}

// AddConstraintReference adds back reference to a constraint to the function.
func (desc *Mutable) AddConstraintReference(id descpb.ID, constraintID descpb.ConstraintID) error {
	for _, dep := range desc.DependsOn {
		if dep == id {
			return errors.Errorf(
				"cannot add dependency from descriptor %d to function %s (%d) because there will be a dependency cycle", id, desc.GetName(), desc.GetID(),
			)
		}
	}
	for i := range desc.DependedOnBy {
		if desc.DependedOnBy[i].ID == id {
			ids := catalog.MakeConstraintIDSet(desc.DependedOnBy[i].ConstraintIDs...)
			ids.Add(constraintID)
			desc.DependedOnBy[i].ConstraintIDs = ids.Ordered()
			return nil
		}
	}
	desc.DependedOnBy = append(
		desc.DependedOnBy,
		descpb.FunctionDescriptor_Reference{
			ID:            id,
			ConstraintIDs: []descpb.ConstraintID{constraintID},
		},
	)
	sort.Slice(desc.DependedOnBy, func(i, j int) bool {
		return desc.DependedOnBy[i].ID < desc.DependedOnBy[j].ID
	})
	return nil
}

// RemoveConstraintReference removes back reference to a constraint from the
// function.
func (desc *Mutable) RemoveConstraintReference(id descpb.ID, constraintID descpb.ConstraintID) {
	for i := range desc.DependedOnBy {
		if desc.DependedOnBy[i].ID == id {
			ids := catalog.MakeConstraintIDSet(desc.DependedOnBy[i].ConstraintIDs...)
			ids.Remove(constraintID)
			desc.DependedOnBy[i].ConstraintIDs = ids.Ordered()
			desc.maybeRemoveTableReference(id)
			return
		}
	}
}

// AddColumnReference adds back reference to a column to the function.
func (desc *Mutable) AddColumnReference(id descpb.ID, colID descpb.ColumnID) error {
	for _, dep := range desc.DependsOn {
		if dep == id {
			return errors.Errorf(
				"cannot add dependency from descriptor %d to function %s (%d) because there will be a dependency cycle", id, desc.GetName(), desc.GetID(),
			)
		}
	}
	for i := range desc.DependedOnBy {
		if desc.DependedOnBy[i].ID == id {
			ids := catalog.MakeTableColSet(desc.DependedOnBy[i].ColumnIDs...)
			ids.Add(colID)
			desc.DependedOnBy[i].ColumnIDs = ids.Ordered()
			return nil
		}
	}
	desc.DependedOnBy = append(
		desc.DependedOnBy,
		descpb.FunctionDescriptor_Reference{
			ID:        id,
			ColumnIDs: []descpb.ColumnID{colID},
		},
	)
	sort.Slice(desc.DependedOnBy, func(i, j int) bool {
		return desc.DependedOnBy[i].ID < desc.DependedOnBy[j].ID
	})
	return nil
}

// RemoveColumnReference removes back reference to a column from the function.
func (desc *Mutable) RemoveColumnReference(id descpb.ID, colID descpb.ColumnID) {
	for i := range desc.DependedOnBy {
		if desc.DependedOnBy[i].ID == id {
			ids := catalog.MakeTableColSet(desc.DependedOnBy[i].ColumnIDs...)
			ids.Remove(colID)
			desc.DependedOnBy[i].ColumnIDs = ids.Ordered()
			desc.maybeRemoveTableReference(id)
			return
		}
	}
}

// maybeRemoveTableReference removes a table's references from the function if
// the column, index and constraint references are all empty. This function is
// only used internally when removing an individual column, index or constraint
// reference.
func (desc *Mutable) maybeRemoveTableReference(id descpb.ID) {
	var ret []descpb.FunctionDescriptor_Reference
	for _, ref := range desc.DependedOnBy {
		if ref.ID == id && len(ref.ColumnIDs) == 0 && len(ref.IndexIDs) == 0 && len(ref.ConstraintIDs) == 0 {
			continue
		}
		ret = append(ret, ref)
	}
	desc.DependedOnBy = ret
}

func (desc *Mutable) RemoveReference(id descpb.ID) {
	var ret []descpb.FunctionDescriptor_Reference
	for _, ref := range desc.DependedOnBy {
		if ref.ID != id {
			ret = append(ret, ref)
		}
	}
	desc.DependedOnBy = ret
}

// ToRoutineObj converts the descriptor to a tree.RoutineObj.
func (desc *immutable) ToRoutineObj() *tree.RoutineObj {
	ret := &tree.RoutineObj{
		FuncName: tree.MakeRoutineNameFromPrefix(tree.ObjectNamePrefix{}, tree.Name(desc.Name)),
		Params:   make(tree.RoutineParams, len(desc.Params)),
	}
	for i := range desc.Params {
		ret.Params[i] = tree.RoutineParam{
			Type: desc.Params[i].Type,
		}
	}
	return ret
}

// GetObjectType implements the Object interface.
func (desc *immutable) GetObjectType() privilege.ObjectType {
	return privilege.Routine
}

// GetObjectTypeString implements the Object interface.
func (desc *immutable) GetObjectTypeString() string {
	if desc.IsProcedure() {
		return "procedure"
	}
	return "function"
}

// FuncDesc implements the catalog.FunctionDescriptor interface.
func (desc *immutable) FuncDesc() *descpb.FunctionDescriptor {
	return &desc.FunctionDescriptor
}

// GetLanguage implements the FunctionDescriptor interface.
func (desc *immutable) GetLanguage() catpb.Function_Language {
	return desc.Lang
}

func (desc *immutable) ToOverload() (ret *tree.Overload, err error) {
	routineType := tree.UDFRoutine
	if desc.IsProcedure() {
		routineType = tree.ProcedureRoutine
	}
	ret = &tree.Overload{
		Oid:        catid.FuncIDToOID(desc.ID),
		ReturnType: tree.FixedReturnType(desc.ReturnType.Type),
		ReturnSet:  desc.ReturnType.ReturnSet,
		Body:       desc.FunctionBody,
		Type:       routineType,
		Version:    uint64(desc.Version),
		Language:   desc.getCreateExprLang(),
	}

	argTypes := make(tree.ParamTypes, 0, len(desc.Params))
	for _, param := range desc.Params {
		argTypes = append(
			argTypes,
			tree.ParamType{Name: param.Name, Typ: param.Type},
		)
	}
	ret.Types = argTypes
	ret.Volatility, err = desc.getOverloadVolatility()
	if err != nil {
		return nil, err
	}
	ret.CalledOnNullInput, err = desc.calledOnNullInput()
	if err != nil {
		return nil, err
	}
	if desc.ReturnType.ReturnSet {
		ret.Class = tree.GeneratorClass
	}

	return ret, nil
}

func (desc *immutable) getOverloadVolatility() (volatility.V, error) {
	var ret volatility.V
	switch desc.Volatility {
	case catpb.Function_VOLATILE:
		ret = volatility.Volatile
	case catpb.Function_STABLE:
		ret = volatility.Stable
	case catpb.Function_IMMUTABLE:
		ret = volatility.Immutable
	default:
		return 0, errors.Newf("unknown volatility")
	}
	if desc.LeakProof {
		if desc.Volatility != catpb.Function_IMMUTABLE {
			return 0, errors.Newf("function %d is leakproof but not immutable", desc.ID)
		}
		ret = volatility.Leakproof
	}
	return ret, nil
}

// calledOnNullInput returns true if the function should be called when any of
// its input arguments are NULL. See Overload.CalledOnNullInput for more
// details.
func (desc *immutable) calledOnNullInput() (bool, error) {
	switch desc.NullInputBehavior {
	case catpb.Function_CALLED_ON_NULL_INPUT:
		return true, nil
	case catpb.Function_RETURNS_NULL_ON_NULL_INPUT, catpb.Function_STRICT:
		return false, nil
	default:
		return false, errors.Newf("unknown null input behavior")
	}
}

// ToCreateExpr implements the FunctionDescriptor interface.
func (desc *immutable) ToCreateExpr() (ret *tree.CreateRoutine, err error) {
	ret = &tree.CreateRoutine{
		Name:        tree.MakeRoutineNameFromPrefix(tree.ObjectNamePrefix{}, tree.Name(desc.Name)),
		IsProcedure: desc.IsProcedure(),
		ReturnType: tree.RoutineReturnType{
			Type:  desc.ReturnType.Type,
			SetOf: desc.ReturnType.ReturnSet,
		},
	}
	ret.Params = make(tree.RoutineParams, len(desc.Params))
	for i := range desc.Params {
		ret.Params[i] = tree.RoutineParam{
			Name:  tree.Name(desc.Params[i].Name),
			Type:  desc.Params[i].Type,
			Class: toTreeNodeParamClass(desc.Params[i].Class),
		}
		if desc.Params[i].DefaultExpr != nil {
			ret.Params[i].DefaultVal, err = parser.ParseExpr(*desc.Params[i].DefaultExpr)
			if err != nil {
				return nil, err
			}
		}
	}
	// We only store 5 function attributes at the moment. We may extend the
	// pre-allocated capacity in the future.
	ret.Options = make(tree.RoutineOptions, 0, 5)
	ret.Options = append(ret.Options, desc.getCreateExprVolatility())
	ret.Options = append(ret.Options, tree.RoutineLeakproof(desc.LeakProof))
	ret.Options = append(ret.Options, desc.getCreateExprNullInputBehavior())
	ret.Options = append(ret.Options, tree.RoutineBodyStr(desc.FunctionBody))
	ret.Options = append(ret.Options, desc.getCreateExprLang())
	return ret, nil
}

// IsProcedure implements the FunctionDescriptor interface.
func (desc *immutable) IsProcedure() bool {
	return desc.FunctionDescriptor.IsProcedure
}

func (desc *immutable) getCreateExprLang() tree.RoutineLanguage {
	switch desc.Lang {
	case catpb.Function_SQL:
		return tree.RoutineLangSQL
	case catpb.Function_PLPGSQL:
		return tree.RoutineLangPLpgSQL
	}
	return tree.RoutineLangUnknown
}

func (desc *immutable) getCreateExprVolatility() tree.RoutineVolatility {
	switch desc.Volatility {
	case catpb.Function_IMMUTABLE:
		return tree.RoutineImmutable
	case catpb.Function_STABLE:
		return tree.RoutineStable
	case catpb.Function_VOLATILE:
		return tree.RoutineVolatile
	}
	return 0
}

func (desc *immutable) getCreateExprNullInputBehavior() tree.RoutineNullInputBehavior {
	switch desc.NullInputBehavior {
	case catpb.Function_CALLED_ON_NULL_INPUT:
		return tree.RoutineCalledOnNullInput
	case catpb.Function_RETURNS_NULL_ON_NULL_INPUT:
		return tree.RoutineReturnsNullOnNullInput
	case catpb.Function_STRICT:
		return tree.RoutineStrict
	}
	return 0
}

func toTreeNodeParamClass(class catpb.Function_Param_Class) tree.RoutineParamClass {
	switch class {
	case catpb.Function_Param_IN:
		return tree.RoutineParamIn
	case catpb.Function_Param_OUT:
		return tree.RoutineParamOut
	case catpb.Function_Param_IN_OUT:
		return tree.RoutineParamInOut
	case catpb.Function_Param_VARIADIC:
		return tree.RoutineParamVariadic
	}
	return 0
}

// UserDefinedFunctionOIDToID converts a UDF OID into a descriptor ID.
// Returns zero if the OID is not for something user-defined.
func UserDefinedFunctionOIDToID(oid oid.Oid) descpb.ID {
	return catid.UserDefinedOIDToID(oid)
}

// IsOIDUserDefinedFunc returns true if an oid is a user-defined function oid.
func IsOIDUserDefinedFunc(oid oid.Oid) bool {
	return catid.IsOIDUserDefined(oid)
}
