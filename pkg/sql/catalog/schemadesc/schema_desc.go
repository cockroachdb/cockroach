// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemadesc

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

	// changed represents how the descriptor was changed after
	// RunPostDeserializationChanges.
	changes catalog.PostDeserializationChanges

	// This is the raw bytes (tag + data) of the schema descriptor in storage.
	rawBytesInStorage []byte
}

func (desc *immutable) SchemaKind() catalog.ResolvedSchemaKind {
	return catalog.SchemaUserDefined
}

// SafeMessage makes immutable a SafeMessager.
func (desc *immutable) SafeMessage() string {
	return formatSafeMessage("schemadesc.immutable", desc)
}

func (desc *immutable) GetFunction(name string) (descpb.SchemaDescriptor_Function, bool) {
	fn, found := desc.Functions[name]
	return fn, found
}

// SkipNamespace implements the descriptor interface.
func (desc *immutable) SkipNamespace() bool {
	return false
}

// GetRawBytesInStorage implements the catalog.Descriptor interface.
func (desc *immutable) GetRawBytesInStorage() []byte {
	return desc.rawBytesInStorage
}

// ForEachUDTDependentForHydration implements the catalog.Descriptor interface.
func (desc *immutable) ForEachUDTDependentForHydration(fn func(t *types.T) error) error {
	for _, f := range desc.Functions {
		for _, sig := range f.Signatures {
			for _, typ := range sig.ArgTypes {
				if !catid.IsOIDUserDefined(typ.Oid()) {
					continue
				}
				if err := fn(typ); err != nil {
					return iterutil.Map(err)
				}
			}
			for _, typ := range sig.OutParamTypes {
				if !catid.IsOIDUserDefined(typ.Oid()) {
					continue
				}
				if err := fn(typ); err != nil {
					return iterutil.Map(err)
				}
			}
			if !catid.IsOIDUserDefined(sig.ReturnType.Oid()) {
				continue
			}
			if err := fn(sig.ReturnType); err != nil {
				return iterutil.Map(err)
			}
		}
	}
	return nil
}

// MaybeRequiresTypeHydration implements the catalog.Descriptor interface.
func (desc *immutable) MaybeRequiresTypeHydration() bool {
	for _, f := range desc.Functions {
		for _, sig := range f.Signatures {
			if catid.IsOIDUserDefined(sig.ReturnType.Oid()) {
				return true
			}
			for _, typ := range sig.ArgTypes {
				if catid.IsOIDUserDefined(typ.Oid()) {
					return true
				}
			}
			for _, typ := range sig.OutParamTypes {
				if catid.IsOIDUserDefined(typ.Oid()) {
					return true
				}
			}
		}
	}
	return false
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
// large amount of space to question this version of each
// descriptor type. Maybe it makes no sense but we're running with it for the
// moment. This is an intermediate state on the road to descriptors being
// handled outside of the catalog entirely as interfaces.
type Mutable struct {
	immutable

	ClusterVersion *immutable
}

var _ redact.SafeMessager = (*immutable)(nil)

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

// ByteSize implements the Descriptor interface.
func (desc *immutable) ByteSize() int64 {
	return int64(desc.Size())
}

// GetDeclarativeSchemaChangerState is part of the catalog.MutableDescriptor
// interface.
func (desc *immutable) GetDeclarativeSchemaChangerState() *scpb.DescriptorState {
	return desc.DeclarativeSchemaChangerState.Clone()
}

// NewBuilder implements the catalog.Descriptor interface.
//
// It overrides the wrapper's implementation to deal with the fact that
// mutable has overridden the definition of IsUncommittedVersion.
func (desc *Mutable) NewBuilder() catalog.DescriptorBuilder {
	b := newBuilder(desc.SchemaDesc(), hlc.Timestamp{}, desc.IsUncommittedVersion(), desc.changes)
	b.SetRawBytesInStorage(desc.GetRawBytesInStorage())
	return b
}

// NewBuilder implements the catalog.Descriptor interface.
func (desc *immutable) NewBuilder() catalog.DescriptorBuilder {
	b := newBuilder(desc.SchemaDesc(), hlc.Timestamp{}, desc.IsUncommittedVersion(), desc.changes)
	b.SetRawBytesInStorage(desc.GetRawBytesInStorage())
	return b
}

// ValidateSelf implements the catalog.Descriptor interface.
func (desc *immutable) ValidateSelf(vea catalog.ValidationErrorAccumulator) {
	// Validate local properties of the descriptor.
	vea.Report(catalog.ValidateName(desc))
	if desc.GetID() == descpb.InvalidID {
		vea.Report(fmt.Errorf("invalid schema ID %d", desc.GetID()))
	}

	// Validate the privilege descriptor.
	if desc.Privileges == nil {
		vea.Report(errors.AssertionFailedf("privileges not set"))
	} else {
		vea.Report(catprivilege.Validate(*desc.Privileges, desc, privilege.Schema))
	}

	if desc.GetDefaultPrivileges() != nil {
		// Validate the default privilege descriptor.
		vea.Report(catprivilege.ValidateDefaultPrivileges(*desc.GetDefaultPrivileges()))
	}

	for _, f := range desc.Functions {
		for _, sig := range f.Signatures {
			if sig.ID == descpb.InvalidID {
				vea.Report(fmt.Errorf("invalid function ID %d", sig.ID))
			}
		}
	}
}

// GetReferencedDescIDs returns the IDs of all descriptors referenced by
// this descriptor, including itself.
func (desc *immutable) GetReferencedDescIDs(
	level catalog.ValidationLevel,
) (catalog.DescriptorIDSet, error) {
	ret := catalog.MakeDescriptorIDSet(desc.GetID(), desc.GetParentID())
	// We only need to resolve functions in this schema if we are validating
	// back references as well.
	if level&catalog.ValidationLevelBackReferences == catalog.ValidationLevelBackReferences {
		for _, f := range desc.Functions {
			for _, sig := range f.Signatures {
				ret.Add(sig.ID)
			}
		}
	}

	return ret, nil
}

// ValidateForwardReferences implements the catalog.Descriptor interface.
func (desc *immutable) ValidateForwardReferences(
	vea catalog.ValidationErrorAccumulator, vdg catalog.ValidationDescGetter,
) {
	// Check schema parent reference.
	db, err := vdg.GetDatabaseDescriptor(desc.GetParentID())
	if err != nil {
		vea.Report(err)
		return
	}
	if db.Dropped() {
		vea.Report(errors.AssertionFailedf("parent database %q (%d) is dropped",
			db.GetName(), db.GetID()))
	}
}

// ValidateBackReferences implements the catalog.Descriptor interface.
func (desc *immutable) ValidateBackReferences(
	vea catalog.ValidationErrorAccumulator, vdg catalog.ValidationDescGetter,
) {
	// Check that parent database has correct entry in schemas mapping.
	if db, err := vdg.GetDatabaseDescriptor(desc.GetParentID()); err == nil {
		isInDBSchemas := false
		_ = db.ForEachSchema(func(id descpb.ID, name string) error {
			if id == desc.GetID() {
				if name != desc.GetName() {
					vea.Report(errors.AssertionFailedf("present in parent database [%d] schemas mapping but under name %q",
						desc.GetParentID(), errors.Safe(name)))
					return nil
				}
				isInDBSchemas = true
				return nil
			}
			if name == desc.GetName() {
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

	// Check that all functions exist.
	for _, function := range desc.Functions {
		for _, sig := range function.Signatures {
			_, err := vdg.GetFunctionDescriptor(sig.ID)
			if err != nil {
				vea.Report(errors.AssertionFailedf("invalid function %d in schema %q (%d)",
					sig.ID, desc.GetName(), desc.GetID()))
			}
		}
	}
}

// ValidateTxnCommit implements the catalog.Descriptor interface.
func (desc *immutable) ValidateTxnCommit(
	_ catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
	// No-op.
}

// GetDefaultPrivilegeDescriptor returns a DefaultPrivilegeDescriptor.
func (desc *immutable) GetDefaultPrivilegeDescriptor() catalog.DefaultPrivilegeDescriptor {
	defaultPrivilegeDescriptor := desc.GetDefaultPrivileges()
	if defaultPrivilegeDescriptor == nil {
		defaultPrivilegeDescriptor = catprivilege.MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_SCHEMA)
	}
	return catprivilege.MakeDefaultPrivileges(defaultPrivilegeDescriptor)
}

// GetPostDeserializationChanges implements the Descriptor interface.
func (desc *immutable) GetPostDeserializationChanges() catalog.PostDeserializationChanges {
	return desc.changes
}

// HasConcurrentSchemaChanges implements catalog.Descriptor.
func (desc *immutable) HasConcurrentSchemaChanges() bool {
	return desc.DeclarativeSchemaChangerState != nil &&
		desc.DeclarativeSchemaChangerState.JobID != catpb.InvalidJobID
}

// ConcurrentSchemaChangeJobIDs implements catalog.Descriptor.
func (desc *immutable) ConcurrentSchemaChangeJobIDs() (ret []catpb.JobID) {
	if desc.DeclarativeSchemaChangerState != nil &&
		desc.DeclarativeSchemaChangerState.JobID != catpb.InvalidJobID {
		ret = append(ret, desc.DeclarativeSchemaChangerState.JobID)
	}
	return ret
}

// MaybeIncrementVersion implements the MutableDescriptor interface.
func (desc *Mutable) MaybeIncrementVersion() {
	// Already incremented, no-op.
	if desc.ClusterVersion == nil || desc.Version == desc.ClusterVersion.Version+1 {
		return
	}
	desc.Version++
	desc.ResetModificationTime()
}

// ResetModificationTime implements the catalog.MutableDescriptor interface.
func (desc *Mutable) ResetModificationTime() {
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
	return desc.NewBuilder().BuildImmutable()
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

// SetName sets the name of the schema.
func (desc *Mutable) SetName(name string) {
	desc.Name = name
}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *Mutable) IsUncommittedVersion() bool {
	return desc.IsNew() || desc.GetVersion() != desc.ClusterVersion.GetVersion()
}

// GetMutableDefaultPrivilegeDescriptor returns a catprivilege.Mutable.
func (desc *Mutable) GetMutableDefaultPrivilegeDescriptor() *catprivilege.Mutable {
	defaultPrivilegeDescriptor := desc.GetDefaultPrivileges()
	if defaultPrivilegeDescriptor == nil {
		defaultPrivilegeDescriptor = catprivilege.MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_SCHEMA)
	}
	return catprivilege.NewMutableDefaultPrivileges(defaultPrivilegeDescriptor)
}

// SetDefaultPrivilegeDescriptor sets the default privilege descriptor
// for the database.
func (desc *Mutable) SetDefaultPrivilegeDescriptor(
	defaultPrivilegeDescriptor *catpb.DefaultPrivilegeDescriptor,
) {
	desc.DefaultPrivileges = defaultPrivilegeDescriptor
}

// GetDeclarativeSchemaChangeState is part of the catalog.MutableDescriptor
// interface.
func (desc *immutable) GetDeclarativeSchemaChangeState() *scpb.DescriptorState {
	return desc.DeclarativeSchemaChangerState.Clone()
}

// SetDeclarativeSchemaChangerState is part of the catalog.MutableDescriptor
// interface.
func (desc *Mutable) SetDeclarativeSchemaChangerState(state *scpb.DescriptorState) {
	desc.DeclarativeSchemaChangerState = state
}

// AddFunction adds a UDF overload signature to the schema descriptor.
func (desc *Mutable) AddFunction(name string, f descpb.SchemaDescriptor_FunctionSignature) {
	if desc.Functions == nil {
		desc.Functions = make(map[string]descpb.SchemaDescriptor_Function)
	}
	if overloads, ok := desc.Functions[name]; !ok {
		desc.Functions[name] = descpb.SchemaDescriptor_Function{Signatures: []descpb.SchemaDescriptor_FunctionSignature{f}}
	} else {
		newSigs := append(overloads.Signatures, f)
		desc.Functions[name] = descpb.SchemaDescriptor_Function{Signatures: newSigs}
	}
}

// RemoveFunction removes a UDF overload signature from the schema descriptor.
func (desc *Mutable) RemoveFunction(name string, id descpb.ID) {
	if fn, ok := desc.Functions[name]; ok {
		var updated []descpb.SchemaDescriptor_FunctionSignature
		for _, sig := range fn.Signatures {
			if sig.ID != id {
				updated = append(updated, sig)
			}
		}
		if len(updated) == 0 {
			delete(desc.Functions, name)
			return
		}
		desc.Functions[name] = descpb.SchemaDescriptor_Function{
			Name:       name,
			Signatures: updated,
		}
	}
}

// ReplaceOverload updates the function signature that matches the existing
// overload with the new one. An error is returned if the function doesn't exist
// or a match is not found.
//
// DefaultExprs in existing are expected to have been type-checked.
func (desc *Mutable) ReplaceOverload(
	name string,
	existing *tree.QualifiedOverload,
	newSignature descpb.SchemaDescriptor_FunctionSignature,
) error {
	fn, ok := desc.Functions[name]
	if !ok {
		return errors.AssertionFailedf("unexpectedly didn't find a function %s", name)
	}
	for i := range fn.Signatures {
		sig := fn.Signatures[i]
		match := existing.Types.Length() == len(sig.ArgTypes) &&
			len(existing.OutParamOrdinals) == len(sig.OutParamOrdinals) &&
			len(existing.DefaultExprs) == len(sig.DefaultExprs)
		for j := 0; match && j < len(sig.ArgTypes); j++ {
			match = existing.Types.GetAt(j).Equivalent(sig.ArgTypes[j])
		}
		for j := 0; match && j < len(sig.OutParamOrdinals); j++ {
			match = existing.OutParamOrdinals[j] == sig.OutParamOrdinals[j] &&
				existing.OutParamTypes.GetAt(j).Equivalent(sig.OutParamTypes[j])
		}
		for j := 0; match && j < len(sig.DefaultExprs); j++ {
			texpr, ok := existing.DefaultExprs[j].(tree.TypedExpr)
			if !ok {
				return errors.AssertionFailedf(
					"expected DEFAULT expr %s to have been type-checked, found %T",
					existing.DefaultExprs[j], existing.DefaultExprs[j],
				)
			}
			match = tree.Serialize(texpr) == sig.DefaultExprs[j]
		}
		if match {
			fn.Signatures[i] = newSignature
			return nil
		}
	}
	return errors.AssertionFailedf("unexpectedly didn't find overload match for function %s with types %v", name, existing.Types.Types())
}

// GetObjectType implements the Object interface.
func (desc *immutable) GetObjectType() privilege.ObjectType {
	return privilege.Schema
}

// GetObjectTypeString implements the Object interface.
func (desc *immutable) GetObjectTypeString() string {
	return string(privilege.Schema)
}

// GetResolvedFuncDefinition implements the SchemaDescriptor interface.
// TODO(mgartner): This should not create tree.Overloads because it cannot fully
// populated them.
func (desc *immutable) GetResolvedFuncDefinition(
	ctx context.Context, name string,
) (*tree.ResolvedFunctionDefinition, bool) {
	funcDescPb, found := desc.GetFunction(name)
	if !found {
		return nil, false
	}
	funcDef := &tree.ResolvedFunctionDefinition{
		Name:      name,
		Overloads: make([]tree.QualifiedOverload, 0, len(funcDescPb.Signatures)),
	}
	for i := range funcDescPb.Signatures {
		sig := &funcDescPb.Signatures[i]
		retType := sig.ReturnType
		routineType := tree.UDFRoutine
		if sig.IsProcedure {
			routineType = tree.ProcedureRoutine
		}
		overload := &tree.Overload{
			Oid: catid.FuncIDToOID(sig.ID),
			ReturnType: func(args []tree.TypedExpr) *types.T {
				return retType
			},
			Type:                     routineType,
			UDFContainsOnlySignature: true,
			OutParamOrdinals:         sig.OutParamOrdinals,
		}
		if funcDescPb.Signatures[i].ReturnSet {
			overload.Class = tree.GeneratorClass
		}
		// There is no need to look at the parameter classes since ArgTypes
		// already contains only parameters that are included into the
		// signature of the overload.
		paramTypes := make(tree.ParamTypes, 0, len(sig.ArgTypes))
		for _, paramType := range sig.ArgTypes {
			paramTypes = append(
				paramTypes,
				tree.ParamType{Typ: paramType},
			)
		}
		overload.Types = paramTypes
		if len(sig.OutParamTypes) > 0 {
			outParamTypes := make(tree.ParamTypes, len(sig.OutParamTypes))
			for j := range outParamTypes {
				outParamTypes[j] = tree.ParamType{Typ: sig.OutParamTypes[j]}
			}
			overload.OutParamTypes = outParamTypes
		}
		if len(sig.DefaultExprs) > 0 {
			overload.DefaultExprs = make(tree.Exprs, len(sig.DefaultExprs))
			for j, expr := range sig.DefaultExprs {
				var err error
				overload.DefaultExprs[j], err = parser.ParseExpr(expr)
				if err != nil {
					// We should never get an error during parsing the default
					// expr.
					inputParamOrdinal := len(sig.ArgTypes) - (len(sig.DefaultExprs) - j)
					log.Errorf(
						ctx, "DEFAULT expr for input param %d for routine %s: %v",
						inputParamOrdinal, name, err,
					)
					return nil, false
				}
			}
		}
		prefixedOverload := tree.MakeQualifiedOverload(desc.GetName(), overload)
		funcDef.Overloads = append(funcDef.Overloads, prefixedOverload)
	}

	return funcDef, true
}

// ForEachFunctionSignature implements the SchemaDescriptor interface.
func (desc *immutable) ForEachFunctionSignature(
	fn func(sig descpb.SchemaDescriptor_FunctionSignature) error,
) error {
	for _, function := range desc.Functions {
		for i := range function.Signatures {
			if err := fn(function.Signatures[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetReplicatedPCRVersion is a part of the catalog.Descriptor
func (desc *immutable) GetReplicatedPCRVersion() descpb.DescriptorVersion {
	return desc.ReplicatedPCRVersion
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
