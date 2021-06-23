// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// DescriptorType is a symbol representing the (sub)type of a descriptor.
type DescriptorType string

const (
	// Any represents any descriptor.
	Any DescriptorType = "any"

	// Database is for database descriptors.
	Database = "database"

	// Table is for table descriptors.
	Table = "relation"

	// Type is for type descriptors.
	Type = "type"

	// Schema is for schema descriptors.
	Schema = "schema"
)

// MutationPublicationFilter is used by MakeFirstMutationPublic to filter the
// mutation types published.
type MutationPublicationFilter int

const (
	// IgnoreConstraints is used in MakeFirstMutationPublic to indicate that the
	// table descriptor returned should not include newly added constraints, which
	// is useful when passing the returned table descriptor to be used in
	// validating constraints to be added.
	IgnoreConstraints MutationPublicationFilter = 1
	// IgnoreConstraintsAndPKSwaps is used in MakeFirstMutationPublic to indicate that the
	// table descriptor returned should include newly added constraints.
	IgnoreConstraintsAndPKSwaps = 2
	// IncludeConstraints is used in MakeFirstMutationPublic to indicate that the
	// table descriptor returned should include newly added constraints.
	IncludeConstraints = 0
)

// DescriptorBuilder interfaces are used to build catalog.Descriptor
// objects.
type DescriptorBuilder interface {

	// DescriptorType returns a symbol identifying the type of the descriptor
	// built by this builder.
	DescriptorType() DescriptorType

	// RunPostDeserializationChanges attempts to perform post-deserialization
	// changes to the descriptor being built.
	// These changes are always done on a best-effort basis, meaning that all
	// arguments other than ctx are optional. As of writing this, the only other
	// argument is a DescGetter and a nil value will cause table foreign-key
	// representation upgrades to be skipped.
	RunPostDeserializationChanges(ctx context.Context, dg DescGetter) error

	// BuildImmutable returns an immutable Descriptor.
	BuildImmutable() Descriptor

	// BuildExistingMutable returns a MutableDescriptor with the cluster version
	// set to the original value of the descriptor used to initialize the builder.
	// This is for descriptors that already exist.
	BuildExistingMutable() MutableDescriptor

	// BuildCreatedMutable returns a MutableDescriptor with a nil cluster version.
	// This is for a descriptor that is created in the same transaction.
	BuildCreatedMutable() MutableDescriptor
}

// IndexOpts configures the behavior of catalog.ForEachIndex and
// catalog.FindIndex.
type IndexOpts struct {
	// NonPhysicalPrimaryIndex should be included.
	NonPhysicalPrimaryIndex bool
	// DropMutations should be included.
	DropMutations bool
	// AddMutations should be included.
	AddMutations bool
}

// NameKey is an interface for objects which have all the components
// of their corresponding namespace table entry.
// Typically these objects are either Descriptor implementations or
// descpb.NameInfo structs.
type NameKey interface {
	GetName() string
	GetParentID() descpb.ID
	GetParentSchemaID() descpb.ID
}

// NameEntry corresponds to an entry in the namespace table.
type NameEntry interface {
	NameKey
	GetID() descpb.ID
}

var _ NameKey = descpb.NameInfo{}

// Descriptor is an interface to be shared by individual descriptor
// types.
type Descriptor interface {
	NameEntry

	// IsUncommittedVersion returns true if this descriptor represent a version
	// which is not the currently committed version. Implementations may return
	// false negatives here in cases where a descriptor may have crossed a
	// serialization boundary. In particular, this can occur during execution on
	// remote nodes as well as during some scenarios in schema changes.
	IsUncommittedVersion() bool

	// Metadata for descriptor leasing.
	GetVersion() descpb.DescriptorVersion
	GetModificationTime() hlc.Timestamp
	GetDrainingNames() []descpb.NameInfo

	GetPrivileges() *descpb.PrivilegeDescriptor
	DescriptorType() DescriptorType
	GetAuditMode() descpb.TableDescriptor_AuditMode

	Public() bool
	Adding() bool
	Dropped() bool
	Offline() bool
	GetOfflineReason() string

	// DescriptorProto prepares this descriptor for serialization.
	DescriptorProto() *descpb.Descriptor

	// GetReferencedDescIDs returns the IDs of all descriptors directly referenced
	// by this descriptor, including itself.
	GetReferencedDescIDs() (DescriptorIDSet, error)

	// ValidateSelf checks the internal consistency of the descriptor.
	ValidateSelf(vea ValidationErrorAccumulator)

	// ValidateCrossReferences performs cross-reference checks.
	ValidateCrossReferences(vea ValidationErrorAccumulator, vdg ValidationDescGetter)

	// ValidateTxnCommit performs pre-commit checks.
	ValidateTxnCommit(vea ValidationErrorAccumulator, vdg ValidationDescGetter)
}

// DatabaseDescriptor encapsulates the concept of a database.
type DatabaseDescriptor interface {
	Descriptor

	DatabaseDesc() *descpb.DatabaseDescriptor

	GetRegionConfig() *descpb.DatabaseDescriptor_RegionConfig
	IsMultiRegion() bool
	PrimaryRegionName() (descpb.RegionName, error)
	MultiRegionEnumID() (descpb.ID, error)
	ForEachSchemaInfo(func(id descpb.ID, name string, isDropped bool) error) error
	GetSchemaID(name string) descpb.ID
	GetNonDroppedSchemaName(schemaID descpb.ID) string
}

// TableDescriptor is an interface around the table descriptor types.
type TableDescriptor interface {
	Descriptor

	TableDesc() *descpb.TableDescriptor

	GetState() descpb.DescriptorState
	GetSequenceOpts() *descpb.TableDescriptor_SequenceOpts
	GetCreateQuery() string
	GetViewQuery() string
	GetLease() *descpb.TableDescriptor_SchemaChangeLease
	GetCreateAsOfTime() hlc.Timestamp
	GetModificationTime() hlc.Timestamp
	GetDropTime() int64
	GetFormatVersion() descpb.FormatVersion

	GetPrimaryIndexID() descpb.IndexID
	GetPrimaryIndex() Index
	IsPartitionAllBy() bool
	PrimaryIndexSpan(codec keys.SQLCodec) roachpb.Span
	IndexSpan(codec keys.SQLCodec, id descpb.IndexID) roachpb.Span
	AllIndexSpans(codec keys.SQLCodec) roachpb.Spans
	TableSpan(codec keys.SQLCodec) roachpb.Span
	GetIndexMutationCapabilities(id descpb.IndexID) (isMutation, isWriteOnly bool)
	KeysPerRow(id descpb.IndexID) (int, error)

	// AllIndexes returns a slice with all indexes, public and non-public,
	// in the underlying proto, in their canonical order:
	// - the primary index,
	// - the public non-primary indexes in the Indexes array, in order,
	// - the non-public indexes present in the Mutations array, in order.
	//
	// See also Index.Ordinal().
	AllIndexes() []Index

	// ActiveIndexes returns a slice with all public indexes in the underlying
	// proto, in their canonical order:
	// - the primary index,
	// - the public non-primary indexes in the Indexes array, in order.
	//
	// See also Index.Ordinal().
	ActiveIndexes() []Index

	// NonDropIndexes returns a slice of all non-drop indexes in the underlying
	// proto, in their canonical order. This means:
	// - the primary index, if the table is a physical table,
	// - the public non-primary indexes in the Indexes array, in order,
	// - the non-public indexes present in the Mutations array, in order,
	//   if the mutation is not a drop.
	//
	// See also Index.Ordinal().
	NonDropIndexes() []Index

	// PartialIndexes returns a slice of all partial indexes in the underlying
	// proto, in their canonical order. This is equivalent to taking the slice
	// produced by AllIndexes and removing indexes with empty expressions.
	PartialIndexes() []Index

	// PublicNonPrimaryIndexes returns a slice of all active secondary indexes,
	// in their canonical order. This is equivalent to the Indexes array in the
	// proto.
	PublicNonPrimaryIndexes() []Index

	// WritableNonPrimaryIndexes returns a slice of all non-primary indexes which
	// allow being written to: public + delete-and-write-only, in their canonical
	// order. This is equivalent to taking the slice produced by
	// DeletableNonPrimaryIndexes and removing the indexes which are in mutations
	// in the delete-only state.
	WritableNonPrimaryIndexes() []Index

	// DeletableNonPrimaryIndexes returns a slice of all non-primary indexes
	// which allow being deleted from: public + delete-and-write-only +
	// delete-only, in  their canonical order. This is equivalent to taking
	// the slice produced by AllIndexes and removing the primary index.
	DeletableNonPrimaryIndexes() []Index

	// DeleteOnlyNonPrimaryIndexes returns a slice of all non-primary indexes
	// which allow only being deleted from, in their canonical order. This is
	// equivalent to taking the slice produced by DeletableNonPrimaryIndexes and
	// removing the indexes which are not in mutations or not in the delete-only
	// state.
	DeleteOnlyNonPrimaryIndexes() []Index

	// FindIndexWithID returns the first catalog.Index that matches the id
	// in the set of all indexes, or an error if none was found. The order of
	// traversal is the canonical order, see Index.Ordinal().
	FindIndexWithID(id descpb.IndexID) (Index, error)

	// FindIndexWithName returns the first catalog.Index that matches the name in
	// the set of all indexes, excluding the primary index of non-physical
	// tables, or an error if none was found. The order of traversal is the
	// canonical order, see Index.Ordinal().
	FindIndexWithName(name string) (Index, error)

	GetNextIndexID() descpb.IndexID

	HasPrimaryKey() bool
	PrimaryKeyString() string

	AllColumns() []Column
	PublicColumns() []Column
	WritableColumns() []Column
	DeletableColumns() []Column
	NonDropColumns() []Column
	VisibleColumns() []Column
	AccessibleColumns() []Column
	ReadableColumns() []Column
	UserDefinedTypeColumns() []Column
	SystemColumns() []Column

	FindColumnWithID(id descpb.ColumnID) (Column, error)
	FindColumnWithName(name tree.Name) (Column, error)

	NamesForColumnIDs(ids descpb.ColumnIDs) ([]string, error)
	ContainsUserDefinedTypes() bool
	GetNextColumnID() descpb.ColumnID
	CheckConstraintUsesColumn(cc *descpb.TableDescriptor_CheckConstraint, colID descpb.ColumnID) (bool, error)

	GetFamilies() []descpb.ColumnFamilyDescriptor
	NumFamilies() int
	FindFamilyByID(id descpb.FamilyID) (*descpb.ColumnFamilyDescriptor, error)
	ForeachFamily(f func(family *descpb.ColumnFamilyDescriptor) error) error
	GetNextFamilyID() descpb.FamilyID

	IsTable() bool
	IsView() bool
	IsSequence() bool
	IsTemporary() bool
	IsVirtualTable() bool
	IsPhysicalTable() bool
	IsInterleaved() bool
	MaterializedView() bool
	IsAs() bool

	HasColumnBackfillMutation() bool
	MakeFirstMutationPublic(includeConstraints MutationPublicationFilter) (TableDescriptor, error)
	MakePublic() TableDescriptor
	AllMutations() []Mutation
	GetGCMutations() []descpb.TableDescriptor_GCDescriptorMutation
	GetMutationJobs() []descpb.TableDescriptor_MutationJob

	GetReplacementOf() descpb.TableDescriptor_Replacement
	GetAllReferencedTypeIDs(
		databaseDesc DatabaseDescriptor, getType func(descpb.ID) (TypeDescriptor, error),
	) (descpb.IDs, error)

	ForeachDependedOnBy(f func(dep *descpb.TableDescriptor_Reference) error) error
	GetDependedOnBy() []descpb.TableDescriptor_Reference
	GetDependsOn() []descpb.ID
	GetDependsOnTypes() []descpb.ID
	GetConstraintInfoWithLookup(fn TableLookupFn) (map[string]descpb.ConstraintDetail, error)
	ForeachOutboundFK(f func(fk *descpb.ForeignKeyConstraint) error) error
	GetChecks() []*descpb.TableDescriptor_CheckConstraint
	AllActiveAndInactiveChecks() []*descpb.TableDescriptor_CheckConstraint
	ActiveChecks() []descpb.TableDescriptor_CheckConstraint
	GetUniqueWithoutIndexConstraints() []descpb.UniqueWithoutIndexConstraint
	AllActiveAndInactiveUniqueWithoutIndexConstraints() []*descpb.UniqueWithoutIndexConstraint
	ForeachInboundFK(f func(fk *descpb.ForeignKeyConstraint) error) error
	GetConstraintInfo() (map[string]descpb.ConstraintDetail, error)
	AllActiveAndInactiveForeignKeys() []*descpb.ForeignKeyConstraint
	GetInboundFKs() []descpb.ForeignKeyConstraint
	GetOutboundFKs() []descpb.ForeignKeyConstraint

	GetLocalityConfig() *descpb.TableDescriptor_LocalityConfig
	IsLocalityRegionalByRow() bool
	IsLocalityRegionalByTable() bool
	IsLocalityGlobal() bool
	GetRegionalByTableRegion() (descpb.RegionName, error)
	GetRegionalByRowTableRegionColumnName() (tree.Name, error)
}

// TypeDescriptor will eventually be called typedesc.Descriptor.
// It is implemented by (Imm|M)utableTypeDescriptor.
type TypeDescriptor interface {
	Descriptor
	TypeDesc() *descpb.TypeDescriptor
	HydrateTypeInfoWithName(ctx context.Context, typ *types.T, name *tree.TypeName, res TypeDescriptorResolver) error
	MakeTypesT(ctx context.Context, name *tree.TypeName, res TypeDescriptorResolver) (*types.T, error)
	HasPendingSchemaChanges() bool
	GetIDClosure() (map[descpb.ID]struct{}, error)
	IsCompatibleWith(other TypeDescriptor) error

	PrimaryRegionName() (descpb.RegionName, error)
	RegionNames() (descpb.RegionNames, error)
	RegionNamesIncludingTransitioning() (descpb.RegionNames, error)
	RegionNamesForValidation() (descpb.RegionNames, error)
	TransitioningRegionNames() (descpb.RegionNames, error)

	GetArrayTypeID() descpb.ID
	GetKind() descpb.TypeDescriptor_Kind

	NumEnumMembers() int
	GetMemberPhysicalRepresentation(enumMemberOrdinal int) []byte
	GetMemberLogicalRepresentation(enumMemberOrdinal int) string
	IsMemberReadOnly(enumMemberOrdinal int) bool

	NumReferencingDescriptors() int
	GetReferencingDescriptorID(refOrdinal int) descpb.ID
}

// TypeDescriptorResolver is an interface used during hydration of type
// metadata in types.T's. It is similar to tree.TypeReferenceResolver, except
// that it has the power to return TypeDescriptor, rather than only a
// types.T. Implementers of tree.TypeReferenceResolver should implement this
// interface as well.
type TypeDescriptorResolver interface {
	// GetTypeDescriptor returns the type descriptor for the input ID.
	GetTypeDescriptor(ctx context.Context, id descpb.ID) (tree.TypeName, TypeDescriptor, error)
}

// FilterDescriptorState inspects the state of a given descriptor and returns an
// error if the state is anything but public. The error describes the state of
// the descriptor.
func FilterDescriptorState(desc Descriptor, flags tree.CommonLookupFlags) error {
	switch {
	case desc.Dropped() && !flags.IncludeDropped:
		return NewInactiveDescriptorError(ErrDescriptorDropped)
	case desc.Offline() && !flags.IncludeOffline:
		err := errors.Errorf("%s %q is offline", desc.DescriptorType(), desc.GetName())
		if desc.GetOfflineReason() != "" {
			err = errors.Errorf("%s %q is offline: %s", desc.DescriptorType(), desc.GetName(), desc.GetOfflineReason())
		}
		return NewInactiveDescriptorError(err)
	case desc.Adding():
		// Only table descriptors can be in the adding state.
		return pgerror.WithCandidateCode(newAddingTableError(desc.(TableDescriptor)),
			pgcode.ObjectNotInPrerequisiteState)
	default:
		return nil
	}
}

// TableLookupFn is used to resolve a table from an ID, particularly when
// getting constraint info.
type TableLookupFn func(descpb.ID) (TableDescriptor, error)

// Descriptors is a sortable list of Descriptors.
type Descriptors []Descriptor

func (d Descriptors) Len() int           { return len(d) }
func (d Descriptors) Less(i, j int) bool { return d[i].GetID() < d[j].GetID() }
func (d Descriptors) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }

// FormatSafeDescriptorProperties is a shared helper function for writing
// un-redacted, common parts of a descriptor. It writes <prop>: value separated
// by commas to w. These key-value pairs would be valid YAML if wrapped in
// curly braces.
func FormatSafeDescriptorProperties(w *redact.StringBuilder, desc Descriptor) {
	w.Printf("ID: %d, Version: %d", desc.GetID(), desc.GetVersion())
	if desc.IsUncommittedVersion() {
		w.Printf(", IsUncommitted: true")
	}
	w.Printf(", ModificationTime: %q", desc.GetModificationTime())
	if parentID := desc.GetParentID(); parentID != 0 {
		w.Printf(", ParentID: %d", parentID)
	}
	if parentSchemaID := desc.GetParentSchemaID(); parentSchemaID != 0 {
		w.Printf(", ParentSchemaID: %d", parentSchemaID)
	}
	{
		var state descpb.DescriptorState
		switch {
		case desc.Public():
			state = descpb.DescriptorState_PUBLIC
		case desc.Dropped():
			state = descpb.DescriptorState_DROP
		case desc.Adding():
			state = descpb.DescriptorState_ADD
		case desc.Offline():
			state = descpb.DescriptorState_OFFLINE
		}
		w.Printf(", State: %v", state)
		if offlineReason := desc.GetOfflineReason(); state == descpb.DescriptorState_OFFLINE &&
			offlineReason != "" {
			w.Printf(", OfflineReason: %q", redact.Safe(offlineReason))
		}
	}
	if drainingNames := desc.GetDrainingNames(); len(drainingNames) > 0 {
		w.Printf(", NumDrainingNames: %d", len(drainingNames))
	}
}
