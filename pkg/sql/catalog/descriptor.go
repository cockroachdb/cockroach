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

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

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

// Descriptor is an interface to be shared by individual descriptor
// types.
type Descriptor interface {
	tree.NameResolutionResult

	GetID() descpb.ID
	GetName() string
	GetParentID() descpb.ID
	GetParentSchemaID() descpb.ID

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
	TypeName() string
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
	GetReferencedDescIDs() DescriptorIDSet

	// ValidateSelf checks the internal consistency of the descriptor.
	ValidateSelf(vea ValidationErrorAccumulator)

	// ValidateCrossReferences performs cross-reference checks.
	ValidateCrossReferences(vea ValidationErrorAccumulator, vdg ValidationDescGetter)

	// ValidateTxnCommit performs pre-commit checks.
	ValidateTxnCommit(vea ValidationErrorAccumulator, vdg ValidationDescGetter)
}

// DatabaseDescriptor will eventually be called dbdesc.Descriptor.
// It is implemented by Immutable.
type DatabaseDescriptor interface {
	Descriptor

	// Note: Prior to user-defined schemas, databases were the schema meta for
	// objects.
	//
	// TODO(ajwerner): Remove this in the 20.2 cycle as part of user-defined
	// schemas.
	tree.SchemaMeta
	DatabaseDesc() *descpb.DatabaseDescriptor

	GetRegionConfig() *descpb.DatabaseDescriptor_RegionConfig
	RegionNames() (descpb.RegionNames, error)
	IsMultiRegion() bool
	PrimaryRegionName() (descpb.RegionName, error)
	MultiRegionEnumID() (descpb.ID, error)
	ForEachSchemaInfo(func(id descpb.ID, name string, isDropped bool) error) error
}

// SchemaDescriptor will eventually be called schemadesc.Descriptor.
// It is implemented by Immutable.
type SchemaDescriptor interface {
	Descriptor
	SchemaDesc() *descpb.SchemaDescriptor
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
	//	// - the public non-primary indexes in the Indexes array, in order,
	//	// - the non-public indexes present in the Mutations array, in order.
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

	// NonDropIndexes returns a slice of all partial indexes in the underlying
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
	MakeFirstMutationPublic(includeConstraints bool) (TableDescriptor, error)
	GetMutations() []descpb.DescriptorMutation
	GetGCMutations() []descpb.TableDescriptor_GCDescriptorMutation
	GetMutationJobs() []descpb.TableDescriptor_MutationJob

	GetReplacementOf() descpb.TableDescriptor_Replacement
	GetAllReferencedTypeIDs(
		databaseDesc DatabaseDescriptor, getType func(descpb.ID) (TypeDescriptor, error),
	) (descpb.IDs, error)

	ForeachDependedOnBy(f func(dep *descpb.TableDescriptor_Reference) error) error
	GetDependsOn() []descpb.ID
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

// Index is an interface around the index descriptor types.
type Index interface {

	// IndexDesc returns the underlying protobuf descriptor.
	// Ideally, this method should be called as rarely as possible.
	IndexDesc() *descpb.IndexDescriptor

	// IndexDescDeepCopy returns a deep copy of the underlying proto.
	IndexDescDeepCopy() descpb.IndexDescriptor

	// Ordinal returns the ordinal of the index in its parent table descriptor.
	//
	// The ordinal of an index in a `tableDesc descpb.TableDescriptor` is
	// defined as follows:
	// - 0 is the ordinal of the primary index,
	// - [1:1+len(tableDesc.Indexes)] is the range of public non-primary indexes,
	// - [1+len(tableDesc.Indexes):] is the range of non-public indexes.
	//
	// In terms of a `table catalog.TableDescriptor` interface, it is defined
	// as the catalog.Index object's position in the table.AllIndexes() slice.
	Ordinal() int

	// Primary returns true iff the index is the primary index for the table
	// descriptor.
	Primary() bool

	// Public returns true iff the index is active, i.e. readable, in the table
	// descriptor.
	Public() bool

	// WriteAndDeleteOnly returns true iff the index is a mutation in the
	// delete-and-write-only state in the table descriptor.
	WriteAndDeleteOnly() bool

	// DeleteOnly returns true iff the index is a mutation in the delete-only
	// state in the table descriptor.
	DeleteOnly() bool

	// Adding returns true iff the index is an add mutation in the table
	// descriptor.
	Adding() bool

	// Dropped returns true iff the index is a drop mutation in the table
	// descriptor.
	Dropped() bool

	// The remaining methods operate on the underlying descpb.IndexDescriptor object.

	GetID() descpb.IndexID
	GetName() string
	IsInterleaved() bool
	IsPartial() bool
	IsUnique() bool
	IsDisabled() bool
	IsSharded() bool
	IsCreatedExplicitly() bool
	GetPredicate() string
	GetType() descpb.IndexDescriptor_Type
	GetGeoConfig() geoindex.Config
	GetVersion() descpb.IndexDescriptorVersion
	GetEncodingType() descpb.IndexDescriptorEncodingType

	GetSharded() descpb.ShardedDescriptor
	GetShardColumnName() string

	IsValidOriginIndex(originColIDs descpb.ColumnIDs) bool
	IsValidReferencedUniqueConstraint(referencedColIDs descpb.ColumnIDs) bool

	GetPartitioning() descpb.PartitioningDescriptor
	FindPartitionByName(name string) descpb.PartitioningDescriptor
	PartitionNames() []string

	NumInterleaveAncestors() int
	GetInterleaveAncestor(ancestorOrdinal int) descpb.InterleaveDescriptor_Ancestor

	NumInterleavedBy() int
	GetInterleavedBy(interleavedByOrdinal int) descpb.ForeignKeyReference

	NumColumns() int
	GetColumnID(columnOrdinal int) descpb.ColumnID
	GetColumnName(columnOrdinal int) string
	GetColumnDirection(columnOrdinal int) descpb.IndexDescriptor_Direction

	ForEachColumnID(func(id descpb.ColumnID) error) error
	ContainsColumnID(colID descpb.ColumnID) bool
	InvertedColumnID() descpb.ColumnID
	InvertedColumnName() string

	NumStoredColumns() int
	GetStoredColumnID(storedColumnOrdinal int) descpb.ColumnID
	GetStoredColumnName(storedColumnOrdinal int) string
	HasOldStoredColumns() bool

	NumExtraColumns() int
	GetExtraColumnID(extraColumnOrdinal int) descpb.ColumnID

	NumCompositeColumns() int
	GetCompositeColumnID(compositeColumnOrdinal int) descpb.ColumnID
}

// Column is an interface around the index descriptor types.
type Column interface {

	// ColumnDesc returns the underlying protobuf descriptor.
	// Ideally, this method should be called as rarely as possible.
	ColumnDesc() *descpb.ColumnDescriptor

	// ColumnDescDeepCopy returns a deep copy of the underlying proto.
	ColumnDescDeepCopy() descpb.ColumnDescriptor

	// DeepCopy returns a deep copy of the receiver.
	DeepCopy() Column

	// Ordinal returns the ordinal of the column in its parent table descriptor.
	//
	// The ordinal of a column in a `tableDesc descpb.TableDescriptor` is
	// defined as follows:
	// - [0:len(tableDesc.Columns)] is the range of public columns,
	// - [len(tableDesc.Columns):] is the range of non-public columns.
	//
	// In terms of a `table catalog.TableDescriptor` interface, it is defined
	// as the catalog.Column object's position in the table.AllColumns() slice.
	Ordinal() int

	// Public returns true iff the column is active, i.e. readable, in the table
	// descriptor.
	Public() bool

	// WriteAndDeleteOnly returns true iff the column is a mutation in the
	// delete-and-write-only state in the table descriptor.
	WriteAndDeleteOnly() bool

	// DeleteOnly returns true iff the column is a mutation in the delete-only
	// state in the table descriptor.
	DeleteOnly() bool

	// Adding returns true iff the column is an add mutation in the table
	// descriptor.
	Adding() bool

	// Dropped returns true iff the column is a drop mutation in the table
	// descriptor.
	Dropped() bool

	// GetID returns the column ID.
	GetID() descpb.ColumnID

	// GetName returns the column name as a string.
	GetName() string

	// ColName returns the column name as a tree.Name.
	ColName() tree.Name

	// HasType returns true iff the column type is set.
	HasType() bool

	// GetType returns the column type.
	GetType() *types.T

	// IsNullable returns true iff the column allows NULL values.
	IsNullable() bool

	// HasDefault returns true iff the column has a default expression set.
	HasDefault() bool

	// GetDefaultExpr returns the column default expression if it exists,
	// empty string otherwise.
	GetDefaultExpr() string

	// IsComputed returns true iff the column is a computed column.
	IsComputed() bool

	// GetComputeExpr returns the column computed expression if it exists,
	// empty string otherwise.
	GetComputeExpr() string

	// IsHidden returns true iff the column is not visible.
	IsHidden() bool

	// NumUsesSequences returns the number of sequences used by this column.
	NumUsesSequences() int

	// GetUsesSequenceID returns the ID of a sequence used by this column.
	GetUsesSequenceID(usesSequenceOrdinal int) descpb.ID

	// NumOwnsSequences returns the number of sequences owned by this column.
	NumOwnsSequences() int

	// GetOwnsSequenceID returns the ID of a sequence owned by this column.
	GetOwnsSequenceID(ownsSequenceOrdinal int) descpb.ID

	// IsVirtual returns true iff the column is a virtual column.
	IsVirtual() bool

	// CheckCanBeInboundFKRef returns whether the given column can be on the
	// referenced (target) side of a foreign key relation.
	CheckCanBeInboundFKRef() error

	// CheckCanBeOutboundFKRef returns whether the given column can be on the
	// referencing (origin) side of a foreign key relation.
	CheckCanBeOutboundFKRef() error

	// GetPGAttributeNum returns the PGAttributeNum of the column descriptor
	// if the PGAttributeNum is set (non-zero). Returns the ID of the
	// column descriptor if the PGAttributeNum is not set.
	GetPGAttributeNum() uint32

	// IsSystemColumn returns true iff the column is a system column.
	IsSystemColumn() bool
}

// TypeDescriptor will eventually be called typedesc.Descriptor.
// It is implemented by (Imm|M)utableTypeDescriptor.
type TypeDescriptor interface {
	Descriptor
	TypeDesc() *descpb.TypeDescriptor
	HydrateTypeInfoWithName(ctx context.Context, typ *types.T, name *tree.TypeName, res TypeDescriptorResolver) error
	MakeTypesT(ctx context.Context, name *tree.TypeName, res TypeDescriptorResolver) (*types.T, error)
	HasPendingSchemaChanges() bool
	GetIDClosure() map[descpb.ID]struct{}

	PrimaryRegionName() (descpb.RegionName, error)
	RegionNames() (descpb.RegionNames, error)
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
		err := errors.Errorf("%s %q is offline", desc.TypeName(), desc.GetName())
		if desc.GetOfflineReason() != "" {
			err = errors.Errorf("%s %q is offline: %s", desc.TypeName(), desc.GetName(), desc.GetOfflineReason())
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

func isIndexInSearchSet(desc TableDescriptor, opts IndexOpts, idx Index) bool {
	if !opts.NonPhysicalPrimaryIndex && idx.Primary() && !desc.IsPhysicalTable() {
		return false
	}
	if !opts.AddMutations && idx.Adding() {
		return false
	}
	if !opts.DropMutations && idx.Dropped() {
		return false
	}
	return true
}

// ForEachIndex runs f over each index in the table descriptor according to
// filter parameters in opts. Indexes are visited in their canonical order,
// see Index.Ordinal(). ForEachIndex supports iterutil.StopIteration().
func ForEachIndex(desc TableDescriptor, opts IndexOpts, f func(idx Index) error) error {
	for _, idx := range desc.AllIndexes() {
		if !isIndexInSearchSet(desc, opts, idx) {
			continue
		}
		if err := f(idx); err != nil {
			if iterutil.Done(err) {
				return nil
			}
			return err
		}
	}
	return nil
}

func forEachIndex(slice []Index, f func(idx Index) error) error {
	for _, idx := range slice {
		if err := f(idx); err != nil {
			if iterutil.Done(err) {
				return nil
			}
			return err
		}
	}
	return nil
}

// ForEachActiveIndex is like ForEachIndex over ActiveIndexes().
func ForEachActiveIndex(desc TableDescriptor, f func(idx Index) error) error {
	return forEachIndex(desc.ActiveIndexes(), f)
}

// ForEachNonDropIndex is like ForEachIndex over NonDropIndexes().
func ForEachNonDropIndex(desc TableDescriptor, f func(idx Index) error) error {
	return forEachIndex(desc.NonDropIndexes(), f)
}

// ForEachPartialIndex is like ForEachIndex over PartialIndexes().
func ForEachPartialIndex(desc TableDescriptor, f func(idx Index) error) error {
	return forEachIndex(desc.PartialIndexes(), f)
}

// ForEachPublicNonPrimaryIndex is like ForEachIndex over
// PublicNonPrimaryIndexes().
func ForEachPublicNonPrimaryIndex(desc TableDescriptor, f func(idx Index) error) error {
	return forEachIndex(desc.PublicNonPrimaryIndexes(), f)
}

// ForEachWritableNonPrimaryIndex is like ForEachIndex over
// WritableNonPrimaryIndexes().
func ForEachWritableNonPrimaryIndex(desc TableDescriptor, f func(idx Index) error) error {
	return forEachIndex(desc.WritableNonPrimaryIndexes(), f)
}

// ForEachDeletableNonPrimaryIndex is like ForEachIndex over
// DeletableNonPrimaryIndexes().
func ForEachDeletableNonPrimaryIndex(desc TableDescriptor, f func(idx Index) error) error {
	return forEachIndex(desc.DeletableNonPrimaryIndexes(), f)
}

// ForEachDeleteOnlyNonPrimaryIndex is like ForEachIndex over
// DeleteOnlyNonPrimaryIndexes().
func ForEachDeleteOnlyNonPrimaryIndex(desc TableDescriptor, f func(idx Index) error) error {
	return forEachIndex(desc.DeleteOnlyNonPrimaryIndexes(), f)
}

// FindIndex returns the first index for which test returns true, nil otherwise,
// according to the parameters in opts just like ForEachIndex.
// Indexes are visited in their canonical order, see Index.Ordinal().
func FindIndex(desc TableDescriptor, opts IndexOpts, test func(idx Index) bool) Index {
	for _, idx := range desc.AllIndexes() {
		if !isIndexInSearchSet(desc, opts, idx) {
			continue
		}
		if test(idx) {
			return idx
		}
	}
	return nil
}

func findIndex(slice []Index, test func(idx Index) bool) Index {
	for _, idx := range slice {
		if test(idx) {
			return idx
		}
	}
	return nil
}

// FindActiveIndex returns the first index in ActiveIndex() for which test
// returns true.
func FindActiveIndex(desc TableDescriptor, test func(idx Index) bool) Index {
	return findIndex(desc.ActiveIndexes(), test)
}

// FindNonDropIndex returns the first index in NonDropIndex() for which test
// returns true.
func FindNonDropIndex(desc TableDescriptor, test func(idx Index) bool) Index {
	return findIndex(desc.NonDropIndexes(), test)
}

// FindPartialIndex returns the first index in PartialIndex() for which test
// returns true.
func FindPartialIndex(desc TableDescriptor, test func(idx Index) bool) Index {
	return findIndex(desc.PartialIndexes(), test)
}

// FindPublicNonPrimaryIndex returns the first index in PublicNonPrimaryIndex()
// for which test returns true.
func FindPublicNonPrimaryIndex(desc TableDescriptor, test func(idx Index) bool) Index {
	return findIndex(desc.PublicNonPrimaryIndexes(), test)
}

// FindWritableNonPrimaryIndex returns the first index in
// WritableNonPrimaryIndex() for which test returns true.
func FindWritableNonPrimaryIndex(desc TableDescriptor, test func(idx Index) bool) Index {
	return findIndex(desc.WritableNonPrimaryIndexes(), test)
}

// FindDeletableNonPrimaryIndex returns the first index in
// DeletableNonPrimaryIndex() for which test returns true.
func FindDeletableNonPrimaryIndex(desc TableDescriptor, test func(idx Index) bool) Index {
	return findIndex(desc.DeletableNonPrimaryIndexes(), test)
}

// FindDeleteOnlyNonPrimaryIndex returns the first index in
// DeleteOnlyNonPrimaryIndex() for which test returns true.
func FindDeleteOnlyNonPrimaryIndex(desc TableDescriptor, test func(idx Index) bool) Index {
	return findIndex(desc.DeleteOnlyNonPrimaryIndexes(), test)
}

// UserDefinedTypeColsHaveSameVersion returns whether one table descriptor's
// columns with user defined type metadata have the same versions of metadata
// as in the other descriptor. Note that this function is only valid on two
// descriptors representing the same table at the same version.
func UserDefinedTypeColsHaveSameVersion(desc TableDescriptor, otherDesc TableDescriptor) bool {
	otherCols := otherDesc.UserDefinedTypeColumns()
	for i, thisCol := range desc.UserDefinedTypeColumns() {
		this, other := thisCol.GetType(), otherCols[i].GetType()
		if this.TypeMeta.Version != other.TypeMeta.Version {
			return false
		}
	}
	return true
}

// ColumnIDToOrdinalMap returns a map from Column ID to the ordinal
// position of that column.
func ColumnIDToOrdinalMap(columns []Column) TableColMap {
	var m TableColMap
	for _, col := range columns {
		m.Set(col.GetID(), col.Ordinal())
	}
	return m
}

// ColumnTypes returns the types of the given columns
func ColumnTypes(columns []Column) []*types.T {
	return ColumnTypesWithVirtualCol(columns, nil)
}

// ColumnTypesWithVirtualCol returns the types of all given columns,
// If virtualCol is non-nil, substitutes the type of the virtual
// column instead of the column with the same ID.
func ColumnTypesWithVirtualCol(columns []Column, virtualCol Column) []*types.T {
	t := make([]*types.T, len(columns))
	for i, col := range columns {
		t[i] = col.GetType()
		if virtualCol != nil && col.GetID() == virtualCol.GetID() {
			t[i] = virtualCol.GetType()
		}
	}
	return t
}
