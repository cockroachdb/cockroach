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

	Regions() (descpb.RegionNames, error)
	IsMultiRegion() bool
	PrimaryRegion() (descpb.RegionName, error)
	Validate() error
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
	GetViewQuery() string
	GetLease() *descpb.TableDescriptor_SchemaChangeLease
	GetDropTime() int64
	GetFormatVersion() descpb.FormatVersion

	GetPrimaryIndexID() descpb.IndexID
	GetPrimaryIndex() Index
	IsPartitionAllBy() bool
	PrimaryIndexSpan(codec keys.SQLCodec) roachpb.Span
	IndexSpan(codec keys.SQLCodec, id descpb.IndexID) roachpb.Span
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

	HasPrimaryKey() bool
	PrimaryKeyString() string

	GetPublicColumns() []descpb.ColumnDescriptor
	ForeachPublicColumn(f func(col *descpb.ColumnDescriptor) error) error
	ForeachNonDropColumn(f func(col *descpb.ColumnDescriptor) error) error
	NamesForColumnIDs(ids descpb.ColumnIDs) ([]string, error)
	FindColumnByName(name tree.Name) (*descpb.ColumnDescriptor, bool, error)
	FindActiveColumnByID(id descpb.ColumnID) (*descpb.ColumnDescriptor, error)
	FindColumnByID(id descpb.ColumnID) (*descpb.ColumnDescriptor, error)
	ColumnIdxMap() TableColMap
	GetColumnAtIdx(idx int) *descpb.ColumnDescriptor
	AllNonDropColumns() []descpb.ColumnDescriptor
	VisibleColumns() []descpb.ColumnDescriptor
	ColumnsWithMutations(includeMutations bool) []descpb.ColumnDescriptor
	ColumnIdxMapWithMutations(includeMutations bool) TableColMap
	DeletableColumns() []descpb.ColumnDescriptor
	MutationColumns() []descpb.ColumnDescriptor
	ContainsUserDefinedTypes() bool
	GetColumnOrdinalsWithUserDefinedTypes() []int
	UserDefinedTypeColsHaveSameVersion(otherDesc TableDescriptor) bool

	GetFamilies() []descpb.ColumnFamilyDescriptor
	NumFamilies() int
	FindFamilyByID(id descpb.FamilyID) (*descpb.ColumnFamilyDescriptor, error)
	ForeachFamily(f func(family *descpb.ColumnFamilyDescriptor) error) error

	IsTable() bool
	IsView() bool
	IsSequence() bool
	IsTemporary() bool
	IsVirtualTable() bool
	IsPhysicalTable() bool
	IsInterleaved() bool
	MaterializedView() bool

	GetMutationJobs() []descpb.TableDescriptor_MutationJob

	GetReplacementOf() descpb.TableDescriptor_Replacement
	GetAllReferencedTypeIDs(
		getType func(descpb.ID) (TypeDescriptor, error),
	) (descpb.IDs, error)

	Validate(ctx context.Context, txn DescGetter) error

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
	FindActiveColumnByName(s string) (*descpb.ColumnDescriptor, error)
	WritableColumns() []descpb.ColumnDescriptor

	GetLocalityConfig() *descpb.TableDescriptor_LocalityConfig
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

// TypeDescriptor will eventually be called typedesc.Descriptor.
// It is implemented by (Imm|M)utableTypeDescriptor.
type TypeDescriptor interface {
	Descriptor
	TypeDesc() *descpb.TypeDescriptor
	HydrateTypeInfoWithName(ctx context.Context, typ *types.T, name *tree.TypeName, res TypeDescriptorResolver) error
	MakeTypesT(ctx context.Context, name *tree.TypeName, res TypeDescriptorResolver) (*types.T, error)
	HasPendingSchemaChanges() bool
	GetIDClosure() map[descpb.ID]struct{}

	PrimaryRegion() (descpb.RegionName, error)
	Validate(ctx context.Context, dg DescGetter) error
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
