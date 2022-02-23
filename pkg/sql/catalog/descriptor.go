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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
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

	// RunPostDeserializationChanges attempts to perform changes to the descriptor
	// being built from a deserialized protobuf.
	RunPostDeserializationChanges()

	// RunRestoreChanges attempts to perform changes to the descriptor being
	// built from a deserialized protobuf obtained by restoring a backup.
	// This is to compensate for the fact that these are not subject to cluster
	// upgrade migrations
	RunRestoreChanges(descLookupFn func(id descpb.ID) Descriptor) error

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

// LeasableDescriptor is an interface for objects which can be leased via the
// descriptor leasing system.
type LeasableDescriptor interface {
	// IsUncommittedVersion returns true if this descriptor represent a version
	// which is not the currently committed version. Implementations may return
	// false negatives here in cases where a descriptor may have crossed a
	// serialization boundary. In particular, this can occur during execution on
	// remote nodes as well as during some scenarios in schema changes.
	IsUncommittedVersion() bool

	// GetVersion returns this descriptor's version. The version field of a
	// descriptor is incremented each time the descriptor is modified. It's used
	// to maintain the "2-version invariant", which says that a descriptor ID can
	// exist around the cluster in at most 2 adjacent versions.
	GetVersion() descpb.DescriptorVersion

	// GetModificationTime returns the timestamp of the transaction which last
	// modified this descriptor.
	GetModificationTime() hlc.Timestamp

	// GetDrainingNames returns the list of "draining names" for this descriptor.
	// Draining names are names that were in use by this descriptor, but are no
	// longer due to renames.
	// TODO(postamar): remove GetDrainingNames method in 22.2
	GetDrainingNames() []descpb.NameInfo // Deprecated
}

// Descriptor is an interface to be shared by individual descriptor
// types.
type Descriptor interface {
	NameEntry
	LeasableDescriptor

	// GetPrivileges returns this descriptor's PrivilegeDescriptor, which
	// describes the set of privileges that users have to use, modify, or delete
	// the object represented by this descriptor. Each descriptor type may have a
	// different set of capabilities represented by the PrivilegeDescriptor.
	GetPrivileges() *catpb.PrivilegeDescriptor
	// DescriptorType returns the type of this descriptor (like relation, type,
	// schema, database).
	DescriptorType() DescriptorType
	// GetAuditMode returns the audit mode for this descriptor. The audit mode
	// describes what kind of auditing (logging) actions should be taken when
	// this descriptor is modified.
	GetAuditMode() descpb.TableDescriptor_AuditMode

	// Public returns true if this descriptor is public. A public descriptor is
	// one that is allowed to be used normally.
	Public() bool
	// Adding returns true if the table is being added.
	Adding() bool
	// Dropped returns true if the table is being dropped.
	Dropped() bool
	// Offline returns true if the table is importing.
	Offline() bool
	// GetOfflineReason returns a user-visible reason for why a table is offline.
	GetOfflineReason() string

	// DescriptorProto prepares this descriptor for serialization.
	DescriptorProto() *descpb.Descriptor

	//ByteSize returns the memory held by the Descriptor.
	ByteSize() int64

	// NewBuilder initializes a DescriptorBuilder with this descriptor.
	NewBuilder() DescriptorBuilder

	// GetReferencedDescIDs returns the IDs of all descriptors directly referenced
	// by this descriptor, including itself.
	GetReferencedDescIDs() (DescriptorIDSet, error)

	// ValidateSelf checks the internal consistency of the descriptor.
	ValidateSelf(vea ValidationErrorAccumulator)

	// ValidateCrossReferences performs cross-reference checks.
	ValidateCrossReferences(vea ValidationErrorAccumulator, vdg ValidationDescGetter)

	// ValidateTxnCommit performs pre-commit checks.
	ValidateTxnCommit(vea ValidationErrorAccumulator, vdg ValidationDescGetter)

	// GetDeclarativeSchemaChangerState returns the state of the declarative
	// schema change currently operating on this descriptor. It will be nil if
	// there is no declarative schema change job. Note that it is a clone of
	// the value on the descriptor. Changes will need to be written back to
	// the descriptor using SetDeclarativeSchemaChangeState.
	GetDeclarativeSchemaChangerState() *scpb.DescriptorState

	// GetPostDeserializationChanges returns the set of ways the Descriptor
	// was changed after running RunPostDeserializationChanges.
	GetPostDeserializationChanges() PostDeserializationChanges
}

// DatabaseDescriptor encapsulates the concept of a database.
type DatabaseDescriptor interface {
	Descriptor

	// DatabaseDesc returns the backing protobuf for this database.
	DatabaseDesc() *descpb.DatabaseDescriptor

	// GetRegionConfig returns region information for this database.
	GetRegionConfig() *descpb.DatabaseDescriptor_RegionConfig
	// IsMultiRegion returns whether the database has multi-region properties
	// configured. If so, GetRegionConfig can be used.
	IsMultiRegion() bool
	// PrimaryRegionName returns the primary region for a multi-region database.
	PrimaryRegionName() (catpb.RegionName, error)
	// MultiRegionEnumID returns the ID of the multi-region enum if the database
	// is a multi-region database, and an error otherwise.
	MultiRegionEnumID() (descpb.ID, error)
	// ForEachSchemaInfo iterates f over each schema info mapping in the
	// descriptor.
	// iterutil.StopIteration is supported.
	// TODO(postamar): remove ForEachSchemaInfo method in 22.2
	ForEachSchemaInfo(func(id descpb.ID, name string, isDropped bool) error) error // Deprecated
	// ForEachNonDroppedSchema iterates f over each non-dropped schema info
	// mapping in the descriptor.
	// iterutil.StopIteration is supported.
	// TODO(postamar): rename this in 22.2 when all mappings are non-dropped.
	ForEachNonDroppedSchema(func(id descpb.ID, name string) error) error
	// GetSchemaID returns the ID in the schema mapping entry for the
	// given name, 0 otherwise.
	GetSchemaID(name string) descpb.ID
	// GetNonDroppedSchemaName returns the name in the schema mapping entry for the
	// given ID, if it's not marked as dropped, empty string otherwise.
	GetNonDroppedSchemaName(schemaID descpb.ID) string
	// GetDefaultPrivilegeDescriptor returns the default privileges for this
	// database.
	GetDefaultPrivilegeDescriptor() DefaultPrivilegeDescriptor
	// HasPublicSchemaWithDescriptor returns true iff the database has a public
	// schema which itself has a descriptor.
	HasPublicSchemaWithDescriptor() bool
}

// TableDescriptor is an interface around the table descriptor types.
type TableDescriptor interface {
	Descriptor

	// TableDesc returns the backing protobuf for this database.
	TableDesc() *descpb.TableDescriptor

	// GetState returns the raw state of this descriptor. This information is
	// mimicked by the Dropped(), Added(), etc information in Descriptor.
	GetState() descpb.DescriptorState

	// IsTable returns true if the TableDescriptor actually describes a
	// Table resource, as opposed to a different resource (like a View).
	IsTable() bool
	// IsView returns true if the TableDescriptor actually describes a
	// View resource rather than a Table.
	IsView() bool
	// IsSequence returns true if the TableDescriptor actually describes a
	// Sequence resource rather than a Table.
	IsSequence() bool
	// IsTemporary returns true if this is a temporary table.
	IsTemporary() bool
	// IsVirtualTable returns true if the TableDescriptor describes a
	// virtual Table (like the information_schema tables) and thus doesn't
	// need to be physically stored.
	IsVirtualTable() bool
	// IsPhysicalTable returns true if the TableDescriptor actually describes a
	// physical Table that needs to be stored in the kv layer, as opposed to a
	// different resource like a view or a virtual table. Physical tables have
	// primary keys, column families, and indexes (unlike virtual tables).
	// Sequences count as physical tables because their values are stored in
	// the KV layer.
	IsPhysicalTable() bool
	// MaterializedView returns whether or not this TableDescriptor is a
	// MaterializedView.
	MaterializedView() bool
	// IsAs returns true if the TableDescriptor describes a Table that was created
	// with a CREATE TABLE AS command.
	IsAs() bool

	// GetSequenceOpts returns the sequence options for this table. Only valid if
	// IsSequence is true.
	GetSequenceOpts() *descpb.TableDescriptor_SequenceOpts

	// GetCreateQuery returns the full CREATE TABLE AS query that was used for
	// table's creation. Only valid if IsAs is true.
	GetCreateQuery() string
	// GetCreateAsOfTime returns the transaction timestamp that this table was
	// created at, for materialized views and CREATE TABLE AS. Only valid if
	// IsAs or MaterializedView returns true.
	GetCreateAsOfTime() hlc.Timestamp

	// GetViewQuery returns this view's CREATE VIEW declaration. Only valid if
	// IsView is true.
	GetViewQuery() string

	// GetLease returns this table's schema change lease.
	GetLease() *descpb.TableDescriptor_SchemaChangeLease
	// GetDropTime returns the timestamp at which the table is truncated or
	// dropped. It's represented as the current time in nanoseconds since the
	// epoch.
	GetDropTime() int64
	// GetFormatVersion returns the format version that the data in this table
	// is encoded into.
	GetFormatVersion() descpb.FormatVersion

	// GetPrimaryIndexID returns the ID of the primary index.
	GetPrimaryIndexID() descpb.IndexID
	// GetPrimaryIndex returns the primary index in the form of a catalog.Index
	// interface.
	GetPrimaryIndex() Index
	// IsPartitionAllBy returns whether the table has a PARTITION ALL BY clause.
	IsPartitionAllBy() bool

	// PrimaryIndexSpan returns the Span that corresponds to the entire primary
	// index; can be used for a full table scan.
	PrimaryIndexSpan(codec keys.SQLCodec) roachpb.Span
	// IndexSpan returns the Span that corresponds to an entire index; can be used
	// for a full index scan.
	IndexSpan(codec keys.SQLCodec, id descpb.IndexID) roachpb.Span
	// AllIndexSpans returns the Spans for each index in the table, including those
	// being added in the mutations.
	AllIndexSpans(codec keys.SQLCodec) roachpb.Spans
	// TableSpan returns the Span that corresponds to the entire table.
	TableSpan(codec keys.SQLCodec) roachpb.Span
	// GetIndexMutationCapabilities returns:
	// 1. Whether the index is a mutation
	// 2. if so, is it in state DELETE_AND_WRITE_ONLY
	GetIndexMutationCapabilities(id descpb.IndexID) (isMutation, isWriteOnly bool)

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
	// delete-only, in their canonical order. This is equivalent to taking
	// the slice produced by AllIndexes and removing the primary index and
	// backfilling indexes.
	DeletableNonPrimaryIndexes() []Index

	// NonPrimaryIndexes returns a slice of all the indexes that
	// are not yet public: backfilling, delete, and
	// delete-and-write-only, in their canonical order. This is
	// equivalent to taking the slice produced by AllIndexes and
	// removing the primary index.
	NonPrimaryIndexes() []Index

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

	// GetNextIndexID returns the next unused index ID for the table. Index IDs
	// are unique within a table, but not globally.
	GetNextIndexID() descpb.IndexID

	// HasPrimaryKey returns true if the table has a primary key. This will be
	// true except for in a transaction where a user has dropped a primary key
	// in preparation to add a new one. In CockroachDB, all tables have primary
	// keys, even if they're not defined by the user.
	HasPrimaryKey() bool

	// AllColumns returns a slice of Column interfaces containing the
	// table's public columns and column mutations, in the canonical order:
	// - all public columns in the same order as in the underlying
	//   desc.TableDesc().Columns slice;
	// - all column mutations in the same order as in the underlying
	//   desc.TableDesc().Mutations slice.
	// - all system columns defined in colinfo.AllSystemColumnDescs.
	AllColumns() []Column
	// PublicColumns returns a slice of Column interfaces containing the
	// table's public columns, in the canonical order.
	PublicColumns() []Column
	// WritableColumns returns a slice of Column interfaces containing the
	// table's public columns and DELETE_AND_WRITE_ONLY mutations, in the canonical
	// order.
	WritableColumns() []Column
	// DeletableColumns returns a slice of Column interfaces containing the
	// table's public columns and mutations, in the canonical order.
	DeletableColumns() []Column
	// NonDropColumns returns a slice of Column interfaces containing the
	// table's public columns and ADD mutations, in the canonical order.
	NonDropColumns() []Column
	// VisibleColumns returns a slice of Column interfaces containing the table's
	// visible columns, in the canonical order. Visible columns are public columns
	// with Hidden=false and Inaccessible=false. See ColumnDescriptor.Hidden and
	// ColumnDescriptor.Inaccessible for more details.
	VisibleColumns() []Column
	// AccessibleColumns returns a slice of Column interfaces containing the table's
	// accessible columns, in the canonical order. Accessible columns are public
	// columns with Inaccessible=false. See ColumnDescriptor.Inaccessible for more
	// details.
	AccessibleColumns() []Column
	// ReadableColumns is a list of columns (including those undergoing a schema
	// change) which can be scanned. Note that mutation columns may produce NULL
	// values when scanned, even if they are marked as not nullable.
	ReadableColumns() []Column
	// UserDefinedTypeColumns returns a slice of Column interfaces
	// containing the table's columns with user defined types, in the
	// canonical order.
	UserDefinedTypeColumns() []Column
	// SystemColumns returns a slice of Column interfaces
	// containing the table's system columns, as defined in
	// colinfo.AllSystemColumnDescs.
	SystemColumns() []Column

	// PublicColumnIDs creates a slice of column IDs corresponding to the public
	// columns.
	PublicColumnIDs() []descpb.ColumnID

	// IndexColumns returns a slice of Column interfaces containing all
	// columns present in the specified Index in any capacity.
	IndexColumns(idx Index) []Column
	// IndexKeyColumns returns a slice of Column interfaces containing all
	// key columns in the specified Index.
	IndexKeyColumns(idx Index) []Column
	// IndexKeyColumnDirections returns a slice of column directions for all
	// key columns in the specified Index.
	IndexKeyColumnDirections(idx Index) []descpb.IndexDescriptor_Direction
	// IndexKeySuffixColumns returns a slice of Column interfaces containing all
	// key suffix columns in the specified Index.
	IndexKeySuffixColumns(idx Index) []Column
	// IndexFullColumns returns a slice of Column interfaces containing all
	// key columns in the specified Index, plus all key suffix columns if that
	// index is not a unique index.
	IndexFullColumns(idx Index) []Column
	// IndexFullColumnDirections returns a slice of column directions for all
	// key columns in the specified Index, plus all key suffix columns if that
	// index is not a unique index.
	IndexFullColumnDirections(idx Index) []descpb.IndexDescriptor_Direction
	// IndexStoredColumns returns a slice of Column interfaces containing all
	// stored columns in the specified Index.
	IndexStoredColumns(idx Index) []Column

	// IndexKeysPerRow returns the maximum number of keys used to encode a row for
	// the given index. If a secondary index doesn't store any columns, then it
	// only has one k/v pair, but if it stores some columns, it can return up to
	// one k/v pair per family in the table, just like a primary index.
	IndexKeysPerRow(idx Index) int

	// IndexFetchSpecKeyAndSuffixColumns returns information about the key and
	// suffix columns, suitable for populating a descpb.IndexFetchSpec.
	IndexFetchSpecKeyAndSuffixColumns(idx Index) []descpb.IndexFetchSpec_KeyColumn

	// FindColumnWithID returns the first column found whose ID matches the
	// provided target ID, in the canonical order.
	// If no column is found then an error is also returned.
	FindColumnWithID(id descpb.ColumnID) (Column, error)
	// FindColumnWithName returns the first column found whose name matches the
	// provided target name, in the canonical order.
	// If no column is found then an error is also returned.
	FindColumnWithName(name tree.Name) (Column, error)

	// NamesForColumnIDs returns the names for the given column ids, or an error
	// if one or more column ids was missing. Note - this allocates! It's not for
	// hot path code.
	NamesForColumnIDs(ids descpb.ColumnIDs) ([]string, error)
	// ContainsUserDefinedTypes returns whether or not this table descriptor has
	// any columns of user defined types.
	ContainsUserDefinedTypes() bool
	// GetNextColumnID returns the next unused column ID for this table. Column
	// IDs are unique per table, but not unique globally.
	GetNextColumnID() descpb.ColumnID
	// GetNextConstraintID returns the next unused constraint ID for this table.
	// Constraint IDs are unique per table, but not unique globally.
	GetNextConstraintID() descpb.ConstraintID
	// CheckConstraintUsesColumn returns whether the check constraint uses the
	// specified column.
	CheckConstraintUsesColumn(cc *descpb.TableDescriptor_CheckConstraint, colID descpb.ColumnID) (bool, error)
	// IsShardColumn returns true if col corresponds to a non-dropped hash sharded
	// index. This method assumes that col is currently a member of desc.
	IsShardColumn(col Column) bool

	// GetFamilies returns the column families of this table. All tables contain
	// at least one column family. The returned list is sorted by family ID.
	GetFamilies() []descpb.ColumnFamilyDescriptor
	// NumFamilies returns the number of column families in the descriptor.
	NumFamilies() int
	// FindFamilyByID finds the family with specified ID.
	FindFamilyByID(id descpb.FamilyID) (*descpb.ColumnFamilyDescriptor, error)
	// ForeachFamily calls f for every column family key in desc until an
	// error is returned.
	ForeachFamily(f func(family *descpb.ColumnFamilyDescriptor) error) error
	// GetNextFamilyID returns the next unused family ID for this table. Family
	// IDs are unique per table, but not unique globally.
	GetNextFamilyID() descpb.FamilyID

	// FamilyDefaultColumns returns the default column IDs for families with a
	// default column. See IndexFetchSpec.FamilyDefaultColumns.
	FamilyDefaultColumns() []descpb.IndexFetchSpec_FamilyDefaultColumn

	// HasColumnBackfillMutation returns whether the table has any queued column
	// mutations that require a backfill.
	HasColumnBackfillMutation() bool
	// MakeFirstMutationPublic creates a Mutable from the
	// immutable by making the first mutation public.
	// This is super valuable when trying to run SQL over data associated
	// with a schema mutation that is still not yet public: Data validation,
	// error reporting.
	MakeFirstMutationPublic(includeConstraints MutationPublicationFilter) (TableDescriptor, error)
	// MakePublic creates a Mutable from the immutable by making the it public.
	MakePublic() TableDescriptor
	// AllMutations returns all of the table descriptor's mutations.
	AllMutations() []Mutation
	// GetMutationJobs returns the table descriptor's mutation jobs.
	GetMutationJobs() []descpb.TableDescriptor_MutationJob

	// GetReplacementOf tracks prior IDs by which this table went -- e.g. when
	// TRUNCATE creates a replacement of a table and swaps it in for the the old
	// one, it should note on the new table the ID of the table it replaced. This
	// can be used when trying to track a table's history across truncations.
	// Note: This was only used in pre-20.2 truncate and 20.2 mixed version and
	// is now deprecated.
	GetReplacementOf() descpb.TableDescriptor_Replacement
	// GetAllReferencedTypeIDs returns all user defined type descriptor IDs that
	// this table references. It takes in a function that returns the TypeDescriptor
	// with the desired ID.
	GetAllReferencedTypeIDs(
		databaseDesc DatabaseDescriptor, getType func(descpb.ID) (TypeDescriptor, error),
	) (referencedAnywhere, referencedInColumns descpb.IDs, _ error)

	// ForeachDependedOnBy runs a function on all indexes, including those being
	// added in the mutations.
	ForeachDependedOnBy(f func(dep *descpb.TableDescriptor_Reference) error) error
	// GetDependedOnBy returns information on all relations that depend on this one.
	GetDependedOnBy() []descpb.TableDescriptor_Reference
	// GetDependsOn returns the IDs of all relations that this view depends on.
	// It's only non-nil if IsView is true.
	GetDependsOn() []descpb.ID
	// GetDependsOnTypes returns the IDs of all types that this view depends on.
	// It's only non-nil if IsView is true.
	GetDependsOnTypes() []descpb.ID

	// GetConstraintInfoWithLookup returns a summary of all constraints on the
	// table using the provided function to fetch a TableDescriptor from an ID.
	GetConstraintInfoWithLookup(fn TableLookupFn) (map[string]descpb.ConstraintDetail, error)
	// GetConstraintInfo returns a summary of all constraints on the table.
	GetConstraintInfo() (map[string]descpb.ConstraintDetail, error)

	// GetUniqueWithoutIndexConstraints returns all the unique constraints defined
	// on this table that are not enforced by an index.
	GetUniqueWithoutIndexConstraints() []descpb.UniqueWithoutIndexConstraint
	// AllActiveAndInactiveUniqueWithoutIndexConstraints returns all unique
	// constraints that are not enforced by an index, including both "active"
	// ones on the table descriptor which are being enforced for all writes, and
	// "inactive" ones queued in the mutations list.
	AllActiveAndInactiveUniqueWithoutIndexConstraints() []*descpb.UniqueWithoutIndexConstraint

	// ForeachOutboundFK calls f for every outbound foreign key in desc until an
	// error is returned.
	ForeachOutboundFK(f func(fk *descpb.ForeignKeyConstraint) error) error
	// ForeachInboundFK calls f for every inbound foreign key in desc until an
	// error is returned.
	ForeachInboundFK(f func(fk *descpb.ForeignKeyConstraint) error) error
	// AllActiveAndInactiveForeignKeys returns all foreign keys, including both
	// "active" ones on the index descriptor which are being enforced for all
	// writes, and "inactive" ones queued in the mutations list. An error is
	// returned if multiple foreign keys (including mutations) are found for the
	// same index.
	AllActiveAndInactiveForeignKeys() []*descpb.ForeignKeyConstraint

	// GetChecks returns information about this table's check constraints, if
	// there are any. Only valid if IsTable returns true.
	GetChecks() []*descpb.TableDescriptor_CheckConstraint
	// AllActiveAndInactiveChecks returns all check constraints, including both
	// "active" ones on the table descriptor which are being enforced for all
	// writes, and "inactive" new checks constraints queued in the mutations list.
	// Additionally,  if there are any dropped mutations queued inside the mutation
	// list, those will not cancel any "active" or "inactive" mutations.
	AllActiveAndInactiveChecks() []*descpb.TableDescriptor_CheckConstraint
	// ActiveChecks returns a list of all check constraints that should be enforced
	// on writes (including constraints being added/validated). The columns
	// referenced by the returned checks are writable, but not necessarily public.
	ActiveChecks() []descpb.TableDescriptor_CheckConstraint

	// GetLocalityConfig returns the locality config for this table, which
	// describes the table's multi-region locality policy if one is set (e.g.
	// GLOBAL or REGIONAL BY ROW).
	GetLocalityConfig() *catpb.LocalityConfig
	// IsLocalityRegionalByRow returns true if the table is REGIONAL BY ROW.
	IsLocalityRegionalByRow() bool
	// IsLocalityRegionalByTable returns true if the table is REGIONAL BY TABLE.
	IsLocalityRegionalByTable() bool
	// IsLocalityGlobal returns true if the table is GLOBAL.
	IsLocalityGlobal() bool
	// GetRegionalByTableRegion returns the region a REGIONAL BY TABLE table is
	// homed in.
	GetRegionalByTableRegion() (catpb.RegionName, error)
	// GetRegionalByRowTableRegionColumnName returns the region column name of a
	// REGIONAL BY ROW table.
	GetRegionalByRowTableRegionColumnName() (tree.Name, error)
	// GetRowLevelTTL returns the row-level TTL config for the table.
	GetRowLevelTTL() *catpb.RowLevelTTL
	// HasRowLevelTTL returns where there is a row-level TTL config for the table.
	HasRowLevelTTL() bool
	// GetExcludeDataFromBackup returns true if the table's row data is configured
	// to be excluded during backup.
	GetExcludeDataFromBackup() bool
	// GetStorageParams returns a list of storage parameters for the table.
	GetStorageParams(spaceBetweenEqual bool) []string
}

// TypeDescriptor will eventually be called typedesc.Descriptor.
// It is implemented by (Imm|M)utableTypeDescriptor.
type TypeDescriptor interface {
	Descriptor
	// TypeDesc returns the backing descriptor for this type, if one exists.
	TypeDesc() *descpb.TypeDescriptor
	// HydrateTypeInfoWithName fills in user defined type metadata for
	// a type and also sets the name in the metadata to the passed in name.
	// This is used when hydrating a type with a known qualified name.
	//
	// Note that if the passed type is already hydrated, regardless of the version
	// with which it has been hydrated, this is a no-op.
	HydrateTypeInfoWithName(ctx context.Context, typ *types.T, name *tree.TypeName, res TypeDescriptorResolver) error
	// MakeTypesT creates a types.T from the input type descriptor.
	MakeTypesT(ctx context.Context, name *tree.TypeName, res TypeDescriptorResolver) (*types.T, error)
	// HasPendingSchemaChanges returns whether or not this descriptor has schema
	// changes that need to be completed.
	HasPendingSchemaChanges() bool
	// GetIDClosure returns all type descriptor IDs that are referenced by this
	// type descriptor.
	GetIDClosure() (map[descpb.ID]struct{}, error)
	// IsCompatibleWith returns whether the type "desc" is compatible with "other".
	// As of now "compatibility" entails that disk encoded data of "desc" can be
	// interpreted and used by "other".
	IsCompatibleWith(other TypeDescriptor) error
	// GetArrayTypeID returns the globally unique ID for the implicitly created
	// array type for this type. It is only set when the type descriptor points to
	// a non-array type.
	GetArrayTypeID() descpb.ID

	// GetKind returns the kind of this type.
	GetKind() descpb.TypeDescriptor_Kind

	// The following fields are only valid for multi-region enum types.

	// PrimaryRegionName returns the primary region for a multi-region enum.
	PrimaryRegionName() (catpb.RegionName, error)
	// RegionNames returns all `PUBLIC` regions on the multi-region enum. Regions
	// that are in the process of being added/removed (`READ_ONLY`) are omitted.
	RegionNames() (catpb.RegionNames, error)
	// RegionNamesIncludingTransitioning returns all the regions on a multi-region
	// enum, including `READ ONLY` regions which are in the process of transitioning.
	RegionNamesIncludingTransitioning() (catpb.RegionNames, error)
	// RegionNamesForValidation returns all regions on the multi-region
	// enum to make validation with the public zone configs and partitons
	// possible.
	// Since the partitions and zone configs are only updated when a transaction
	// commits, this must ignore all regions being added (since they will not be
	// reflected in the zone configuration yet), but it must include all region
	// being dropped (since they will not be dropped from the zone configuration
	// until they are fully removed from the type descriptor, again, at the end
	// of the transaction).
	RegionNamesForValidation() (catpb.RegionNames, error)
	// TransitioningRegionNames returns regions which are transitioning to PUBLIC
	// or are being removed.
	TransitioningRegionNames() (catpb.RegionNames, error)

	// The following fields are set if the type is an enum or a multi-region enum.

	// NumEnumMembers returns the number of enum members if the type is an
	// enumeration type, 0 otherwise.
	NumEnumMembers() int
	// GetMemberPhysicalRepresentation returns the physical representation of the
	// enum member at ordinal enumMemberOrdinal.
	GetMemberPhysicalRepresentation(enumMemberOrdinal int) []byte
	// GetMemberLogicalRepresentation returns the logical representation of the enum
	// member at ordinal enumMemberOrdinal.
	GetMemberLogicalRepresentation(enumMemberOrdinal int) string
	// IsMemberReadOnly returns true iff the enum member at ordinal
	// enumMemberOrdinal is read-only.
	IsMemberReadOnly(enumMemberOrdinal int) bool

	// NumReferencingDescriptors returns the number of descriptors referencing this
	// type, directly or indirectly.
	NumReferencingDescriptors() int
	// GetReferencingDescriptorID returns the ID of the referencing descriptor at
	// ordinal refOrdinal.
	GetReferencingDescriptorID(refOrdinal int) descpb.ID
}

// TypeDescriptorResolver is an interface used during hydration of type
// metadata in types.T's. It is similar to tree.TypeReferenceResolver, except
// that it has the power to return TypeDescriptor, rather than only a
// types.T. Implementers of tree.TypeReferenceResolver should implement this
// interface as well.
type TypeDescriptorResolver interface {
	// GetTypeDescriptor returns the type descriptor for the input ID. Note that
	// the returned type descriptor may be the implicitly-defined record type for
	// a table, if the input ID points to a table descriptor.
	GetTypeDescriptor(ctx context.Context, id descpb.ID) (tree.TypeName, TypeDescriptor, error)
}

// DefaultPrivilegeDescriptor is an interface for default privileges to ensure
// DefaultPrivilegeDescriptor protos are not accessed and interacted
// with directly.
type DefaultPrivilegeDescriptor interface {
	GetDefaultPrivilegesForRole(catpb.DefaultPrivilegesRole) (*catpb.DefaultPrivilegesForRole, bool)
	ForEachDefaultPrivilegeForRole(func(catpb.DefaultPrivilegesForRole) error) error
	GetDefaultPrivilegeDescriptorType() catpb.DefaultPrivilegeDescriptor_DefaultPrivilegeDescriptorType
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
}

// IsSystemDescriptor returns true iff the descriptor is a system or a reserved
// descriptor.
func IsSystemDescriptor(desc Descriptor) bool {
	if desc.GetParentID() == keys.SystemDatabaseID {
		return true
	}
	if desc.GetID() == keys.SystemDatabaseID {
		return true
	}
	return false
}
