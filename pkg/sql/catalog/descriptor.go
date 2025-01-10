// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catalog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
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
	Database DescriptorType = "database"

	// Table is for table descriptors.
	Table DescriptorType = "relation"

	// Type is for type descriptors.
	Type DescriptorType = "type"

	// Schema is for schema descriptors.
	Schema DescriptorType = "schema"

	// Function is for function descriptors.
	Function DescriptorType = "function"
)

// DescExprType describes the type for a serialized expression or statement
// within a descriptor. This determines how the serialized expression should be
// interpreted.
type DescExprType uint8

const (
	// SQLExpr is a serialized SQL expression, such as "1 * 2".
	SQLExpr DescExprType = iota
	// SQLStmt is a serialized SQL statement or series of statements. Ex:
	//   INSERT INTO xy VALUES (1, 2)
	SQLStmt
	// PLpgSQLStmt is a serialized PLpgSQL statement or series of statements. Ex:
	//   BEGIN RAISE NOTICE 'foo'; END;
	PLpgSQLStmt
)

// MutationPublicationFilter is used by MakeFirstMutationPublic to filter the
// mutation types published.
type MutationPublicationFilter int

const (
	// IgnoreConstraints is used in MakeFirstMutationPublic to indicate that the
	// table descriptor returned should not include newly added constraints, which
	// is useful when passing the returned table descriptor to be used in
	// validating constraints to be added.
	IgnoreConstraints MutationPublicationFilter = iota
	// IgnorePKSwaps is used in MakeFirstMutationPublic to indicate that the
	// table descriptor returned should include newly added constraints.
	IgnorePKSwaps
	// RetainDroppingColumns is used in MakeFirstMutationPublic to indicate that
	// the table descriptor should include newly dropped columns.
	RetainDroppingColumns
)

// DescriptorBuilder interfaces are used to build catalog.Descriptor
// objects.
type DescriptorBuilder interface {

	// DescriptorType returns a symbol identifying the type of the descriptor
	// built by this builder.
	DescriptorType() DescriptorType

	// RunPostDeserializationChanges attempts to perform changes to the descriptor
	// being built from a deserialized protobuf.
	// NOTE: any error returned by this function should be treated as an assertion
	// failure, and indicates either corruption or a programming error.
	RunPostDeserializationChanges() error

	// RunRestoreChanges attempts to perform changes to the descriptor being
	// built from a deserialized protobuf obtained by restoring a backup.
	// This is to compensate for the fact that these are not subject to cluster
	// upgrade migrations
	RunRestoreChanges(version clusterversion.ClusterVersion, descLookupFn func(id descpb.ID) Descriptor) error

	// StripDanglingBackReferences attempts to remove any back-references in the
	// descriptor which are known to be dangling references. In other words, if
	// there is a back-reference to a descriptor or a job and we know that the
	// ID isn't valid, then this method removes this back-reference.
	//
	// Back-references are only ever checked when performing schema changes and
	// usually aren't needed by the query planner so any corruption there can
	// remain undetected for a long time. This method provides a mechanism for
	// brute-force repair.
	StripDanglingBackReferences(
		descIDMightExist func(id descpb.ID) bool,
		nonTerminalJobIDMightExist func(id jobspb.JobID) bool,
	) error

	// StripNonExistentRoles removes any privileges granted to roles that
	// don't exist.
	StripNonExistentRoles(roleExists func(role username.SQLUsername) bool) error

	// SetRawBytesInStorage sets `rawBytesInStorage` field by deep-copying `rawBytes`.
	SetRawBytesInStorage(rawBytes []byte)

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

var _ NameKey = descpb.NameInfo{}

func MakeNameInfo(nk NameKey) descpb.NameInfo {
	if ni, ok := nk.(descpb.NameInfo); ok {
		return ni
	}
	return descpb.NameInfo{
		ParentID:       nk.GetParentID(),
		ParentSchemaID: nk.GetParentSchemaID(),
		Name:           nk.GetName(),
	}
}

// NameEntry corresponds to an entry in the namespace table.
type NameEntry interface {
	NameKey
	GetID() descpb.ID
}

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
}

// Descriptor is an interface to be shared by individual descriptor
// types.
type Descriptor interface {
	NameEntry
	LeasableDescriptor
	privilege.Object

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

	// ValidateForwardReferences performs forward-reference checks.
	ValidateForwardReferences(vea ValidationErrorAccumulator, vdg ValidationDescGetter)

	// ValidateBackReferences performs back-reference checks.
	ValidateBackReferences(vea ValidationErrorAccumulator, vdg ValidationDescGetter)

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

	// HasConcurrentSchemaChanges returns true if it has a schema changer
	// in progress, either legacy or declarative.
	HasConcurrentSchemaChanges() bool

	// ConcurrentSchemaChangeJobIDs returns all in-progress schema change
	// jobs, either legacy or declarative.
	ConcurrentSchemaChangeJobIDs() []catpb.JobID

	// SkipNamespace is true when a descriptor should not have a namespace record.
	SkipNamespace() bool

	// GetRawBytesInStorage returns the raw bytes (tag + data) of the descriptor in storage.
	// It is exclusively used in the CPut when persisting an updated descriptor to storage.
	GetRawBytesInStorage() []byte

	// ForEachUDTDependentForHydration iterates over all the user-defined types.T
	// referenced by this descriptor which must be hydrated prior to using it.
	// iterutil.StopIteration is supported.
	ForEachUDTDependentForHydration(func(t *types.T) error) error

	// MaybeRequiresTypeHydration returns false if the descriptor definitely does not
	// depend on any types.T being hydrated.
	MaybeRequiresTypeHydration() bool

	// GetReplicatedPCRVersion return the version from the source
	// tenant if this descriptor is replicated.
	GetReplicatedPCRVersion() descpb.DescriptorVersion
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
	// ForEachSchema iterates f over each schema id-name mapping in the descriptor.
	// iterutil.StopIteration is supported.
	ForEachSchema(func(id descpb.ID, name string) error) error
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
	// MaterializedView returns whether this TableDescriptor is a MaterializedView.
	MaterializedView() bool
	// IsReadOnly returns if this table descriptor has external data, and cannot
	// be written to.
	IsReadOnly() bool
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
	GetPrimaryIndex() UniqueWithIndexConstraint
	// IsPartitionAllBy returns whether the table has a PARTITION ALL BY clause.
	IsPartitionAllBy() bool

	// PrimaryIndexSpan returns the Span that corresponds to the entire primary
	// index; can be used for a full table scan. This will not be an external span
	// even if the table uses external row data.
	PrimaryIndexSpan(codec keys.SQLCodec) roachpb.Span
	// IndexSpan returns the Span that corresponds to an entire index; can be used
	// for a full index scan. This will not be an external span even if the table
	// uses external row data.
	IndexSpan(codec keys.SQLCodec, id descpb.IndexID) roachpb.Span
	// IndexSpanAllowingExternalRowData returns the Span that corresponds to an
	// entire index; can be used for a full index scan. If the table uses external
	// row data, this will be an external span.
	IndexSpanAllowingExternalRowData(codec keys.SQLCodec, id descpb.IndexID) roachpb.Span
	// AllIndexSpans returns the Spans for each index in the table, including
	// those being added in the mutations. These will not be external spans even
	// if the table uses external row data.
	AllIndexSpans(codec keys.SQLCodec) roachpb.Spans
	// TableSpan returns the Span that corresponds to the entire table. This will
	// not be an external span even if the table uses external row data.
	TableSpan(codec keys.SQLCodec) roachpb.Span
	// GetIndexMutationCapabilities returns:
	// 1. Whether the index is a mutation
	// 2. if so, is it in state WRITE_ONLY
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
	// table's public columns and WRITE_ONLY mutations, in the canonical
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
	IndexKeyColumnDirections(idx Index) []catenumpb.IndexColumn_Direction
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
	IndexFullColumnDirections(idx Index) []catenumpb.IndexColumn_Direction
	// IndexStoredColumns returns a slice of Column interfaces containing all
	// stored columns in the specified Index.
	IndexStoredColumns(idx Index) []Column

	// IndexKeysPerRow returns the maximum number of keys used to encode a row for
	// the given index. If a secondary index doesn't store any columns, then it
	// only has one k/v pair, but if it stores some columns, it can return up to
	// one k/v pair per family in the table, just like a primary index.
	IndexKeysPerRow(idx Index) int

	// IndexFetchSpecKeyAndSuffixColumns returns information about the key and
	// suffix columns, suitable for populating a fetchpb.IndexFetchSpec.
	IndexFetchSpecKeyAndSuffixColumns(idx Index) []fetchpb.IndexFetchSpec_KeyColumn

	// GetNextColumnID returns the next unused column ID for this table. Column
	// IDs are unique per table, but not unique globally.
	GetNextColumnID() descpb.ColumnID
	// GetNextConstraintID returns the next unused constraint ID for this table.
	// Constraint IDs are unique per table, but not unique globally.
	GetNextConstraintID() descpb.ConstraintID
	// IsShardColumn returns true if col corresponds to a non-dropped hash sharded
	// index. This method assumes that col is currently a member of desc.
	IsShardColumn(col Column) bool

	// GetFamilies returns the column families of this table. All tables contain
	// at least one column family. The returned list is sorted by family ID.
	GetFamilies() []descpb.ColumnFamilyDescriptor
	// NumFamilies returns the number of column families in the descriptor.
	NumFamilies() int
	// ForeachFamily calls f for every column family key in desc until an
	// error is returned.
	ForeachFamily(f func(family *descpb.ColumnFamilyDescriptor) error) error
	// GetNextFamilyID returns the next unused family ID for this table. Family
	// IDs are unique per table, but not unique globally.
	GetNextFamilyID() descpb.FamilyID

	// FamilyDefaultColumns returns the default column IDs for families with a
	// default column. See IndexFetchSpec.FamilyDefaultColumns.
	FamilyDefaultColumns() []fetchpb.IndexFetchSpec_FamilyDefaultColumn

	// HasColumnBackfillMutation returns whether the table has any queued column
	// mutations that require a backfill.
	HasColumnBackfillMutation() bool
	// MakeFirstMutationPublic creates a descriptor by making the first
	// mutation public.
	// This is super valuable when trying to run SQL over data associated
	// with a schema mutation that is still not yet public: Data validation,
	// error reporting.
	MakeFirstMutationPublic(...MutationPublicationFilter) (TableDescriptor, error)
	// MakePublic creates a descriptor by making the state public.
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

	// GetAllReferencedTableIDs returns all relation IDs that this table
	// references. Table references can be from foreign keys, triggers, or via
	// direct references if the descriptor is a view.
	GetAllReferencedTableIDs() descpb.IDs

	// GetAllReferencedTypeIDs returns all user defined type descriptor IDs that
	// this table references. It takes in a function that returns the TypeDescriptor
	// with the desired ID.
	GetAllReferencedTypeIDs(
		databaseDesc DatabaseDescriptor, getType func(descpb.ID) (TypeDescriptor, error),
	) (referencedAnywhere, referencedInColumns descpb.IDs, _ error)

	// GetAllReferencedFunctionIDs returns descriptor IDs of all user defined
	// functions referenced in this table.
	GetAllReferencedFunctionIDs() (DescriptorIDSet, error)

	// GetAllReferencedFunctionIDsInConstraint returns descriptor IDs of all user
	// defined functions referenced in this check constraint.
	GetAllReferencedFunctionIDsInConstraint(
		cstID descpb.ConstraintID,
	) (DescriptorIDSet, error)

	// GetAllReferencedFunctionIDsInTrigger returns descriptor IDs of all user
	// defined functions referenced in this trigger. This includes the trigger
	// function as well as any functions it references transitively.
	GetAllReferencedFunctionIDsInTrigger(triggerID descpb.TriggerID) DescriptorIDSet

	// GetAllReferencedFunctionIDsInColumnExprs returns descriptor IDs of all user
	// defined functions referenced by expressions in this column.
	// Note: it extracts ids from expression strings, not from the UsesFunctionIDs
	// field of column descriptors.
	GetAllReferencedFunctionIDsInColumnExprs(
		colID descpb.ColumnID,
	) (DescriptorIDSet, error)

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
	// GetDependsOnFunctions returns the IDs of all functions that this view
	// depends on. It's only non-nil if IsView is true.
	GetDependsOnFunctions() []descpb.ID

	// AllConstraints returns all constraints in this table, regardless if
	// they're enforced yet or not. The ordering of the constraints within this
	// slice is partially defined:
	//   - constraints are grouped by kind,
	//   - the order of each kind is undefined,
	//   - for each kind, the enforced constraints appear first
	//   - and in the same order as the slice in the table descriptor protobuf;
	//     Checks, OutboundFKs, etc.
	//   - followed by the constraint mutations, in the same order as in the
	//     table descriptor's Mutations slice.
	AllConstraints() []Constraint
	// EnforcedConstraints returns the subset of constraints in AllConstraints
	// which are enforced for any data written to the table, regardless of
	// whether a constraint is valid for table data present before the
	// constraint's existence. The order from AllConstraints is preserved.
	EnforcedConstraints() []Constraint

	// CheckConstraints returns the subset of check constraints in
	// AllConstraints for this table, in the same order.
	CheckConstraints() []CheckConstraint
	// EnforcedCheckConstraints returns the subset of check constraints in
	// EnforcedConstraints for this table, in the same order.
	EnforcedCheckConstraints() []CheckConstraint

	// OutboundForeignKeys returns the subset of foreign key constraints in
	// AllConstraints for this table, in the same order.
	OutboundForeignKeys() []ForeignKeyConstraint
	// EnforcedOutboundForeignKeys returns the subset of foreign key constraints
	// in EnforcedConstraints for this table, in the same order.
	EnforcedOutboundForeignKeys() []ForeignKeyConstraint
	// InboundForeignKeys returns all foreign key back-references from this
	// table, in the same order as in the InboundFKs slice.
	InboundForeignKeys() []ForeignKeyConstraint

	// UniqueConstraintsWithIndex returns the subset of index-backed unique
	// constraints in AllConstraints for this table, in the same order.
	UniqueConstraintsWithIndex() []UniqueWithIndexConstraint
	// EnforcedUniqueConstraintsWithIndex returns the subset of index-backed
	// unique constraints in EnforcedConstraints for this table, in the same
	// order.
	EnforcedUniqueConstraintsWithIndex() []UniqueWithIndexConstraint

	// UniqueConstraintsWithoutIndex returns the subset of non-index-backed
	// unique constraints in AllConstraints for this table, in the same order.
	UniqueConstraintsWithoutIndex() []UniqueWithoutIndexConstraint
	// EnforcedUniqueConstraintsWithoutIndex returns the subset of
	// non-index-backed unique constraints in EnforcedConstraints for this table,
	// in the same order.
	EnforcedUniqueConstraintsWithoutIndex() []UniqueWithoutIndexConstraint

	// CheckConstraintColumns returns the slice of columns referenced by a check
	// constraint.
	CheckConstraintColumns(ck CheckConstraint) []Column
	// ForeignKeyReferencedColumns returns the slice of columns referenced by an
	// inbound foreign key.
	ForeignKeyReferencedColumns(fk ForeignKeyConstraint) []Column
	// ForeignKeyOriginColumns returns the slice of columns originating in this
	// table for an outbound foreign key.
	ForeignKeyOriginColumns(fk ForeignKeyConstraint) []Column
	// UniqueWithoutIndexColumns returns the slice of columns which are
	// defined as unique by a non-index-backed constraint.
	UniqueWithoutIndexColumns(uwoi UniqueWithoutIndexConstraint) []Column

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
	// NoAutoStatsSettingsOverrides is true if no auto stats related settings are
	// set at the table level for the given table.
	NoAutoStatsSettingsOverrides() bool
	// AutoStatsCollectionEnabled indicates if automatic statistics collection is
	// explicitly enabled or disabled for this table.
	AutoStatsCollectionEnabled() catpb.AutoStatsCollectionStatus
	// AutoPartialStatsCollectionEnabled indicates if automatic partial statistics
	// collection is explicitly enabled or disabled for this table.
	AutoPartialStatsCollectionEnabled() catpb.AutoPartialStatsCollectionStatus
	// AutoStatsMinStaleRows indicates the setting of
	// sql_stats_automatic_collection_min_stale_rows for this table.
	// If ok is true, then the minStaleRows value is valid, otherwise this has not
	// been set at the table level.
	AutoStatsMinStaleRows() (minStaleRows int64, ok bool)
	// AutoStatsFractionStaleRows indicates the setting of
	// sql_stats_automatic_collection_fraction_stale_rows for this table. If ok is
	// true, then the fractionStaleRows value is valid, otherwise this has not
	// been set at the table level.
	AutoStatsFractionStaleRows() (fractionStaleRows float64, ok bool)
	// GetAutoStatsSettings returns the table settings related to automatic
	// statistics collection. May return nil if none are set.
	GetAutoStatsSettings() *catpb.AutoStatsSettings
	// ForecastStatsEnabled indicates whether statistics forecasting is explicitly
	// enabled or disabled for this table. If ok is true, then the enabled value
	// is valid, otherwise this has not been set at the table level.
	ForecastStatsEnabled() (enabled bool, ok bool)
	// HistogramSamplesCount indicates the number of rows to sample when building
	// a histogram for this table. If ok is true, then the histogramSamplesCount
	// value is valid, otherwise this has not been set at the table level.
	HistogramSamplesCount() (histogramSamplesCount uint32, ok bool)
	// HistogramBucketsCount indicates the number of buckets to build when
	// constructing a histogram for this table. If ok is true, then the
	// histogramBucketsCount value is valid, otherwise this has not been set at
	// the table level.
	HistogramBucketsCount() (histogramBucketsCount uint32, ok bool)
	// IsRefreshViewRequired indicates if a REFRESH VIEW operation needs to be called
	// on a materialized view.
	IsRefreshViewRequired() bool
	// GetInProgressImportStartTime returns the start wall time of the in progress import,
	// if it exists.
	GetInProgressImportStartTime() int64
	// IsSchemaLocked returns true if we don't allow performing schema changes
	// on this table descriptor.
	IsSchemaLocked() bool
	// IsPrimaryKeySwapMutation returns true if the mutation is a primary key
	// swap mutation or a secondary index used by the declarative schema changer
	// for a primary index swap.
	IsPrimaryKeySwapMutation(m *descpb.DescriptorMutation) bool
	// ExternalRowData indicates where the row data for this object is stored if
	// it is stored outside the span of the object.
	ExternalRowData() *descpb.ExternalRowData
	// GetTriggers returns a slice with all triggers defined on the table.
	GetTriggers() []descpb.TriggerDescriptor
	// GetNextTriggerID returns the next unused trigger ID for this table.
	// Trigger IDs are unique per table, but not unique globally.
	GetNextTriggerID() descpb.TriggerID
	// GetPolicies returns a slice with all policies defined on the table.
	GetPolicies() []descpb.PolicyDescriptor
	// GetNextPolicyID returns the next unused policy ID for this table.
	// Policy IDs are unique per table.
	GetNextPolicyID() descpb.PolicyID
}

// MutableTableDescriptor is both a MutableDescriptor and a TableDescriptor.
type MutableTableDescriptor interface {
	TableDescriptor
	MutableDescriptor
}

// TypeDescriptor is an interface around the type descriptor types.
type TypeDescriptor interface {
	Descriptor
	// TypeDesc returns the backing descriptor for this type, if one exists.
	TypeDesc() *descpb.TypeDescriptor
	// HasPendingSchemaChanges returns whether or not this descriptor has schema
	// changes that need to be completed.
	HasPendingSchemaChanges() bool
	// GetIDClosure returns all type descriptor IDs that are referenced by this
	// type descriptor.
	GetIDClosure() DescriptorIDSet
	// IsCompatibleWith returns whether the type "desc" is compatible with "other".
	// As of now "compatibility" entails that disk encoded data of "desc" can be
	// interpreted and used by "other".
	IsCompatibleWith(other TypeDescriptor) error
	// AsTypesT returns a reference to a types.T corresponding to this type
	// descriptor. No guarantees are provided as to whether this object is a
	// singleton or not, or whether it's hydrated or not. The returned type can be
	// hydrated by the caller.
	AsTypesT() *types.T
	// GetKind returns the kind of this type.
	GetKind() descpb.TypeDescriptor_Kind

	// NumReferencingDescriptors returns the number of descriptors referencing this
	// type, directly or indirectly.
	NumReferencingDescriptors() int
	// GetReferencingDescriptorID returns the ID of the referencing descriptor at
	// ordinal refOrdinal.
	GetReferencingDescriptorID(refOrdinal int) descpb.ID

	// AsEnumTypeDescriptor returns this instance cast to EnumTypeDescriptor
	// if this type is an enum type, nil otherwise.
	AsEnumTypeDescriptor() EnumTypeDescriptor

	// AsRegionEnumTypeDescriptor returns this instance cast to
	// RegionEnumTypeDescriptor if this type is a multi-region enum type,
	// nil otherwise.
	AsRegionEnumTypeDescriptor() RegionEnumTypeDescriptor

	// AsAliasTypeDescriptor returns this instance cast to
	// AliasTypeDescriptor if this type is an alias type,
	// nil otherwise.
	AsAliasTypeDescriptor() AliasTypeDescriptor

	// AsCompositeTypeDescriptor returns this instance cast to
	// CompositeTypeDescriptor if this type is a composite type,
	// nil otherwise.
	AsCompositeTypeDescriptor() CompositeTypeDescriptor

	// AsTableImplicitRecordTypeDescriptor returns this instance cast to
	// TableImplicitRecordTypeDescriptor if this type is an implicit table record
	// type, nil otherwise.
	AsTableImplicitRecordTypeDescriptor() TableImplicitRecordTypeDescriptor
}

// NonAliasTypeDescriptor is the TypeDescriptor subtype for concrete user-defined
// types.
type NonAliasTypeDescriptor interface {
	TypeDescriptor

	// GetArrayTypeID returns the globally unique ID for the implicitly created
	// array type for this type.
	GetArrayTypeID() descpb.ID
}

// EnumTypeDescriptor is the TypeDescriptor subtype for enums (incl. regions).
type EnumTypeDescriptor interface {
	NonAliasTypeDescriptor

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
}

// RegionEnumTypeDescriptor is the TypeDescriptor subtype for multi-region enums.
type RegionEnumTypeDescriptor interface {
	EnumTypeDescriptor

	// PrimaryRegion returns the primary region name.
	PrimaryRegion() catpb.RegionName

	// ForEachPublicRegion is like ForEachRegion but limited to regions
	// which are public, i.e. not in the process of being added or removed.
	ForEachPublicRegion(f func(regionName catpb.RegionName) error) error

	// ForEachRegion applies f on each region name,
	// Supports iterutil.StopIteration().
	ForEachRegion(f func(regionName catpb.RegionName, transition descpb.TypeDescriptor_EnumMember_Direction) error) error

	// ForEachSuperRegion applies f on each super-region name.
	// Supports iterutil.StopIteration().
	ForEachSuperRegion(f func(superRegionName string) error) error

	// ForEachRegionInSuperRegion applies f on each region in the super region.
	// Supports iterutil.StopIteration().
	ForEachRegionInSuperRegion(
		superRegion string,
		f func(region catpb.RegionName) error,
	) error
}

// AliasTypeDescriptor is the TypeDescriptor subtype for alias types.
// This is used for array subtypes.
type AliasTypeDescriptor interface {
	TypeDescriptor

	// Aliased returns the types.T which is being aliased.
	Aliased() *types.T
}

// CompositeTypeDescriptor is the TypeDescriptor subtype for composite types,
// which are union types.
type CompositeTypeDescriptor interface {
	NonAliasTypeDescriptor

	// NumElements returns the number of elements forming the composite type.
	NumElements() int

	// GetElementLabel returns the label of the composite type element at the
	// given ordinal.
	GetElementLabel(ordinal int) string

	// GetElementType returns the type of the composite type element at the
	// given ordinal.
	GetElementType(ordinal int) *types.T
}

// TableImplicitRecordTypeDescriptor is the TypeDescriptor subtype for the
// record type implicitly defined by a table.
type TableImplicitRecordTypeDescriptor interface {
	NonAliasTypeDescriptor

	// UnderlyingTableDescriptor returns the table descriptor underlying this
	// implicit type.
	UnderlyingTableDescriptor() TableDescriptor
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

// FunctionDescriptor is an interface around the function descriptor types.
type FunctionDescriptor interface {
	Descriptor

	// GetReturnType returns the function's return type.
	GetReturnType() descpb.FunctionDescriptor_ReturnType

	// GetVolatility returns the function's volatility attribute.
	GetVolatility() catpb.Function_Volatility

	// GetLeakProof returns the function's leakproof attribute.
	GetLeakProof() bool

	// GetNullInputBehavior returns the function's attribute on null inputs.
	GetNullInputBehavior() catpb.Function_NullInputBehavior

	// GetFunctionBody returns the function body string.
	GetFunctionBody() string

	// GetParams returns a list of all parameters of the function.
	GetParams() []descpb.FunctionDescriptor_Parameter

	// GetDependsOn returns a list of IDs of the relation this function depends on.
	GetDependsOn() []descpb.ID

	// GetDependsOnTypes returns a list of IDs of the types this function depends on.
	GetDependsOnTypes() []descpb.ID

	// GetDependsOnFunctions returns a list of IDs of functions this function depends
	// on.
	GetDependsOnFunctions() []descpb.ID

	// GetDependedOnBy returns a list of back-references of this function.
	GetDependedOnBy() []descpb.FunctionDescriptor_Reference

	// FuncDesc returns the function's underlying protobuf descriptor.
	FuncDesc() *descpb.FunctionDescriptor

	// ToOverload converts the function descriptor to tree.Overload object which
	// can be used for execution.
	ToOverload() (ret *tree.Overload, err error)

	// GetLanguage returns the language of this function.
	GetLanguage() catpb.Function_Language

	// ToCreateExpr converts a function descriptor back to a CREATE FUNCTION or
	// CREATE PROCEDURE statement. This is mainly used for formatting, e.g.,
	// SHOW CREATE FUNCTION and SHOW CREATE PROCEDURE.
	ToCreateExpr() (*tree.CreateRoutine, error)

	// IsProcedure returns true if the descriptor represents a procedure. It
	// returns false if the descriptor represents a user-defined function.
	IsProcedure() bool

	// GetSecurity returns the security specification of this function.
	GetSecurity() catpb.Function_Security
}

// FilterDroppedDescriptor returns an error if the descriptor state is DROP.
func FilterDroppedDescriptor(desc Descriptor) error {
	if !desc.Dropped() {
		return nil

	}
	return NewInactiveDescriptorError(ErrDescriptorDropped)
}

// FilterOfflineDescriptor returns an error if the descriptor state is OFFLINE.
func FilterOfflineDescriptor(desc Descriptor) error {
	if !desc.Offline() {
		return nil
	}
	err := errors.Errorf("%s %q is offline", desc.DescriptorType(), desc.GetName())
	if desc.GetOfflineReason() != "" {
		err = errors.Errorf("%s %q is offline: %s", desc.DescriptorType(), desc.GetName(), desc.GetOfflineReason())
	}
	return NewInactiveDescriptorError(err)
}

// FilterAddingDescriptor returns an error if the descriptor state is ADD.
func FilterAddingDescriptor(desc Descriptor) error {
	if !desc.Adding() {
		return nil
	}
	return pgerror.WithCandidateCode(newAddingDescriptorError(desc), pgcode.ObjectNotInPrerequisiteState)
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

// HasConcurrentDeclarativeSchemaChange returns true iff the descriptors has
// a concurrent declarative schema change. This declarative schema changer is
// extremely disciplined and only writes state information during the pre-commit
// phase, so if a descriptor has a declarative state, we know an ongoing
// declarative schema change active. The legacy schema changer will tag descriptors
// with job IDs even during the statement phase, so we cannot rely on similar
// checks to block concurrent schema changes. Hence, Descriptor.HasConcurrentSchemaChanges
// is not equivalent to this operation (the former can lead to false positives
// for the legacy schema changer).
func HasConcurrentDeclarativeSchemaChange(desc Descriptor) bool {
	return desc.GetDeclarativeSchemaChangerState() != nil
}
