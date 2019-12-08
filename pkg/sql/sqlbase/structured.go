// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// ID, ColumnID, FamilyID, and IndexID are all uint32, but are each given a
// type alias to prevent accidental use of one of the types where
// another is expected.

// ID is a custom type for {Database,Table}Descriptor IDs.
type ID tree.ID

// InvalidID is the uninitialised descriptor id.
const InvalidID ID = 0

// IDs is a sortable list of IDs.
type IDs []ID

func (ids IDs) Len() int           { return len(ids) }
func (ids IDs) Less(i, j int) bool { return ids[i] < ids[j] }
func (ids IDs) Swap(i, j int)      { ids[i], ids[j] = ids[j], ids[i] }

// TableDescriptors is a sortable list of *TableDescriptors.
type TableDescriptors []*TableDescriptor

// TablesByID is a shorthand for the common map of tables keyed by ID.
type TablesByID map[ID]*TableDescriptor

func (t TableDescriptors) Len() int           { return len(t) }
func (t TableDescriptors) Less(i, j int) bool { return t[i].ID < t[j].ID }
func (t TableDescriptors) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }

// ColumnID is a custom type for ColumnDescriptor IDs.
type ColumnID tree.ColumnID

// ColumnIDs is a slice of ColumnDescriptor IDs.
type ColumnIDs []ColumnID

func (c ColumnIDs) Len() int           { return len(c) }
func (c ColumnIDs) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c ColumnIDs) Less(i, j int) bool { return c[i] < c[j] }

// HasPrefix returns true if the input list is a prefix of this list.
func (c ColumnIDs) HasPrefix(input ColumnIDs) bool {
	if len(input) > len(c) {
		return false
	}
	for i := range input {
		if input[i] != c[i] {
			return false
		}
	}
	return true
}

// FamilyID is a custom type for ColumnFamilyDescriptor IDs.
type FamilyID uint32

// IndexID is a custom type for IndexDescriptor IDs.
type IndexID tree.IndexID

// DescriptorVersion is a custom type for TableDescriptor Versions.
type DescriptorVersion uint32

// FormatVersion is a custom type for TableDescriptor versions of the sql to
// key:value mapping.
//go:generate stringer -type=FormatVersion
type FormatVersion uint32

const (
	_ FormatVersion = iota
	// BaseFormatVersion corresponds to the encoding described in
	// https://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/.
	BaseFormatVersion
	// FamilyFormatVersion corresponds to the encoding described in
	// https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20151214_sql_column_families.md
	FamilyFormatVersion
	// InterleavedFormatVersion corresponds to the encoding described in
	// https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20160624_sql_interleaved_tables.md
	InterleavedFormatVersion
)

// IndexDescriptorVersion is a custom type for IndexDescriptor Versions.
type IndexDescriptorVersion uint32

const (
	// BaseIndexFormatVersion corresponds to the original encoding of secondary indexes that
	// don't respect table level column family definitions. We allow the 0 value of the type to
	// have a value so that existing index descriptors are denoted as having the base format.
	BaseIndexFormatVersion IndexDescriptorVersion = iota
	// SecondaryIndexFamilyFormatVersion corresponds to the encoding of secondary indexes that
	// use table level column family definitions.
	SecondaryIndexFamilyFormatVersion
)

// MutationID is a custom type for TableDescriptor mutations.
type MutationID uint32

// MutableTableDescriptor is a custom type for TableDescriptors
// going through schema mutations.
type MutableTableDescriptor struct {
	TableDescriptor

	// ClusterVersion represents the version of the table descriptor read from the store.
	ClusterVersion TableDescriptor
}

// ImmutableTableDescriptor is a custom type for TableDescriptors
// It holds precomputed values and the underlying TableDescriptor
// should be const.
type ImmutableTableDescriptor struct {
	TableDescriptor

	// publicAndNonPublicCols is a list of public and non-public columns.
	// It is partitioned by the state of the column: public, write-only, delete-only
	publicAndNonPublicCols []ColumnDescriptor

	// publicAndNonPublicCols is a list of public and non-public indexes.
	// It is partitioned by the state of the index: public, write-only, delete-only
	publicAndNonPublicIndexes []IndexDescriptor

	writeOnlyColCount   int
	writeOnlyIndexCount int

	allChecks []TableDescriptor_CheckConstraint

	// ReadableColumns is a list of columns (including those undergoing a schema change)
	// which can be scanned. Columns in the process of a schema change
	// are all set to nullable while column backfilling is still in
	// progress, as mutation columns may have NULL values.
	ReadableColumns []ColumnDescriptor

	// TODO (lucy): populate these and use them
	// inboundFKs  []*ForeignKeyConstraint
	// outboundFKs []*ForeignKeyConstraint
}

// InvalidMutationID is the uninitialised mutation id.
const InvalidMutationID MutationID = 0

const (
	// PrimaryKeyIndexName is the name of the index for the primary key.
	PrimaryKeyIndexName = "primary"
)

// ErrMissingColumns indicates a table with no columns.
var ErrMissingColumns = errors.New("table must contain at least 1 column")

// ErrMissingPrimaryKey indicates a table with no primary key.
var ErrMissingPrimaryKey = errors.New("table must contain a primary key")

func validateName(name, typ string) error {
	if len(name) == 0 {
		return fmt.Errorf("empty %s name", typ)
	}
	// TODO(pmattis): Do we want to be more restrictive than this?
	return nil
}

// ToEncodingDirection converts a direction from the proto to an encoding.Direction.
func (dir IndexDescriptor_Direction) ToEncodingDirection() (encoding.Direction, error) {
	switch dir {
	case IndexDescriptor_ASC:
		return encoding.Ascending, nil
	case IndexDescriptor_DESC:
		return encoding.Descending, nil
	default:
		return encoding.Ascending, errors.Errorf("invalid direction: %s", dir)
	}
}

// ErrDescriptorNotFound is returned by GetTableDescFromID to signal that a
// descriptor could not be found with the given id.
var ErrDescriptorNotFound = errors.New("descriptor not found")

// ErrIndexGCMutationsList is returned by FindIndexByID to signal that the
// index with the given ID does not have a descriptor and is in the garbage
// collected mutations list.
var ErrIndexGCMutationsList = errors.New("index in GC mutations list")

// NewMutableCreatedTableDescriptor returns a MutableTableDescriptor from the
// given TableDescriptor with the cluster version being the zero table. This
// is for a table that is created in the transaction.
func NewMutableCreatedTableDescriptor(tbl TableDescriptor) *MutableTableDescriptor {
	return &MutableTableDescriptor{TableDescriptor: tbl}
}

// NewMutableExistingTableDescriptor returns a MutableTableDescriptor from the
// given TableDescriptor with the cluster version also set to the descriptor.
// This is for an existing table.
func NewMutableExistingTableDescriptor(tbl TableDescriptor) *MutableTableDescriptor {
	return &MutableTableDescriptor{TableDescriptor: tbl, ClusterVersion: tbl}
}

// NewImmutableTableDescriptor returns a ImmutableTableDescriptor from the
// given TableDescriptor.
func NewImmutableTableDescriptor(tbl TableDescriptor) *ImmutableTableDescriptor {
	publicAndNonPublicCols := tbl.Columns
	publicAndNonPublicIndexes := tbl.Indexes

	readableCols := tbl.Columns

	desc := &ImmutableTableDescriptor{TableDescriptor: tbl}

	if len(tbl.Mutations) > 0 {
		publicAndNonPublicCols = make([]ColumnDescriptor, 0, len(tbl.Columns)+len(tbl.Mutations))
		publicAndNonPublicIndexes = make([]IndexDescriptor, 0, len(tbl.Indexes)+len(tbl.Mutations))
		readableCols = make([]ColumnDescriptor, 0, len(tbl.Columns)+len(tbl.Mutations))

		publicAndNonPublicCols = append(publicAndNonPublicCols, tbl.Columns...)
		publicAndNonPublicIndexes = append(publicAndNonPublicIndexes, tbl.Indexes...)
		readableCols = append(readableCols, tbl.Columns...)

		// Fill up mutations into the column/index lists by placing the writable columns/indexes
		// before the delete only columns/indexes.
		for _, m := range tbl.Mutations {
			switch m.State {
			case DescriptorMutation_DELETE_AND_WRITE_ONLY:
				if idx := m.GetIndex(); idx != nil {
					publicAndNonPublicIndexes = append(publicAndNonPublicIndexes, *idx)
					desc.writeOnlyIndexCount++
				} else if col := m.GetColumn(); col != nil {
					publicAndNonPublicCols = append(publicAndNonPublicCols, *col)
					desc.writeOnlyColCount++
				}
			}
		}

		for _, m := range tbl.Mutations {
			switch m.State {
			case DescriptorMutation_DELETE_ONLY:
				if idx := m.GetIndex(); idx != nil {
					publicAndNonPublicIndexes = append(publicAndNonPublicIndexes, *idx)
				} else if col := m.GetColumn(); col != nil {
					publicAndNonPublicCols = append(publicAndNonPublicCols, *col)
				}
			}
		}

		// Iterate through all mutation columns.
		for _, c := range publicAndNonPublicCols[len(tbl.Columns):] {
			// Mutation column may need to be fetched, but may not be completely backfilled
			// and have be null values (even though they may be configured as NOT NULL).
			c.Nullable = true
			readableCols = append(readableCols, c)
		}
	}

	desc.ReadableColumns = readableCols
	desc.publicAndNonPublicCols = publicAndNonPublicCols
	desc.publicAndNonPublicIndexes = publicAndNonPublicIndexes

	desc.allChecks = make([]TableDescriptor_CheckConstraint, len(tbl.Checks))
	for i, c := range tbl.Checks {
		desc.allChecks[i] = *c
	}

	return desc
}

// protoGetter is a sub-interface of client.Txn that can fetch protobufs in a
// transaction.
type protoGetter interface {
	// GetProtoTs retrieves a protoutil.Message that's stored at key, storing it
	// into the input msg parameter. If the key doesn't exist, the input proto
	// will be reset.
	GetProtoTs(ctx context.Context, key interface{}, msg protoutil.Message) (hlc.Timestamp, error)
}

// GetDatabaseDescFromID retrieves the database descriptor for the database
// ID passed in using an existing proto getter. Returns an error if the
// descriptor doesn't exist or if it exists and is not a database.
func GetDatabaseDescFromID(
	ctx context.Context, protoGetter protoGetter, id ID,
) (*DatabaseDescriptor, error) {
	desc := &Descriptor{}
	descKey := MakeDescMetadataKey(id)
	_, err := protoGetter.GetProtoTs(ctx, descKey, desc)
	if err != nil {
		return nil, err
	}
	db := desc.GetDatabase()
	if db == nil {
		return nil, ErrDescriptorNotFound
	}
	return db, nil
}

// GetTableDescFromID retrieves the table descriptor for the table
// ID passed in using an existing proto getter. Returns an error if the
// descriptor doesn't exist or if it exists and is not a table.
func GetTableDescFromID(
	ctx context.Context, protoGetter protoGetter, id ID,
) (*TableDescriptor, error) {
	table, err := getTableDescFromIDRaw(ctx, protoGetter, id)
	if err != nil {
		return nil, err
	}

	if err := table.MaybeFillInDescriptor(ctx, protoGetter); err != nil {
		return nil, err
	}

	return table, nil
}

// getTableDescFromIDRaw retrieves the table descriptor for the table
// ID passed in using an existing proto getter. Returns an error if the
// descriptor doesn't exist or if it exists and is not a table. Note that it
// does not "fill in" the descriptor, which performs various upgrade steps for
// migrations and is *required* before ordinary presentation to other code. This
// method is for internal use only and shouldn't get exposed.
func getTableDescFromIDRaw(
	ctx context.Context, protoGetter protoGetter, id ID,
) (*TableDescriptor, error) {
	desc := &Descriptor{}
	descKey := MakeDescMetadataKey(id)
	ts, err := protoGetter.GetProtoTs(ctx, descKey, desc)
	if err != nil {
		return nil, err
	}
	table := desc.Table(ts)
	if table == nil {
		return nil, ErrDescriptorNotFound
	}
	return table, nil
}

// GetMutableTableDescFromID retrieves the table descriptor for the table
// ID passed in using an existing proto getter. Returns an error if the
// descriptor doesn't exist or if it exists and is not a table.
// Otherwise a mutable copy of the table is returned.
func GetMutableTableDescFromID(
	ctx context.Context, protoGetter protoGetter, id ID,
) (*MutableTableDescriptor, error) {
	table, err := GetTableDescFromID(ctx, protoGetter, id)
	if err != nil {
		return nil, err
	}
	return NewMutableExistingTableDescriptor(*table), nil
}

// RunOverAllColumns applies its argument fn to each of the column IDs in desc.
// If there is an error, that error is returned immediately.
func (desc *IndexDescriptor) RunOverAllColumns(fn func(id ColumnID) error) error {
	for _, colID := range desc.ColumnIDs {
		if err := fn(colID); err != nil {
			return err
		}
	}
	for _, colID := range desc.ExtraColumnIDs {
		if err := fn(colID); err != nil {
			return err
		}
	}
	for _, colID := range desc.StoreColumnIDs {
		if err := fn(colID); err != nil {
			return err
		}
	}
	return nil
}

// FindPartitionByName searches this partitioning descriptor for a partition
// whose name is the input and returns it, or nil if no match is found.
func (desc *PartitioningDescriptor) FindPartitionByName(name string) *PartitioningDescriptor {
	for _, l := range desc.List {
		if l.Name == name {
			return desc
		}
		if s := l.Subpartitioning.FindPartitionByName(name); s != nil {
			return s
		}
	}
	for _, r := range desc.Range {
		if r.Name == name {
			return desc
		}
	}
	return nil

}

// FindPartitionByName searches this index descriptor for a partition whose name
// is the input and returns it, or nil if no match is found.
func (desc *IndexDescriptor) FindPartitionByName(name string) *PartitioningDescriptor {
	return desc.Partitioning.FindPartitionByName(name)
}

// allocateName sets desc.Name to a value that is not EqualName to any
// of tableDesc's indexes. allocateName roughly follows PostgreSQL's
// convention for automatically-named indexes.
func (desc *IndexDescriptor) allocateName(tableDesc *MutableTableDescriptor) {
	segments := make([]string, 0, len(desc.ColumnNames)+2)
	segments = append(segments, tableDesc.Name)
	segments = append(segments, desc.ColumnNames...)
	if desc.Unique {
		segments = append(segments, "key")
	} else {
		segments = append(segments, "idx")
	}

	baseName := strings.Join(segments, "_")
	name := baseName

	exists := func(name string) bool {
		_, _, err := tableDesc.FindIndexByName(name)
		return err == nil
	}
	for i := 1; exists(name); i++ {
		name = fmt.Sprintf("%s%d", baseName, i)
	}

	desc.Name = name
}

// FillColumns sets the column names and directions in desc.
func (desc *IndexDescriptor) FillColumns(elems tree.IndexElemList) error {
	desc.ColumnNames = make([]string, 0, len(elems))
	desc.ColumnDirections = make([]IndexDescriptor_Direction, 0, len(elems))
	for _, c := range elems {
		desc.ColumnNames = append(desc.ColumnNames, string(c.Column))
		switch c.Direction {
		case tree.Ascending, tree.DefaultDirection:
			desc.ColumnDirections = append(desc.ColumnDirections, IndexDescriptor_ASC)
		case tree.Descending:
			desc.ColumnDirections = append(desc.ColumnDirections, IndexDescriptor_DESC)
		default:
			return fmt.Errorf("invalid direction %s for column %s", c.Direction, c.Column)
		}
	}
	return nil
}

type returnTrue struct{}

func (returnTrue) Error() string { panic("unimplemented") }

var returnTruePseudoError error = returnTrue{}

// ContainsColumnID returns true if the index descriptor contains the specified
// column ID either in its explicit column IDs, the extra column IDs, or the
// stored column IDs.
func (desc *IndexDescriptor) ContainsColumnID(colID ColumnID) bool {
	return desc.RunOverAllColumns(func(id ColumnID) error {
		if id == colID {
			return returnTruePseudoError
		}
		return nil
	}) != nil
}

// FullColumnIDs returns the index column IDs including any extra (implicit or
// stored (old STORING encoding)) column IDs for non-unique indexes. It also
// returns the direction with which each column was encoded.
func (desc *IndexDescriptor) FullColumnIDs() ([]ColumnID, []IndexDescriptor_Direction) {
	if desc.Unique {
		return desc.ColumnIDs, desc.ColumnDirections
	}
	// Non-unique indexes have some of the primary-key columns appended to
	// their key.
	columnIDs := append([]ColumnID(nil), desc.ColumnIDs...)
	columnIDs = append(columnIDs, desc.ExtraColumnIDs...)
	dirs := append([]IndexDescriptor_Direction(nil), desc.ColumnDirections...)
	for range desc.ExtraColumnIDs {
		// Extra columns are encoded in ascending order.
		dirs = append(dirs, IndexDescriptor_ASC)
	}
	return columnIDs, dirs
}

// ColNamesFormat writes a string describing the column names and directions
// in this index to the given buffer.
func (desc *IndexDescriptor) ColNamesFormat(ctx *tree.FmtCtx) {
	for i := range desc.ColumnNames {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNameP(&desc.ColumnNames[i])
		if desc.Type != IndexDescriptor_INVERTED {
			ctx.WriteByte(' ')
			ctx.WriteString(desc.ColumnDirections[i].String())
		}
	}
}

// TODO (tyler): Issue #39771 This method needs more thorough testing, probably
// in structured_test.go. Or possibly replace it with a format method taking
// a format context as argument.

// ColNamesString returns a string describing the column names and directions
// in this index.
func (desc *IndexDescriptor) ColNamesString() string {
	f := tree.NewFmtCtx(tree.FmtSimple)
	desc.ColNamesFormat(f)
	return f.CloseAndGetString()
}

// TODO (tyler): Issue #39771 Same comment as ColNamesString above.

// SQLString returns the SQL string describing this index. If non-empty,
// "ON tableName" is included in the output in the correct place.
func (desc *IndexDescriptor) SQLString(tableName *tree.TableName) string {
	f := tree.NewFmtCtx(tree.FmtSimple)
	if desc.Unique {
		f.WriteString("UNIQUE ")
	}
	if desc.Type == IndexDescriptor_INVERTED {
		f.WriteString("INVERTED ")
	}
	f.WriteString("INDEX ")
	f.FormatNameP(&desc.Name)
	if *tableName != AnonymousTable {
		f.WriteString(" ON ")
		f.FormatNode(tableName)
	}
	f.WriteString(" (")
	desc.ColNamesFormat(f)
	f.WriteByte(')')

	if len(desc.StoreColumnNames) > 0 {
		f.WriteString(" STORING (")
		for i := range desc.StoreColumnNames {
			if i > 0 {
				f.WriteString(", ")
			}
			f.FormatNameP(&desc.StoreColumnNames[i])
		}
		f.WriteByte(')')
	}
	return f.CloseAndGetString()
}

// IsInterleaved returns whether the index is interleaved or not.
func (desc *IndexDescriptor) IsInterleaved() bool {
	return len(desc.Interleave.Ancestors) > 0 || len(desc.InterleavedBy) > 0
}

// SetID implements the DescriptorProto interface.
func (desc *TableDescriptor) SetID(id ID) {
	desc.ID = id
}

// TypeName returns the plain type of this descriptor.
func (desc *TableDescriptor) TypeName() string {
	return "relation"
}

// SetName implements the DescriptorProto interface.
func (desc *TableDescriptor) SetName(name string) {
	desc.Name = name
}

// IsTable returns true if the TableDescriptor actually describes a
// Table resource, as opposed to a different resource (like a View).
func (desc *TableDescriptor) IsTable() bool {
	return !desc.IsView() && !desc.IsSequence()
}

// IsView returns true if the TableDescriptor actually describes a
// View resource rather than a Table.
func (desc *TableDescriptor) IsView() bool {
	return desc.ViewQuery != ""
}

// IsAs returns true if the TableDescriptor actually describes
// a Table resource with an As source.
func (desc *TableDescriptor) IsAs() bool {
	return desc.CreateQuery != ""
}

// IsSequence returns true if the TableDescriptor actually describes a
// Sequence resource rather than a Table.
func (desc *TableDescriptor) IsSequence() bool {
	return desc.SequenceOpts != nil
}

// IsVirtualTable returns true if the TableDescriptor describes a
// virtual Table (like the information_schema tables) and thus doesn't
// need to be physically stored.
func (desc *TableDescriptor) IsVirtualTable() bool {
	return IsVirtualTable(desc.ID)
}

// GetParentSchemaID returns the ParentSchemaID if the descriptor has
// one. If the descriptor was created before the field was added, then the
// descriptor belongs to a table under the `public` physical schema. The static
// public schema ID is returned in that case.
func (desc *TableDescriptor) GetParentSchemaID() ID {
	parentSchemaID := desc.GetUnexposedParentSchemaID()
	if parentSchemaID == InvalidID {
		parentSchemaID = keys.PublicSchemaID
	}
	return parentSchemaID
}

// IsVirtualTable returns true if the TableDescriptor describes a
// virtual Table (like the informationgi_schema tables) and thus doesn't
// need to be physically stored.
func IsVirtualTable(id ID) bool {
	return MinVirtualID <= id
}

// IsPhysicalTable returns true if the TableDescriptor actually describes a
// physical Table that needs to be stored in the kv layer, as opposed to a
// different resource like a view or a virtual table. Physical tables have
// primary keys, column families, and indexes (unlike virtual tables).
// Sequences count as physical tables because their values are stored in
// the KV layer.
func (desc *TableDescriptor) IsPhysicalTable() bool {
	return desc.IsSequence() || (desc.IsTable() && !desc.IsVirtualTable())
}

// KeysPerRow returns the maximum number of keys used to encode a row for the
// given index. For secondary indexes, we always only use one, but for primary
// indexes, we can encode up to one kv per column family.
func (desc *TableDescriptor) KeysPerRow(indexID IndexID) int {
	if desc.PrimaryIndex.ID == indexID {
		return len(desc.Families)
	}
	return 1
}

// AllNonDropColumns returns all the columns, including those being added
// in the mutations.
func (desc *TableDescriptor) AllNonDropColumns() []ColumnDescriptor {
	cols := make([]ColumnDescriptor, 0, len(desc.Columns)+len(desc.Mutations))
	cols = append(cols, desc.Columns...)
	for _, m := range desc.Mutations {
		if col := m.GetColumn(); col != nil {
			if m.Direction == DescriptorMutation_ADD {
				cols = append(cols, *col)
			}
		}
	}
	return cols
}

// AllNonDropIndexes returns all the indexes, including those being added
// in the mutations.
func (desc *TableDescriptor) AllNonDropIndexes() []*IndexDescriptor {
	indexes := make([]*IndexDescriptor, 0, 1+len(desc.Indexes)+len(desc.Mutations))
	if desc.IsPhysicalTable() {
		indexes = append(indexes, &desc.PrimaryIndex)
	}
	for i := range desc.Indexes {
		indexes = append(indexes, &desc.Indexes[i])
	}
	for _, m := range desc.Mutations {
		if idx := m.GetIndex(); idx != nil {
			if m.Direction == DescriptorMutation_ADD {
				indexes = append(indexes, idx)
			}
		}
	}
	return indexes
}

// AllActiveAndInactiveChecks returns all check constraints, including both
// "active" ones on the table descriptor which are being enforced for all
// writes, and "inactive" ones queued in the mutations list.
func (desc *TableDescriptor) AllActiveAndInactiveChecks() []*TableDescriptor_CheckConstraint {
	// A check constraint could be both on the table descriptor and in the
	// list of mutations while the constraint is validated for existing rows. In
	// that case, the constraint is in the Validating state, and we avoid
	// including it twice. (Note that even though unvalidated check constraints
	// cannot be added as of 19.1, they can still exist if they were created under
	// previous versions.)
	checks := make([]*TableDescriptor_CheckConstraint, 0, len(desc.Checks))
	for _, c := range desc.Checks {
		// While a constraint is being validated for existing rows or being dropped,
		// the constraint is present both on the table descriptor and in the
		// mutations list in the Validating or Dropping state, so those constraints
		// are excluded here to avoid double-counting.
		if c.Validity != ConstraintValidity_Validating && c.Validity != ConstraintValidity_Dropping {
			checks = append(checks, c)
		}
	}
	for _, m := range desc.Mutations {
		if c := m.GetConstraint(); c != nil && c.ConstraintType == ConstraintToUpdate_CHECK {
			checks = append(checks, &c.Check)
		}
	}
	return checks
}

// AllActiveAndInactiveForeignKeys returns all foreign keys, including both
// "active" ones on the index descriptor which are being enforced for all
// writes, and "inactive" ones queued in the mutations list. An error is
// returned if multiple foreign keys (including mutations) are found for the
// same index.
func (desc *TableDescriptor) AllActiveAndInactiveForeignKeys() []*ForeignKeyConstraint {
	fks := make([]*ForeignKeyConstraint, 0, len(desc.OutboundFKs))
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		// While a constraint is being validated for existing rows or being dropped,
		// the constraint is present both on the table descriptor and in the
		// mutations list in the Validating or Dropping state, so those constraints
		// are excluded here to avoid double-counting.
		if fk.Validity != ConstraintValidity_Validating && fk.Validity != ConstraintValidity_Dropping {
			fks = append(fks, fk)
		}
	}
	for i := range desc.Mutations {
		if c := desc.Mutations[i].GetConstraint(); c != nil && c.ConstraintType == ConstraintToUpdate_FOREIGN_KEY {
			fks = append(fks, &c.ForeignKey)
		}
	}
	return fks
}

// ForeachNonDropIndex runs a function on all indexes, including those being
// added in the mutations.
func (desc *TableDescriptor) ForeachNonDropIndex(f func(*IndexDescriptor) error) error {
	if desc.IsPhysicalTable() {
		if err := f(&desc.PrimaryIndex); err != nil {
			return err
		}
	}
	for i := range desc.Indexes {
		if err := f(&desc.Indexes[i]); err != nil {
			return err
		}
	}
	for _, m := range desc.Mutations {
		if idx := m.GetIndex(); idx != nil && m.Direction == DescriptorMutation_ADD {
			if err := f(idx); err != nil {
				return err
			}
		}
	}
	return nil
}

func generatedFamilyName(familyID FamilyID, columnNames []string) string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "fam_%d", familyID)
	for _, n := range columnNames {
		buf.WriteString(`_`)
		buf.WriteString(n)
	}
	return buf.String()
}

// MaybeFillInDescriptor performs any modifications needed to the table descriptor.
// This includes format upgrades and optional changes that can be handled by all version
// (for example: additional default privileges).
// Returns true if any changes were made.
func (desc *TableDescriptor) MaybeFillInDescriptor(
	ctx context.Context, protoGetter protoGetter,
) error {
	desc.maybeUpgradeFormatVersion()
	desc.Privileges.MaybeFixPrivileges(desc.ID)
	if protoGetter != nil {
		if _, err := desc.MaybeUpgradeForeignKeyRepresentation(ctx, protoGetter, false /* skipFKsWithNoMatchingTable*/); err != nil {
			return err
		}
	}
	return nil
}

// MapProtoGetter is a protoGetter that has a hard-coded map of keys to proto
// messages.
type MapProtoGetter struct {
	Protos map[interface{}]protoutil.Message
}

// getProto implements the protoGetter interface.
func (m MapProtoGetter) getProto(
	ctx context.Context, key interface{}, msg protoutil.Message,
) error {
	msg.Reset()
	if other, ok := m.Protos[string(key.(roachpb.Key))]; ok {
		bytes := make([]byte, other.Size())
		if _, err := other.MarshalTo(bytes); err != nil {
			return err
		}
		if err := protoutil.Unmarshal(bytes, msg); err != nil {
			return err
		}
	}
	return nil
}

// GetProtoTs implements the protoGetter interface.
func (m MapProtoGetter) GetProtoTs(
	ctx context.Context, key interface{}, msg protoutil.Message,
) (hlc.Timestamp, error) {
	return hlc.Timestamp{}, m.getProto(ctx, key, msg)
}

// MaybeUpgradeForeignKeyRepresentation destructively modifies the input table
// descriptor by replacing all old-style foreign key references (the ForeignKey
// and ReferencedBy fields on IndexDescriptor) with new-style foreign key
// references (the InboundFKs and OutboundFKs fields on TableDescriptor). It
// uses the supplied proto getter to look up the referenced descriptor on
// outgoing FKs and the origin descriptor on incoming FKs. It returns true in
// the first position if the descriptor was upgraded at all (i.e. had old-style
// references on it) and an error if the descriptor was unable to be upgraded
// for some reason.
// If skipFKsWithNoMatchingTable is set to true, if a *table* that's supposed to
// contain the matching forward/back-reference for an FK is not found, the FK
// is dropped from the table and no error is returned.
// TODO(lucy): Write tests for when skipFKsWithNoMatchingTable is true.
func (desc *TableDescriptor) MaybeUpgradeForeignKeyRepresentation(
	ctx context.Context, protoGetter protoGetter, skipFKsWithNoMatchingTable bool,
) (bool, error) {
	if desc.Dropped() {
		// If the table has been dropped, it's permitted to have corrupted foreign
		// keys, so we have no chance to properly upgrade it. Just return as-is.
		return false, nil
	}
	otherUnupgradedTables := make(map[ID]*TableDescriptor)
	changed := false
	// No need to process mutations, since only descriptors written on a 19.2
	// cluster (after finalizing the upgrade) have foreign key mutations.
	for i := range desc.Indexes {
		newChanged, err := maybeUpgradeForeignKeyRepOnIndex(
			ctx, protoGetter, otherUnupgradedTables, desc, &desc.Indexes[i], skipFKsWithNoMatchingTable,
		)
		if err != nil {
			return false, err
		}
		changed = changed || newChanged
	}
	newChanged, err := maybeUpgradeForeignKeyRepOnIndex(
		ctx, protoGetter, otherUnupgradedTables, desc, &desc.PrimaryIndex, skipFKsWithNoMatchingTable,
	)
	if err != nil {
		return false, err
	}
	changed = changed || newChanged

	return changed, nil
}

// maybeUpgradeForeignKeyRepOnIndex is the meat of the previous function - it
// tries to upgrade a particular index's foreign key representation.
func maybeUpgradeForeignKeyRepOnIndex(
	ctx context.Context,
	protoGetter protoGetter,
	otherUnupgradedTables map[ID]*TableDescriptor,
	desc *TableDescriptor,
	idx *IndexDescriptor,
	skipFKsWithNoMatchingTable bool,
) (bool, error) {
	var changed bool
	if idx.ForeignKey.IsSet() {
		ref := &idx.ForeignKey
		if _, ok := otherUnupgradedTables[ref.Table]; !ok {
			tbl, err := getTableDescFromIDRaw(ctx, protoGetter, ref.Table)
			if err != nil {
				if err == ErrDescriptorNotFound && skipFKsWithNoMatchingTable {
					// Ignore this FK and keep going.
				} else {
					return false, err
				}
			} else {
				otherUnupgradedTables[ref.Table] = tbl
			}
		}
		if tbl, ok := otherUnupgradedTables[ref.Table]; ok {
			referencedIndex, err := tbl.FindIndexByID(ref.Index)
			if err != nil {
				return false, err
			}
			numCols := ref.SharedPrefixLen
			outFK := ForeignKeyConstraint{
				OriginTableID:         desc.ID,
				OriginColumnIDs:       idx.ColumnIDs[:numCols],
				ReferencedTableID:     ref.Table,
				ReferencedColumnIDs:   referencedIndex.ColumnIDs[:numCols],
				Name:                  ref.Name,
				Validity:              ref.Validity,
				OnDelete:              ref.OnDelete,
				OnUpdate:              ref.OnUpdate,
				Match:                 ref.Match,
				LegacyOriginIndex:     idx.ID,
				LegacyReferencedIndex: referencedIndex.ID,
			}
			desc.OutboundFKs = append(desc.OutboundFKs, outFK)
		}
		changed = true
		idx.ForeignKey = ForeignKeyReference{}
	}

	for refIdx := range idx.ReferencedBy {
		ref := &(idx.ReferencedBy[refIdx])
		if _, ok := otherUnupgradedTables[ref.Table]; !ok {
			tbl, err := getTableDescFromIDRaw(ctx, protoGetter, ref.Table)
			if err != nil {
				if err == ErrDescriptorNotFound && skipFKsWithNoMatchingTable {
					// Ignore this FK and keep going.
				} else {
					return false, err
				}
			} else {
				otherUnupgradedTables[ref.Table] = tbl
			}
		}

		if otherTable, ok := otherUnupgradedTables[ref.Table]; ok {
			originIndex, err := otherTable.FindIndexByID(ref.Index)
			if err != nil {
				return false, err
			}
			// There are two cases. Either the other table is old (not upgraded yet),
			// or it's new (already upgraded).
			var inFK ForeignKeyConstraint
			if !originIndex.ForeignKey.IsSet() {
				// The other table has either no foreign key, indicating a corrupt
				// reference, or the other table was upgraded. Assume the second for now.
				// If we also find no matching reference in the new-style foreign keys,
				// that indicates a corrupt reference.
				var forwardFK *ForeignKeyConstraint
				for i := range otherTable.OutboundFKs {
					otherFK := &otherTable.OutboundFKs[i]
					// To find a match, we need to compare the reference's table id and
					// index id, which are the only two available fields on backreferences
					// in the old representation. Note that we have to compare the index id
					// to the matching new forward reference's LegacyOriginIndex field,
					// which although marked as Legacy, is populated every time we create
					// a new-style fk during the duration of 19.2.
					if otherFK.ReferencedTableID == desc.ID &&
						otherFK.LegacyOriginIndex == ref.Index {
						// Found a match.
						forwardFK = otherFK
						break
					}
				}
				if forwardFK == nil {
					// Corrupted foreign key - there was no forward reference for the back
					// reference.
					return false, errors.AssertionFailedf(
						"error finding foreign key on table %d for backref %+v",
						otherTable.ID, ref)
				}
				inFK = ForeignKeyConstraint{
					OriginTableID:         ref.Table,
					OriginColumnIDs:       forwardFK.OriginColumnIDs,
					ReferencedTableID:     desc.ID,
					ReferencedColumnIDs:   forwardFK.ReferencedColumnIDs,
					Name:                  forwardFK.Name,
					Validity:              forwardFK.Validity,
					OnDelete:              forwardFK.OnDelete,
					OnUpdate:              forwardFK.OnUpdate,
					Match:                 forwardFK.Match,
					LegacyOriginIndex:     originIndex.ID,
					LegacyReferencedIndex: idx.ID,
				}
			} else {
				// We have an old (not upgraded yet) table, with a matching forward
				// foreign key.
				numCols := originIndex.ForeignKey.SharedPrefixLen
				inFK = ForeignKeyConstraint{
					OriginTableID:         ref.Table,
					OriginColumnIDs:       originIndex.ColumnIDs[:numCols],
					ReferencedTableID:     desc.ID,
					ReferencedColumnIDs:   idx.ColumnIDs[:numCols],
					Name:                  originIndex.ForeignKey.Name,
					Validity:              originIndex.ForeignKey.Validity,
					OnDelete:              originIndex.ForeignKey.OnDelete,
					OnUpdate:              originIndex.ForeignKey.OnUpdate,
					Match:                 originIndex.ForeignKey.Match,
					LegacyOriginIndex:     originIndex.ID,
					LegacyReferencedIndex: idx.ID,
				}
			}
			desc.InboundFKs = append(desc.InboundFKs, inFK)
		}
		changed = true
	}
	idx.ReferencedBy = nil
	return changed, nil
}

// maybeUpgradeFormatVersion transforms the TableDescriptor to the latest
// FormatVersion (if it's not already there) and returns true if any changes
// were made.
// This method should be called through MaybeFillInDescriptor, not directly.
func (desc *TableDescriptor) maybeUpgradeFormatVersion() bool {
	if desc.FormatVersion >= InterleavedFormatVersion {
		return false
	}
	desc.maybeUpgradeToFamilyFormatVersion()
	desc.FormatVersion = InterleavedFormatVersion
	return true
}

func (desc *TableDescriptor) maybeUpgradeToFamilyFormatVersion() bool {
	if desc.FormatVersion >= FamilyFormatVersion {
		return false
	}

	primaryIndexColumnIds := make(map[ColumnID]struct{}, len(desc.PrimaryIndex.ColumnIDs))
	for _, colID := range desc.PrimaryIndex.ColumnIDs {
		primaryIndexColumnIds[colID] = struct{}{}
	}

	desc.Families = []ColumnFamilyDescriptor{
		{ID: 0, Name: "primary"},
	}
	desc.NextFamilyID = desc.Families[0].ID + 1
	addFamilyForCol := func(col *ColumnDescriptor) {
		if _, ok := primaryIndexColumnIds[col.ID]; ok {
			desc.Families[0].ColumnNames = append(desc.Families[0].ColumnNames, col.Name)
			desc.Families[0].ColumnIDs = append(desc.Families[0].ColumnIDs, col.ID)
			return
		}
		colNames := []string{col.Name}
		family := ColumnFamilyDescriptor{
			ID:              FamilyID(col.ID),
			Name:            generatedFamilyName(FamilyID(col.ID), colNames),
			ColumnNames:     colNames,
			ColumnIDs:       []ColumnID{col.ID},
			DefaultColumnID: col.ID,
		}
		desc.Families = append(desc.Families, family)
		if family.ID >= desc.NextFamilyID {
			desc.NextFamilyID = family.ID + 1
		}
	}

	for i := range desc.Columns {
		addFamilyForCol(&desc.Columns[i])
	}
	for i := range desc.Mutations {
		m := &desc.Mutations[i]
		if c := m.GetColumn(); c != nil {
			addFamilyForCol(c)
		}
	}

	desc.FormatVersion = FamilyFormatVersion

	return true
}

// AllocateIDs allocates column, family, and index ids for any column, family,
// or index which has an ID of 0.
func (desc *MutableTableDescriptor) AllocateIDs() error {
	// Only physical tables can have / need a primary key.
	if desc.IsPhysicalTable() {
		if err := desc.ensurePrimaryKey(); err != nil {
			return err
		}
	}

	if desc.NextColumnID == 0 {
		desc.NextColumnID = 1
	}
	if desc.Version == 0 {
		desc.Version = 1
	}
	if desc.NextMutationID == InvalidMutationID {
		desc.NextMutationID = 1
	}

	columnNames := map[string]ColumnID{}
	fillColumnID := func(c *ColumnDescriptor) {
		columnID := c.ID
		if columnID == 0 {
			columnID = desc.NextColumnID
			desc.NextColumnID++
		}
		columnNames[c.Name] = columnID
		c.ID = columnID
	}
	for i := range desc.Columns {
		fillColumnID(&desc.Columns[i])
	}
	for _, m := range desc.Mutations {
		if c := m.GetColumn(); c != nil {
			fillColumnID(c)
		}
	}

	// Only physical tables can have / need indexes and column families.
	if desc.IsPhysicalTable() {
		if err := desc.allocateIndexIDs(columnNames); err != nil {
			return err
		}
		desc.allocateColumnFamilyIDs(columnNames)
	}

	// This is sort of ugly. If the descriptor does not have an ID, we hack one in
	// to pass the table ID check. We use a non-reserved ID, reserved ones being set
	// before AllocateIDs.
	savedID := desc.ID
	if desc.ID == 0 {
		desc.ID = keys.MinUserDescID
	}
	err := desc.ValidateTable()
	desc.ID = savedID
	return err
}

func (desc *MutableTableDescriptor) ensurePrimaryKey() error {
	if len(desc.PrimaryIndex.ColumnNames) == 0 && desc.IsPhysicalTable() {
		// Ensure a Primary Key exists.
		s := "unique_rowid()"
		col := &ColumnDescriptor{
			Name:        "rowid",
			Type:        *types.Int,
			DefaultExpr: &s,
			Hidden:      true,
			Nullable:    false,
		}
		desc.AddColumn(col)
		idx := IndexDescriptor{
			Unique:           true,
			ColumnNames:      []string{col.Name},
			ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
		}
		if err := desc.AddIndex(idx, true); err != nil {
			return err
		}
	}
	return nil
}

// HasCompositeKeyEncoding returns true if key columns of the given kind can
// have a composite encoding. For such types, it can be decided on a
// case-by-base basis whether a given Datum requires the composite encoding.
//
// As an example of a composite encoding, collated string key columns are
// encoded partly as a key and partly as a value. The key part is the collation
// key, so that different strings that collate equal cannot both be used as
// keys. The value part is the usual UTF-8 encoding of the string, stored so
// that it can be recovered later for inspection/display.
func HasCompositeKeyEncoding(semanticType types.Family) bool {
	switch semanticType {
	case types.CollatedStringFamily,
		types.FloatFamily,
		types.DecimalFamily:
		return true
	}
	return false
}

// DatumTypeHasCompositeKeyEncoding is a version of HasCompositeKeyEncoding
// which works on datum types.
func DatumTypeHasCompositeKeyEncoding(typ *types.T) bool {
	return HasCompositeKeyEncoding(typ.Family())
}

// MustBeValueEncoded returns true if columns of the given kind can only be value
// encoded.
func MustBeValueEncoded(semanticType types.Family) bool {
	return semanticType == types.ArrayFamily ||
		semanticType == types.JsonFamily ||
		semanticType == types.TupleFamily
}

// HasOldStoredColumns returns whether the index has stored columns in the old
// format (data encoded the same way as if they were in an implicit column).
func (desc *IndexDescriptor) HasOldStoredColumns() bool {
	return len(desc.ExtraColumnIDs) > 0 && len(desc.StoreColumnIDs) < len(desc.StoreColumnNames)
}

func (desc *MutableTableDescriptor) allocateIndexIDs(columnNames map[string]ColumnID) error {
	if desc.NextIndexID == 0 {
		desc.NextIndexID = 1
	}

	// Keep track of unnamed indexes.
	anonymousIndexes := make([]*IndexDescriptor, 0, len(desc.Indexes)+len(desc.Mutations))

	// Create a slice of modifiable index descriptors.
	indexes := make([]*IndexDescriptor, 0, 1+len(desc.Indexes)+len(desc.Mutations))
	indexes = append(indexes, &desc.PrimaryIndex)
	collectIndexes := func(index *IndexDescriptor) {
		if len(index.Name) == 0 {
			anonymousIndexes = append(anonymousIndexes, index)
		}
		indexes = append(indexes, index)
	}
	for i := range desc.Indexes {
		collectIndexes(&desc.Indexes[i])
	}
	for _, m := range desc.Mutations {
		if index := m.GetIndex(); index != nil {
			collectIndexes(index)
		}
	}

	for _, index := range anonymousIndexes {
		index.allocateName(desc)
	}

	isCompositeColumn := make(map[ColumnID]struct{})
	for i := range desc.Columns {
		col := &desc.Columns[i]
		if HasCompositeKeyEncoding(col.Type.Family()) {
			isCompositeColumn[col.ID] = struct{}{}
		}
	}

	// Populate IDs.
	for _, index := range indexes {
		if index.ID != 0 {
			// This index has already been populated. Nothing to do.
			continue
		}
		index.ID = desc.NextIndexID
		desc.NextIndexID++

		for j, colName := range index.ColumnNames {
			if len(index.ColumnIDs) <= j {
				index.ColumnIDs = append(index.ColumnIDs, 0)
			}
			if index.ColumnIDs[j] == 0 {
				index.ColumnIDs[j] = columnNames[colName]
			}
		}

		if index != &desc.PrimaryIndex {
			indexHasOldStoredColumns := index.HasOldStoredColumns()
			// Need to clear ExtraColumnIDs and StoreColumnIDs because they are used
			// by ContainsColumnID.
			index.ExtraColumnIDs = nil
			index.StoreColumnIDs = nil
			var extraColumnIDs []ColumnID
			for _, primaryColID := range desc.PrimaryIndex.ColumnIDs {
				if !index.ContainsColumnID(primaryColID) {
					extraColumnIDs = append(extraColumnIDs, primaryColID)
				}
			}
			index.ExtraColumnIDs = extraColumnIDs

			for _, colName := range index.StoreColumnNames {
				col, _, err := desc.FindColumnByName(tree.Name(colName))
				if err != nil {
					return err
				}
				if desc.PrimaryIndex.ContainsColumnID(col.ID) {
					// If the primary index contains a stored column, we don't need to
					// store it - it's already part of the index.
					err = pgerror.Newf(
						pgcode.DuplicateColumn, "index %q already contains column %q", index.Name, col.Name)
					err = errors.WithDetailf(err, "column %q is part of the primary index and therefore implicit in all indexes", col.Name)
					return err
				}
				if index.ContainsColumnID(col.ID) {
					return pgerror.Newf(
						pgcode.DuplicateColumn,
						"index %q already contains column %q", index.Name, col.Name)
				}
				if indexHasOldStoredColumns {
					index.ExtraColumnIDs = append(index.ExtraColumnIDs, col.ID)
				} else {
					index.StoreColumnIDs = append(index.StoreColumnIDs, col.ID)
				}
			}
		}

		index.CompositeColumnIDs = nil
		for _, colID := range index.ColumnIDs {
			if _, ok := isCompositeColumn[colID]; ok {
				index.CompositeColumnIDs = append(index.CompositeColumnIDs, colID)
			}
		}
		for _, colID := range index.ExtraColumnIDs {
			if _, ok := isCompositeColumn[colID]; ok {
				index.CompositeColumnIDs = append(index.CompositeColumnIDs, colID)
			}
		}
	}
	return nil
}

func (desc *MutableTableDescriptor) allocateColumnFamilyIDs(columnNames map[string]ColumnID) {
	if desc.NextFamilyID == 0 {
		if len(desc.Families) == 0 {
			desc.Families = []ColumnFamilyDescriptor{
				{ID: 0, Name: "primary"},
			}
		}
		desc.NextFamilyID = 1
	}

	columnsInFamilies := make(map[ColumnID]struct{}, len(desc.Columns))
	for i := range desc.Families {
		family := &desc.Families[i]
		if family.ID == 0 && i != 0 {
			family.ID = desc.NextFamilyID
			desc.NextFamilyID++
		}

		for j, colName := range family.ColumnNames {
			if len(family.ColumnIDs) <= j {
				family.ColumnIDs = append(family.ColumnIDs, 0)
			}
			if family.ColumnIDs[j] == 0 {
				family.ColumnIDs[j] = columnNames[colName]
			}
			columnsInFamilies[family.ColumnIDs[j]] = struct{}{}
		}

		desc.Families[i] = *family
	}

	primaryIndexColIDs := make(map[ColumnID]struct{}, len(desc.PrimaryIndex.ColumnIDs))
	for _, colID := range desc.PrimaryIndex.ColumnIDs {
		primaryIndexColIDs[colID] = struct{}{}
	}

	ensureColumnInFamily := func(col *ColumnDescriptor) {
		if _, ok := columnsInFamilies[col.ID]; ok {
			return
		}
		if _, ok := primaryIndexColIDs[col.ID]; ok {
			// Primary index columns are required to be assigned to family 0.
			desc.Families[0].ColumnNames = append(desc.Families[0].ColumnNames, col.Name)
			desc.Families[0].ColumnIDs = append(desc.Families[0].ColumnIDs, col.ID)
			return
		}
		var familyID FamilyID
		if desc.ParentID == keys.SystemDatabaseID {
			// TODO(dan): This assigns families such that the encoding is exactly the
			// same as before column families. It's used for all system tables because
			// reads of them don't go through the normal sql layer, which is where the
			// knowledge of families lives. Fix that and remove this workaround.
			familyID = FamilyID(col.ID)
			desc.Families = append(desc.Families, ColumnFamilyDescriptor{
				ID:          familyID,
				ColumnNames: []string{col.Name},
				ColumnIDs:   []ColumnID{col.ID},
			})
		} else {
			idx, ok := fitColumnToFamily(desc, *col)
			if !ok {
				idx = len(desc.Families)
				desc.Families = append(desc.Families, ColumnFamilyDescriptor{
					ID:          desc.NextFamilyID,
					ColumnNames: []string{},
					ColumnIDs:   []ColumnID{},
				})
			}
			familyID = desc.Families[idx].ID
			desc.Families[idx].ColumnNames = append(desc.Families[idx].ColumnNames, col.Name)
			desc.Families[idx].ColumnIDs = append(desc.Families[idx].ColumnIDs, col.ID)
		}
		if familyID >= desc.NextFamilyID {
			desc.NextFamilyID = familyID + 1
		}
	}
	for i := range desc.Columns {
		ensureColumnInFamily(&desc.Columns[i])
	}
	for _, m := range desc.Mutations {
		if c := m.GetColumn(); c != nil {
			ensureColumnInFamily(c)
		}
	}

	for i := range desc.Families {
		family := &desc.Families[i]
		if len(family.Name) == 0 {
			family.Name = generatedFamilyName(family.ID, family.ColumnNames)
		}

		if family.DefaultColumnID == 0 {
			defaultColumnID := ColumnID(0)
			for _, colID := range family.ColumnIDs {
				if _, ok := primaryIndexColIDs[colID]; !ok {
					if defaultColumnID == 0 {
						defaultColumnID = colID
					} else {
						defaultColumnID = ColumnID(0)
						break
					}
				}
			}
			family.DefaultColumnID = defaultColumnID
		}

		desc.Families[i] = *family
	}
}

// MaybeIncrementVersion increments the version of a descriptor if necessary.
func (desc *MutableTableDescriptor) MaybeIncrementVersion(
	ctx context.Context, txn *client.Txn, settings *cluster.Settings,
) error {
	// Already incremented, no-op.
	if desc.Version == desc.ClusterVersion.Version+1 {
		return nil
	}
	desc.Version++

	// Before 19.2 we needed to observe the transaction CommitTimestamp to ensure
	// that ModificationTime reflected the timestamp at which the transaction
	// committed. Starting in 19.2 we use a zero-valued ModificationTime when
	// incrementing the version and then upon reading use the MVCC timestamp to
	// populate the ModificationTime.
	//
	// TODO(ajwerner): remove this check in 20.1.
	var modTime hlc.Timestamp
	if !cluster.Version.IsActive(ctx, settings, cluster.VersionTableDescModificationTimeFromMVCC) {
		modTime = txn.CommitTimestamp()
	}
	desc.ModificationTime = modTime
	log.Infof(ctx, "publish: descID=%d (%s) version=%d mtime=%s",
		desc.ID, desc.Name, desc.Version, modTime.GoTime())
	return nil
}

// Validate validates that the table descriptor is well formed. Checks include
// both single table and cross table invariants.
func (desc *TableDescriptor) Validate(ctx context.Context, txn *client.Txn) error {
	err := desc.ValidateTable()
	if err != nil {
		return err
	}
	if desc.Dropped() {
		return nil
	}
	return desc.validateCrossReferences(ctx, txn)
}

// validateCrossReferences validates that each reference to another table is
// resolvable and that the necessary back references exist.
func (desc *TableDescriptor) validateCrossReferences(ctx context.Context, txn *client.Txn) error {
	// Check that parent DB exists.
	{
		res, err := txn.Get(ctx, MakeDescMetadataKey(desc.ParentID))
		if err != nil {
			return err
		}
		if !res.Exists() {
			return errors.AssertionFailedf("parentID %d does not exist", errors.Safe(desc.ParentID))
		}
	}

	tablesByID := map[ID]*TableDescriptor{desc.ID: desc}
	getTable := func(id ID) (*TableDescriptor, error) {
		if table, ok := tablesByID[id]; ok {
			return table, nil
		}
		table, err := GetTableDescFromID(ctx, txn, id)
		if err != nil {
			return nil, err
		}
		tablesByID[id] = table
		return table, nil
	}

	findTargetIndex := func(tableID ID, indexID IndexID) (*TableDescriptor, *IndexDescriptor, error) {
		targetTable, err := getTable(tableID)
		if err != nil {
			return nil, nil, errors.Wrapf(err,
				"missing table=%d index=%d", errors.Safe(tableID), errors.Safe(indexID))
		}
		targetIndex, err := targetTable.FindIndexByID(indexID)
		if err != nil {
			return nil, nil, errors.Wrapf(err,
				"missing table=%s index=%d", targetTable.Name, errors.Safe(indexID))
		}
		return targetTable, targetIndex, nil
	}

	// Check foreign keys.
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		referencedTable, err := getTable(fk.ReferencedTableID)
		if err != nil {
			return errors.Wrapf(err,
				"invalid foreign key: missing table=%d", errors.Safe(fk.ReferencedTableID))
		}
		found := false
		for i := range referencedTable.InboundFKs {
			backref := &referencedTable.InboundFKs[i]
			if backref.OriginTableID == desc.ID && backref.Name == fk.Name {
				found = true
				break
			}
		}
		if !found {
			return errors.AssertionFailedf("missing fk back reference %q to %q from %q",
				fk.Name, desc.Name, referencedTable.Name)
		}
	}
	for i := range desc.InboundFKs {
		backref := &desc.InboundFKs[i]
		originTable, err := getTable(backref.OriginTableID)
		if err != nil {
			return errors.Wrapf(err,
				"invalid foreign key backreference: missing table=%d", errors.Safe(backref.OriginTableID))
		}
		found := false
		for i := range originTable.OutboundFKs {
			fk := &originTable.OutboundFKs[i]
			if fk.ReferencedTableID == desc.ID && fk.Name == backref.Name {
				found = true
				break
			}
		}
		if !found {
			return errors.AssertionFailedf("missing fk forward reference %q to %q from %q",
				backref.Name, desc.Name, originTable.Name)
		}
	}

	for _, index := range desc.AllNonDropIndexes() {
		// Check interleaves.
		if len(index.Interleave.Ancestors) > 0 {
			// Only check the most recent ancestor, the rest of them don't point
			// back.
			ancestor := index.Interleave.Ancestors[len(index.Interleave.Ancestors)-1]
			targetTable, targetIndex, err := findTargetIndex(ancestor.TableID, ancestor.IndexID)
			if err != nil {
				return errors.Wrapf(err, "invalid interleave")
			}
			found := false
			for _, backref := range targetIndex.InterleavedBy {
				if backref.Table == desc.ID && backref.Index == index.ID {
					found = true
					break
				}
			}
			if !found {
				return errors.AssertionFailedf(
					"missing interleave back reference to %q@%q from %q@%q",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
		}
		interleaveBackrefs := make(map[ForeignKeyReference]struct{})
		for _, backref := range index.InterleavedBy {
			if _, ok := interleaveBackrefs[backref]; ok {
				return errors.AssertionFailedf("duplicated interleave backreference %+v", backref)
			}
			interleaveBackrefs[backref] = struct{}{}
			targetTable, err := getTable(backref.Table)
			if err != nil {
				return errors.Wrapf(err,
					"invalid interleave backreference table=%d index=%d",
					backref.Table, backref.Index)
			}
			targetIndex, err := targetTable.FindIndexByID(backref.Index)
			if err != nil {
				return errors.Wrapf(err,
					"invalid interleave backreference table=%s index=%d",
					targetTable.Name, backref.Index)
			}
			if len(targetIndex.Interleave.Ancestors) == 0 {
				return errors.AssertionFailedf(
					"broken interleave backward reference from %q@%q to %q@%q",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
			// The last ancestor is required to be a backreference.
			ancestor := targetIndex.Interleave.Ancestors[len(targetIndex.Interleave.Ancestors)-1]
			if ancestor.TableID != desc.ID || ancestor.IndexID != index.ID {
				return errors.AssertionFailedf(
					"broken interleave backward reference from %q@%q to %q@%q",
					desc.Name, index.Name, targetTable.Name, targetIndex.Name)
			}
		}
	}
	// TODO(dan): Also validate SharedPrefixLen in the interleaves.
	return nil
}

// ValidateTable validates that the table descriptor is well formed. Checks
// include validating the table, column and index names, verifying that column
// names and index names are unique and verifying that column IDs and index IDs
// are consistent. Use Validate to validate that cross-table references are
// correct.
// If version is supplied, the descriptor is checked for version incompatibilities.
func (desc *TableDescriptor) ValidateTable() error {
	if err := validateName(desc.Name, "table"); err != nil {
		return err
	}
	if desc.ID == 0 {
		return errors.AssertionFailedf("invalid table ID %d", errors.Safe(desc.ID))
	}

	// TODO(dt, nathan): virtual descs don't validate (missing privs, PK, etc).
	if desc.IsVirtualTable() {
		return nil
	}

	if desc.IsSequence() {
		return nil
	}

	// ParentID is the ID of the database holding this table.
	// It is often < ID, except when a table gets moved across databases.
	if desc.ParentID == 0 {
		return errors.AssertionFailedf("invalid parent ID %d", errors.Safe(desc.ParentID))
	}

	// We maintain forward compatibility, so if you see this error message with a
	// version older that what this client supports, then there's a
	// MaybeFillInDescriptor missing from some codepath.
	if v := desc.GetFormatVersion(); v != FamilyFormatVersion && v != InterleavedFormatVersion {
		// TODO(dan): We're currently switching from FamilyFormatVersion to
		// InterleavedFormatVersion. After a beta is released with this dual version
		// support, then:
		// - Upgrade the bidirectional reference version to that beta
		// - Start constructing all TableDescriptors with InterleavedFormatVersion
		// - Change maybeUpgradeFormatVersion to output InterleavedFormatVersion
		// - Change this check to only allow InterleavedFormatVersion
		return errors.AssertionFailedf(
			"table %q is encoded using using version %d, but this client only supports version %d and %d",
			desc.Name, errors.Safe(desc.GetFormatVersion()),
			errors.Safe(FamilyFormatVersion), errors.Safe(InterleavedFormatVersion))
	}

	if len(desc.Columns) == 0 {
		return ErrMissingColumns
	}

	if err := desc.CheckUniqueConstraints(); err != nil {
		return err
	}

	columnNames := make(map[string]ColumnID, len(desc.Columns))
	columnIDs := make(map[ColumnID]string, len(desc.Columns))
	for _, column := range desc.AllNonDropColumns() {
		if err := validateName(column.Name, "column"); err != nil {
			return err
		}
		if column.ID == 0 {
			return errors.AssertionFailedf("invalid column ID %d", errors.Safe(column.ID))
		}

		if _, ok := columnNames[column.Name]; ok {
			for i := range desc.Columns {
				if desc.Columns[i].Name == column.Name {
					return fmt.Errorf("duplicate column name: %q", column.Name)
				}
			}
			return fmt.Errorf("duplicate: column %q in the middle of being added, not yet public", column.Name)
		}
		columnNames[column.Name] = column.ID

		if other, ok := columnIDs[column.ID]; ok {
			return fmt.Errorf("column %q duplicate ID of column %q: %d",
				column.Name, other, column.ID)
		}
		columnIDs[column.ID] = column.Name

		if column.ID >= desc.NextColumnID {
			return errors.AssertionFailedf("column %q invalid ID (%d) >= next column ID (%d)",
				column.Name, errors.Safe(column.ID), errors.Safe(desc.NextColumnID))
		}
	}

	for _, m := range desc.Mutations {
		unSetEnums := m.State == DescriptorMutation_UNKNOWN || m.Direction == DescriptorMutation_NONE
		switch desc := m.Descriptor_.(type) {
		case *DescriptorMutation_Column:
			col := desc.Column
			if unSetEnums {
				return errors.AssertionFailedf(
					"mutation in state %s, direction %s, col %q, id %v",
					errors.Safe(m.State), errors.Safe(m.Direction), col.Name, errors.Safe(col.ID))
			}
			columnIDs[col.ID] = col.Name
		case *DescriptorMutation_Index:
			if unSetEnums {
				idx := desc.Index
				return errors.AssertionFailedf(
					"mutation in state %s, direction %s, index %s, id %v",
					errors.Safe(m.State), errors.Safe(m.Direction), idx.Name, errors.Safe(idx.ID))
			}
		case *DescriptorMutation_Constraint:
			if unSetEnums {
				return errors.AssertionFailedf(
					"mutation in state %s, direction %s, constraint %v",
					errors.Safe(m.State), errors.Safe(m.Direction), desc.Constraint.Name)
			}
		default:
			return errors.AssertionFailedf(
				"mutation in state %s, direction %s, and no column/index descriptor",
				errors.Safe(m.State), errors.Safe(m.Direction))
		}
	}

	// TODO(dt): Validate each column only appears at-most-once in any FKs.

	// Only validate column families and indexes if this is actually a table, not
	// if it's just a view.
	if desc.IsPhysicalTable() {
		colIDToFamilyID, err := desc.validateColumnFamilies(columnIDs)
		if err != nil {
			return err
		}
		if err := desc.validateTableIndexes(columnNames, colIDToFamilyID); err != nil {
			return err
		}
		if err := desc.validatePartitioning(); err != nil {
			return err
		}
	}

	// Fill in any incorrect privileges that may have been missed due to mixed-versions.
	// TODO(mberhault): remove this in 2.1 (maybe 2.2) when privilege-fixing migrations have been
	// run again and mixed-version clusters always write "good" descriptors.
	desc.Privileges.MaybeFixPrivileges(desc.GetID())

	// Validate the privilege descriptor.
	return desc.Privileges.Validate(desc.GetID())
}

func (desc *TableDescriptor) validateColumnFamilies(
	columnIDs map[ColumnID]string,
) (map[ColumnID]FamilyID, error) {
	if len(desc.Families) < 1 {
		return nil, fmt.Errorf("at least 1 column family must be specified")
	}
	if desc.Families[0].ID != FamilyID(0) {
		return nil, fmt.Errorf("the 0th family must have ID 0")
	}

	familyNames := map[string]struct{}{}
	familyIDs := map[FamilyID]string{}
	colIDToFamilyID := map[ColumnID]FamilyID{}
	for i := range desc.Families {
		family := &desc.Families[i]
		if err := validateName(family.Name, "family"); err != nil {
			return nil, err
		}

		if _, ok := familyNames[family.Name]; ok {
			return nil, fmt.Errorf("duplicate family name: %q", family.Name)
		}
		familyNames[family.Name] = struct{}{}

		if other, ok := familyIDs[family.ID]; ok {
			return nil, fmt.Errorf("family %q duplicate ID of family %q: %d",
				family.Name, other, family.ID)
		}
		familyIDs[family.ID] = family.Name

		if family.ID >= desc.NextFamilyID {
			return nil, fmt.Errorf("family %q invalid family ID (%d) > next family ID (%d)",
				family.Name, family.ID, desc.NextFamilyID)
		}

		if len(family.ColumnIDs) != len(family.ColumnNames) {
			return nil, fmt.Errorf("mismatched column ID size (%d) and name size (%d)",
				len(family.ColumnIDs), len(family.ColumnNames))
		}

		for i, colID := range family.ColumnIDs {
			name, ok := columnIDs[colID]
			if !ok {
				return nil, fmt.Errorf("family %q contains unknown column \"%d\"", family.Name, colID)
			}
			if name != family.ColumnNames[i] {
				return nil, fmt.Errorf("family %q column %d should have name %q, but found name %q",
					family.Name, colID, name, family.ColumnNames[i])
			}
		}

		for _, colID := range family.ColumnIDs {
			if famID, ok := colIDToFamilyID[colID]; ok {
				return nil, fmt.Errorf("column %d is in both family %d and %d", colID, famID, family.ID)
			}
			colIDToFamilyID[colID] = family.ID
		}
	}
	for colID := range columnIDs {
		if _, ok := colIDToFamilyID[colID]; !ok {
			return nil, fmt.Errorf("column %d is not in any column family", colID)
		}
	}
	return colIDToFamilyID, nil
}

// validateTableIndexes validates that indexes are well formed. Checks include
// validating the columns involved in the index, verifying the index names and
// IDs are unique, and the family of the primary key is 0. This does not check
// if indexes are unique (i.e. same set of columns, direction, and uniqueness)
// as there are practical uses for them.
func (desc *TableDescriptor) validateTableIndexes(
	columnNames map[string]ColumnID, colIDToFamilyID map[ColumnID]FamilyID,
) error {
	if len(desc.PrimaryIndex.ColumnIDs) == 0 {
		return ErrMissingPrimaryKey
	}

	indexNames := map[string]struct{}{}
	indexIDs := map[IndexID]string{}
	for _, index := range desc.AllNonDropIndexes() {
		if err := validateName(index.Name, "index"); err != nil {
			return err
		}
		if index.ID == 0 {
			return fmt.Errorf("invalid index ID %d", index.ID)
		}

		if _, ok := indexNames[index.Name]; ok {
			for i := range desc.Indexes {
				if desc.Indexes[i].Name == index.Name {
					return fmt.Errorf("duplicate index name: %q", index.Name)
				}
			}
			return fmt.Errorf("duplicate: index %q in the middle of being added, not yet public", index.Name)
		}
		indexNames[index.Name] = struct{}{}

		if other, ok := indexIDs[index.ID]; ok {
			return fmt.Errorf("index %q duplicate ID of index %q: %d",
				index.Name, other, index.ID)
		}
		indexIDs[index.ID] = index.Name

		if index.ID >= desc.NextIndexID {
			return fmt.Errorf("index %q invalid index ID (%d) > next index ID (%d)",
				index.Name, index.ID, desc.NextIndexID)
		}

		if len(index.ColumnIDs) != len(index.ColumnNames) {
			return fmt.Errorf("mismatched column IDs (%d) and names (%d)",
				len(index.ColumnIDs), len(index.ColumnNames))
		}
		if len(index.ColumnIDs) != len(index.ColumnDirections) {
			return fmt.Errorf("mismatched column IDs (%d) and directions (%d)",
				len(index.ColumnIDs), len(index.ColumnDirections))
		}

		if len(index.ColumnIDs) == 0 {
			return fmt.Errorf("index %q must contain at least 1 column", index.Name)
		}

		validateIndexDup := make(map[ColumnID]struct{})
		for i, name := range index.ColumnNames {
			colID, ok := columnNames[name]
			if !ok {
				return fmt.Errorf("index %q contains unknown column %q", index.Name, name)
			}
			if colID != index.ColumnIDs[i] {
				return fmt.Errorf("index %q column %q should have ID %d, but found ID %d",
					index.Name, name, colID, index.ColumnIDs[i])
			}
			if _, ok := validateIndexDup[colID]; ok {
				return fmt.Errorf("index %q contains duplicate column %q", index.Name, name)
			}
			validateIndexDup[colID] = struct{}{}
		}
	}

	for _, colID := range desc.PrimaryIndex.ColumnIDs {
		famID, ok := colIDToFamilyID[colID]
		if !ok || famID != FamilyID(0) {
			return fmt.Errorf("primary key column %d is not in column family 0", colID)
		}
	}

	return nil
}

// PrimaryKeyString returns the pretty-printed primary key declaration for a
// table descriptor.
func (desc *TableDescriptor) PrimaryKeyString() string {
	return fmt.Sprintf("PRIMARY KEY (%s)",
		desc.PrimaryIndex.ColNamesString(),
	)
}

// validatePartitioningDescriptor validates that a PartitioningDescriptor, which
// may represent a subpartition, is well-formed. Checks include validating the
// index-level uniqueness of all partition names, validating that the encoded
// tuples match the corresponding column types, and that range partitions are
// stored sorted by upper bound. colOffset is non-zero for subpartitions and
// indicates how many index columns to skip over.
func (desc *TableDescriptor) validatePartitioningDescriptor(
	a *DatumAlloc,
	idxDesc *IndexDescriptor,
	partDesc *PartitioningDescriptor,
	colOffset int,
	partitionNames map[string]string,
) error {
	if partDesc.NumColumns == 0 {
		return nil
	}

	// TODO(dan): The sqlccl.GenerateSubzoneSpans logic is easier if we disallow
	// setting zone configs on indexes that are interleaved into another index.
	// InterleavedBy is fine, so using the root of the interleave hierarchy will
	// work. It is expected that this is sufficient for real-world use cases.
	// Revisit this restriction if that expectation is wrong.
	if len(idxDesc.Interleave.Ancestors) > 0 {
		return errors.Errorf("cannot set a zone config for interleaved index %s; "+
			"set it on the root of the interleaved hierarchy instead", idxDesc.Name)
	}

	// We don't need real prefixes in the DecodePartitionTuple calls because we're
	// only using it to look for collisions and the prefix would be the same for
	// all of them. Faking them out with DNull allows us to make O(list partition)
	// calls to DecodePartitionTuple instead of O(list partition entry).
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	if len(partDesc.List) == 0 && len(partDesc.Range) == 0 {
		return fmt.Errorf("at least one of LIST or RANGE partitioning must be used")
	}
	if len(partDesc.List) > 0 && len(partDesc.Range) > 0 {
		return fmt.Errorf("only one LIST or RANGE partitioning may used")
	}

	checkName := func(name string) error {
		if len(name) == 0 {
			return fmt.Errorf("PARTITION name must be non-empty")
		}
		if indexName, exists := partitionNames[name]; exists {
			if indexName == idxDesc.Name {
				return fmt.Errorf("PARTITION %s: name must be unique (used twice in index %q)",
					name, indexName)
			}
		}
		partitionNames[name] = idxDesc.Name
		return nil
	}

	if len(partDesc.List) > 0 {
		listValues := make(map[string]struct{}, len(partDesc.List))
		for _, p := range partDesc.List {
			if err := checkName(p.Name); err != nil {
				return err
			}

			if len(p.Values) == 0 {
				return fmt.Errorf("PARTITION %s: must contain values", p.Name)
			}
			// NB: key encoding is used to check uniqueness because it has
			// to match the behavior of the value when indexed.
			for _, valueEncBuf := range p.Values {
				tuple, keyPrefix, err := DecodePartitionTuple(
					a, desc, idxDesc, partDesc, valueEncBuf, fakePrefixDatums)
				if err != nil {
					return fmt.Errorf("PARTITION %s: %v", p.Name, err)
				}
				if _, exists := listValues[string(keyPrefix)]; exists {
					return fmt.Errorf("%s cannot be present in more than one partition", tuple)
				}
				listValues[string(keyPrefix)] = struct{}{}
			}

			newColOffset := colOffset + int(partDesc.NumColumns)
			if err := desc.validatePartitioningDescriptor(
				a, idxDesc, &p.Subpartitioning, newColOffset, partitionNames,
			); err != nil {
				return err
			}
		}
	}

	if len(partDesc.Range) > 0 {
		tree := interval.NewTree(interval.ExclusiveOverlapper)
		for _, p := range partDesc.Range {
			if err := checkName(p.Name); err != nil {
				return err
			}

			// NB: key encoding is used to check uniqueness because it has to match
			// the behavior of the value when indexed.
			fromDatums, fromKey, err := DecodePartitionTuple(
				a, desc, idxDesc, partDesc, p.FromInclusive, fakePrefixDatums)
			if err != nil {
				return fmt.Errorf("PARTITION %s: %v", p.Name, err)
			}
			toDatums, toKey, err := DecodePartitionTuple(
				a, desc, idxDesc, partDesc, p.ToExclusive, fakePrefixDatums)
			if err != nil {
				return fmt.Errorf("PARTITION %s: %v", p.Name, err)
			}
			pi := partitionInterval{p.Name, fromKey, toKey}
			if overlaps := tree.Get(pi.Range()); len(overlaps) > 0 {
				return fmt.Errorf("partitions %s and %s overlap",
					overlaps[0].(partitionInterval).name, p.Name)
			}
			if err := tree.Insert(pi, false /* fast */); err == interval.ErrEmptyRange {
				return fmt.Errorf("PARTITION %s: empty range: lower bound %s is equal to upper bound %s",
					p.Name, fromDatums, toDatums)
			} else if err == interval.ErrInvertedRange {
				return fmt.Errorf("PARTITION %s: empty range: lower bound %s is greater than upper bound %s",
					p.Name, fromDatums, toDatums)
			} else if err != nil {
				return errors.Wrapf(err, "PARTITION %s", p.Name)
			}
		}
	}

	return nil
}

// FindIndexesWithPartition returns all IndexDescriptors (potentially including
// the primary index) which have a partition with the given name.
func (desc *TableDescriptor) FindIndexesWithPartition(name string) []*IndexDescriptor {
	var indexes []*IndexDescriptor
	for _, idx := range desc.AllNonDropIndexes() {
		if idx.FindPartitionByName(name) != nil {
			indexes = append(indexes, idx)
		}
	}
	return indexes
}

// validatePartitioning validates that any PartitioningDescriptors contained in
// table indexes are well-formed. See validatePartitioningDesc for details.
func (desc *TableDescriptor) validatePartitioning() error {
	partitionNames := make(map[string]string)

	a := &DatumAlloc{}
	return desc.ForeachNonDropIndex(func(idxDesc *IndexDescriptor) error {
		return desc.validatePartitioningDescriptor(
			a, idxDesc, &idxDesc.Partitioning, 0 /* colOffset */, partitionNames,
		)
	})
}

// FamilyHeuristicTargetBytes is the target total byte size of columns that the
// current heuristic will assign to a family.
const FamilyHeuristicTargetBytes = 256

// fitColumnToFamily attempts to fit a new column into the existing column
// families. If the heuristics find a fit, true is returned along with the
// index of the selected family. Otherwise, false is returned and the column
// should be put in a new family.
//
// Current heuristics:
// - Put all columns in family 0.
func fitColumnToFamily(desc *MutableTableDescriptor, col ColumnDescriptor) (int, bool) {
	// Fewer column families means fewer kv entries, which is generally faster.
	// On the other hand, an update to any column in a family requires that they
	// all are read and rewritten, so large (or numerous) columns that are not
	// updated at the same time as other columns in the family make things
	// slower.
	//
	// The initial heuristic used for family assignment tried to pack
	// fixed-width columns into families up to a certain size and would put any
	// variable-width column into its own family. This was conservative to
	// guarantee that we avoid the worst-case behavior of a very large immutable
	// blob in the same family as frequently updated columns.
	//
	// However, our initial customers have revealed that this is backward.
	// Repeatedly, they have recreated existing schemas without any tuning and
	// found lackluster performance. Each of these has turned out better as a
	// single family (sometimes 100% faster or more), the most aggressive tuning
	// possible.
	//
	// Further, as the WideTable benchmark shows, even the worst-case isn't that
	// bad (33% slower with an immutable 1MB blob, which is the upper limit of
	// what we'd recommend for column size regardless of families). This
	// situation also appears less frequent than we feared.
	//
	// The result is that we put all columns in one family and require the user
	// to manually specify family assignments when this is incorrect.
	return 0, true
}

// ColumnTypeIsIndexable returns whether the type t is valid as an indexed column.
func ColumnTypeIsIndexable(t *types.T) bool {
	return !MustBeValueEncoded(t.Family())
}

// ColumnTypeIsInvertedIndexable returns whether the type t is valid to be indexed
// using an inverted index.
func ColumnTypeIsInvertedIndexable(t *types.T) bool {
	return t.Family() == types.JsonFamily
}

func notIndexableError(cols []ColumnDescriptor, inverted bool) error {
	if len(cols) == 0 {
		return nil
	}
	var msg string
	var typInfo string
	if len(cols) == 1 {
		col := &cols[0]
		msg = "column %s is of type %s and thus is not indexable"
		if inverted {
			msg += " with an inverted index"
		}
		typInfo = col.Type.DebugString()
		msg = fmt.Sprintf(msg, col.Name, col.Type.Name())
	} else {
		msg = "the following columns are not indexable due to their type: "
		for i := range cols {
			col := &cols[i]
			msg += fmt.Sprintf("%s (type %s)", col.Name, col.Type.Name())
			typInfo += col.Type.DebugString()
			if i != len(cols)-1 {
				msg += ", "
				typInfo += ","
			}
		}
	}
	return unimplemented.NewWithIssueDetailf(35730, typInfo, msg)
}

func checkColumnsValidForIndex(tableDesc *MutableTableDescriptor, indexColNames []string) error {
	invalidColumns := make([]ColumnDescriptor, 0, len(indexColNames))
	for _, indexCol := range indexColNames {
		for _, col := range tableDesc.AllNonDropColumns() {
			if col.Name == indexCol {
				if !ColumnTypeIsIndexable(&col.Type) {
					invalidColumns = append(invalidColumns, col)
				}
			}
		}
	}
	if len(invalidColumns) > 0 {
		return notIndexableError(invalidColumns, false)
	}
	return nil
}

func checkColumnsValidForInvertedIndex(
	tableDesc *MutableTableDescriptor, indexColNames []string,
) error {
	if len((indexColNames)) > 1 {
		return errors.New("indexing more than one column with an inverted index is not supported")
	}
	invalidColumns := make([]ColumnDescriptor, 0, len(indexColNames))
	for _, indexCol := range indexColNames {
		for _, col := range tableDesc.AllNonDropColumns() {
			if col.Name == indexCol {
				if !ColumnTypeIsInvertedIndexable(&col.Type) {
					invalidColumns = append(invalidColumns, col)
				}
			}
		}
	}
	if len(invalidColumns) > 0 {
		return notIndexableError(invalidColumns, true)
	}
	return nil
}

// AddColumn adds a column to the table.
func (desc *MutableTableDescriptor) AddColumn(col *ColumnDescriptor) {
	desc.Columns = append(desc.Columns, *col)
}

// AddFamily adds a family to the table.
func (desc *MutableTableDescriptor) AddFamily(fam ColumnFamilyDescriptor) {
	desc.Families = append(desc.Families, fam)
}

// AddIndex adds an index to the table.
func (desc *MutableTableDescriptor) AddIndex(idx IndexDescriptor, primary bool) error {
	if idx.Type == IndexDescriptor_FORWARD {
		if err := checkColumnsValidForIndex(desc, idx.ColumnNames); err != nil {
			return err
		}

		if primary {
			// PrimaryIndex is unset.
			if desc.PrimaryIndex.Name == "" {
				if idx.Name == "" {
					// Only override the index name if it hasn't been set by the user.
					idx.Name = PrimaryKeyIndexName
				}
				desc.PrimaryIndex = idx
			} else {
				return fmt.Errorf("multiple primary keys for table %q are not allowed", desc.Name)
			}
		} else {
			desc.Indexes = append(desc.Indexes, idx)
		}

	} else {
		if err := checkColumnsValidForInvertedIndex(desc, idx.ColumnNames); err != nil {
			return err
		}
		desc.Indexes = append(desc.Indexes, idx)
	}

	return nil
}

// AddColumnToFamilyMaybeCreate adds the specified column to the specified
// family. If it doesn't exist and create is true, creates it. If it does exist
// adds it unless "strict" create (`true` for create but `false` for
// ifNotExists) is specified.
//
// AllocateIDs must be called before the TableDescriptor will be valid.
func (desc *MutableTableDescriptor) AddColumnToFamilyMaybeCreate(
	col string, family string, create bool, ifNotExists bool,
) error {
	idx := int(-1)
	if len(family) > 0 {
		for i := range desc.Families {
			if desc.Families[i].Name == family {
				idx = i
				break
			}
		}
	}

	if idx == -1 {
		if create {
			// NB: When AllocateIDs encounters an empty `Name`, it'll generate one.
			desc.AddFamily(ColumnFamilyDescriptor{Name: family, ColumnNames: []string{col}})
			return nil
		}
		return fmt.Errorf("unknown family %q", family)
	}

	if create && !ifNotExists {
		return fmt.Errorf("family %q already exists", family)
	}
	desc.Families[idx].ColumnNames = append(desc.Families[idx].ColumnNames, col)
	return nil
}

// RemoveColumnFromFamily removes a colID from the family it's assigned to.
func (desc *MutableTableDescriptor) RemoveColumnFromFamily(colID ColumnID) {
	for i := range desc.Families {
		for j, c := range desc.Families[i].ColumnIDs {
			if c == colID {
				desc.Families[i].ColumnIDs = append(
					desc.Families[i].ColumnIDs[:j], desc.Families[i].ColumnIDs[j+1:]...)
				desc.Families[i].ColumnNames = append(
					desc.Families[i].ColumnNames[:j], desc.Families[i].ColumnNames[j+1:]...)
				if len(desc.Families[i].ColumnIDs) == 0 {
					desc.Families = append(desc.Families[:i], desc.Families[i+1:]...)
				}
				return
			}
		}
	}
}

// RenameColumnDescriptor updates all references to a column name in
// a table descriptor including indexes and families.
func (desc *MutableTableDescriptor) RenameColumnDescriptor(
	column *ColumnDescriptor, newColName string,
) {
	colID := column.ID
	column.Name = newColName

	for i := range desc.Families {
		for j := range desc.Families[i].ColumnIDs {
			if desc.Families[i].ColumnIDs[j] == colID {
				desc.Families[i].ColumnNames[j] = newColName
			}
		}
	}

	renameColumnInIndex := func(idx *IndexDescriptor) {
		for i, id := range idx.ColumnIDs {
			if id == colID {
				idx.ColumnNames[i] = newColName
			}
		}
		for i, id := range idx.StoreColumnIDs {
			if id == colID {
				idx.StoreColumnNames[i] = newColName
			}
		}
	}
	renameColumnInIndex(&desc.PrimaryIndex)
	for i := range desc.Indexes {
		renameColumnInIndex(&desc.Indexes[i])
	}
	for _, m := range desc.Mutations {
		if idx := m.GetIndex(); idx != nil {
			renameColumnInIndex(idx)
		}
	}
}

// FindActiveColumnsByNames finds all requested columns (in the requested order)
// or returns an error.
func (desc *TableDescriptor) FindActiveColumnsByNames(
	names tree.NameList,
) ([]ColumnDescriptor, error) {
	cols := make([]ColumnDescriptor, len(names))
	for i := range names {
		c, err := desc.FindActiveColumnByName(string(names[i]))
		if err != nil {
			return nil, err
		}
		cols[i] = *c
	}
	return cols, nil
}

// FindColumnByName finds the column with the specified name. It returns
// an active column or a column from the mutation list. It returns true
// if the column is being dropped.
func (desc *TableDescriptor) FindColumnByName(name tree.Name) (*ColumnDescriptor, bool, error) {
	for i := range desc.Columns {
		c := &desc.Columns[i]
		if c.Name == string(name) {
			return c, false, nil
		}
	}
	for i := range desc.Mutations {
		m := &desc.Mutations[i]
		if c := m.GetColumn(); c != nil {
			if c.Name == string(name) {
				return c, m.Direction == DescriptorMutation_DROP, nil
			}
		}
	}
	return nil, false, NewUndefinedColumnError(string(name))
}

// ColumnIdxMap returns a map from Column ID to the ordinal position of that
// column.
func (desc *TableDescriptor) ColumnIdxMap() map[ColumnID]int {
	return desc.ColumnIdxMapWithMutations(false)
}

// ColumnIdxMapWithMutations returns a map from Column ID to the ordinal
// position of that column, optionally including mutation columns if the input
// bool is true.
func (desc *TableDescriptor) ColumnIdxMapWithMutations(mutations bool) map[ColumnID]int {
	colIdxMap := make(map[ColumnID]int, len(desc.Columns))
	for i := range desc.Columns {
		id := desc.Columns[i].ID
		colIdxMap[id] = i
	}
	if mutations {
		idx := len(desc.Columns)
		for i := range desc.Mutations {
			col := desc.Mutations[i].GetColumn()
			if col != nil {
				colIdxMap[col.ID] = idx
				idx++
			}
		}
	}
	return colIdxMap
}

// FindActiveColumnByName finds an active column with the specified name.
func (desc *TableDescriptor) FindActiveColumnByName(name string) (*ColumnDescriptor, error) {
	for i := range desc.Columns {
		c := &desc.Columns[i]
		if c.Name == name {
			return c, nil
		}
	}
	return nil, NewUndefinedColumnError(name)
}

// FindColumnByID finds the column with specified ID.
func (desc *TableDescriptor) FindColumnByID(id ColumnID) (*ColumnDescriptor, error) {
	for i := range desc.Columns {
		c := &desc.Columns[i]
		if c.ID == id {
			return c, nil
		}
	}
	for i := range desc.Mutations {
		if c := desc.Mutations[i].GetColumn(); c != nil {
			if c.ID == id {
				return c, nil
			}
		}
	}
	return nil, fmt.Errorf("column-id \"%d\" does not exist", id)
}

// FindActiveColumnByID finds the active column with specified ID.
func (desc *TableDescriptor) FindActiveColumnByID(id ColumnID) (*ColumnDescriptor, error) {
	for i := range desc.Columns {
		c := &desc.Columns[i]
		if c.ID == id {
			return c, nil
		}
	}
	return nil, fmt.Errorf("column-id \"%d\" does not exist", id)
}

// FindReadableColumnByID finds the readable column with specified ID. The
// column may be undergoing a schema change and is marked nullable regardless
// of its configuration. It returns true if the column is undergoing a
// schema change.
func (desc *ImmutableTableDescriptor) FindReadableColumnByID(
	id ColumnID,
) (*ColumnDescriptor, bool, error) {
	for i := range desc.ReadableColumns {
		c := &desc.ReadableColumns[i]
		if c.ID == id {
			return c, i >= len(desc.Columns), nil
		}
	}
	return nil, false, fmt.Errorf("column-id \"%d\" does not exist", id)
}

// FindFamilyByID finds the family with specified ID.
func (desc *TableDescriptor) FindFamilyByID(id FamilyID) (*ColumnFamilyDescriptor, error) {
	for i := range desc.Families {
		family := &desc.Families[i]
		if family.ID == id {
			return family, nil
		}
	}
	return nil, fmt.Errorf("family-id \"%d\" does not exist", id)
}

// FindIndexByName finds the index with the specified name in the active
// list or the mutations list. It returns true if the index is being dropped.
func (desc *TableDescriptor) FindIndexByName(name string) (*IndexDescriptor, bool, error) {
	if desc.IsPhysicalTable() && desc.PrimaryIndex.Name == name {
		return &desc.PrimaryIndex, false, nil
	}
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		if idx.Name == name {
			return idx, false, nil
		}
	}
	for _, m := range desc.Mutations {
		if idx := m.GetIndex(); idx != nil {
			if idx.Name == name {
				return idx, m.Direction == DescriptorMutation_DROP, nil
			}
		}
	}
	return nil, false, fmt.Errorf("index %q does not exist", name)
}

// FindCheckByName finds the check constraint with the specified name.
func (desc *TableDescriptor) FindCheckByName(
	name string,
) (*TableDescriptor_CheckConstraint, error) {
	for _, c := range desc.Checks {
		if c.Name == name {
			return c, nil
		}
	}
	return nil, fmt.Errorf("check %q does not exist", name)
}

// NamesForColumnIDs returns the names for the given column ids, or an error
// if one or more column ids was missing. Note - this allocates! It's not for
// hot path code.
func (desc *TableDescriptor) NamesForColumnIDs(ids ColumnIDs) ([]string, error) {
	names := make([]string, len(ids))
	for i, id := range ids {
		col, err := desc.FindColumnByID(id)
		if err != nil {
			return nil, err
		}
		names[i] = col.Name
	}
	return names, nil
}

// RenameIndexDescriptor renames an index descriptor.
func (desc *MutableTableDescriptor) RenameIndexDescriptor(
	index *IndexDescriptor, name string,
) error {
	id := index.ID
	if id == desc.PrimaryIndex.ID {
		desc.PrimaryIndex.Name = name
		return nil
	}
	for i := range desc.Indexes {
		if desc.Indexes[i].ID == id {
			desc.Indexes[i].Name = name
			return nil
		}
	}
	for _, m := range desc.Mutations {
		if idx := m.GetIndex(); idx != nil && idx.ID == id {
			idx.Name = name
			return nil
		}
	}
	return fmt.Errorf("index with id = %d does not exist", id)
}

// DropConstraint drops a constraint, either by removing it from the table
// descriptor or by queuing a mutation for a schema change.
func (desc *MutableTableDescriptor) DropConstraint(
	ctx context.Context,
	name string,
	detail ConstraintDetail,
	removeFK func(*MutableTableDescriptor, *ForeignKeyConstraint) error,
	settings *cluster.Settings,
) error {
	switch detail.Kind {
	case ConstraintTypePK:
		return unimplemented.NewWithIssueDetailf(19141, "drop-constraint-pk", "cannot drop primary key")

	case ConstraintTypeUnique:
		return unimplemented.NewWithIssueDetailf(42840, "drop-constraint-unique",
			"cannot drop UNIQUE constraint %q using ALTER TABLE DROP CONSTRAINT, use DROP INDEX CASCADE instead",
			tree.ErrNameStringP(&detail.Index.Name))

	case ConstraintTypeCheck:
		if detail.CheckConstraint.Validity == ConstraintValidity_Validating {
			return unimplemented.NewWithIssueDetailf(42844, "drop-constraint-check-mutation",
				"constraint %q in the middle of being added, try again later", name)
		}
		if detail.CheckConstraint.Validity == ConstraintValidity_Dropping {
			return unimplemented.NewWithIssueDetailf(42844, "drop-constraint-check-mutation",
				"constraint %q in the middle of being dropped", name)
		}
		for i, c := range desc.Checks {
			if c.Name == name {
				// If the constraint is unvalidated, there's no assumption that it must
				// hold for all rows, so it can be dropped immediately.
				// We also drop the constraint immediately instead of queuing a mutation
				// unless the cluster is fully upgraded to 19.2, for backward
				// compatibility.
				if detail.CheckConstraint.Validity == ConstraintValidity_Unvalidated {
					desc.Checks = append(desc.Checks[:i], desc.Checks[i+1:]...)
					return nil
				}
				c.Validity = ConstraintValidity_Dropping
				desc.AddCheckMutation(c, DescriptorMutation_DROP)
				return nil
			}
		}
		return errors.Errorf("constraint %q not found on table %q", name, desc.Name)

	case ConstraintTypeFK:
		if detail.FK.Validity == ConstraintValidity_Validating {
			return unimplemented.NewWithIssueDetailf(42844,
				"constraint %q in the middle of being added, try again later", name)
		}
		if detail.FK.Validity == ConstraintValidity_Dropping {
			return unimplemented.NewWithIssueDetailf(42844,
				"drop-constraint-fk-mutation",
				"constraint %q in the middle of being dropped", name)
		}
		// Search through the descriptor's foreign key constraints and delete the
		// one that we're supposed to be deleting.
		for i := range desc.OutboundFKs {
			ref := &desc.OutboundFKs[i]
			if ref.Name == name {
				// If the constraint is unvalidated, there's no assumption that it must
				// hold for all rows, so it can be dropped immediately.
				if detail.FK.Validity == ConstraintValidity_Unvalidated {
					// Remove the backreference.
					if err := removeFK(desc, detail.FK); err != nil {
						return err
					}
					desc.OutboundFKs = append(desc.OutboundFKs[:i], desc.OutboundFKs[i+1:]...)
					return nil
				}
				ref.Validity = ConstraintValidity_Dropping
				desc.AddForeignKeyMutation(ref, DescriptorMutation_DROP)
				return nil
			}
		}
		return errors.AssertionFailedf("constraint %q not found on table %q", name, desc.Name)

	default:
		return unimplemented.Newf(fmt.Sprintf("drop-constraint-%s", detail.Kind),
			"constraint %q has unsupported type", tree.ErrNameString(name))
	}

}

// RenameConstraint renames a constraint.
func (desc *MutableTableDescriptor) RenameConstraint(
	detail ConstraintDetail,
	oldName, newName string,
	dependentViewRenameError func(string, ID) error,
	renameFK func(*MutableTableDescriptor, *ForeignKeyConstraint, string) error,
) error {
	switch detail.Kind {
	case ConstraintTypePK, ConstraintTypeUnique:
		for _, tableRef := range desc.DependedOnBy {
			if tableRef.IndexID != detail.Index.ID {
				continue
			}
			return dependentViewRenameError("index", tableRef.ID)
		}
		return desc.RenameIndexDescriptor(detail.Index, newName)

	case ConstraintTypeFK:
		if detail.FK.Validity == ConstraintValidity_Validating {
			return unimplemented.NewWithIssueDetailf(42844,
				"rename-constraint-fk-mutation",
				"constraint %q in the middle of being added, try again later",
				tree.ErrNameStringP(&detail.FK.Name))
		}
		// Update the name on the referenced table descriptor.
		if err := renameFK(desc, detail.FK, newName); err != nil {
			return err
		}
		// Update the name on this table descriptor.
		fk, err := desc.FindFKByName(detail.FK.Name)
		if err != nil {
			return err
		}
		fk.Name = newName
		return nil

	case ConstraintTypeCheck:
		if detail.CheckConstraint.Validity == ConstraintValidity_Validating {
			return unimplemented.NewWithIssueDetailf(42844,
				"rename-constraint-check-mutation",
				"constraint %q in the middle of being added, try again later",
				tree.ErrNameStringP(&detail.CheckConstraint.Name))
		}
		detail.CheckConstraint.Name = newName
		return nil

	default:
		return unimplemented.Newf(fmt.Sprintf("rename-constraint-%s", detail.Kind),
			"constraint %q has unsupported type", tree.ErrNameString(oldName))
	}
}

// FindIndexByID finds an index (active or inactive) with the specified ID.
// Must return a pointer to the IndexDescriptor in the TableDescriptor, so that
// callers can use returned values to modify the TableDesc.
func (desc *TableDescriptor) FindIndexByID(id IndexID) (*IndexDescriptor, error) {
	if desc.PrimaryIndex.ID == id {
		return &desc.PrimaryIndex, nil
	}
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		if idx.ID == id {
			return idx, nil
		}
	}
	for _, m := range desc.Mutations {
		if idx := m.GetIndex(); idx != nil && idx.ID == id {
			return idx, nil
		}
	}
	for _, m := range desc.GCMutations {
		if m.IndexID == id {
			return nil, ErrIndexGCMutationsList
		}
	}
	return nil, fmt.Errorf("index-id \"%d\" does not exist", id)
}

// FindIndexByIndexIdx returns an active index with the specified
// index's index which has a domain of [0, # of secondary indexes] and whether
// the index is a secondary index.
// The primary index has an index of 0 and the first secondary index
// (if it exists) has an index of 1.
func (desc *TableDescriptor) FindIndexByIndexIdx(
	indexIdx int,
) (index *IndexDescriptor, isSecondary bool, err error) {
	// indexIdx is 0 for the primary index, or 1 to <num-indexes> for a
	// secondary index.
	if indexIdx < 0 || indexIdx > len(desc.Indexes) {
		return nil, false, errors.Errorf("invalid indexIdx %d", indexIdx)
	}

	if indexIdx > 0 {
		return &desc.Indexes[indexIdx-1], true, nil
	}

	return &desc.PrimaryIndex, false, nil
}

// GetIndexMutationCapabilities returns:
// 1. Whether the index is a mutation
// 2. if so, is it in state DELETE_AND_WRITE_ONLY
func (desc *TableDescriptor) GetIndexMutationCapabilities(id IndexID) (bool, bool) {
	for _, mutation := range desc.Mutations {
		if mutationIndex := mutation.GetIndex(); mutationIndex != nil {
			if mutationIndex.ID == id {
				return true,
					mutation.State == DescriptorMutation_DELETE_AND_WRITE_ONLY
			}
		}
	}
	return false, false
}

// FindFKByName returns the FK constraint on the table with the given name.
// Must return a pointer to the FK in the TableDescriptor, so that
// callers can use returned values to modify the TableDesc.
func (desc *TableDescriptor) FindFKByName(name string) (*ForeignKeyConstraint, error) {
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		if fk.Name == name {
			return fk, nil
		}
	}
	return nil, fmt.Errorf("fk %q does not exist", name)
}

// FindFKForBackRef searches the table descriptor for the foreign key constraint
// that matches the supplied backref, which is present on the supplied table id.
// Our current restriction that each column in a table can be on the referencing
// side of at most one FK relationship means that there's no possibility of
// ambiguity when finding the forward FK reference for a given backreference,
// since ambiguity would require an identical list of referencing columns. This
// would continue to hold even if we were to lift that restriction (see #38850), as long as
// it were still prohibited to create multiple foreign key relationships with
// exactly identical lists of referenced and referencing columns, which we think
// is a reasonable restriction (even though Postgres does allow doing this).
func (desc *TableDescriptor) FindFKForBackRef(
	referencedTableID ID, backref *ForeignKeyConstraint,
) (*ForeignKeyConstraint, error) {
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		if fk.ReferencedTableID == referencedTableID && fk.Name == backref.Name {
			if fk.LegacyReferencedIndex != backref.LegacyReferencedIndex ||
				fk.LegacyOriginIndex != backref.LegacyOriginIndex {
				return nil, errors.AssertionFailedf("fk found for backref %s but pinned indexes were different: "+
					"%d != %d || %d != %d", fk.Name, fk.LegacyOriginIndex, backref.LegacyOriginIndex,
					fk.LegacyReferencedIndex, backref.LegacyReferencedIndex)
			}

			return fk, nil
		}
	}
	return nil, errors.AssertionFailedf("could not find fk for backref %v", backref)
}

// IsInterleaved returns true if any part of this this table is interleaved with
// another table's data.
func (desc *TableDescriptor) IsInterleaved() bool {
	for _, index := range desc.AllNonDropIndexes() {
		if index.IsInterleaved() {
			return true
		}
	}
	return false
}

// MakeMutationComplete updates the descriptor upon completion of a mutation.
// There are three Validity types for the mutations:
// Validated   - The constraint has already been added and validated, should
//               never be the case for a validated constraint to enter this
//               method.
// Validating  - The constraint has already been added, and just needs to be
//               marked as validated.
// Unvalidated - The constraint has not yet been added, and needs to be added
//               for the first time.
func (desc *MutableTableDescriptor) MakeMutationComplete(m DescriptorMutation) error {
	switch m.Direction {
	case DescriptorMutation_ADD:
		switch t := m.Descriptor_.(type) {
		case *DescriptorMutation_Column:
			desc.AddColumn(t.Column)

		case *DescriptorMutation_Index:
			if err := desc.AddIndex(*t.Index, false); err != nil {
				return err
			}

		case *DescriptorMutation_Constraint:
			switch t.Constraint.ConstraintType {
			case ConstraintToUpdate_CHECK:
				switch t.Constraint.Check.Validity {
				case ConstraintValidity_Validating:
					// Constraint already added, just mark it as Validated
					for _, c := range desc.Checks {
						if c.Name == t.Constraint.Name {
							c.Validity = ConstraintValidity_Validated
							break
						}
					}
				case ConstraintValidity_Unvalidated:
					// add the constraint to the list of check constraints on the table
					// descriptor
					desc.Checks = append(desc.Checks, &t.Constraint.Check)
				default:
					return errors.AssertionFailedf("invalid constraint validity state: %d", t.Constraint.Check.Validity)
				}
			case ConstraintToUpdate_FOREIGN_KEY:
				switch t.Constraint.ForeignKey.Validity {
				case ConstraintValidity_Validating:
					// Constraint already added, just mark it as Validated
					for i := range desc.OutboundFKs {
						fk := &desc.OutboundFKs[i]
						if fk.Name == t.Constraint.Name {
							fk.Validity = ConstraintValidity_Validated
							break
						}
					}
				case ConstraintValidity_Unvalidated:
					// Takes care of adding the Foreign Key to the table index. Adding the
					// backreference to the referenced table index must be taken care of
					// in another call.
					// TODO (tyler): Combine both of these tasks in the same place.
					desc.OutboundFKs = append(desc.OutboundFKs, t.Constraint.ForeignKey)
				}
			case ConstraintToUpdate_NOT_NULL:
				// Remove the dummy check constraint that was in place during validation
				for i, c := range desc.Checks {
					if c.Name == t.Constraint.Check.Name {
						desc.Checks = append(desc.Checks[:i], desc.Checks[i+1:]...)
					}
				}
				col, err := desc.FindColumnByID(t.Constraint.NotNullColumn)
				if err != nil {
					return err
				}
				col.Nullable = false
			default:
				return errors.Errorf("unsupported constraint type: %d", t.Constraint.ConstraintType)
			}
		}

	case DescriptorMutation_DROP:
		switch t := m.Descriptor_.(type) {
		case *DescriptorMutation_Column:
			desc.RemoveColumnFromFamily(t.Column.ID)
		}
		// Nothing else to be done. The column/index was already removed from the
		// set of column/index descriptors at mutation creation time.
		// Constraints to be dropped are dropped before column/index backfills.
	}
	return nil
}

// AddCheckMutation adds a check constraint mutation to desc.Mutations.
func (desc *MutableTableDescriptor) AddCheckMutation(
	ck *TableDescriptor_CheckConstraint, direction DescriptorMutation_Direction,
) {
	m := DescriptorMutation{
		Descriptor_: &DescriptorMutation_Constraint{
			Constraint: &ConstraintToUpdate{
				ConstraintType: ConstraintToUpdate_CHECK, Name: ck.Name, Check: *ck,
			},
		},
		Direction: direction,
	}
	desc.addMutation(m)
}

// AddForeignKeyMutation adds a foreign key constraint mutation to desc.Mutations.
func (desc *MutableTableDescriptor) AddForeignKeyMutation(
	fk *ForeignKeyConstraint, direction DescriptorMutation_Direction,
) {
	m := DescriptorMutation{
		Descriptor_: &DescriptorMutation_Constraint{
			Constraint: &ConstraintToUpdate{
				ConstraintType: ConstraintToUpdate_FOREIGN_KEY,
				Name:           fk.Name,
				ForeignKey:     *fk,
			},
		},
		Direction: direction,
	}
	desc.addMutation(m)
}

// MakeNotNullCheckConstraint creates a dummy check constraint equivalent to a
// NOT NULL constraint on a column, so that NOT NULL constraints can be added
// and dropped correctly in the schema changer. This function mutates inuseNames
// to add the new constraint name.
func MakeNotNullCheckConstraint(
	colName string, colID ColumnID, inuseNames map[string]struct{}, validity ConstraintValidity,
) *TableDescriptor_CheckConstraint {
	name := fmt.Sprintf("%s_auto_not_null", colName)
	// If generated name isn't unique, attempt to add a number to the end to
	// get a unique name, as in generateNameForCheckConstraint().
	if _, ok := inuseNames[name]; ok {
		i := 1
		for {
			appended := fmt.Sprintf("%s%d", name, i)
			if _, ok := inuseNames[appended]; !ok {
				name = appended
				break
			}
			i++
		}
	}
	if inuseNames != nil {
		inuseNames[name] = struct{}{}
	}

	expr := &tree.ComparisonExpr{
		Operator: tree.IsDistinctFrom,
		Left:     &tree.ColumnItem{ColumnName: tree.Name(colName)},
		Right:    tree.DNull,
	}

	return &TableDescriptor_CheckConstraint{
		Name:                name,
		Expr:                tree.Serialize(expr),
		Validity:            validity,
		ColumnIDs:           []ColumnID{colID},
		IsNonNullConstraint: true,
	}
}

// AddNotNullMutation adds a not null constraint mutation to desc.Mutations.
// Similarly to other schema elements, adding or dropping a non-null
// constraint requires a multi-state schema change, including a bulk validation
// step, before the Nullable flag can be set to false on the descriptor. This is
// done by adding a dummy check constraint of the form "x IS NOT NULL" that is
// treated like other check constraints being added, until the completion of the
// schema change, at which the check constraint is deleted. This function
// mutates inuseNames to add the new constraint name.
func (desc *MutableTableDescriptor) AddNotNullMutation(
	ck *TableDescriptor_CheckConstraint, direction DescriptorMutation_Direction,
) {
	m := DescriptorMutation{
		Descriptor_: &DescriptorMutation_Constraint{
			Constraint: &ConstraintToUpdate{
				ConstraintType: ConstraintToUpdate_NOT_NULL,
				Name:           ck.Name,
				NotNullColumn:  ck.ColumnIDs[0],
				Check:          *ck,
			},
		},
		Direction: direction,
	}
	desc.addMutation(m)
}

// AddColumnMutation adds a column mutation to desc.Mutations. Callers must take
// care not to further mutate the column descriptor, since this method retains
// a pointer to it.
func (desc *MutableTableDescriptor) AddColumnMutation(
	c *ColumnDescriptor, direction DescriptorMutation_Direction,
) {
	m := DescriptorMutation{Descriptor_: &DescriptorMutation_Column{Column: c}, Direction: direction}
	desc.addMutation(m)
}

// AddIndexMutation adds an index mutation to desc.Mutations.
func (desc *MutableTableDescriptor) AddIndexMutation(
	idx *IndexDescriptor, direction DescriptorMutation_Direction,
) error {

	switch idx.Type {
	case IndexDescriptor_FORWARD:
		if err := checkColumnsValidForIndex(desc, idx.ColumnNames); err != nil {
			return err
		}
	case IndexDescriptor_INVERTED:
		if err := checkColumnsValidForInvertedIndex(desc, idx.ColumnNames); err != nil {
			return err
		}
	}

	m := DescriptorMutation{Descriptor_: &DescriptorMutation_Index{Index: idx}, Direction: direction}
	desc.addMutation(m)
	return nil
}

func (desc *MutableTableDescriptor) addMutation(m DescriptorMutation) {
	switch m.Direction {
	case DescriptorMutation_ADD:
		m.State = DescriptorMutation_DELETE_ONLY

	case DescriptorMutation_DROP:
		m.State = DescriptorMutation_DELETE_AND_WRITE_ONLY
	}
	// For tables created in the same transaction the next mutation ID will
	// not have been allocated and the added mutation will use an invalid ID.
	// This is fine because the mutation will be processed immediately.
	m.MutationID = desc.ClusterVersion.NextMutationID
	desc.NextMutationID = desc.ClusterVersion.NextMutationID + 1
	desc.Mutations = append(desc.Mutations, m)
}

// IgnoreConstraints is used in MakeFirstMutationPublic to indicate that the
// table descriptor returned should not include newly added constraints, which
// is useful when passing the returned table descriptor to be used in
// validating constraints to be added.
const IgnoreConstraints = false

// IncludeConstraints is used in MakeFirstMutationPublic to indicate that the
// table descriptor returned should include newly added constraints.
const IncludeConstraints = true

// MakeFirstMutationPublic creates a MutableTableDescriptor from the
// ImmutableTableDescriptor by making the first mutation public.
// This is super valuable when trying to run SQL over data associated
// with a schema mutation that is still not yet public: Data validation,
// error reporting.
func (desc *ImmutableTableDescriptor) MakeFirstMutationPublic(
	includeConstraints bool,
) (*MutableTableDescriptor, error) {
	// Clone the ImmutableTable descriptor because we want to create an Immutable one.
	table := NewMutableExistingTableDescriptor(*protoutil.Clone(desc.TableDesc()).(*TableDescriptor))
	mutationID := desc.Mutations[0].MutationID
	i := 0
	for _, mutation := range desc.Mutations {
		if mutation.MutationID != mutationID {
			// Mutations are applied in a FIFO order. Only apply the first set
			// of mutations if they have the mutation ID we're looking for.
			break
		}
		if includeConstraints || mutation.GetConstraint() == nil {
			if err := table.MakeMutationComplete(mutation); err != nil {
				return nil, err
			}
		}
		i++
	}
	table.Mutations = table.Mutations[i:]
	table.Version++
	return table, nil
}

// ColumnNeedsBackfill returns true if adding the given column requires a
// backfill (dropping a column always requires a backfill).
func ColumnNeedsBackfill(desc *ColumnDescriptor) bool {
	if desc.HasNullDefault() {
		return false
	}
	return desc.HasDefault() || !desc.Nullable || desc.IsComputed()
}

// HasColumnBackfillMutation returns whether the table has any queued column
// mutations that require a backfill.
func (desc *TableDescriptor) HasColumnBackfillMutation() bool {
	for _, m := range desc.Mutations {
		col := m.GetColumn()
		if col == nil {
			// Index backfills don't affect changefeeds.
			continue
		}
		// It's unfortunate that there's no one method we can call to check if a
		// mutation will be a backfill or not, but this logic was extracted from
		// backfill.go.
		if m.Direction == DescriptorMutation_DROP || ColumnNeedsBackfill(col) {
			return true
		}
	}
	return false
}

// GoingOffline returns true if the table is being dropped or is importing.
func (desc *TableDescriptor) GoingOffline() bool {
	return desc.Dropped() || desc.State == TableDescriptor_OFFLINE
}

// Dropped returns true if the table is being dropped.
func (desc *TableDescriptor) Dropped() bool {
	return desc.State == TableDescriptor_DROP
}

// Adding returns true if the table is being added.
func (desc *TableDescriptor) Adding() bool {
	return desc.State == TableDescriptor_ADD
}

// IsNewTable returns true if the table was created in the current
// transaction.
func (desc *MutableTableDescriptor) IsNewTable() bool {
	return desc.ClusterVersion.ID == InvalidID
}

// HasDrainingNames returns true if a draining name exists.
func (desc *TableDescriptor) HasDrainingNames() bool {
	return len(desc.DrainingNames) > 0
}

// VisibleColumns returns all non hidden columns.
func (desc *TableDescriptor) VisibleColumns() []ColumnDescriptor {
	var cols []ColumnDescriptor
	for i := range desc.Columns {
		col := &desc.Columns[i]
		if !col.Hidden {
			cols = append(cols, *col)
		}
	}
	return cols
}

// ColumnTypes returns the types of all columns.
func (desc *TableDescriptor) ColumnTypes() []types.T {
	return desc.ColumnTypesWithMutations(false)
}

// ColumnTypesWithMutations returns the types of all columns, optionally
// including mutation columns, which will be returned if the input bool is true.
func (desc *TableDescriptor) ColumnTypesWithMutations(mutations bool) []types.T {
	nCols := len(desc.Columns)
	if mutations {
		nCols += len(desc.Mutations)
	}
	types := make([]types.T, 0, nCols)
	for i := range desc.Columns {
		types = append(types, desc.Columns[i].Type)
	}
	if mutations {
		for i := range desc.Mutations {
			if col := desc.Mutations[i].GetColumn(); col != nil {
				types = append(types, col.Type)
			}
		}
	}
	return types
}

// ColumnsSelectors generates Select expressions for cols.
func ColumnsSelectors(cols []ColumnDescriptor) tree.SelectExprs {
	exprs := make(tree.SelectExprs, len(cols))
	colItems := make([]tree.ColumnItem, len(cols))
	for i, col := range cols {
		colItems[i].ColumnName = tree.Name(col.Name)
		exprs[i].Expr = &colItems[i]
	}
	return exprs
}

// SetID implements the DescriptorProto interface.
func (desc *DatabaseDescriptor) SetID(id ID) {
	desc.ID = id
}

// TypeName returns the plain type of this descriptor.
func (desc *DatabaseDescriptor) TypeName() string {
	return "database"
}

// SetName implements the DescriptorProto interface.
func (desc *DatabaseDescriptor) SetName(name string) {
	desc.Name = name
}

// Validate validates that the database descriptor is well formed.
// Checks include validate the database name, and verifying that there
// is at least one read and write user.
func (desc *DatabaseDescriptor) Validate() error {
	if err := validateName(desc.Name, "descriptor"); err != nil {
		return err
	}
	if desc.ID == 0 {
		return fmt.Errorf("invalid database ID %d", desc.ID)
	}

	// Fill in any incorrect privileges that may have been missed due to mixed-versions.
	// TODO(mberhault): remove this in 2.1 (maybe 2.2) when privilege-fixing migrations have been
	// run again and mixed-version clusters always write "good" descriptors.
	desc.Privileges.MaybeFixPrivileges(desc.GetID())

	// Validate the privilege descriptor.
	return desc.Privileges.Validate(desc.GetID())
}

// GetID returns the ID of the descriptor.
func (desc *Descriptor) GetID() ID {
	switch t := desc.Union.(type) {
	case *Descriptor_Table:
		return t.Table.ID
	case *Descriptor_Database:
		return t.Database.ID
	default:
		return 0
	}
}

// GetName returns the Name of the descriptor.
func (desc *Descriptor) GetName() string {
	switch t := desc.Union.(type) {
	case *Descriptor_Table:
		return t.Table.Name
	case *Descriptor_Database:
		return t.Database.Name
	default:
		return ""
	}
}

// Table is a replacement for GetTable() which seeks to ensure that clients
// which unmarshal Descriptor structs properly set the ModificationTime on
// tables based on the MVCC timestamp at which the descriptor was read.
//
// A linter should ensure that GetTable() is not called.
func (desc *Descriptor) Table(ts hlc.Timestamp) *TableDescriptor {
	t := desc.GetTable()
	if t != nil {
		t.maybeSetTimeFromMVCCTimestamp(ts)
	}
	return t
}

// maybeSetTimeFromMVCCTimestamp will update ModificationTime and possible
// CreateAsOfTime with the provided timestamp. If desc.ModificationTime is
// non-zero it must be the case that it is not after the provided timestamp.
//
// When table descriptor versions are incremented they are written with a
// zero-valued ModificationTime. This is done to avoid the need to observe
// the commit timestamp for the writing transaction which would prevent
// pushes. This method is used in the read path to set the modification time
// based on the MVCC timestamp of row which contained this descriptor. If
// the ModificationTime is non-zero then we know that either this table
// descriptor was written by older version of cockroach which included the
// exact commit timestamp or it was re-written in which case it will include
// a timestamp which was set by this method.
//
// It is vital that users which read table descriptor values from the KV store
// call this method.
func (desc *TableDescriptor) maybeSetTimeFromMVCCTimestamp(ts hlc.Timestamp) {
	// CreateAsOfTime is used for CREATE TABLE ... AS ... and was introduced in
	// v19.1. In general it is not critical to set except for tables in the ADD
	// ADD state which were created from CTAS so we should not assert on its not
	// being set. It's not always sensical to set it from the passed MVCC
	// timestamp. However, starting in 19.2 the CreateAsOfTime and
	// ModificationTime fields are both unset for the first Version of a
	// TableDescriptor and the code relies on the value being set based on the
	// MVCC timestamp.
	if !ts.IsEmpty() &&
		desc.ModificationTime.IsEmpty() &&
		desc.CreateAsOfTime.IsEmpty() &&
		desc.Version == 1 {
		desc.CreateAsOfTime = ts
	}

	// Ensure that if the table is in the process of being added and relies on
	// CreateAsOfTime that it is now set.
	if desc.Adding() && desc.IsAs() && desc.CreateAsOfTime.IsEmpty() {
		log.Fatalf(context.TODO(), "table descriptor for %q (%d.%d) is in the "+
			"ADD state and was created with CREATE TABLE ... AS but does not have a "+
			"CreateAsOfTime set", desc.Name, desc.ParentID, desc.ID)
	}

	// Set the ModificationTime based on the passed ts if we should.
	// Table descriptors can be updated in place after their version has been
	// incremented (e.g. to include a schema change lease).
	// When this happens we permit the ModificationTime to be written explicitly
	// with the value that lives on the in-memory copy. That value should contain
	// a timestamp set by this method. Thus if the ModificationTime is set it
	// must not be after the MVCC timestamp we just read it at.
	if desc.ModificationTime.IsEmpty() && ts.IsEmpty() {
		log.Fatalf(context.TODO(), "read table descriptor for %q (%d.%d) without ModificationTime "+
			"with zero MVCC timestamp", desc.Name, desc.ParentID, desc.ID)
	} else if desc.ModificationTime.IsEmpty() {
		desc.ModificationTime = ts
	} else if !ts.IsEmpty() && ts.Less(desc.ModificationTime) {
		log.Fatalf(context.TODO(), "read table descriptor %q (%d.%d) which has a ModificationTime "+
			"after its MVCC timestamp: has %v, expected %v",
			desc.Name, desc.ParentID, desc.ID, desc.ModificationTime, ts)
	}
}

// IsSet returns whether or not the foreign key actually references a table.
func (f ForeignKeyReference) IsSet() bool {
	return f.Table != 0
}

// InvalidateFKConstraints sets all FK constraints to un-validated.
func (desc *TableDescriptor) InvalidateFKConstraints() {
	// We don't use GetConstraintInfo because we want to edit the passed desc.
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		fk.Validity = ConstraintValidity_Unvalidated
	}
}

// AllIndexSpans returns the Spans for each index in the table, including those
// being added in the mutations.
func (desc *TableDescriptor) AllIndexSpans() roachpb.Spans {
	var spans roachpb.Spans
	err := desc.ForeachNonDropIndex(func(index *IndexDescriptor) error {
		spans = append(spans, desc.IndexSpan(index.ID))
		return nil
	})
	if err != nil {
		panic(err)
	}
	return spans
}

// PrimaryIndexSpan returns the Span that corresponds to the entire primary
// index; can be used for a full table scan.
func (desc *TableDescriptor) PrimaryIndexSpan() roachpb.Span {
	return desc.IndexSpan(desc.PrimaryIndex.ID)
}

// IndexSpan returns the Span that corresponds to an entire index; can be used
// for a full index scan.
func (desc *TableDescriptor) IndexSpan(indexID IndexID) roachpb.Span {
	prefix := roachpb.Key(MakeIndexKeyPrefix(desc, indexID))
	return roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
}

// TableSpan returns the Span that corresponds to the entire table.
func (desc *TableDescriptor) TableSpan() roachpb.Span {
	prefix := roachpb.Key(keys.MakeTablePrefix(uint32(desc.ID)))
	return roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
}

// SQLString returns the SQL statement describing the column.
func (desc *ColumnDescriptor) SQLString() string {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.FormatNameP(&desc.Name)
	f.WriteByte(' ')
	f.WriteString(desc.Type.SQLString())
	if desc.Nullable {
		f.WriteString(" NULL")
	} else {
		f.WriteString(" NOT NULL")
	}
	if desc.DefaultExpr != nil {
		f.WriteString(" DEFAULT ")
		f.WriteString(*desc.DefaultExpr)
	}
	if desc.IsComputed() {
		f.WriteString(" AS (")
		f.WriteString(*desc.ComputeExpr)
		f.WriteString(") STORED")
	}
	return f.CloseAndGetString()
}

// ColumnsUsed returns the IDs of the columns used in the check constraint's
// expression. v2.0 binaries will populate this during table creation, but older
// binaries will not, in which case this needs to be computed when requested.
//
// TODO(nvanbenschoten): we can remove this in v2.1 and replace it with a sql
// migration to backfill all TableDescriptor_CheckConstraint.ColumnIDs slices.
// See #22322.
func (cc *TableDescriptor_CheckConstraint) ColumnsUsed(desc *TableDescriptor) ([]ColumnID, error) {
	if len(cc.ColumnIDs) > 0 {
		// Already populated.
		return cc.ColumnIDs, nil
	}

	parsed, err := parser.ParseExpr(cc.Expr)
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.Syntax,
			"could not parse check constraint %s", cc.Expr)
	}

	colIDsUsed := make(map[ColumnID]struct{})
	visitFn := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		if vBase, ok := expr.(tree.VarName); ok {
			v, err := vBase.NormalizeVarName()
			if err != nil {
				return false, nil, err
			}
			if c, ok := v.(*tree.ColumnItem); ok {
				col, dropped, err := desc.FindColumnByName(c.ColumnName)
				if err != nil || dropped {
					return false, nil, pgerror.Newf(pgcode.UndefinedColumn,
						"column %q not found for constraint %q",
						c.ColumnName, parsed.String())
				}
				colIDsUsed[col.ID] = struct{}{}
			}
			return false, v, nil
		}
		return true, expr, nil
	}
	if _, err := tree.SimpleVisit(parsed, visitFn); err != nil {
		return nil, err
	}

	cc.ColumnIDs = make([]ColumnID, 0, len(colIDsUsed))
	for colID := range colIDsUsed {
		cc.ColumnIDs = append(cc.ColumnIDs, colID)
	}
	sort.Sort(ColumnIDs(cc.ColumnIDs))
	return cc.ColumnIDs, nil
}

// UsesColumn returns whether the check constraint uses the specified column.
func (cc *TableDescriptor_CheckConstraint) UsesColumn(
	desc *TableDescriptor, colID ColumnID,
) (bool, error) {
	colsUsed, err := cc.ColumnsUsed(desc)
	if err != nil {
		return false, err
	}
	i := sort.Search(len(colsUsed), func(i int) bool {
		return colsUsed[i] >= colID
	})
	return i < len(colsUsed) && colsUsed[i] == colID, nil
}

// CompositeKeyMatchMethodValue allows the conversion from a
// tree.ReferenceCompositeKeyMatchMethod to a ForeignKeyReference_Match.
var CompositeKeyMatchMethodValue = [...]ForeignKeyReference_Match{
	tree.MatchSimple:  ForeignKeyReference_SIMPLE,
	tree.MatchFull:    ForeignKeyReference_FULL,
	tree.MatchPartial: ForeignKeyReference_PARTIAL,
}

// ForeignKeyReferenceMatchValue allows the conversion from a
// ForeignKeyReference_Match to a tree.ReferenceCompositeKeyMatchMethod.
// This should match CompositeKeyMatchMethodValue.
var ForeignKeyReferenceMatchValue = [...]tree.CompositeKeyMatchMethod{
	ForeignKeyReference_SIMPLE:  tree.MatchSimple,
	ForeignKeyReference_FULL:    tree.MatchFull,
	ForeignKeyReference_PARTIAL: tree.MatchPartial,
}

// String implements the fmt.Stringer interface.
func (x ForeignKeyReference_Match) String() string {
	switch x {
	case ForeignKeyReference_SIMPLE:
		return "MATCH SIMPLE"
	case ForeignKeyReference_FULL:
		return "MATCH FULL"
	case ForeignKeyReference_PARTIAL:
		return "MATCH PARTIAL"
	default:
		return strconv.Itoa(int(x))
	}
}

// ForeignKeyReferenceActionType allows the conversion between a
// tree.ReferenceAction and a ForeignKeyReference_Action.
var ForeignKeyReferenceActionType = [...]tree.ReferenceAction{
	ForeignKeyReference_NO_ACTION:   tree.NoAction,
	ForeignKeyReference_RESTRICT:    tree.Restrict,
	ForeignKeyReference_SET_DEFAULT: tree.SetDefault,
	ForeignKeyReference_SET_NULL:    tree.SetNull,
	ForeignKeyReference_CASCADE:     tree.Cascade,
}

// ForeignKeyReferenceActionValue allows the conversion between a
// ForeignKeyReference_Action and a tree.ReferenceAction.
var ForeignKeyReferenceActionValue = [...]ForeignKeyReference_Action{
	tree.NoAction:   ForeignKeyReference_NO_ACTION,
	tree.Restrict:   ForeignKeyReference_RESTRICT,
	tree.SetDefault: ForeignKeyReference_SET_DEFAULT,
	tree.SetNull:    ForeignKeyReference_SET_NULL,
	tree.Cascade:    ForeignKeyReference_CASCADE,
}

// String implements the fmt.Stringer interface.
func (x ForeignKeyReference_Action) String() string {
	switch x {
	case ForeignKeyReference_RESTRICT:
		return "RESTRICT"
	case ForeignKeyReference_SET_DEFAULT:
		return "SET DEFAULT"
	case ForeignKeyReference_SET_NULL:
		return "SET NULL"
	case ForeignKeyReference_CASCADE:
		return "CASCADE"
	default:
		return strconv.Itoa(int(x))
	}
}

var _ cat.Column = &ColumnDescriptor{}

// IsNullable is part of the cat.Column interface.
func (desc *ColumnDescriptor) IsNullable() bool {
	return desc.Nullable
}

// HasNullDefault checks that the column descriptor has a default of NULL.
func (desc *ColumnDescriptor) HasNullDefault() bool {
	if !desc.HasDefault() {
		return false
	}
	defaultExpr, err := parser.ParseExpr(*desc.DefaultExpr)
	if err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err,
			"failed to parse default expression %s", *desc.DefaultExpr))
	}
	return defaultExpr == tree.DNull
}

// ColID is part of the cat.Column interface.
func (desc *ColumnDescriptor) ColID() cat.StableID {
	return cat.StableID(desc.ID)
}

// ColName is part of the cat.Column interface.
func (desc *ColumnDescriptor) ColName() tree.Name {
	return tree.Name(desc.Name)
}

// DatumType is part of the cat.Column interface.
func (desc *ColumnDescriptor) DatumType() *types.T {
	return &desc.Type
}

// ColTypePrecision is part of the cat.Column interface.
func (desc *ColumnDescriptor) ColTypePrecision() int {
	if desc.Type.Family() == types.ArrayFamily {
		if desc.Type.ArrayContents().Family() == types.ArrayFamily {
			panic(errors.AssertionFailedf("column type should never be a nested array"))
		}
		return int(desc.Type.ArrayContents().Precision())
	}
	return int(desc.Type.Precision())
}

// ColTypeWidth is part of the cat.Column interface.
func (desc *ColumnDescriptor) ColTypeWidth() int {
	if desc.Type.Family() == types.ArrayFamily {
		if desc.Type.ArrayContents().Family() == types.ArrayFamily {
			panic(errors.AssertionFailedf("column type should never be a nested array"))
		}
		return int(desc.Type.ArrayContents().Width())
	}
	return int(desc.Type.Width())
}

// ColTypeStr is part of the cat.Column interface.
func (desc *ColumnDescriptor) ColTypeStr() string {
	return desc.Type.SQLString()
}

// IsHidden is part of the cat.Column interface.
func (desc *ColumnDescriptor) IsHidden() bool {
	return desc.Hidden
}

// HasDefault is part of the cat.Column interface.
func (desc *ColumnDescriptor) HasDefault() bool {
	return desc.DefaultExpr != nil
}

// IsComputed is part of the cat.Column interface.
func (desc *ColumnDescriptor) IsComputed() bool {
	return desc.ComputeExpr != nil
}

// DefaultExprStr is part of the cat.Column interface.
func (desc *ColumnDescriptor) DefaultExprStr() string {
	return *desc.DefaultExpr
}

// ComputedExprStr is part of the cat.Column interface.
func (desc *ColumnDescriptor) ComputedExprStr() string {
	return *desc.ComputeExpr
}

// CheckCanBeFKRef returns whether the given column is computed.
func (desc *ColumnDescriptor) CheckCanBeFKRef() error {
	if desc.IsComputed() {
		return pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"computed column %q cannot be a foreign key reference",
			desc.Name,
		)
	}
	return nil
}

// PartitionNames returns a slice containing the name of every partition and
// subpartition in an arbitrary order.
func (desc *TableDescriptor) PartitionNames() []string {
	var names []string
	for _, index := range desc.AllNonDropIndexes() {
		names = append(names, index.Partitioning.PartitionNames()...)
	}
	return names
}

// PartitionNames returns a slice containing the name of every partition and
// subpartition in an arbitrary order.
func (desc *PartitioningDescriptor) PartitionNames() []string {
	var names []string
	for _, l := range desc.List {
		names = append(names, l.Name)
		names = append(names, l.Subpartitioning.PartitionNames()...)
	}
	for _, r := range desc.Range {
		names = append(names, r.Name)
	}
	return names
}

// SetAuditMode configures the audit mode on the descriptor.
func (desc *TableDescriptor) SetAuditMode(mode tree.AuditMode) (bool, error) {
	prev := desc.AuditMode
	switch mode {
	case tree.AuditModeDisable:
		desc.AuditMode = TableDescriptor_DISABLED
	case tree.AuditModeReadWrite:
		desc.AuditMode = TableDescriptor_READWRITE
	default:
		return false, pgerror.Newf(pgcode.InvalidParameterValue,
			"unknown audit mode: %s (%d)", mode, mode)
	}
	return prev != desc.AuditMode, nil
}

// GetAuditMode is part of the DescriptorProto interface.
// This is a stub until per-database auditing is enabled.
func (desc *DatabaseDescriptor) GetAuditMode() TableDescriptor_AuditMode {
	return TableDescriptor_DISABLED
}

// FindAllReferences returns all the references from a table.
func (desc *TableDescriptor) FindAllReferences() (map[ID]struct{}, error) {
	refs := map[ID]struct{}{}
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		refs[fk.ReferencedTableID] = struct{}{}
	}
	for i := range desc.InboundFKs {
		fk := &desc.InboundFKs[i]
		refs[fk.OriginTableID] = struct{}{}
	}
	if err := desc.ForeachNonDropIndex(func(index *IndexDescriptor) error {
		for _, a := range index.Interleave.Ancestors {
			refs[a.TableID] = struct{}{}
		}
		for _, c := range index.InterleavedBy {
			refs[c.Table] = struct{}{}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	for _, c := range desc.AllNonDropColumns() {
		for _, id := range c.UsesSequenceIds {
			refs[id] = struct{}{}
		}
	}

	for _, dest := range desc.DependsOn {
		refs[dest] = struct{}{}
	}

	for _, c := range desc.DependedOnBy {
		refs[c.ID] = struct{}{}
	}
	return refs, nil
}

// ActiveChecks returns a list of all check constraints that should be enforced
// on writes (including constraints being added/validated). The columns
// referenced by the returned checks are writable, but not necessarily public.
func (desc *ImmutableTableDescriptor) ActiveChecks() []TableDescriptor_CheckConstraint {
	return desc.allChecks
}

// WritableColumns returns a list of public and write-only mutation columns.
func (desc *ImmutableTableDescriptor) WritableColumns() []ColumnDescriptor {
	return desc.publicAndNonPublicCols[:len(desc.Columns)+desc.writeOnlyColCount]
}

// DeletableColumns returns a list of public and non-public columns.
func (desc *ImmutableTableDescriptor) DeletableColumns() []ColumnDescriptor {
	return desc.publicAndNonPublicCols
}

// MutationColumns returns a list of mutation columns.
func (desc *ImmutableTableDescriptor) MutationColumns() []ColumnDescriptor {
	return desc.publicAndNonPublicCols[len(desc.Columns):]
}

// WritableIndexes returns a list of public and write-only mutation indexes.
func (desc *ImmutableTableDescriptor) WritableIndexes() []IndexDescriptor {
	return desc.publicAndNonPublicIndexes[:len(desc.Indexes)+desc.writeOnlyIndexCount]
}

// DeletableIndexes returns a list of public and non-public indexes.
func (desc *ImmutableTableDescriptor) DeletableIndexes() []IndexDescriptor {
	return desc.publicAndNonPublicIndexes
}

// DeleteOnlyIndexes returns a list of delete-only mutation indexes.
func (desc *ImmutableTableDescriptor) DeleteOnlyIndexes() []IndexDescriptor {
	return desc.publicAndNonPublicIndexes[len(desc.Indexes)+desc.writeOnlyIndexCount:]
}

// TableDesc implements the ObjectDescriptor interface.
func (desc *MutableTableDescriptor) TableDesc() *TableDescriptor {
	return &desc.TableDescriptor
}

// TableDesc implements the ObjectDescriptor interface.
func (desc *ImmutableTableDescriptor) TableDesc() *TableDescriptor {
	return &desc.TableDescriptor
}

// DatabaseKey implements DescriptorKey.
type DatabaseKey struct {
	name string
}

// NewDatabaseKey returns a new DatabaseKey.
func NewDatabaseKey(name string) DatabaseKey {
	return DatabaseKey{name: name}
}

// Key implements DescriptorKey interface.
func (dk DatabaseKey) Key() roachpb.Key {
	return MakeNameMetadataKey(keys.RootNamespaceID, keys.RootNamespaceID, dk.name)
}

// Name implements DescriptorKey interface.
func (dk DatabaseKey) Name() string {
	return dk.name
}

// TableKey implements DescriptorKey interface.
type TableKey struct {
	parentID       ID
	parentSchemaID ID
	name           string
}

// NewPublicTableKey returns a new TableKey scoped under the public schema.
func NewPublicTableKey(parentID ID, name string) TableKey {
	return TableKey{parentID: parentID, parentSchemaID: keys.PublicSchemaID, name: name}
}

// NewTableKey returns a new TableKey
func NewTableKey(parentID ID, parentSchemaID ID, name string) TableKey {
	return TableKey{parentID: parentID, parentSchemaID: parentSchemaID, name: name}
}

// Key implements DescriptorKey interface.
func (tk TableKey) Key() roachpb.Key {
	return MakeNameMetadataKey(tk.parentID, tk.parentSchemaID, tk.name)
}

// Name implements DescriptorKey interface.
func (tk TableKey) Name() string {
	return tk.name
}

// SchemaKey implements DescriptorKey interface.
type SchemaKey struct {
	parentID ID
	name     string
}

// NewSchemaKey returns a new SchemaKey
func NewSchemaKey(parentID ID, name string) SchemaKey {
	return SchemaKey{parentID: parentID, name: name}
}

// NewPublicSchemaKey returns a new SchemaKey specific to the public schema.
func NewPublicSchemaKey(parentID ID) SchemaKey {
	return SchemaKey{parentID: parentID, name: tree.PublicSchema}
}

// Key implements DescriptorKey interface.
func (sk SchemaKey) Key() roachpb.Key {
	return MakeNameMetadataKey(sk.parentID, keys.RootNamespaceID, sk.name)
}

// Name implements DescriptorKey interface.
func (sk SchemaKey) Name() string {
	return sk.name
}

// DeprecatedTableKey implements DescriptorKey interface.
type DeprecatedTableKey struct {
	parentID ID
	name     string
}

// NewDeprecatedTableKey returns a new DeprecatedTableKey
func NewDeprecatedTableKey(parentID ID, name string) DeprecatedTableKey {
	return DeprecatedTableKey{parentID, name}
}

// Key implements DescriptorKey interface.
func (dtk DeprecatedTableKey) Key() roachpb.Key {
	return MakeDeprecatedNameMetadataKey(dtk.parentID, dtk.name)
}

// Name implements DescriptorKey interface.
func (dtk DeprecatedTableKey) Name() string {
	return dtk.name
}

// DeprecatedDatabaseKey implements DescriptorKey interface.
type DeprecatedDatabaseKey struct {
	name string
}

// NewDeprecatedDatabaseKey returns a new DeprecatedDatabaseKey
func NewDeprecatedDatabaseKey(name string) DeprecatedDatabaseKey {
	return DeprecatedDatabaseKey{name: name}
}

// Key implements DescriptorKey interface.
func (ddk DeprecatedDatabaseKey) Key() roachpb.Key {
	return MakeDeprecatedNameMetadataKey(keys.RootNamespaceID, ddk.name)
}

// Name implements DescriptorKey interface.
func (ddk DeprecatedDatabaseKey) Name() string {
	return ddk.name
}
