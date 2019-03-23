// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlbase

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/descid"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
)

// MutableTableDescriptor is a custom type for TableDescriptors
// going through schema mutations.
type MutableTableDescriptor struct {
	catpb.TableDescriptor

	// ClusterVersion represents the version of the table descriptor read from the store.
	ClusterVersion catpb.TableDescriptor
}

// ImmutableTableDescriptor is a custom type for TableDescriptors
// It holds precomputed values and the underlying TableDescriptor
// should be const.
type ImmutableTableDescriptor struct {
	catpb.TableDescriptor

	// publicAndNonPublicCols is a list of public and non-public columns.
	// It is partitioned by the state of the column: public, write-only, delete-only
	publicAndNonPublicCols []catpb.ColumnDescriptor

	// publicAndNonPublicCols is a list of public and non-public indexes.
	// It is partitioned by the state of the index: public, write-only, delete-only
	publicAndNonPublicIndexes []catpb.IndexDescriptor

	writeOnlyColCount   int
	writeOnlyIndexCount int

	allChecks []catpb.TableDescriptor_CheckConstraint

	// ReadableColumns is a list of columns (including those undergoing a schema change)
	// which can be scanned. Columns in the process of a schema change
	// are all set to nullable while column backfilling is still in
	// progress, as mutation columns may have NULL values.
	ReadableColumns []catpb.ColumnDescriptor
}

// InvalidMutationID is the uninitialised mutation id.
const InvalidMutationID catpb.MutationID = 0

const (
	// PrimaryKeyIndexName is the name of the index for the primary key.
	PrimaryKeyIndexName = "primary"
)

// ErrMissingColumns indicates a table with no columns.
var ErrMissingColumns = errors.New("table must contain at least 1 column")

// ErrMissingPrimaryKey indicates a table with no primary key.
var ErrMissingPrimaryKey = errors.New("table must contain a primary key")

// ErrDescriptorNotFound is returned by GetTableDescFromID to signal that a
// descriptor could not be found with the given id.
var ErrDescriptorNotFound = errors.New("descriptor not found")

// NewMutableCreatedTableDescriptor returns a MutableTableDescriptor from the
// given TableDescriptor with the cluster version being the zero table. This
// is for a table that is created in the transaction.
func NewMutableCreatedTableDescriptor(tbl catpb.TableDescriptor) *MutableTableDescriptor {
	return &MutableTableDescriptor{TableDescriptor: tbl}
}

// NewMutableExistingTableDescriptor returns a MutableTableDescriptor from the
// given TableDescriptor with the cluster version also set to the descriptor.
// This is for an existing table.
func NewMutableExistingTableDescriptor(tbl catpb.TableDescriptor) *MutableTableDescriptor {
	return &MutableTableDescriptor{TableDescriptor: tbl, ClusterVersion: tbl}
}

// NewImmutableTableDescriptor returns a ImmutableTableDescriptor from the
// given TableDescriptor.
func NewImmutableTableDescriptor(tbl catpb.TableDescriptor) *ImmutableTableDescriptor {
	publicAndNonPublicCols := tbl.Columns
	publicAndNonPublicIndexes := tbl.Indexes

	readableCols := tbl.Columns

	desc := &ImmutableTableDescriptor{TableDescriptor: tbl}

	if len(tbl.Mutations) > 0 {
		publicAndNonPublicCols = make([]catpb.ColumnDescriptor, 0, len(tbl.Columns)+len(tbl.Mutations))
		publicAndNonPublicIndexes = make([]catpb.IndexDescriptor, 0, len(tbl.Indexes)+len(tbl.Mutations))
		readableCols = make([]catpb.ColumnDescriptor, 0, len(tbl.Columns)+len(tbl.Mutations))

		publicAndNonPublicCols = append(publicAndNonPublicCols, tbl.Columns...)
		publicAndNonPublicIndexes = append(publicAndNonPublicIndexes, tbl.Indexes...)
		readableCols = append(readableCols, tbl.Columns...)

		// Fill up mutations into the column/index lists by placing the writable columns/indexes
		// before the delete only columns/indexes.
		for _, m := range tbl.Mutations {
			switch m.State {
			case catpb.DescriptorMutation_DELETE_AND_WRITE_ONLY:
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
			case catpb.DescriptorMutation_DELETE_ONLY:
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

	desc.allChecks = make([]catpb.TableDescriptor_CheckConstraint, len(tbl.Checks))
	for i, c := range tbl.Checks {
		desc.allChecks[i] = *c
	}

	return desc
}

// GetDatabaseDescFromID retrieves the database descriptor for the database
// ID passed in using an existing txn. Returns an error if the descriptor
// doesn't exist or if it exists and is not a database.
func GetDatabaseDescFromID(
	ctx context.Context, txn *client.Txn, id descid.T,
) (*catpb.DatabaseDescriptor, error) {
	desc := &catpb.Descriptor{}
	descKey := MakeDescMetadataKey(id)

	if err := txn.GetProto(ctx, descKey, desc); err != nil {
		return nil, err
	}
	db := desc.GetDatabase()
	if db == nil {
		return nil, ErrDescriptorNotFound
	}
	return db, nil
}

// GetTableDescFromID retrieves the table descriptor for the table
// ID passed in using an existing txn. Returns an error if the
// descriptor doesn't exist or if it exists and is not a table.
func GetTableDescFromID(
	ctx context.Context, txn *client.Txn, id descid.T,
) (*catpb.TableDescriptor, error) {
	desc := &catpb.Descriptor{}
	descKey := MakeDescMetadataKey(id)

	if err := txn.GetProto(ctx, descKey, desc); err != nil {
		return nil, err
	}
	table := desc.GetTable()
	if table == nil {
		return nil, ErrDescriptorNotFound
	}
	return table, nil
}

// GetMutableTableDescFromID retrieves the table descriptor for the table
// ID passed in using an existing txn. Returns an error if the
// descriptor doesn't exist or if it exists and is not a table.
// Otherwise a mutable copy of the table is returned.
func GetMutableTableDescFromID(
	ctx context.Context, txn *client.Txn, id descid.T,
) (*MutableTableDescriptor, error) {
	table, err := GetTableDescFromID(ctx, txn, id)
	if err != nil {
		return nil, err
	}
	return NewMutableExistingTableDescriptor(*table), nil
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

	columnNames := map[string]catpb.ColumnID{}
	fillColumnID := func(c *catpb.ColumnDescriptor) {
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
	err := ValidateSingleTable(desc.TableDesc(), nil)
	desc.ID = savedID
	return err
}

func (desc *MutableTableDescriptor) ensurePrimaryKey() error {
	if len(desc.PrimaryIndex.ColumnNames) == 0 && desc.IsPhysicalTable() {
		// Ensure a Primary Key exists.
		s := "unique_rowid()"
		col := catpb.ColumnDescriptor{
			Name: "rowid",
			Type: catpb.ColumnType{
				SemanticType: catpb.ColumnType_INT,
			},
			DefaultExpr: &s,
			Hidden:      true,
			Nullable:    false,
		}
		desc.AddColumn(col)
		idx := catpb.IndexDescriptor{
			Unique:           true,
			ColumnNames:      []string{col.Name},
			ColumnDirections: []catpb.IndexDescriptor_Direction{catpb.IndexDescriptor_ASC},
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
func HasCompositeKeyEncoding(semanticType catpb.ColumnType_SemanticType) bool {
	switch semanticType {
	case catpb.ColumnType_COLLATEDSTRING,
		catpb.ColumnType_FLOAT,
		catpb.ColumnType_DECIMAL:
		return true
	}
	return false
}

// DatumTypeHasCompositeKeyEncoding is a version of HasCompositeKeyEncoding
// which works on datum types.
func DatumTypeHasCompositeKeyEncoding(typ types.T) bool {
	colType, err := catpb.DatumTypeToColumnSemanticType(typ)
	return err == nil && HasCompositeKeyEncoding(colType)
}

// MustBeValueEncoded returns true if columns of the given kind can only be value
// encoded.
func MustBeValueEncoded(semanticType catpb.ColumnType_SemanticType) bool {
	return semanticType == catpb.ColumnType_ARRAY ||
		semanticType == catpb.ColumnType_JSONB ||
		semanticType == catpb.ColumnType_TUPLE
}

// allocateName sets desc.Name to a value that is not EqualName to any
// of tableDesc's indexes. allocateName roughly follows PostgreSQL's
// convention for automatically-named indexes.
func allocateName(desc *catpb.IndexDescriptor, tableDesc *MutableTableDescriptor) {
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

func (desc *MutableTableDescriptor) allocateIndexIDs(columnNames map[string]catpb.ColumnID) error {
	if desc.NextIndexID == 0 {
		desc.NextIndexID = 1
	}

	// Keep track of unnamed indexes.
	anonymousIndexes := make([]*catpb.IndexDescriptor, 0, len(desc.Indexes)+len(desc.Mutations))

	// Create a slice of modifiable index descriptors.
	indexes := make([]*catpb.IndexDescriptor, 0, 1+len(desc.Indexes)+len(desc.Mutations))
	indexes = append(indexes, &desc.PrimaryIndex)
	collectIndexes := func(index *catpb.IndexDescriptor) {
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
		allocateName(index, desc)
	}

	isCompositeColumn := make(map[catpb.ColumnID]struct{})
	for _, col := range desc.Columns {
		if HasCompositeKeyEncoding(col.Type.SemanticType) {
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
			var extraColumnIDs []catpb.ColumnID
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
					return pgerror.NewErrorf(
						pgerror.CodeDuplicateColumnError, "index %q already contains column %q", index.Name, col.Name).
						SetDetailf("column %q is part of the primary index and therefore implicit in all indexes", col.Name)
				}
				if index.ContainsColumnID(col.ID) {
					return pgerror.NewErrorf(
						pgerror.CodeDuplicateColumnError,
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

func (desc *MutableTableDescriptor) allocateColumnFamilyIDs(columnNames map[string]catpb.ColumnID) {
	if desc.NextFamilyID == 0 {
		if len(desc.Families) == 0 {
			desc.Families = []catpb.ColumnFamilyDescriptor{
				{ID: 0, Name: "primary"},
			}
		}
		desc.NextFamilyID = 1
	}

	columnsInFamilies := make(map[catpb.ColumnID]struct{}, len(desc.Columns))
	for i, family := range desc.Families {
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

		desc.Families[i] = family
	}

	primaryIndexColIDs := make(map[catpb.ColumnID]struct{}, len(desc.PrimaryIndex.ColumnIDs))
	for _, colID := range desc.PrimaryIndex.ColumnIDs {
		primaryIndexColIDs[colID] = struct{}{}
	}

	ensureColumnInFamily := func(col *catpb.ColumnDescriptor) {
		if _, ok := columnsInFamilies[col.ID]; ok {
			return
		}
		if _, ok := primaryIndexColIDs[col.ID]; ok {
			// Primary index columns are required to be assigned to family 0.
			desc.Families[0].ColumnNames = append(desc.Families[0].ColumnNames, col.Name)
			desc.Families[0].ColumnIDs = append(desc.Families[0].ColumnIDs, col.ID)
			return
		}
		var familyID catpb.FamilyID
		if desc.ParentID == keys.SystemDatabaseID {
			// TODO(dan): This assigns families such that the encoding is exactly the
			// same as before column families. It's used for all system tables because
			// reads of them don't go through the normal sql layer, which is where the
			// knowledge of families lives. Fix that and remove this workaround.
			familyID = catpb.FamilyID(col.ID)
			desc.Families = append(desc.Families, catpb.ColumnFamilyDescriptor{
				ID:          familyID,
				ColumnNames: []string{col.Name},
				ColumnIDs:   []catpb.ColumnID{col.ID},
			})
		} else {
			idx, ok := fitColumnToFamily(desc, *col)
			if !ok {
				idx = len(desc.Families)
				desc.Families = append(desc.Families, catpb.ColumnFamilyDescriptor{
					ID:          desc.NextFamilyID,
					ColumnNames: []string{},
					ColumnIDs:   []catpb.ColumnID{},
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

	for i, family := range desc.Families {
		if len(family.Name) == 0 {
			family.Name = family.ID.GeneratedFamilyName(family.ColumnNames)
		}

		if family.DefaultColumnID == 0 {
			defaultColumnID := catpb.ColumnID(0)
			for _, colID := range family.ColumnIDs {
				if _, ok := primaryIndexColIDs[colID]; !ok {
					if defaultColumnID == 0 {
						defaultColumnID = colID
					} else {
						defaultColumnID = catpb.ColumnID(0)
						break
					}
				}
			}
			family.DefaultColumnID = defaultColumnID
		}

		desc.Families[i] = family
	}
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
func fitColumnToFamily(desc *MutableTableDescriptor, col catpb.ColumnDescriptor) (int, bool) {
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

// columnTypeIsIndexable returns whether the type t is valid as an indexed column.
func columnTypeIsIndexable(t catpb.ColumnType) bool {
	return !MustBeValueEncoded(t.SemanticType)
}

// columnTypeIsInvertedIndexable returns whether the type t is valid to be indexed
// using an inverted index.
func columnTypeIsInvertedIndexable(t catpb.ColumnType) bool {
	return t.SemanticType == catpb.ColumnType_JSONB
}

func notIndexableError(cols []catpb.ColumnDescriptor, inverted bool) error {
	if len(cols) == 0 {
		return nil
	}
	var msg string
	var typInfo string
	if len(cols) == 1 {
		col := cols[0]
		msg = "column %s is of type %s and thus is not indexable"
		if inverted {
			msg += " with an inverted index"
		}
		typInfo = col.Type.String()
		msg = fmt.Sprintf(msg, col.Name, col.Type.SemanticType)
	} else {
		msg = "the following columns are not indexable due to their type: "
		for i, col := range cols {
			msg += fmt.Sprintf("%s (type %s)", col.Name, col.Type.SemanticType)
			typInfo += col.Type.String()
			if i != len(cols)-1 {
				msg += ", "
				typInfo += ","
			}
		}
	}
	return pgerror.UnimplementedWithIssueDetailErrorf(35730, typInfo, msg)
}

func checkColumnsValidForIndex(tableDesc *MutableTableDescriptor, indexColNames []string) error {
	invalidColumns := make([]catpb.ColumnDescriptor, 0, len(indexColNames))
	for _, indexCol := range indexColNames {
		for _, col := range tableDesc.AllNonDropColumns() {
			if col.Name == indexCol {
				if !columnTypeIsIndexable(col.Type) {
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
	invalidColumns := make([]catpb.ColumnDescriptor, 0, len(indexColNames))
	for _, indexCol := range indexColNames {
		for _, col := range tableDesc.AllNonDropColumns() {
			if col.Name == indexCol {
				if !columnTypeIsInvertedIndexable(col.Type) {
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
func (desc *MutableTableDescriptor) AddColumn(col catpb.ColumnDescriptor) {
	desc.Columns = append(desc.Columns, col)
}

// AddFamily adds a family to the table.
func (desc *MutableTableDescriptor) AddFamily(fam catpb.ColumnFamilyDescriptor) {
	desc.Families = append(desc.Families, fam)
}

// UpdateColumnDescriptor updates an existing column descriptor.
func (desc *MutableTableDescriptor) UpdateColumnDescriptor(column catpb.ColumnDescriptor) {
	for i := range desc.Columns {
		if desc.Columns[i].ID == column.ID {
			desc.Columns[i] = column
			return
		}
	}
	for i, m := range desc.Mutations {
		if col := m.GetColumn(); col != nil && col.ID == column.ID {
			desc.Mutations[i].Descriptor_ = &catpb.DescriptorMutation_Column{Column: &column}
			return
		}
	}

	panic(sqlerrors.NewUndefinedColumnError(column.Name))
}

// AddIndex adds an index to the table.
func (desc *MutableTableDescriptor) AddIndex(idx catpb.IndexDescriptor, primary bool) error {
	if idx.Type == catpb.IndexDescriptor_FORWARD {
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
			desc.AddFamily(catpb.ColumnFamilyDescriptor{Name: family, ColumnNames: []string{col}})
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
func (desc *MutableTableDescriptor) RemoveColumnFromFamily(colID catpb.ColumnID) {
	for i, family := range desc.Families {
		for j, c := range family.ColumnIDs {
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
	column catpb.ColumnDescriptor, newColName string,
) {
	colID := column.ID
	column.Name = newColName
	desc.UpdateColumnDescriptor(column)
	for i := range desc.Families {
		for j := range desc.Families[i].ColumnIDs {
			if desc.Families[i].ColumnIDs[j] == colID {
				desc.Families[i].ColumnNames[j] = newColName
			}
		}
	}

	renameColumnInIndex := func(idx *catpb.IndexDescriptor) {
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

// FindReadableColumnByID finds the readable column with specified ID. The
// column may be undergoing a schema change and is marked nullable regardless
// of its configuration. It returns true if the column is undergoing a
// schema change.
func (desc *ImmutableTableDescriptor) FindReadableColumnByID(
	id catpb.ColumnID,
) (*catpb.ColumnDescriptor, bool, error) {
	for i, c := range desc.ReadableColumns {
		if c.ID == id {
			return &desc.ReadableColumns[i], i >= len(desc.Columns), nil
		}
	}
	return nil, false, fmt.Errorf("column-id \"%d\" does not exist", id)
}

// RenameIndexDescriptor renames an index descriptor.
func (desc *MutableTableDescriptor) RenameIndexDescriptor(
	index *catpb.IndexDescriptor, name string,
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

// DropConstraint drops a constraint.
func (desc *MutableTableDescriptor) DropConstraint(
	name string,
	detail catpb.ConstraintDetail,
	removeFK func(*MutableTableDescriptor, *catpb.IndexDescriptor) error,
) error {
	switch detail.Kind {
	case catpb.ConstraintTypePK:
		return pgerror.Unimplemented("drop-constraint-pk", "cannot drop primary key")

	case catpb.ConstraintTypeUnique:
		return pgerror.Unimplemented("drop-constraint-unique",
			"cannot drop UNIQUE constraint %q using ALTER TABLE DROP CONSTRAINT, use DROP INDEX CASCADE instead",
			tree.ErrNameStringP(&detail.Index.Name))

	case catpb.ConstraintTypeCheck:
		if detail.CheckConstraint.Validity == catpb.ConstraintValidity_Validating {
			return pgerror.Unimplemented("rename-constraint-check-mutation",
				"constraint %q in the middle of being added, try again later",
				tree.ErrNameStringP(&detail.CheckConstraint.Name))
		}
		for i, c := range desc.Checks {
			if c.Name == name {
				desc.Checks = append(desc.Checks[:i], desc.Checks[i+1:]...)
				break
			}
		}
		return nil

	case catpb.ConstraintTypeFK:
		idx, err := desc.FindIndexByID(detail.Index.ID)
		if err != nil {
			return err
		}
		if err := removeFK(desc, idx); err != nil {
			return err
		}
		idx.ForeignKey = catpb.ForeignKeyReference{}
		return nil

	default:
		return pgerror.Unimplemented(fmt.Sprintf("drop-constraint-%s", detail.Kind),
			"constraint %q has unsupported type", tree.ErrNameString(name))
	}

}

// RenameConstraint renames a constraint.
func (desc *MutableTableDescriptor) RenameConstraint(
	detail catpb.ConstraintDetail,
	oldName, newName string,
	dependentViewRenameError func(string, descid.T) error,
) error {
	switch detail.Kind {
	case catpb.ConstraintTypePK, catpb.ConstraintTypeUnique:
		for _, tableRef := range desc.DependedOnBy {
			if tableRef.IndexID != detail.Index.ID {
				continue
			}
			return dependentViewRenameError("index", tableRef.ID)
		}
		return desc.RenameIndexDescriptor(detail.Index, newName)

	case catpb.ConstraintTypeFK:
		idx, err := desc.FindIndexByID(detail.Index.ID)
		if err != nil {
			return err
		}
		if !idx.ForeignKey.IsSet() || idx.ForeignKey.Name != oldName {
			return pgerror.NewAssertionErrorf("constraint %q not found",
				tree.ErrNameString(newName))
		}
		idx.ForeignKey.Name = newName
		return nil

	case catpb.ConstraintTypeCheck:
		if detail.CheckConstraint.Validity == catpb.ConstraintValidity_Validating {
			return pgerror.Unimplemented("rename-constraint-check-mutation",
				"constraint %q in the middle of being added, try again later",
				tree.ErrNameStringP(&detail.CheckConstraint.Name))
		}
		detail.CheckConstraint.Name = newName
		return nil

	default:
		return pgerror.Unimplemented(fmt.Sprintf("rename-constraint-%s", detail.Kind),
			"constraint %q has unsupported type", tree.ErrNameString(oldName))
	}
}

// MakeMutationComplete updates the descriptor upon completion of a mutation.
func (desc *MutableTableDescriptor) MakeMutationComplete(m catpb.DescriptorMutation) error {
	switch m.Direction {
	case catpb.DescriptorMutation_ADD:
		switch t := m.Descriptor_.(type) {
		case *catpb.DescriptorMutation_Column:
			desc.AddColumn(*t.Column)

		case *catpb.DescriptorMutation_Index:
			if err := desc.AddIndex(*t.Index, false); err != nil {
				return err
			}

		case *catpb.DescriptorMutation_Constraint:
			switch t.Constraint.ConstraintType {
			case catpb.ConstraintToUpdate_CHECK:
				for _, c := range desc.Checks {
					if c.Name == t.Constraint.Name {
						c.Validity = catpb.ConstraintValidity_Validated
						break
					}
				}
			default:
				return errors.Errorf("unsupported constraint type: %d", t.Constraint.ConstraintType)
			}
		}

	case catpb.DescriptorMutation_DROP:
		switch t := m.Descriptor_.(type) {
		case *catpb.DescriptorMutation_Column:
			desc.RemoveColumnFromFamily(t.Column.ID)
		}
		// Nothing else to be done. The column/index was already removed from the
		// set of column/index descriptors at mutation creation time.
	}
	return nil
}

// AddCheckValidationMutation adds a check constraint validation mutation to desc.Mutations.
func (desc *MutableTableDescriptor) AddCheckValidationMutation(
	ck *catpb.TableDescriptor_CheckConstraint,
) {
	m := catpb.DescriptorMutation{
		Descriptor_: &catpb.DescriptorMutation_Constraint{
			Constraint: &catpb.ConstraintToUpdate{
				ConstraintType: catpb.ConstraintToUpdate_CHECK, Name: ck.Name, Check: *ck,
			},
		},
		Direction: catpb.DescriptorMutation_ADD,
	}
	desc.addMutation(m)
}

// AddColumnMutation adds a column mutation to desc.Mutations.
func (desc *MutableTableDescriptor) AddColumnMutation(
	c catpb.ColumnDescriptor, direction catpb.DescriptorMutation_Direction,
) {
	m := catpb.DescriptorMutation{Descriptor_: &catpb.DescriptorMutation_Column{Column: &c}, Direction: direction}
	desc.addMutation(m)
}

// AddIndexMutation adds an index mutation to desc.Mutations.
func (desc *MutableTableDescriptor) AddIndexMutation(
	idx *catpb.IndexDescriptor, direction catpb.DescriptorMutation_Direction,
) error {

	switch idx.Type {
	case catpb.IndexDescriptor_FORWARD:
		if err := checkColumnsValidForIndex(desc, idx.ColumnNames); err != nil {
			return err
		}
	case catpb.IndexDescriptor_INVERTED:
		if err := checkColumnsValidForInvertedIndex(desc, idx.ColumnNames); err != nil {
			return err
		}
	}

	m := catpb.DescriptorMutation{Descriptor_: &catpb.DescriptorMutation_Index{Index: idx}, Direction: direction}
	desc.addMutation(m)
	return nil
}

func (desc *MutableTableDescriptor) addMutation(m catpb.DescriptorMutation) {
	switch m.Direction {
	case catpb.DescriptorMutation_ADD:
		m.State = catpb.DescriptorMutation_DELETE_ONLY

	case catpb.DescriptorMutation_DROP:
		m.State = catpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
	}
	// For tables created in the same transaction the next mutation ID will
	// not have been allocated and the added mutation will use an invalid ID.
	// This is fine because the mutation will be processed immediately.
	m.MutationID = desc.ClusterVersion.NextMutationID
	desc.NextMutationID = desc.ClusterVersion.NextMutationID + 1
	desc.Mutations = append(desc.Mutations, m)
}

// MakeFirstMutationPublic creates a MutableTableDescriptor from the
// ImmutableTableDescriptor by making the first mutation public.
// This is super valuable when trying to run SQL over data associated
// with a schema mutation that is still not yet public: Data validation,
// error reporting.
func (desc *ImmutableTableDescriptor) MakeFirstMutationPublic() (*MutableTableDescriptor, error) {
	// Clone the ImmutableTable descriptor because we want to create an Immutable one.
	table := NewMutableExistingTableDescriptor(*protoutil.Clone(desc.TableDesc()).(*catpb.TableDescriptor))
	mutationID := desc.Mutations[0].MutationID
	i := 0
	for _, mutation := range desc.Mutations {
		if mutation.MutationID != mutationID {
			// Mutations are applied in a FIFO order. Only apply the first set
			// of mutations if they have the mutation ID we're looking for.
			break
		}
		if err := table.MakeMutationComplete(mutation); err != nil {
			return nil, err
		}
		i++
	}
	table.Mutations = table.Mutations[i:]
	table.Version++
	return table, nil
}

// IsNewTable returns true if the table was created in the current
// transaction.
func (desc *MutableTableDescriptor) IsNewTable() bool {
	return desc.ClusterVersion.ID == descid.InvalidID
}

// ColumnsSelectors generates Select expressions for cols.
func ColumnsSelectors(cols []catpb.ColumnDescriptor, forUpdateOrDelete bool) tree.SelectExprs {
	exprs := make(tree.SelectExprs, len(cols))
	colItems := make([]tree.ColumnItem, len(cols))
	for i, col := range cols {
		colItems[i].ColumnName = tree.Name(col.Name)
		colItems[i].ForUpdateOrDelete = forUpdateOrDelete
		exprs[i].Expr = &colItems[i]
	}
	return exprs
}

// ActiveChecks returns a list of all check constraints that should be enforced
// on writes (including constraints being added/validated). The columns
// referenced by the returned checks are writable, but not necessarily public.
func (desc *ImmutableTableDescriptor) ActiveChecks() []catpb.TableDescriptor_CheckConstraint {
	return desc.allChecks
}

// WritableColumns returns a list of public and write-only mutation columns.
func (desc *ImmutableTableDescriptor) WritableColumns() []catpb.ColumnDescriptor {
	return desc.publicAndNonPublicCols[:len(desc.Columns)+desc.writeOnlyColCount]
}

// DeletableColumns returns a list of public and non-public columns.
func (desc *ImmutableTableDescriptor) DeletableColumns() []catpb.ColumnDescriptor {
	return desc.publicAndNonPublicCols
}

// MutationColumns returns a list of mutation columns.
func (desc *ImmutableTableDescriptor) MutationColumns() []catpb.ColumnDescriptor {
	return desc.publicAndNonPublicCols[len(desc.Columns):]
}

// WritableIndexes returns a list of public and write-only mutation indexes.
func (desc *ImmutableTableDescriptor) WritableIndexes() []catpb.IndexDescriptor {
	return desc.publicAndNonPublicIndexes[:len(desc.Indexes)+desc.writeOnlyIndexCount]
}

// DeletableIndexes returns a list of public and non-public indexes.
func (desc *ImmutableTableDescriptor) DeletableIndexes() []catpb.IndexDescriptor {
	return desc.publicAndNonPublicIndexes
}

// MutationIndexes returns a list of mutation indexes.
func (desc *ImmutableTableDescriptor) MutationIndexes() []catpb.IndexDescriptor {
	return desc.publicAndNonPublicIndexes[len(desc.Indexes):]
}

// DeleteOnlyIndexes returns a list of delete-only mutation indexes.
func (desc *ImmutableTableDescriptor) DeleteOnlyIndexes() []catpb.IndexDescriptor {
	return desc.publicAndNonPublicIndexes[len(desc.Indexes)+desc.writeOnlyIndexCount:]
}

// TableDesc implements the ObjectDescriptor interface.
func (desc *MutableTableDescriptor) TableDesc() *catpb.TableDescriptor {
	return &desc.TableDescriptor
}

// TableDesc implements the ObjectDescriptor interface.
func (desc *ImmutableTableDescriptor) TableDesc() *catpb.TableDescriptor {
	return &desc.TableDescriptor
}

// ValidateTable validates that the table descriptor is well formed. Checks
// include validating the table, column and index names, verifying that column
// names and index names are unique and verifying that column IDs and index IDs
// are consistent. Use ValidateTableDescriptor to validate that cross-table
// references are correct.
func ValidateSingleTable(desc *catpb.TableDescriptor, st *cluster.Settings) error {
	if err := catpb.ValidateDescriptorName(desc.Name, "table"); err != nil {
		return err
	}
	if desc.ID == 0 {
		return pgerror.NewAssertionErrorf("invalid table ID %d", log.Safe(desc.ID))
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
		return pgerror.NewAssertionErrorf("invalid parent ID %d", log.Safe(desc.ParentID))
	}

	// We maintain forward compatibility, so if you see this error message with a
	// version older that what this client supports, then there's a
	// MaybeFillInDescriptor missing from some codepath.
	if v := desc.GetFormatVersion(); v != catpb.FamilyFormatVersion && v != catpb.InterleavedFormatVersion {
		// TODO(dan): We're currently switching from FamilyFormatVersion to
		// InterleavedFormatVersion. After a beta is released with this dual version
		// support, then:
		// - Upgrade the bidirectional reference version to that beta
		// - Start constructing all TableDescriptors with InterleavedFormatVersion
		// - Change maybeUpgradeFormatVersion to output InterleavedFormatVersion
		// - Change this check to only allow InterleavedFormatVersion
		return pgerror.NewAssertionErrorf(
			"table %q is encoded using using version %d, but this client only supports version %d and %d",
			desc.Name, log.Safe(desc.GetFormatVersion()),
			log.Safe(catpb.FamilyFormatVersion), log.Safe(catpb.InterleavedFormatVersion))
	}

	if len(desc.Columns) == 0 {
		return ErrMissingColumns
	}

	if err := desc.CheckUniqueConstraints(); err != nil {
		return err
	}

	columnNames := make(map[string]catpb.ColumnID, len(desc.Columns))
	columnIDs := make(map[catpb.ColumnID]string, len(desc.Columns))
	for _, column := range desc.AllNonDropColumns() {
		if err := catpb.ValidateDescriptorName(column.Name, "column"); err != nil {
			return err
		}
		if column.ID == 0 {
			return pgerror.NewAssertionErrorf("invalid column ID %d", log.Safe(column.ID))
		}

		if _, ok := columnNames[column.Name]; ok {
			for _, col := range desc.Columns {
				if col.Name == column.Name {
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
			return pgerror.NewAssertionErrorf("column %q invalid ID (%d) >= next column ID (%d)",
				column.Name, log.Safe(column.ID), log.Safe(desc.NextColumnID))
		}
	}

	if st != nil && st.Version.IsInitialized() {
		if !st.Version.IsActive(cluster.VersionBitArrayColumns) {
			for _, def := range desc.Columns {
				if def.Type.SemanticType == catpb.ColumnType_BIT {
					return fmt.Errorf("cluster version does not support BIT (required: %s)",
						cluster.VersionByKey(cluster.VersionBitArrayColumns))
				}
			}
		}
	}

	for _, m := range desc.Mutations {
		unSetEnums := m.State == catpb.DescriptorMutation_UNKNOWN || m.Direction == catpb.DescriptorMutation_NONE
		switch desc := m.Descriptor_.(type) {
		case *catpb.DescriptorMutation_Column:
			col := desc.Column
			if unSetEnums {
				return pgerror.NewAssertionErrorf(
					"mutation in state %s, direction %s, col %q, id %v",
					log.Safe(m.State), log.Safe(m.Direction), col.Name, log.Safe(col.ID))
			}
			columnIDs[col.ID] = col.Name
		case *catpb.DescriptorMutation_Index:
			if unSetEnums {
				idx := desc.Index
				return pgerror.NewAssertionErrorf(
					"mutation in state %s, direction %s, index %s, id %v",
					log.Safe(m.State), log.Safe(m.Direction), idx.Name, log.Safe(idx.ID))
			}
		case *catpb.DescriptorMutation_Constraint:
			if unSetEnums {
				return pgerror.NewAssertionErrorf(
					"mutation in state %s, direction %s, constraint %v",
					log.Safe(m.State), log.Safe(m.Direction), desc.Constraint.Name)
			}
		default:
			return pgerror.NewAssertionErrorf(
				"mutation in state %s, direction %s, and no column/index descriptor",
				log.Safe(m.State), log.Safe(m.Direction))
		}
	}

	// TODO(dt): Validate each column only appears at-most-once in any FKs.

	// Only validate column families and indexes if this is actually a table, not
	// if it's just a view.
	if desc.IsPhysicalTable() {
		colIDToFamilyID, err := validateColumnFamilies(desc, columnIDs)
		if err != nil {
			return err
		}
		if err := validateTableIndexes(desc, columnNames, colIDToFamilyID); err != nil {
			return err
		}
		if err := validatePartitioning(desc); err != nil {
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

func validateColumnFamilies(
	desc *catpb.TableDescriptor, columnIDs map[catpb.ColumnID]string,
) (map[catpb.ColumnID]catpb.FamilyID, error) {
	if len(desc.Families) < 1 {
		return nil, fmt.Errorf("at least 1 column family must be specified")
	}
	if desc.Families[0].ID != catpb.FamilyID(0) {
		return nil, fmt.Errorf("the 0th family must have ID 0")
	}

	familyNames := map[string]struct{}{}
	familyIDs := map[catpb.FamilyID]string{}
	colIDToFamilyID := map[catpb.ColumnID]catpb.FamilyID{}
	for _, family := range desc.Families {
		if err := catpb.ValidateDescriptorName(family.Name, "family"); err != nil {
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
func validateTableIndexes(
	desc *catpb.TableDescriptor,
	columnNames map[string]catpb.ColumnID,
	colIDToFamilyID map[catpb.ColumnID]catpb.FamilyID,
) error {
	if len(desc.PrimaryIndex.ColumnIDs) == 0 {
		return ErrMissingPrimaryKey
	}

	indexNames := map[string]struct{}{}
	indexIDs := map[catpb.IndexID]string{}
	for _, index := range desc.AllNonDropIndexes() {
		if err := catpb.ValidateDescriptorName(index.Name, "index"); err != nil {
			return err
		}
		if index.ID == 0 {
			return fmt.Errorf("invalid index ID %d", index.ID)
		}

		if _, ok := indexNames[index.Name]; ok {
			for _, idx := range desc.Indexes {
				if idx.Name == index.Name {
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

		validateIndexDup := make(map[catpb.ColumnID]struct{})
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
		if !ok || famID != catpb.FamilyID(0) {
			return fmt.Errorf("primary key column %d is not in column family 0", colID)
		}
	}

	return nil
}

// validatePartitioning validates that any PartitioningDescriptors contained in
// table indexes are well-formed. See validatePartitioningDesc for details.
func validatePartitioning(desc *catpb.TableDescriptor) error {
	partitionNames := make(map[string]string)

	a := &tree.DatumAlloc{}
	return desc.ForeachNonDropIndex(func(idxDesc *catpb.IndexDescriptor) error {
		return validatePartitioningDescriptor(desc,
			a, idxDesc, &idxDesc.Partitioning, 0 /* colOffset */, partitionNames,
		)
	})
}

// validatePartitioningDescriptor validates that a PartitioningDescriptor, which
// may represent a subpartition, is well-formed. Checks include validating the
// table-level uniqueness of all partition names, validating that the encoded
// tuples match the corresponding column types, and that range partitions are
// stored sorted by upper bound. colOffset is non-zero for subpartitions and
// indicates how many index columns to skip over.
func validatePartitioningDescriptor(
	desc *catpb.TableDescriptor,
	a *tree.DatumAlloc,
	idxDesc *catpb.IndexDescriptor,
	partDesc *catpb.PartitioningDescriptor,
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
			return fmt.Errorf("PARTITION %s: name must be unique (used in both index %q and index %q)",
				name, indexName, idxDesc.Name)
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
			if err := validatePartitioningDescriptor(
				desc, a, idxDesc, &p.Subpartitioning, newColOffset, partitionNames,
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
				return pgerror.Wrapf(err, pgerror.CodeDataExceptionError,
					"PARTITION %s", p.Name)
			}
		}
	}

	return nil
}

type partitionInterval struct {
	name  string
	start roachpb.Key
	end   roachpb.Key
}

var _ interval.Interface = partitionInterval{}

// ID is part of `interval.Interface` but unused in validatePartitioningDescriptor.
func (ps partitionInterval) ID() uintptr { return 0 }

// Range is part of `interval.Interface`.
func (ps partitionInterval) Range() interval.Range {
	return interval.Range{Start: []byte(ps.start), End: []byte(ps.end)}
}
