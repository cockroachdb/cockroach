// Copyright 2018 The Cockroach Authors.
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

package catpb

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/descid"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/virtualid"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/pkg/errors"
)

type ID = descid.T

// TableDescriptors is a sortable list of *TableDescriptors.
type TableDescriptors []*TableDescriptor

// TablesByID is a shorthand for the common map of tables keyed by ID.
type TablesByID map[descid.T]*TableDescriptor

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

// ID, ColumnID, FamilyID, and IndexID are all uint32, but are each given a
// type alias to prevent accidental use of one of the types where
// another is expected.

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

// MutationID is a custom type for TableDescriptor mutations.
type MutationID uint32

func (id FamilyID) GeneratedFamilyName(columnNames []string) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "fam_%d", id)
	for _, n := range columnNames {
		buf.WriteString(`_`)
		buf.WriteString(n)
	}
	return buf.String()
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

// ColNamesString returns a string describing the column names and directions
// in this index.
func (desc *IndexDescriptor) ColNamesString() string {
	f := tree.NewFmtCtx(tree.FmtSimple)
	desc.ColNamesFormat(f)
	return f.CloseAndGetString()
}

var anonymousTable = tree.TableName{}

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
	if *tableName != anonymousTable {
		f.WriteString("ON ")
		f.FormatNode(tableName)
	}
	f.FormatNameP(&desc.Name)
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
func (desc *TableDescriptor) SetID(id descid.T) {
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

// IsEmpty checks if the descriptor is uninitialized.
func (desc *TableDescriptor) IsEmpty() bool {
	// Valid tables cannot have an ID of 0.
	return desc.ID == 0
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

// IsVirtualTable returns true if the TableDescriptor describes a
// virtual Table (like the informationgi_schema tables) and thus doesn't
// need to be physically stored.
func IsVirtualTable(id descid.T) bool {
	return virtualid.MinVirtualID <= id
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
	// For now, a check constraint is either in the mutations list or Validated.
	// If it shows up twice after combining those two slices, it's a duplicate.
	checks := make([]*TableDescriptor_CheckConstraint, 0, len(desc.Checks)+len(desc.Mutations))
	for _, c := range desc.Checks {
		if c.Validity == ConstraintValidity_Validated {
			checks = append(checks, c)
		}
	}
	for _, m := range desc.Mutations {
		if c := m.GetConstraint(); c != nil {
			checks = append(checks, &c.Check)
		}
	}
	return checks
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

// MaybeFillInDescriptor performs any modifications needed to the table descriptor.
// This includes format upgrades and optional changes that can be handled by all version
// (for example: additional default privileges).
// Returns true if any changes were made.
func (desc *TableDescriptor) MaybeFillInDescriptor() bool {
	changedVersion := desc.maybeUpgradeFormatVersion()
	changedPrivileges := desc.Privileges.MaybeFixPrivileges(desc.ID)
	return changedVersion || changedPrivileges
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
	addFamilyForCol := func(col ColumnDescriptor) {
		if _, ok := primaryIndexColumnIds[col.ID]; ok {
			desc.Families[0].ColumnNames = append(desc.Families[0].ColumnNames, col.Name)
			desc.Families[0].ColumnIDs = append(desc.Families[0].ColumnIDs, col.ID)
			return
		}
		colNames := []string{col.Name}
		family := ColumnFamilyDescriptor{
			ID:              FamilyID(col.ID),
			Name:            FamilyID(col.ID).GeneratedFamilyName(colNames),
			ColumnNames:     colNames,
			ColumnIDs:       []ColumnID{col.ID},
			DefaultColumnID: col.ID,
		}
		desc.Families = append(desc.Families, family)
		if family.ID >= desc.NextFamilyID {
			desc.NextFamilyID = family.ID + 1
		}
	}

	for _, c := range desc.Columns {
		addFamilyForCol(c)
	}
	for _, m := range desc.Mutations {
		if c := m.GetColumn(); c != nil {
			addFamilyForCol(*c)
		}
	}

	desc.FormatVersion = FamilyFormatVersion

	return true
}

// IsNullable is part of the cat.Column interface.
func (desc *ColumnDescriptor) IsNullable() bool {
	return desc.Nullable
}

// ColID is part of the cat.Column interface.
func (desc *ColumnDescriptor) ColID() uint64 {
	return uint64(desc.ID)
}

// ColName is part of the cat.Column interface.
func (desc *ColumnDescriptor) ColName() tree.Name {
	return tree.Name(desc.Name)
}

// DatumType is part of the cat.Column interface.
func (desc *ColumnDescriptor) DatumType() types.T {
	return desc.Type.ToDatumType()
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
		return pgerror.NewErrorf(
			pgerror.CodeInvalidTableDefinitionError,
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
		return false, pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError,
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
func (desc *TableDescriptor) FindAllReferences() (map[descid.T]struct{}, error) {
	refs := map[ID]struct{}{}
	if err := desc.ForeachNonDropIndex(func(index *IndexDescriptor) error {
		for _, a := range index.Interleave.Ancestors {
			refs[a.TableID] = struct{}{}
		}
		for _, c := range index.InterleavedBy {
			refs[c.Table] = struct{}{}
		}

		if index.ForeignKey.IsSet() {
			to := index.ForeignKey.Table
			refs[to] = struct{}{}
		}

		for _, c := range index.ReferencedBy {
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
		cols[i] = c
	}
	return cols, nil
}

// FindColumnByName finds the column with the specified name. It returns
// an active column or a column from the mutation list. It returns true
// if the column is being dropped.
func (desc *TableDescriptor) FindColumnByName(name tree.Name) (ColumnDescriptor, bool, error) {
	for i, c := range desc.Columns {
		if c.Name == string(name) {
			return desc.Columns[i], false, nil
		}
	}
	for _, m := range desc.Mutations {
		if c := m.GetColumn(); c != nil {
			if c.Name == string(name) {
				return *c, m.Direction == DescriptorMutation_DROP, nil
			}
		}
	}
	return ColumnDescriptor{}, false, sqlerrors.NewUndefinedColumnError(string(name))
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
	for i, c := range desc.Columns {
		colIdxMap[c.ID] = i
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
func (desc *TableDescriptor) FindActiveColumnByName(name string) (ColumnDescriptor, error) {
	for _, c := range desc.Columns {
		if c.Name == name {
			return c, nil
		}
	}
	return ColumnDescriptor{}, sqlerrors.NewUndefinedColumnError(name)
}

// FindColumnByID finds the column with specified ID.
func (desc *TableDescriptor) FindColumnByID(id ColumnID) (*ColumnDescriptor, error) {
	for i, c := range desc.Columns {
		if c.ID == id {
			return &desc.Columns[i], nil
		}
	}
	for _, m := range desc.Mutations {
		if c := m.GetColumn(); c != nil {
			if c.ID == id {
				return c, nil
			}
		}
	}
	return nil, fmt.Errorf("column-id \"%d\" does not exist", id)
}

// FindActiveColumnByID finds the active column with specified ID.
func (desc *TableDescriptor) FindActiveColumnByID(id ColumnID) (*ColumnDescriptor, error) {
	for i, c := range desc.Columns {
		if c.ID == id {
			return &desc.Columns[i], nil
		}
	}
	return nil, fmt.Errorf("column-id \"%d\" does not exist", id)
}

// FindFamilyByID finds the family with specified ID.
func (desc *TableDescriptor) FindFamilyByID(id FamilyID) (*ColumnFamilyDescriptor, error) {
	for i, f := range desc.Families {
		if f.ID == id {
			return &desc.Families[i], nil
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
	for i, idx := range desc.Indexes {
		if idx.Name == name {
			return &desc.Indexes[i], false, nil
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

// FindNonDropPartitionByName returns the PartitionDescriptor and the
// IndexDescriptor that the partition with the specified name belongs to. If no
// such partition exists, an error is returned.
func (desc *TableDescriptor) FindNonDropPartitionByName(
	name string,
) (*PartitioningDescriptor, *IndexDescriptor, error) {
	var find func(p PartitioningDescriptor) *PartitioningDescriptor
	find = func(p PartitioningDescriptor) *PartitioningDescriptor {
		for _, l := range p.List {
			if l.Name == name {
				return &p
			}
			if s := find(l.Subpartitioning); s != nil {
				return s
			}
		}
		for _, r := range p.Range {
			if r.Name == name {
				return &p
			}
		}
		return nil
	}
	for _, idx := range desc.AllNonDropIndexes() {
		if p := find(idx.Partitioning); p != nil {
			return p, idx, nil
		}
	}
	return nil, nil, fmt.Errorf("partition %q does not exist", name)
}

// PrimaryKeyString returns the pretty-printed primary key declaration for a
// table descriptor.
func (desc *TableDescriptor) PrimaryKeyString() string {
	return fmt.Sprintf("PRIMARY KEY (%s)",
		desc.PrimaryIndex.ColNamesString(),
	)
}

// HasOldStoredColumns returns whether the index has stored columns in the old
// format (data encoded the same way as if they were in an implicit column).
func (desc *IndexDescriptor) HasOldStoredColumns() bool {
	return len(desc.ExtraColumnIDs) > 0 && len(desc.StoreColumnIDs) < len(desc.StoreColumnNames)
}

// SetID implements the DescriptorProto interface.
func (desc *DatabaseDescriptor) SetID(id descid.T) {
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
	if err := ValidateDescriptorName(desc.Name, "descriptor"); err != nil {
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
func (desc *Descriptor) GetID() descid.T {
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

// IsSet returns whether or not the foreign key actually references a table.
func (f ForeignKeyReference) IsSet() bool {
	return f.Table != 0
}

// InvalidateFKConstraints sets all FK constraints to un-validated.
func (desc *TableDescriptor) InvalidateFKConstraints() {
	// We don't use GetConstraintInfo because we want to edit the passed desc.
	if desc.PrimaryIndex.ForeignKey.IsSet() {
		desc.PrimaryIndex.ForeignKey.Validity = ConstraintValidity_Unvalidated
	}
	for i := range desc.Indexes {
		if desc.Indexes[i].ForeignKey.IsSet() {
			desc.Indexes[i].ForeignKey.Validity = ConstraintValidity_Unvalidated
		}
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
		return nil, pgerror.Wrapf(err, pgerror.CodeSyntaxError,
			"could not parse check constraint %s", cc.Expr)
	}

	colIDsUsed := make(map[ColumnID]struct{})
	visitFn := func(expr tree.Expr) (err error, recurse bool, newExpr tree.Expr) {
		if vBase, ok := expr.(tree.VarName); ok {
			v, err := vBase.NormalizeVarName()
			if err != nil {
				return err, false, nil
			}
			if c, ok := v.(*tree.ColumnItem); ok {
				col, dropped, err := desc.FindColumnByName(c.ColumnName)
				if err != nil || dropped {
					return pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
						"column %q not found for constraint %q",
						c.ColumnName, parsed.String()), false, nil
				}
				colIDsUsed[col.ID] = struct{}{}
			}
			return nil, false, v
		}
		return nil, true, expr
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

// HasDrainingNames returns true if a draining name exists.
func (desc *TableDescriptor) HasDrainingNames() bool {
	return len(desc.DrainingNames) > 0
}

// VisibleColumns returns all non hidden columns.
func (desc *TableDescriptor) VisibleColumns() []ColumnDescriptor {
	var cols []ColumnDescriptor
	for _, col := range desc.Columns {
		if !col.Hidden {
			cols = append(cols, col)
		}
	}
	return cols
}

// ColumnTypes returns the types of all columns.
func (desc *TableDescriptor) ColumnTypes() []ColumnType {
	return desc.ColumnTypesWithMutations(false)
}

// ColumnTypesWithMutations returns the types of all columns, optionally
// including mutation columns, which will be returned if the input bool is true.
func (desc *TableDescriptor) ColumnTypesWithMutations(mutations bool) []ColumnType {
	nCols := len(desc.Columns)
	if mutations {
		nCols += len(desc.Mutations)
	}
	types := make([]ColumnType, 0, nCols)
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

// Dropped returns true if the table is being dropped.
func (desc *TableDescriptor) Dropped() bool {
	return desc.State == TableDescriptor_DROP
}

// Adding returns true if the table is being added.
func (desc *TableDescriptor) Adding() bool {
	return desc.State == TableDescriptor_ADD
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
		if m.Direction == DescriptorMutation_DROP || col.NeedsBackfill() {
			return true
		}
	}
	return false
}

// NeedsBackfill returns true if adding the given column requires a
// backfill (dropping a column always requires a backfill).
func (desc *ColumnDescriptor) NeedsBackfill() bool {
	return desc.DefaultExpr != nil || !desc.Nullable || desc.IsComputed()
}

// FindIndexByID finds an index (active or inactive) with the specified ID.
// Must return a pointer to the IndexDescriptor in the TableDescriptor, so that
// callers can use returned values to modify the TableDesc.
func (desc *TableDescriptor) FindIndexByID(id IndexID) (*IndexDescriptor, error) {
	if desc.PrimaryIndex.ID == id {
		return &desc.PrimaryIndex, nil
	}
	for i, c := range desc.Indexes {
		if c.ID == id {
			return &desc.Indexes[i], nil
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
// The primary index has an index of 0 and the first secondary index (if it exists)
// has an index of 1.
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

// ErrIndexGCMutationsList is returned by FindIndexByID to signal that the
// index with the given ID does not have a descriptor and is in the garbage
// collected mutations list.
var ErrIndexGCMutationsList = errors.New("index in GC mutations list")

// NameResolutionResult implements the tree.NameResolutionResult interface.
func (*TableDescriptor) NameResolutionResult() {}

// SchemaMeta implements the tree.SchemaMeta interface.
func (DatabaseDescriptor) SchemaMeta() {}

// SchemaMeta implements the tree.SchemaMeta interface.
func (Descriptor) SchemaMeta() {}

// NameResolutionResult implements the tree.NameResolutionResult interface.
func (Descriptor) NameResolutionResult() {}

// ValidateDescriptorName validates that the input name is valid for a
// descriptor. typ is used to construct an error message.
func ValidateDescriptorName(name, typ string) error {
	if len(name) == 0 {
		return fmt.Errorf("empty %s name", typ)
	}
	// TODO(pmattis): Do we want to be more restrictive than this?
	return nil
}
