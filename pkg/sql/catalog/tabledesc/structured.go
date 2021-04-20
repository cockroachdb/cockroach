// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tabledesc

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
	"google.golang.org/protobuf/proto"
)

// Mutable is a custom type for TableDescriptors
// going through schema mutations.
type Mutable struct {
	wrapper

	// ClusterVersion represents the version of the table descriptor read from the store.
	ClusterVersion descpb.TableDescriptor
}

const (
	// PrimaryKeyIndexName is the name of the index for the primary key.
	PrimaryKeyIndexName = "primary"
	// SequenceColumnID is the ID of the sole column in a sequence.
	SequenceColumnID = 1
	// SequenceColumnName is the name of the sole column in a sequence.
	SequenceColumnName = "value"
)

// ErrMissingColumns indicates a table with no columns.
var ErrMissingColumns = errors.New("table must contain at least 1 column")

// ErrMissingPrimaryKey indicates a table with no primary key.
var ErrMissingPrimaryKey = errors.New("table must contain a primary key")

// ErrIndexGCMutationsList is returned by FindIndexWithID to signal that the
// index with the given ID does not have a descriptor and is in the garbage
// collected mutations list.
var ErrIndexGCMutationsList = errors.New("index in GC mutations list")

// PostDeserializationTableDescriptorChanges are a set of booleans to indicate
// which types of upgrades or fixes occurred when filling in the descriptor
// after deserialization.
type PostDeserializationTableDescriptorChanges struct {
	// UpgradedFormatVersion indicates that the FormatVersion was upgraded.
	UpgradedFormatVersion bool

	// FixedPrivileges indicates that the privileges were fixed.
	//
	// TODO(ajwerner): Determine whether this still needs to exist of can be
	// removed.
	FixedPrivileges bool

	// UpgradedForeignKeyRepresentation indicates that the foreign key
	// representation was upgraded.
	UpgradedForeignKeyRepresentation bool
}

// FindIndexPartitionByName searches this index descriptor for a partition whose name
// is the input and returns it, or nil if no match is found.
func FindIndexPartitionByName(
	desc *descpb.IndexDescriptor, name string,
) *descpb.PartitioningDescriptor {
	return desc.Partitioning.FindPartitionByName(name)
}

// DescriptorType returns the type of this descriptor.
func (desc *wrapper) DescriptorType() catalog.DescriptorType {
	return catalog.Table
}

// SetName implements the DescriptorProto interface.
func (desc *Mutable) SetName(name string) {
	desc.Name = name
}

// IsPartitionAllBy returns whether the table has a PARTITION ALL BY clause.
func (desc *wrapper) IsPartitionAllBy() bool {
	return desc.PartitionAllBy
}

// GetParentSchemaID returns the ParentSchemaID if the descriptor has
// one. If the descriptor was created before the field was added, then the
// descriptor belongs to a table under the `public` physical schema. The static
// public schema ID is returned in that case.
func (desc *wrapper) GetParentSchemaID() descpb.ID {
	parentSchemaID := desc.GetUnexposedParentSchemaID()
	if parentSchemaID == descpb.InvalidID {
		parentSchemaID = keys.PublicSchemaID
	}
	return parentSchemaID
}

// KeysPerRow returns the maximum number of keys used to encode a row for the
// given index. If a secondary index doesn't store any columns, then it only
// has one k/v pair, but if it stores some columns, it can return up to one
// k/v pair per family in the table, just like a primary index.
func (desc *wrapper) KeysPerRow(indexID descpb.IndexID) (int, error) {
	if desc.PrimaryIndex.ID == indexID {
		return len(desc.Families), nil
	}
	idx, err := desc.FindIndexWithID(indexID)
	if err != nil {
		return 0, err
	}
	if idx.NumStoredColumns() == 0 {
		return 1, nil
	}
	return len(desc.Families), nil
}

// buildIndexName sets desc.Name to a value that is not EqualName to any
// of tableDesc's indexes. allocateName roughly follows PostgreSQL's
// convention for automatically-named indexes.
func buildIndexName(tableDesc *Mutable, index catalog.Index) string {
	idx := index.IndexDesc()
	segments := make([]string, 0, len(idx.ColumnNames)+2)
	segments = append(segments, tableDesc.Name)
	segments = append(segments, idx.ColumnNames[idx.ExplicitColumnStartIdx():]...)
	if idx.Unique {
		segments = append(segments, "key")
	} else {
		segments = append(segments, "idx")
	}

	baseName := strings.Join(segments, "_")
	name := baseName
	for i := 1; ; i++ {
		foundIndex, _ := tableDesc.FindIndexWithName(name)
		if foundIndex == nil {
			break
		}
		name = fmt.Sprintf("%s%d", baseName, i)
	}

	return name
}

// AllActiveAndInactiveChecks returns all check constraints, including both
// "active" ones on the table descriptor which are being enforced for all
// writes, and "inactive" new checks constraints queued in the mutations list.
// Additionally,  if there are any dropped mutations queued inside the mutation
// list, those will not cancel any "active" or "inactive" mutations.
func (desc *wrapper) AllActiveAndInactiveChecks() []*descpb.TableDescriptor_CheckConstraint {
	// A check constraint could be both on the table descriptor and in the
	// list of mutations while the constraint is validated for existing rows. In
	// that case, the constraint is in the Validating state, and we avoid
	// including it twice. (Note that even though unvalidated check constraints
	// cannot be added as of 19.1, they can still exist if they were created under
	// previous versions.)
	checks := make([]*descpb.TableDescriptor_CheckConstraint, 0, len(desc.Checks))
	for _, c := range desc.Checks {
		// While a constraint is being validated for existing rows or being dropped,
		// the constraint is present both on the table descriptor and in the
		// mutations list in the Validating or Dropping state, so those constraints
		// are excluded here to avoid double-counting.
		if c.Validity != descpb.ConstraintValidity_Validating &&
			c.Validity != descpb.ConstraintValidity_Dropping {
			checks = append(checks, c)
		}
	}
	for _, m := range desc.Mutations {
		if c := m.GetConstraint(); c != nil && c.ConstraintType == descpb.ConstraintToUpdate_CHECK {
			// Any mutations that are dropped should be
			// excluded to avoid returning duplicates.
			if m.Direction != descpb.DescriptorMutation_DROP {
				checks = append(checks, &c.Check)
			}
		}
	}
	return checks
}

// GetColumnFamilyForShard returns the column family that a newly added shard column
// should be assigned to, given the set of columns it's computed from.
//
// This is currently the column family of the first column in the set of index columns.
func GetColumnFamilyForShard(desc *Mutable, idxColumns []string) string {
	for _, f := range desc.Families {
		for _, fCol := range f.ColumnNames {
			if fCol == idxColumns[0] {
				return f.Name
			}
		}
	}
	return ""
}

// AllActiveAndInactiveUniqueWithoutIndexConstraints returns all unique
// constraints that are not enforced by an index, including both "active"
// ones on the table descriptor which are being enforced for all writes, and
// "inactive" ones queued in the mutations list.
func (desc *wrapper) AllActiveAndInactiveUniqueWithoutIndexConstraints() []*descpb.UniqueWithoutIndexConstraint {
	ucs := make([]*descpb.UniqueWithoutIndexConstraint, 0, len(desc.UniqueWithoutIndexConstraints))
	for i := range desc.UniqueWithoutIndexConstraints {
		uc := &desc.UniqueWithoutIndexConstraints[i]
		// While a constraint is being validated for existing rows or being dropped,
		// the constraint is present both on the table descriptor and in the
		// mutations list in the Validating or Dropping state, so those constraints
		// are excluded here to avoid double-counting.
		if uc.Validity != descpb.ConstraintValidity_Validating &&
			uc.Validity != descpb.ConstraintValidity_Dropping {
			ucs = append(ucs, uc)
		}
	}
	for i := range desc.Mutations {
		if c := desc.Mutations[i].GetConstraint(); c != nil &&
			c.ConstraintType == descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX {
			ucs = append(ucs, &c.UniqueWithoutIndexConstraint)
		}
	}
	return ucs
}

// AllActiveAndInactiveForeignKeys returns all foreign keys, including both
// "active" ones on the index descriptor which are being enforced for all
// writes, and "inactive" ones queued in the mutations list. An error is
// returned if multiple foreign keys (including mutations) are found for the
// same index.
func (desc *wrapper) AllActiveAndInactiveForeignKeys() []*descpb.ForeignKeyConstraint {
	fks := make([]*descpb.ForeignKeyConstraint, 0, len(desc.OutboundFKs))
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		// While a constraint is being validated for existing rows or being dropped,
		// the constraint is present both on the table descriptor and in the
		// mutations list in the Validating or Dropping state, so those constraints
		// are excluded here to avoid double-counting.
		if fk.Validity != descpb.ConstraintValidity_Validating && fk.Validity != descpb.ConstraintValidity_Dropping {
			fks = append(fks, fk)
		}
	}
	for i := range desc.Mutations {
		if c := desc.Mutations[i].GetConstraint(); c != nil && c.ConstraintType == descpb.ConstraintToUpdate_FOREIGN_KEY {
			fks = append(fks, &c.ForeignKey)
		}
	}
	return fks
}

// ForeachDependedOnBy runs a function on all indexes, including those being
// added in the mutations.
func (desc *wrapper) ForeachDependedOnBy(
	f func(dep *descpb.TableDescriptor_Reference) error,
) error {
	for i := range desc.DependedOnBy {
		if err := f(&desc.DependedOnBy[i]); err != nil {
			return err
		}
	}
	return nil
}

// ForeachOutboundFK calls f for every outbound foreign key in desc until an
// error is returned.
func (desc *wrapper) ForeachOutboundFK(
	f func(constraint *descpb.ForeignKeyConstraint) error,
) error {
	for i := range desc.OutboundFKs {
		if err := f(&desc.OutboundFKs[i]); err != nil {
			return err
		}
	}
	return nil
}

// ForeachInboundFK calls f for every inbound foreign key in desc until an
// error is returned.
func (desc *wrapper) ForeachInboundFK(f func(fk *descpb.ForeignKeyConstraint) error) error {
	for i := range desc.InboundFKs {
		if err := f(&desc.InboundFKs[i]); err != nil {
			return err
		}
	}
	return nil
}

// NumFamilies returns the number of column families in the descriptor.
func (desc *wrapper) NumFamilies() int {
	return len(desc.Families)
}

// ForeachFamily calls f for every column family key in desc until an
// error is returned.
func (desc *wrapper) ForeachFamily(f func(family *descpb.ColumnFamilyDescriptor) error) error {
	for i := range desc.Families {
		if err := f(&desc.Families[i]); err != nil {
			return err
		}
	}
	return nil
}

func generatedFamilyName(familyID descpb.FamilyID, columnNames []string) string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "fam_%d", familyID)
	for _, n := range columnNames {
		buf.WriteString(`_`)
		buf.WriteString(n)
	}
	return buf.String()
}

func indexHasDeprecatedForeignKeyRepresentation(idx *descpb.IndexDescriptor) bool {
	return idx.ForeignKey.IsSet() || len(idx.ReferencedBy) > 0
}

// TableHasDeprecatedForeignKeyRepresentation returns true if the table is not
// dropped and any of the indexes on the table have deprecated foreign key
// representations.
func TableHasDeprecatedForeignKeyRepresentation(desc *descpb.TableDescriptor) bool {
	if desc.Dropped() {
		return false
	}
	if indexHasDeprecatedForeignKeyRepresentation(&desc.PrimaryIndex) {
		return true
	}
	for i := range desc.Indexes {
		if indexHasDeprecatedForeignKeyRepresentation(&desc.Indexes[i]) {
			return true
		}
	}
	return false
}

// ForEachExprStringInTableDesc runs a closure for each expression string
// within a TableDescriptor. The closure takes in a string pointer so that
// it can mutate the TableDescriptor if desired.
func ForEachExprStringInTableDesc(descI catalog.TableDescriptor, f func(expr *string) error) error {
	var desc *wrapper
	switch descV := descI.(type) {
	case *wrapper:
		desc = descV
	case *immutable:
		desc = &descV.wrapper
	case *Mutable:
		desc = &descV.wrapper
	default:
		return errors.AssertionFailedf("unexpected type of table %T", descI)
	}
	// Helpers for each schema element type that can contain an expression.
	doCol := func(c *descpb.ColumnDescriptor) error {
		if c.HasDefault() {
			if err := f(c.DefaultExpr); err != nil {
				return err
			}
		}
		if c.IsComputed() {
			if err := f(c.ComputeExpr); err != nil {
				return err
			}
		}
		return nil
	}
	doIndex := func(i catalog.Index) error {
		if i.IsPartial() {
			return f(&i.IndexDesc().Predicate)
		}
		return nil
	}
	doCheck := func(c *descpb.TableDescriptor_CheckConstraint) error {
		return f(&c.Expr)
	}

	// Process columns.
	for i := range desc.Columns {
		if err := doCol(&desc.Columns[i]); err != nil {
			return err
		}
	}

	// Process all indexes.
	if err := catalog.ForEachIndex(descI, catalog.IndexOpts{
		NonPhysicalPrimaryIndex: true,
		DropMutations:           true,
		AddMutations:            true,
	}, doIndex); err != nil {
		return err
	}

	// Process checks.
	for i := range desc.Checks {
		if err := doCheck(desc.Checks[i]); err != nil {
			return err
		}
	}

	// Process all non-index mutations.
	for _, mut := range desc.Mutations {
		if c := mut.GetColumn(); c != nil {
			if err := doCol(c); err != nil {
				return err
			}
		}
		if c := mut.GetConstraint(); c != nil &&
			c.ConstraintType == descpb.ConstraintToUpdate_CHECK {
			if err := doCheck(&c.Check); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetAllReferencedTypeIDs returns all user defined type descriptor IDs that
// this table references. It takes in a function that returns the TypeDescriptor
// with the desired ID.
func (desc *wrapper) GetAllReferencedTypeIDs(
	dbDesc catalog.DatabaseDescriptor, getType func(descpb.ID) (catalog.TypeDescriptor, error),
) (descpb.IDs, error) {
	ids, err := desc.getAllReferencedTypesInTableColumns(getType)
	if err != nil {
		return nil, err
	}

	// REGIONAL BY TABLE tables may have a dependency with the multi-region enum.
	exists := desc.GetMultiRegionEnumDependencyIfExists()
	if exists {
		regionEnumID, err := dbDesc.MultiRegionEnumID()
		if err != nil {
			return nil, err
		}
		ids[regionEnumID] = struct{}{}
	}

	// Add any other type dependencies that are not
	// used in a column (specifically for views).
	for _, id := range desc.DependsOnTypes {
		ids[id] = struct{}{}
	}

	// Construct the output.
	result := make(descpb.IDs, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}

	// Sort the output so that the order is deterministic.
	sort.Sort(result)
	return result, nil
}

// getAllReferencedTypesInTableColumns returns a map of all user defined
// type descriptor IDs that this table references. Consider using
// GetAllReferencedTypeIDs when constructing the list of type descriptor IDs
// referenced by a table -- being used by a column is a sufficient but not
// necessary condition for a table to reference a type.
// One example of a table having a type descriptor dependency but no column to
// show for it is a REGIONAL BY TABLE table (homed in the non-primary region).
// These use a value from the multi-region enum to denote the homing region, but
// do so in the locality config as opposed to through a column.
// GetAllReferencedTypesByID accounts for this dependency.
func (desc *wrapper) getAllReferencedTypesInTableColumns(
	getType func(descpb.ID) (catalog.TypeDescriptor, error),
) (map[descpb.ID]struct{}, error) {
	// All serialized expressions within a table descriptor are serialized
	// with type annotations as ID's, so this visitor will collect them all.
	visitor := &tree.TypeCollectorVisitor{
		OIDs: make(map[oid.Oid]struct{}),
	}

	addOIDsInExpr := func(exprStr *string) error {
		expr, err := parser.ParseExpr(*exprStr)
		if err != nil {
			return err
		}
		tree.WalkExpr(visitor, expr)
		return nil
	}

	if err := ForEachExprStringInTableDesc(desc, addOIDsInExpr); err != nil {
		return nil, err
	}

	// For each of the collected type IDs in the table descriptor expressions,
	// collect the closure of ID's referenced.
	ids := make(map[descpb.ID]struct{})
	for id := range visitor.OIDs {
		typDesc, err := getType(typedesc.UserDefinedTypeOIDToID(id))
		if err != nil {
			return nil, err
		}
		for child := range typDesc.GetIDClosure() {
			ids[child] = struct{}{}
		}
	}

	// Now add all of the column types in the table.
	addIDsInColumn := func(c *descpb.ColumnDescriptor) {
		for id := range typedesc.GetTypeDescriptorClosure(c.Type) {
			ids[id] = struct{}{}
		}
	}
	for i := range desc.Columns {
		addIDsInColumn(&desc.Columns[i])
	}
	for _, mut := range desc.Mutations {
		if c := mut.GetColumn(); c != nil {
			addIDsInColumn(c)
		}
	}

	return ids, nil
}

func (desc *Mutable) initIDs() {
	if desc.NextColumnID == 0 {
		desc.NextColumnID = 1
	}
	if desc.Version == 0 {
		desc.Version = 1
	}
	if desc.NextMutationID == descpb.InvalidMutationID {
		desc.NextMutationID = 1
	}
}

// MaybeFillColumnID assigns a column ID to the given column if the said column has an ID
// of 0.
func (desc *Mutable) MaybeFillColumnID(
	c *descpb.ColumnDescriptor, columnNames map[string]descpb.ColumnID,
) {
	desc.initIDs()

	columnID := c.ID
	if columnID == 0 {
		columnID = desc.NextColumnID
		desc.NextColumnID++
	}
	columnNames[c.Name] = columnID
	c.ID = columnID
}

// AllocateIDs allocates column, family, and index ids for any column, family,
// or index which has an ID of 0.
func (desc *Mutable) AllocateIDs(ctx context.Context) error {
	// Only tables with physical data can have / need a primary key.
	if desc.IsPhysicalTable() {
		if err := desc.ensurePrimaryKey(); err != nil {
			return err
		}
	}

	desc.initIDs()
	columnNames := map[string]descpb.ColumnID{}
	for i := range desc.Columns {
		desc.MaybeFillColumnID(&desc.Columns[i], columnNames)
	}
	for _, m := range desc.Mutations {
		if c := m.GetColumn(); c != nil {
			desc.MaybeFillColumnID(c, columnNames)
		}
	}

	// Only tables and materialized views can have / need indexes and column families.
	if desc.IsTable() || desc.MaterializedView() {
		if err := desc.allocateIndexIDs(columnNames); err != nil {
			return err
		}
		// Virtual tables don't have column families.
		if desc.IsPhysicalTable() {
			desc.allocateColumnFamilyIDs(columnNames)
		}
	}

	// This is sort of ugly. If the descriptor does not have an ID, we hack one in
	// to pass the table ID check. We use a non-reserved ID, reserved ones being set
	// before AllocateIDs.
	savedID := desc.ID
	if desc.ID == 0 {
		desc.ID = keys.MinUserDescID
	}
	err := catalog.ValidateSelf(desc)
	desc.ID = savedID
	return err
}

func (desc *Mutable) ensurePrimaryKey() error {
	if len(desc.PrimaryIndex.ColumnNames) == 0 && desc.IsPhysicalTable() {
		// Ensure a Primary Key exists.
		nameExists := func(name string) bool {
			_, err := desc.FindColumnWithName(tree.Name(name))
			return err == nil
		}
		s := "unique_rowid()"
		col := &descpb.ColumnDescriptor{
			Name:        GenerateUniqueConstraintName("rowid", nameExists),
			Type:        types.Int,
			DefaultExpr: &s,
			Hidden:      true,
			Nullable:    false,
		}
		desc.AddColumn(col)
		idx := descpb.IndexDescriptor{
			Unique:           true,
			ColumnNames:      []string{col.Name},
			ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
		}
		if err := desc.AddIndex(idx, true); err != nil {
			return err
		}
	}
	return nil
}

func (desc *Mutable) allocateIndexIDs(columnNames map[string]descpb.ColumnID) error {
	if desc.NextIndexID == 0 {
		desc.NextIndexID = 1
	}

	// Assign names to unnamed indexes.
	_ = catalog.ForEachDeletableNonPrimaryIndex(desc, func(idx catalog.Index) error {
		if len(idx.GetName()) == 0 {
			idx.IndexDesc().Name = buildIndexName(desc, idx)
		}
		return nil
	})

	var compositeColIDs catalog.TableColSet
	for i := range desc.Columns {
		col := &desc.Columns[i]
		if colinfo.HasCompositeKeyEncoding(col.Type) {
			compositeColIDs.Add(col.ID)
		}
	}

	// Populate IDs.
	for _, idx := range desc.AllIndexes() {
		if idx.GetID() != 0 {
			// This index has already been populated. Nothing to do.
			continue
		}
		index := idx.IndexDesc()
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

		if !idx.Primary() && index.EncodingType == descpb.SecondaryIndexEncoding {
			indexHasOldStoredColumns := index.HasOldStoredColumns()
			// Need to clear ExtraColumnIDs and StoreColumnIDs because they are used
			// by ContainsColumnID.
			index.ExtraColumnIDs = nil
			index.StoreColumnIDs = nil
			var extraColumnIDs []descpb.ColumnID
			for _, primaryColID := range desc.PrimaryIndex.ColumnIDs {
				if !index.ContainsColumnID(primaryColID) {
					extraColumnIDs = append(extraColumnIDs, primaryColID)
				}
			}
			index.ExtraColumnIDs = extraColumnIDs

			for _, colName := range index.StoreColumnNames {
				col, err := desc.FindColumnWithName(tree.Name(colName))
				if err != nil {
					return err
				}
				if desc.PrimaryIndex.ContainsColumnID(col.GetID()) {
					// If the primary index contains a stored column, we don't need to
					// store it - it's already part of the index.
					err = pgerror.Newf(
						pgcode.DuplicateColumn, "index %q already contains column %q", index.Name, col.GetName())
					err = errors.WithDetailf(err, "column %q is part of the primary index and therefore implicit in all indexes", col.GetName())
					return err
				}
				if index.ContainsColumnID(col.GetID()) {
					return pgerror.Newf(
						pgcode.DuplicateColumn,
						"index %q already contains column %q", index.Name, col.GetName())
				}
				if indexHasOldStoredColumns {
					index.ExtraColumnIDs = append(index.ExtraColumnIDs, col.GetID())
				} else {
					index.StoreColumnIDs = append(index.StoreColumnIDs, col.GetID())
				}
			}
		}

		index.CompositeColumnIDs = nil
		for _, colID := range index.ColumnIDs {
			if compositeColIDs.Contains(colID) {
				index.CompositeColumnIDs = append(index.CompositeColumnIDs, colID)
			}
		}
		for _, colID := range index.ExtraColumnIDs {
			if compositeColIDs.Contains(colID) {
				index.CompositeColumnIDs = append(index.CompositeColumnIDs, colID)
			}
		}
	}
	return nil
}

func (desc *Mutable) allocateColumnFamilyIDs(columnNames map[string]descpb.ColumnID) {
	if desc.NextFamilyID == 0 {
		if len(desc.Families) == 0 {
			desc.Families = []descpb.ColumnFamilyDescriptor{
				{ID: 0, Name: "primary"},
			}
		}
		desc.NextFamilyID = 1
	}

	var columnsInFamilies catalog.TableColSet
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
			columnsInFamilies.Add(family.ColumnIDs[j])
		}

		desc.Families[i] = *family
	}

	var primaryIndexColIDs catalog.TableColSet
	for _, colID := range desc.PrimaryIndex.ColumnIDs {
		primaryIndexColIDs.Add(colID)
	}

	ensureColumnInFamily := func(col *descpb.ColumnDescriptor) {
		if col.Virtual {
			// Virtual columns don't need to be part of families.
			return
		}
		if columnsInFamilies.Contains(col.ID) {
			return
		}
		if primaryIndexColIDs.Contains(col.ID) {
			// Primary index columns are required to be assigned to family 0.
			desc.Families[0].ColumnNames = append(desc.Families[0].ColumnNames, col.Name)
			desc.Families[0].ColumnIDs = append(desc.Families[0].ColumnIDs, col.ID)
			return
		}
		var familyID descpb.FamilyID
		if desc.ParentID == keys.SystemDatabaseID {
			// TODO(dan): This assigns families such that the encoding is exactly the
			// same as before column families. It's used for all system tables because
			// reads of them don't go through the normal sql layer, which is where the
			// knowledge of families lives. Fix that and remove this workaround.
			familyID = descpb.FamilyID(col.ID)
			desc.Families = append(desc.Families, descpb.ColumnFamilyDescriptor{
				ID:          familyID,
				ColumnNames: []string{col.Name},
				ColumnIDs:   []descpb.ColumnID{col.ID},
			})
		} else {
			idx, ok := fitColumnToFamily(desc, *col)
			if !ok {
				idx = len(desc.Families)
				desc.Families = append(desc.Families, descpb.ColumnFamilyDescriptor{
					ID:          desc.NextFamilyID,
					ColumnNames: []string{},
					ColumnIDs:   []descpb.ColumnID{},
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
			defaultColumnID := descpb.ColumnID(0)
			for _, colID := range family.ColumnIDs {
				if !primaryIndexColIDs.Contains(colID) {
					if defaultColumnID == 0 {
						defaultColumnID = colID
					} else {
						defaultColumnID = descpb.ColumnID(0)
						break
					}
				}
			}
			family.DefaultColumnID = defaultColumnID
		}

		desc.Families[i] = *family
	}
}

// fitColumnToFamily attempts to fit a new column into the existing column
// families. If the heuristics find a fit, true is returned along with the
// index of the selected family. Otherwise, false is returned and the column
// should be put in a new family.
//
// Current heuristics:
//  - Put all columns in family 0.
func fitColumnToFamily(desc *Mutable, col descpb.ColumnDescriptor) (int, bool) {
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

// MaybeIncrementVersion implements the MutableDescriptor interface.
func (desc *Mutable) MaybeIncrementVersion() {
	// Already incremented, no-op.
	if desc.Version == desc.ClusterVersion.Version+1 || desc.ClusterVersion.Version == 0 {
		return
	}
	desc.Version++

	// Starting in 19.2 we use a zero-valued ModificationTime when incrementing
	// the version, and then, upon reading, use the MVCC timestamp to populate
	// the ModificationTime.
	desc.ModificationTime = hlc.Timestamp{}
}

// OriginalName implements the MutableDescriptor interface.
func (desc *Mutable) OriginalName() string {
	return desc.ClusterVersion.Name
}

// OriginalID implements the MutableDescriptor interface.
func (desc *Mutable) OriginalID() descpb.ID {
	return desc.ClusterVersion.ID
}

// OriginalVersion implements the MutableDescriptor interface.
func (desc *Mutable) OriginalVersion() descpb.DescriptorVersion {
	return desc.ClusterVersion.Version
}

// FormatTableLocalityConfig formats the table locality.
func FormatTableLocalityConfig(c *descpb.TableDescriptor_LocalityConfig, f *tree.FmtCtx) error {
	switch v := c.Locality.(type) {
	case *descpb.TableDescriptor_LocalityConfig_Global_:
		f.WriteString("GLOBAL")
	case *descpb.TableDescriptor_LocalityConfig_RegionalByTable_:
		f.WriteString("REGIONAL BY TABLE IN ")
		if v.RegionalByTable.Region != nil {
			region := tree.Name(*v.RegionalByTable.Region)
			f.FormatNode(&region)
		} else {
			f.WriteString("PRIMARY REGION")
		}
	case *descpb.TableDescriptor_LocalityConfig_RegionalByRow_:
		f.WriteString("REGIONAL BY ROW")
		if v.RegionalByRow.As != nil {
			f.WriteString(" AS ")
			col := tree.Name(*v.RegionalByRow.As)
			f.FormatNode(&col)
		}
	default:
		return errors.Newf("unknown locality: %T", v)
	}
	return nil
}

// ValidateIndexNameIsUnique validates that the index name does not exist.
func (desc *wrapper) ValidateIndexNameIsUnique(indexName string) error {
	if catalog.FindNonDropIndex(desc, func(idx catalog.Index) bool {
		return idx.GetName() == indexName
	}) != nil {
		return sqlerrors.NewRelationAlreadyExistsError(indexName)
	}
	return nil
}

// PrimaryKeyString returns the pretty-printed primary key declaration for a
// table descriptor.
func (desc *wrapper) PrimaryKeyString() string {
	var primaryKeyString strings.Builder
	primaryKeyString.WriteString("PRIMARY KEY (%s)")
	if desc.PrimaryIndex.IsSharded() {
		fmt.Fprintf(&primaryKeyString, " USING HASH WITH BUCKET_COUNT = %v",
			desc.PrimaryIndex.Sharded.ShardBuckets)
	}
	return fmt.Sprintf(primaryKeyString.String(),
		desc.PrimaryIndex.ColNamesString(),
	)
}

// FamilyHeuristicTargetBytes is the target total byte size of columns that the
// current heuristic will assign to a family.
const FamilyHeuristicTargetBytes = 256

func notIndexableError(cols []descpb.ColumnDescriptor) error {
	if len(cols) == 0 {
		return nil
	}
	var msg string
	var typInfo string
	if len(cols) == 1 {
		col := &cols[0]
		msg = "column %s is of type %s and thus is not indexable"
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
	return unimplemented.NewWithIssueDetailf(35730, typInfo, "%s", msg)
}

func checkColumnsValidForIndex(tableDesc *Mutable, indexColNames []string) error {
	invalidColumns := make([]descpb.ColumnDescriptor, 0, len(indexColNames))
	for _, indexCol := range indexColNames {
		for _, col := range tableDesc.NonDropColumns() {
			if col.GetName() == indexCol {
				if !colinfo.ColumnTypeIsIndexable(col.GetType()) {
					invalidColumns = append(invalidColumns, *col.ColumnDesc())
				}
			}
		}
	}
	if len(invalidColumns) > 0 {
		return notIndexableError(invalidColumns)
	}
	return nil
}

func checkColumnsValidForInvertedIndex(tableDesc *Mutable, indexColNames []string) error {
	lastCol := len(indexColNames) - 1
	for i, indexCol := range indexColNames {
		for _, col := range tableDesc.NonDropColumns() {
			if col.GetName() == indexCol {
				// The last column indexed by an inverted index must be
				// inverted indexable.
				if i == lastCol && !colinfo.ColumnTypeIsInvertedIndexable(col.GetType()) {
					return errors.WithHint(
						pgerror.Newf(
							pgcode.FeatureNotSupported,
							"column %s of type %s is not allowed as the last column in an inverted index",
							col.GetName(),
							col.GetType().Name(),
						),
						"see the documentation for more information about inverted indexes",
					)

				}
				// Any preceding columns must not be inverted indexable.
				if i < lastCol && !colinfo.ColumnTypeIsIndexable(col.GetType()) {
					return errors.WithHint(
						pgerror.Newf(
							pgcode.FeatureNotSupported,
							"column %s of type %s is only allowed as the last column in an inverted index",
							col.GetName(),
							col.GetType().Name(),
						),
						"see the documentation for more information about inverted indexes",
					)
				}
			}
		}
	}
	return nil
}

// AddColumn adds a column to the table.
func (desc *Mutable) AddColumn(col *descpb.ColumnDescriptor) {
	desc.Columns = append(desc.Columns, *col)
}

// AddFamily adds a family to the table.
func (desc *Mutable) AddFamily(fam descpb.ColumnFamilyDescriptor) {
	desc.Families = append(desc.Families, fam)
}

// AddIndex adds an index to the table.
func (desc *Mutable) AddIndex(idx descpb.IndexDescriptor, primary bool) error {
	if idx.Type == descpb.IndexDescriptor_FORWARD {
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
				desc.SetPrimaryIndex(idx)
			} else {
				return fmt.Errorf("multiple primary keys for table %q are not allowed", desc.Name)
			}
		} else {
			desc.AddPublicNonPrimaryIndex(idx)
		}

	} else {
		if err := checkColumnsValidForInvertedIndex(desc, idx.ColumnNames); err != nil {
			return err
		}
		desc.AddPublicNonPrimaryIndex(idx)
	}

	return nil
}

// AddColumnToFamilyMaybeCreate adds the specified column to the specified
// family. If it doesn't exist and create is true, creates it. If it does exist
// adds it unless "strict" create (`true` for create but `false` for
// ifNotExists) is specified.
//
// AllocateIDs must be called before the TableDescriptor will be valid.
func (desc *Mutable) AddColumnToFamilyMaybeCreate(
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
			desc.AddFamily(descpb.ColumnFamilyDescriptor{Name: family, ColumnNames: []string{col}})
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
func (desc *Mutable) RemoveColumnFromFamily(colID descpb.ColumnID) {
	for i := range desc.Families {
		for j, c := range desc.Families[i].ColumnIDs {
			if c == colID {
				desc.Families[i].ColumnIDs = append(
					desc.Families[i].ColumnIDs[:j], desc.Families[i].ColumnIDs[j+1:]...)
				desc.Families[i].ColumnNames = append(
					desc.Families[i].ColumnNames[:j], desc.Families[i].ColumnNames[j+1:]...)
				// Due to a complication with allowing primary key columns to not be restricted
				// to family 0, we might end up deleting all the columns from family 0. We will
				// allow empty column families now, but will disallow this in the future.
				// TODO (rohany): remove this once the reliance on sentinel family 0 has been removed.
				if len(desc.Families[i].ColumnIDs) == 0 && desc.Families[i].ID != 0 {
					desc.Families = append(desc.Families[:i], desc.Families[i+1:]...)
				}
				return
			}
		}
	}
}

// RenameColumnDescriptor updates all references to a column name in
// a table descriptor including indexes and families.
func (desc *Mutable) RenameColumnDescriptor(column *descpb.ColumnDescriptor, newColName string) {
	colID := column.ID
	column.Name = newColName

	for i := range desc.Families {
		for j := range desc.Families[i].ColumnIDs {
			if desc.Families[i].ColumnIDs[j] == colID {
				desc.Families[i].ColumnNames[j] = newColName
			}
		}
	}

	for _, idx := range desc.AllIndexes() {
		idxDesc := idx.IndexDesc()
		for i, id := range idxDesc.ColumnIDs {
			if id == colID {
				idxDesc.ColumnNames[i] = newColName
			}
		}
		for i, id := range idxDesc.StoreColumnIDs {
			if id == colID {
				idxDesc.StoreColumnNames[i] = newColName
			}
		}
	}
}

// FindActiveOrNewColumnByName finds the column with the specified name.
// It returns either an active column or a column that was added in the
// same transaction that is currently running.
func (desc *Mutable) FindActiveOrNewColumnByName(name tree.Name) (catalog.Column, error) {
	currentMutationID := desc.ClusterVersion.NextMutationID
	for _, col := range desc.DeletableColumns() {
		if (col.Public() && col.ColName() == name) ||
			(col.Adding() && col.MutationID() == currentMutationID) {
			return col, nil
		}
	}
	return nil, colinfo.NewUndefinedColumnError(string(name))
}

// ContainsUserDefinedTypes returns whether or not this table descriptor has
// any columns of user defined types.
func (desc *wrapper) ContainsUserDefinedTypes() bool {
	return len(desc.UserDefinedTypeColumns()) > 0
}

// FindFamilyByID finds the family with specified ID.
func (desc *wrapper) FindFamilyByID(id descpb.FamilyID) (*descpb.ColumnFamilyDescriptor, error) {
	for i := range desc.Families {
		family := &desc.Families[i]
		if family.ID == id {
			return family, nil
		}
	}
	return nil, fmt.Errorf("family-id \"%d\" does not exist", id)
}

// NamesForColumnIDs returns the names for the given column ids, or an error
// if one or more column ids was missing. Note - this allocates! It's not for
// hot path code.
func (desc *wrapper) NamesForColumnIDs(ids descpb.ColumnIDs) ([]string, error) {
	names := make([]string, len(ids))
	for i, id := range ids {
		col, err := desc.FindColumnWithID(id)
		if err != nil {
			return nil, err
		}
		names[i] = col.GetName()
	}
	return names, nil
}

// RenameIndexDescriptor renames an index descriptor.
func (desc *Mutable) RenameIndexDescriptor(index *descpb.IndexDescriptor, name string) error {
	id := index.ID
	if id == desc.PrimaryIndex.ID {
		idx := desc.PrimaryIndex
		idx.Name = name
		desc.SetPrimaryIndex(idx)
		return nil
	}
	for i, idx := range desc.Indexes {
		if idx.ID == id {
			idx.Name = name
			desc.SetPublicNonPrimaryIndex(i+1, idx)
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
func (desc *Mutable) DropConstraint(
	ctx context.Context,
	name string,
	detail descpb.ConstraintDetail,
	removeFK func(*Mutable, *descpb.ForeignKeyConstraint) error,
	settings *cluster.Settings,
) error {
	switch detail.Kind {
	case descpb.ConstraintTypePK:
		{
			primaryIndex := desc.PrimaryIndex
			primaryIndex.Disabled = true
			desc.SetPrimaryIndex(primaryIndex)
		}
		return nil

	case descpb.ConstraintTypeUnique:
		if detail.Index != nil {
			return unimplemented.NewWithIssueDetailf(42840, "drop-constraint-unique",
				"cannot drop UNIQUE constraint %q using ALTER TABLE DROP CONSTRAINT, use DROP INDEX CASCADE instead",
				tree.ErrNameStringP(&detail.Index.Name))
		}
		if detail.UniqueWithoutIndexConstraint == nil {
			return errors.AssertionFailedf(
				"Index or UniqueWithoutIndexConstraint must be non-nil for a unique constraint",
			)
		}
		if detail.UniqueWithoutIndexConstraint.Validity == descpb.ConstraintValidity_Validating {
			return unimplemented.NewWithIssueDetailf(42844,
				"drop-constraint-unique-validating",
				"constraint %q in the middle of being added, try again later", name)
		}
		if detail.UniqueWithoutIndexConstraint.Validity == descpb.ConstraintValidity_Dropping {
			return unimplemented.NewWithIssueDetailf(42844,
				"drop-constraint-unique-mutation",
				"constraint %q in the middle of being dropped", name)
		}
		// Search through the descriptor's unique constraints and delete the
		// one that we're supposed to be deleting.
		for i := range desc.UniqueWithoutIndexConstraints {
			ref := &desc.UniqueWithoutIndexConstraints[i]
			if ref.Name == name {
				// If the constraint is unvalidated, there's no assumption that it must
				// hold for all rows, so it can be dropped immediately.
				if detail.UniqueWithoutIndexConstraint.Validity == descpb.ConstraintValidity_Unvalidated {
					desc.UniqueWithoutIndexConstraints = append(
						desc.UniqueWithoutIndexConstraints[:i], desc.UniqueWithoutIndexConstraints[i+1:]...,
					)
					return nil
				}
				ref.Validity = descpb.ConstraintValidity_Dropping
				desc.AddUniqueWithoutIndexMutation(ref, descpb.DescriptorMutation_DROP)
				return nil
			}
		}

	case descpb.ConstraintTypeCheck:
		if detail.CheckConstraint.Validity == descpb.ConstraintValidity_Validating {
			return unimplemented.NewWithIssueDetailf(42844, "drop-constraint-check-mutation",
				"constraint %q in the middle of being added, try again later", name)
		}
		if detail.CheckConstraint.Validity == descpb.ConstraintValidity_Dropping {
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
				if detail.CheckConstraint.Validity == descpb.ConstraintValidity_Unvalidated {
					desc.Checks = append(desc.Checks[:i], desc.Checks[i+1:]...)
					return nil
				}
				c.Validity = descpb.ConstraintValidity_Dropping
				desc.AddCheckMutation(c, descpb.DescriptorMutation_DROP)
				return nil
			}
		}

	case descpb.ConstraintTypeFK:
		if detail.FK.Validity == descpb.ConstraintValidity_Validating {
			return unimplemented.NewWithIssueDetailf(42844,
				"drop-constraint-fk-validating",
				"constraint %q in the middle of being added, try again later", name)
		}
		if detail.FK.Validity == descpb.ConstraintValidity_Dropping {
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
				if detail.FK.Validity == descpb.ConstraintValidity_Unvalidated {
					// Remove the backreference.
					if err := removeFK(desc, detail.FK); err != nil {
						return err
					}
					desc.OutboundFKs = append(desc.OutboundFKs[:i], desc.OutboundFKs[i+1:]...)
					return nil
				}
				ref.Validity = descpb.ConstraintValidity_Dropping
				desc.AddForeignKeyMutation(ref, descpb.DescriptorMutation_DROP)
				return nil
			}
		}

	default:
		return unimplemented.Newf(fmt.Sprintf("drop-constraint-%s", detail.Kind),
			"constraint %q has unsupported type", tree.ErrNameString(name))
	}

	// Check if the constraint can be found in a mutation, complain appropriately.
	for i := range desc.Mutations {
		m := &desc.Mutations[i]
		if m.GetConstraint() != nil && m.GetConstraint().Name == name {
			switch m.Direction {
			case descpb.DescriptorMutation_ADD:
				return unimplemented.NewWithIssueDetailf(42844,
					"drop-constraint-mutation",
					"constraint %q in the middle of being added, try again later", name)
			case descpb.DescriptorMutation_DROP:
				return unimplemented.NewWithIssueDetailf(42844,
					"drop-constraint-mutation",
					"constraint %q in the middle of being dropped", name)
			}
		}
	}
	return errors.AssertionFailedf("constraint %q not found on table %q", name, desc.Name)
}

// RenameConstraint renames a constraint.
func (desc *Mutable) RenameConstraint(
	detail descpb.ConstraintDetail,
	oldName, newName string,
	dependentViewRenameError func(string, descpb.ID) error,
	renameFK func(*Mutable, *descpb.ForeignKeyConstraint, string) error,
) error {
	switch detail.Kind {
	case descpb.ConstraintTypePK:
		for _, tableRef := range desc.DependedOnBy {
			if tableRef.IndexID != detail.Index.ID {
				continue
			}
			return dependentViewRenameError("index", tableRef.ID)
		}
		return desc.RenameIndexDescriptor(detail.Index, newName)

	case descpb.ConstraintTypeUnique:
		if detail.Index != nil {
			for _, tableRef := range desc.DependedOnBy {
				if tableRef.IndexID != detail.Index.ID {
					continue
				}
				return dependentViewRenameError("index", tableRef.ID)
			}
			if err := desc.RenameIndexDescriptor(detail.Index, newName); err != nil {
				return err
			}
		} else if detail.UniqueWithoutIndexConstraint != nil {
			if detail.UniqueWithoutIndexConstraint.Validity == descpb.ConstraintValidity_Validating {
				return unimplemented.NewWithIssueDetailf(42844,
					"rename-constraint-unique-mutation",
					"constraint %q in the middle of being added, try again later",
					tree.ErrNameStringP(&detail.UniqueWithoutIndexConstraint.Name))
			}
			detail.UniqueWithoutIndexConstraint.Name = newName
		} else {
			return errors.AssertionFailedf(
				"Index or UniqueWithoutIndexConstraint must be non-nil for a unique constraint",
			)
		}
		return nil

	case descpb.ConstraintTypeFK:
		if detail.FK.Validity == descpb.ConstraintValidity_Validating {
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

	case descpb.ConstraintTypeCheck:
		if detail.CheckConstraint.Validity == descpb.ConstraintValidity_Validating {
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

// GetIndexMutationCapabilities returns:
// 1. Whether the index is a mutation
// 2. if so, is it in state DELETE_AND_WRITE_ONLY
func (desc *wrapper) GetIndexMutationCapabilities(id descpb.IndexID) (bool, bool) {
	for _, mutation := range desc.Mutations {
		if mutationIndex := mutation.GetIndex(); mutationIndex != nil {
			if mutationIndex.ID == id {
				return true,
					mutation.State == descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
			}
		}
	}
	return false, false
}

// FindFKByName returns the FK constraint on the table with the given name.
// Must return a pointer to the FK in the TableDescriptor, so that
// callers can use returned values to modify the TableDesc.
func (desc *wrapper) FindFKByName(name string) (*descpb.ForeignKeyConstraint, error) {
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		if fk.Name == name {
			return fk, nil
		}
	}
	return nil, fmt.Errorf("fk %q does not exist", name)
}

// IsInterleaved returns true if any part of this this table is interleaved with
// another table's data.
func (desc *wrapper) IsInterleaved() bool {
	return nil != catalog.FindNonDropIndex(desc, func(idx catalog.Index) bool {
		return idx.IsInterleaved()
	})
}

// IsPrimaryIndexDefaultRowID returns whether or not the table's primary
// index is the default primary key on the hidden rowid column.
func (desc *wrapper) IsPrimaryIndexDefaultRowID() bool {
	if len(desc.PrimaryIndex.ColumnIDs) != 1 {
		return false
	}
	col, err := desc.FindColumnWithID(desc.PrimaryIndex.ColumnIDs[0])
	if err != nil {
		// Should never be in this case.
		panic(err)
	}
	return col.IsHidden()
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
func (desc *Mutable) MakeMutationComplete(m descpb.DescriptorMutation) error {
	switch m.Direction {
	case descpb.DescriptorMutation_ADD:
		switch t := m.Descriptor_.(type) {
		case *descpb.DescriptorMutation_Column:
			desc.AddColumn(t.Column)

		case *descpb.DescriptorMutation_Index:
			if err := desc.AddIndex(*t.Index, false); err != nil {
				return err
			}

		case *descpb.DescriptorMutation_Constraint:
			switch t.Constraint.ConstraintType {
			case descpb.ConstraintToUpdate_CHECK:
				switch t.Constraint.Check.Validity {
				case descpb.ConstraintValidity_Validating:
					// Constraint already added, just mark it as Validated.
					for _, c := range desc.Checks {
						if c.Name == t.Constraint.Name {
							c.Validity = descpb.ConstraintValidity_Validated
							break
						}
					}
				case descpb.ConstraintValidity_Unvalidated:
					// add the constraint to the list of check constraints on the table
					// descriptor.
					desc.Checks = append(desc.Checks, &t.Constraint.Check)
				default:
					return errors.AssertionFailedf("invalid constraint validity state: %d", t.Constraint.Check.Validity)
				}
			case descpb.ConstraintToUpdate_FOREIGN_KEY:
				switch t.Constraint.ForeignKey.Validity {
				case descpb.ConstraintValidity_Validating:
					// Constraint already added, just mark it as Validated.
					for i := range desc.OutboundFKs {
						fk := &desc.OutboundFKs[i]
						if fk.Name == t.Constraint.Name {
							fk.Validity = descpb.ConstraintValidity_Validated
							break
						}
					}
				case descpb.ConstraintValidity_Unvalidated:
					// Takes care of adding the Foreign Key to the table index. Adding the
					// backreference to the referenced table index must be taken care of
					// in another call.
					// TODO (tyler): Combine both of these tasks in the same place.
					desc.OutboundFKs = append(desc.OutboundFKs, t.Constraint.ForeignKey)
				}
			case descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX:
				switch t.Constraint.UniqueWithoutIndexConstraint.Validity {
				case descpb.ConstraintValidity_Validating:
					// Constraint already added, just mark it as Validated.
					for i := range desc.UniqueWithoutIndexConstraints {
						uc := &desc.UniqueWithoutIndexConstraints[i]
						if uc.Name == t.Constraint.Name {
							uc.Validity = descpb.ConstraintValidity_Validated
							break
						}
					}
				case descpb.ConstraintValidity_Unvalidated:
					// add the constraint to the list of unique without index constraints
					// on the table descriptor.
					desc.UniqueWithoutIndexConstraints = append(
						desc.UniqueWithoutIndexConstraints, t.Constraint.UniqueWithoutIndexConstraint,
					)
				default:
					return errors.AssertionFailedf("invalid constraint validity state: %d",
						t.Constraint.UniqueWithoutIndexConstraint.Validity,
					)
				}
			case descpb.ConstraintToUpdate_NOT_NULL:
				// Remove the dummy check constraint that was in place during
				// validation.
				for i, c := range desc.Checks {
					if c.Name == t.Constraint.Check.Name {
						desc.Checks = append(desc.Checks[:i], desc.Checks[i+1:]...)
					}
				}
				col, err := desc.FindColumnWithID(t.Constraint.NotNullColumn)
				if err != nil {
					return err
				}
				col.ColumnDesc().Nullable = false
			default:
				return errors.Errorf("unsupported constraint type: %d", t.Constraint.ConstraintType)
			}
		case *descpb.DescriptorMutation_PrimaryKeySwap:
			args := t.PrimaryKeySwap
			getIndexIdxByID := func(id descpb.IndexID) (int, error) {
				for i, idx := range desc.Indexes {
					if idx.ID == id {
						return i + 1, nil
					}
				}
				return 0, errors.New("index was not in list of indexes")
			}

			// Update the old primary index's descriptor to denote that it uses the primary
			// index encoding and stores all columns. This ensures that it will be properly
			// encoded and decoded when it is accessed after it is no longer the primary key
			// but before it is dropped entirely during the index drop process.
			primaryIndexCopy := protoutil.Clone(&desc.PrimaryIndex).(*descpb.IndexDescriptor)
			primaryIndexCopy.EncodingType = descpb.PrimaryIndexEncoding
			for _, col := range desc.Columns {
				containsCol := false
				for _, colID := range primaryIndexCopy.ColumnIDs {
					if colID == col.ID {
						containsCol = true
						break
					}
				}
				if !containsCol {
					primaryIndexCopy.StoreColumnIDs = append(primaryIndexCopy.StoreColumnIDs, col.ID)
					primaryIndexCopy.StoreColumnNames = append(primaryIndexCopy.StoreColumnNames, col.Name)
				}
			}
			// Move the old primary index from the table descriptor into the mutations queue
			// to schedule it for deletion.
			if err := desc.AddIndexMutation(primaryIndexCopy, descpb.DescriptorMutation_DROP); err != nil {
				return err
			}

			// Promote the new primary index into the primary index position on the descriptor,
			// and remove it from the secondary indexes list.
			newIndex, err := desc.FindIndexWithID(args.NewPrimaryIndexId)
			if err != nil {
				return err
			}

			{
				primaryIndex := newIndex.IndexDescDeepCopy()
				if args.NewPrimaryIndexName == "" {
					primaryIndex.Name = PrimaryKeyIndexName
				} else {
					primaryIndex.Name = args.NewPrimaryIndexName
				}
				// The primary index "implicitly" stores all columns in the table.
				// Explicitly including them in the stored columns list is incorrect.
				primaryIndex.StoreColumnNames, primaryIndex.StoreColumnIDs = nil, nil
				desc.SetPrimaryIndex(primaryIndex)
			}

			idx, err := getIndexIdxByID(newIndex.GetID())
			if err != nil {
				return err
			}
			desc.RemovePublicNonPrimaryIndex(idx)

			// Swap out the old indexes with their rewritten versions.
			for j := range args.OldIndexes {
				oldID := args.OldIndexes[j]
				newID := args.NewIndexes[j]
				// All our new indexes have been inserted into the table descriptor by now, since the primary key swap
				// is the last mutation processed in a group of mutations under the same mutation ID.
				newIndex, err := desc.FindIndexWithID(newID)
				if err != nil {
					return err
				}
				oldIndexIdx, err := getIndexIdxByID(oldID)
				if err != nil {
					return err
				}
				if oldIndexIdx >= len(desc.ActiveIndexes()) {
					return errors.Errorf("invalid indexIdx %d", oldIndexIdx)
				}
				oldIndex := desc.ActiveIndexes()[oldIndexIdx].IndexDesc()
				oldIndexCopy := protoutil.Clone(oldIndex).(*descpb.IndexDescriptor)
				newIndex.IndexDesc().Name = oldIndexCopy.Name
				// Splice out old index from the indexes list.
				desc.RemovePublicNonPrimaryIndex(oldIndexIdx)
				// Add a drop mutation for the old index. The code that calls this function will schedule
				// a schema change job to pick up all of these index drop mutations.
				if err := desc.AddIndexMutation(oldIndexCopy, descpb.DescriptorMutation_DROP); err != nil {
					return err
				}
			}
		case *descpb.DescriptorMutation_ComputedColumnSwap:
			if err := desc.performComputedColumnSwap(t.ComputedColumnSwap); err != nil {
				return err
			}

		case *descpb.DescriptorMutation_MaterializedViewRefresh:
			// Completing a refresh mutation just means overwriting the table's
			// indexes with the new indexes that have been backfilled already.
			desc.SetPrimaryIndex(t.MaterializedViewRefresh.NewPrimaryIndex)
			desc.SetPublicNonPrimaryIndexes(t.MaterializedViewRefresh.NewIndexes)
		}

	case descpb.DescriptorMutation_DROP:
		switch t := m.Descriptor_.(type) {
		// Nothing else to be done. The column/index was already removed from the
		// set of column/index descriptors at mutation creation time.
		// Constraints to be dropped are dropped before column/index backfills.
		case *descpb.DescriptorMutation_Column:
			desc.RemoveColumnFromFamily(t.Column.ID)
		}
	}
	return nil
}

func (desc *Mutable) performComputedColumnSwap(swap *descpb.ComputedColumnSwap) error {
	// Get the old and new columns from the descriptor.
	oldCol, err := desc.FindColumnWithID(swap.OldColumnId)
	if err != nil {
		return err
	}
	newCol, err := desc.FindColumnWithID(swap.NewColumnId)
	if err != nil {
		return err
	}

	// Mark newCol as no longer a computed column.
	newCol.ColumnDesc().ComputeExpr = nil

	// Make the oldCol a computed column by setting its computed expression.
	oldCol.ColumnDesc().ComputeExpr = &swap.InverseExpr

	// Generate unique name for old column.
	nameExists := func(name string) bool {
		_, err := desc.FindColumnWithName(tree.Name(name))
		return err == nil
	}

	uniqueName := GenerateUniqueConstraintName(newCol.GetName(), nameExists)

	// Remember the name of oldCol, because newCol will take it.
	oldColName := oldCol.GetName()

	// Rename old column to this new name, and rename newCol to oldCol's name.
	desc.RenameColumnDescriptor(oldCol.ColumnDesc(), uniqueName)
	desc.RenameColumnDescriptor(newCol.ColumnDesc(), oldColName)

	// Swap Column Family ordering for oldCol and newCol.
	// Both columns must be in the same family since the new column is
	// created explicitly with the same column family as the old column.
	// This preserves the ordering of column families when querying
	// for column families.
	oldColColumnFamily, err := desc.GetFamilyOfColumn(oldCol.GetID())
	if err != nil {
		return err
	}
	newColColumnFamily, err := desc.GetFamilyOfColumn(newCol.GetID())
	if err != nil {
		return err
	}

	if oldColColumnFamily.ID != newColColumnFamily.ID {
		return errors.Newf("expected the column families of the old and new columns to match,"+
			"oldCol column family: %v, newCol column family: %v",
			oldColColumnFamily.ID, newColColumnFamily.ID)
	}

	for i := range oldColColumnFamily.ColumnIDs {
		if oldColColumnFamily.ColumnIDs[i] == oldCol.GetID() {
			oldColColumnFamily.ColumnIDs[i] = newCol.GetID()
			oldColColumnFamily.ColumnNames[i] = newCol.GetName()
		} else if oldColColumnFamily.ColumnIDs[i] == newCol.GetID() {
			oldColColumnFamily.ColumnIDs[i] = oldCol.GetID()
			oldColColumnFamily.ColumnNames[i] = oldCol.GetName()
		}
	}

	// Set newCol's PGAttributeNum to oldCol's ID. This makes
	// newCol display like oldCol in catalog tables.
	newCol.ColumnDesc().PGAttributeNum = oldCol.GetPGAttributeNum()
	oldCol.ColumnDesc().PGAttributeNum = 0

	// Mark oldCol as being the result of an AlterColumnType. This allows us
	// to generate better errors for failing inserts.
	oldCol.ColumnDesc().AlterColumnTypeInProgress = true

	// Clone oldColDesc so that we can queue it up as a mutation.
	// Use oldColCopy to queue mutation in case oldCol's memory address
	// gets overwritten during mutation.
	oldColCopy := oldCol.ColumnDescDeepCopy()
	newColCopy := newCol.ColumnDescDeepCopy()
	desc.AddColumnMutation(&oldColCopy, descpb.DescriptorMutation_DROP)

	// Remove the new column from the TableDescriptor first so we can reinsert
	// it into the position where the old column is.
	for i := range desc.Columns {
		if desc.Columns[i].ID == newCol.GetID() {
			desc.Columns = append(desc.Columns[:i:i], desc.Columns[i+1:]...)
			break
		}
	}

	// Replace the old column with the new column.
	for i := range desc.Columns {
		if desc.Columns[i].ID == oldCol.GetID() {
			desc.Columns[i] = newColCopy
		}
	}

	return nil
}

// AddCheckMutation adds a check constraint mutation to desc.Mutations.
func (desc *Mutable) AddCheckMutation(
	ck *descpb.TableDescriptor_CheckConstraint, direction descpb.DescriptorMutation_Direction,
) {
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Constraint{
			Constraint: &descpb.ConstraintToUpdate{
				ConstraintType: descpb.ConstraintToUpdate_CHECK, Name: ck.Name, Check: *ck,
			},
		},
		Direction: direction,
	}
	desc.addMutation(m)
}

// AddForeignKeyMutation adds a foreign key constraint mutation to desc.Mutations.
func (desc *Mutable) AddForeignKeyMutation(
	fk *descpb.ForeignKeyConstraint, direction descpb.DescriptorMutation_Direction,
) {
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Constraint{
			Constraint: &descpb.ConstraintToUpdate{
				ConstraintType: descpb.ConstraintToUpdate_FOREIGN_KEY,
				Name:           fk.Name,
				ForeignKey:     *fk,
			},
		},
		Direction: direction,
	}
	desc.addMutation(m)
}

// AddUniqueWithoutIndexMutation adds a unique without index constraint mutation
// to desc.Mutations.
func (desc *Mutable) AddUniqueWithoutIndexMutation(
	uc *descpb.UniqueWithoutIndexConstraint, direction descpb.DescriptorMutation_Direction,
) {
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Constraint{
			Constraint: &descpb.ConstraintToUpdate{
				ConstraintType:               descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX,
				Name:                         uc.Name,
				UniqueWithoutIndexConstraint: *uc,
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
// TODO(mgartner): Move this to schemaexpr.CheckConstraintBuilder.
func MakeNotNullCheckConstraint(
	colName string,
	colID descpb.ColumnID,
	inuseNames map[string]struct{},
	validity descpb.ConstraintValidity,
) *descpb.TableDescriptor_CheckConstraint {
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

	expr := &tree.IsNotNullExpr{
		Expr: &tree.ColumnItem{ColumnName: tree.Name(colName)},
	}

	return &descpb.TableDescriptor_CheckConstraint{
		Name:                name,
		Expr:                tree.Serialize(expr),
		Validity:            validity,
		ColumnIDs:           []descpb.ColumnID{colID},
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
func (desc *Mutable) AddNotNullMutation(
	ck *descpb.TableDescriptor_CheckConstraint, direction descpb.DescriptorMutation_Direction,
) {
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Constraint{
			Constraint: &descpb.ConstraintToUpdate{
				ConstraintType: descpb.ConstraintToUpdate_NOT_NULL,
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
func (desc *Mutable) AddColumnMutation(
	c *descpb.ColumnDescriptor, direction descpb.DescriptorMutation_Direction,
) {
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Column{Column: c},
		Direction:   direction,
	}
	desc.addMutation(m)
}

// AddIndexMutation adds an index mutation to desc.Mutations.
func (desc *Mutable) AddIndexMutation(
	idx *descpb.IndexDescriptor, direction descpb.DescriptorMutation_Direction,
) error {

	switch idx.Type {
	case descpb.IndexDescriptor_FORWARD:
		if err := checkColumnsValidForIndex(desc, idx.ColumnNames); err != nil {
			return err
		}
	case descpb.IndexDescriptor_INVERTED:
		if err := checkColumnsValidForInvertedIndex(desc, idx.ColumnNames); err != nil {
			return err
		}
	}

	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Index{Index: idx},
		Direction:   direction,
	}
	desc.addMutation(m)
	return nil
}

// AddPrimaryKeySwapMutation adds a PrimaryKeySwap mutation to the table descriptor.
func (desc *Mutable) AddPrimaryKeySwapMutation(swap *descpb.PrimaryKeySwap) {
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_PrimaryKeySwap{PrimaryKeySwap: swap},
		Direction:   descpb.DescriptorMutation_ADD,
	}
	desc.addMutation(m)
}

// AddMaterializedViewRefreshMutation adds a MaterializedViewRefreshMutation to
// the table descriptor.
func (desc *Mutable) AddMaterializedViewRefreshMutation(refresh *descpb.MaterializedViewRefresh) {
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_MaterializedViewRefresh{MaterializedViewRefresh: refresh},
		Direction:   descpb.DescriptorMutation_ADD,
	}
	desc.addMutation(m)
}

// AddComputedColumnSwapMutation adds a ComputedColumnSwap mutation to the table descriptor.
func (desc *Mutable) AddComputedColumnSwapMutation(swap *descpb.ComputedColumnSwap) {
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_ComputedColumnSwap{ComputedColumnSwap: swap},
		Direction:   descpb.DescriptorMutation_ADD,
	}
	desc.addMutation(m)
}

func (desc *Mutable) addMutation(m descpb.DescriptorMutation) {
	switch m.Direction {
	case descpb.DescriptorMutation_ADD:
		m.State = descpb.DescriptorMutation_DELETE_ONLY

	case descpb.DescriptorMutation_DROP:
		m.State = descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
	}
	// For tables created in the same transaction the next mutation ID will
	// not have been allocated and the added mutation will use an invalid ID.
	// This is fine because the mutation will be processed immediately.
	m.MutationID = desc.ClusterVersion.NextMutationID
	desc.NextMutationID = desc.ClusterVersion.NextMutationID + 1
	desc.Mutations = append(desc.Mutations, m)
}

// MakeFirstMutationPublic creates a Mutable from the
// immutable by making the first mutation public.
// This is super valuable when trying to run SQL over data associated
// with a schema mutation that is still not yet public: Data validation,
// error reporting.
func (desc *wrapper) MakeFirstMutationPublic(
	includeConstraints catalog.MutationPublicationFilter,
) (catalog.TableDescriptor, error) {
	// Clone the ImmutableTable descriptor because we want to create an ImmutableCopy one.
	table := NewBuilder(desc.TableDesc()).BuildExistingMutableTable()
	mutationID := desc.Mutations[0].MutationID
	i := 0
	for _, mutation := range desc.Mutations {
		if mutation.MutationID != mutationID {
			// Mutations are applied in a FIFO order. Only apply the first set
			// of mutations if they have the mutation ID we're looking for.
			break
		}
		i++
		if mutation.GetPrimaryKeySwap() != nil && includeConstraints == catalog.IgnoreConstraintsAndPKSwaps {
			continue
		} else if mutation.GetConstraint() != nil && includeConstraints > catalog.IncludeConstraints {
			continue
		}
		if err := table.MakeMutationComplete(mutation); err != nil {
			return nil, err
		}
	}
	table.Mutations = table.Mutations[i:]
	table.Version++
	return table, nil
}

// MakePublic creates a Mutable from the immutable by making the it public.
func (desc *wrapper) MakePublic() catalog.TableDescriptor {
	// Clone the ImmutableTable descriptor because we want to create an ImmutableCopy one.
	table := NewBuilder(desc.TableDesc()).BuildExistingMutableTable()
	table.State = descpb.DescriptorState_PUBLIC
	table.Version++
	return table
}

// ColumnNeedsBackfill returns true if adding or dropping (according to
// the direction) the given column requires backfill.
func ColumnNeedsBackfill(
	direction descpb.DescriptorMutation_Direction, desc *descpb.ColumnDescriptor,
) bool {
	if desc.Virtual {
		// Virtual columns are not stored in the primary index, so they do not need
		// backfill.
		return false
	}
	if direction == descpb.DescriptorMutation_DROP {
		// In all other cases, DROP requires backfill.
		return true
	}
	// ADD requires backfill for:
	//  - columns with non-NULL default value
	//  - computed columns
	//  - non-nullable columns (note: if a non-nullable column doesn't have a
	//    default value, the backfill will fail unless the table is empty).
	if desc.HasNullDefault() {
		return false
	}
	return desc.HasDefault() || !desc.Nullable || desc.IsComputed()
}

// HasPrimaryKey returns true if the table has a primary key.
func (desc *wrapper) HasPrimaryKey() bool {
	return !desc.PrimaryIndex.Disabled
}

// HasColumnBackfillMutation returns whether the table has any queued column
// mutations that require a backfill.
func (desc *wrapper) HasColumnBackfillMutation() bool {
	for _, m := range desc.Mutations {
		col := m.GetColumn()
		if col == nil {
			// Index backfills don't affect changefeeds.
			continue
		}
		if ColumnNeedsBackfill(m.Direction, col) {
			return true
		}
	}
	return false
}

// IsNew returns true if the table was created in the current
// transaction.
func (desc *Mutable) IsNew() bool {
	return desc.ClusterVersion.ID == descpb.InvalidID
}

// ColumnsSelectors generates Select expressions for cols.
func ColumnsSelectors(cols []catalog.Column) tree.SelectExprs {
	exprs := make(tree.SelectExprs, len(cols))
	colItems := make([]tree.ColumnItem, len(cols))
	for i, col := range cols {
		colItems[i].ColumnName = col.ColName()
		exprs[i].Expr = &colItems[i]
	}
	return exprs
}

// InvalidateFKConstraints sets all FK constraints to un-validated.
func (desc *wrapper) InvalidateFKConstraints() {
	// We don't use GetConstraintInfo because we want to edit the passed desc.
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		fk.Validity = descpb.ConstraintValidity_Unvalidated
	}
}

// AllIndexSpans returns the Spans for each index in the table, including those
// being added in the mutations.
func (desc *wrapper) AllIndexSpans(codec keys.SQLCodec) roachpb.Spans {
	var spans roachpb.Spans
	for _, index := range desc.NonDropIndexes() {
		spans = append(spans, desc.IndexSpan(codec, index.GetID()))
	}
	return spans
}

// PrimaryIndexSpan returns the Span that corresponds to the entire primary
// index; can be used for a full table scan.
func (desc *wrapper) PrimaryIndexSpan(codec keys.SQLCodec) roachpb.Span {
	return desc.IndexSpan(codec, desc.PrimaryIndex.ID)
}

// IndexSpan returns the Span that corresponds to an entire index; can be used
// for a full index scan.
func (desc *wrapper) IndexSpan(codec keys.SQLCodec, indexID descpb.IndexID) roachpb.Span {
	prefix := roachpb.Key(rowenc.MakeIndexKeyPrefix(codec, desc, indexID))
	return roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
}

// TableSpan returns the Span that corresponds to the entire table.
func (desc *wrapper) TableSpan(codec keys.SQLCodec) roachpb.Span {
	// TODO(jordan): Why does IndexSpan consider interleaves but TableSpan does
	// not? Should it?
	prefix := codec.TablePrefix(uint32(desc.ID))
	return roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
}

// ColumnsUsed returns the IDs of the columns used in the check constraint's
// expression. v2.0 binaries will populate this during table creation, but older
// binaries will not, in which case this needs to be computed when requested.
//
// TODO(nvanbenschoten): we can remove this in v2.1 and replace it with a sql
// migration to backfill all descpb.TableDescriptor_CheckConstraint.ColumnIDs slices.
// See #22322.
func (desc *wrapper) ColumnsUsed(
	cc *descpb.TableDescriptor_CheckConstraint,
) ([]descpb.ColumnID, error) {
	if len(cc.ColumnIDs) > 0 {
		// Already populated.
		return cc.ColumnIDs, nil
	}

	parsed, err := parser.ParseExpr(cc.Expr)
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.Syntax,
			"could not parse check constraint %s", cc.Expr)
	}

	var colIDsUsed catalog.TableColSet
	visitFn := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		if vBase, ok := expr.(tree.VarName); ok {
			v, err := vBase.NormalizeVarName()
			if err != nil {
				return false, nil, err
			}
			if c, ok := v.(*tree.ColumnItem); ok {
				col, err := desc.FindColumnWithName(c.ColumnName)
				if err != nil || col.Dropped() {
					return false, nil, pgerror.Newf(pgcode.UndefinedColumn,
						"column %q not found for constraint %q",
						c.ColumnName, parsed.String())
				}
				colIDsUsed.Add(col.GetID())
			}
			return false, v, nil
		}
		return true, expr, nil
	}
	if _, err := tree.SimpleVisit(parsed, visitFn); err != nil {
		return nil, err
	}

	cc.ColumnIDs = make([]descpb.ColumnID, 0, colIDsUsed.Len())
	for colID, ok := colIDsUsed.Next(0); ok; colID, ok = colIDsUsed.Next(colID + 1) {
		cc.ColumnIDs = append(cc.ColumnIDs, colID)
	}
	sort.Sort(descpb.ColumnIDs(cc.ColumnIDs))
	return cc.ColumnIDs, nil
}

// CheckConstraintUsesColumn returns whether the check constraint uses the
// specified column.
func (desc *wrapper) CheckConstraintUsesColumn(
	cc *descpb.TableDescriptor_CheckConstraint, colID descpb.ColumnID,
) (bool, error) {
	colsUsed, err := desc.ColumnsUsed(cc)
	if err != nil {
		return false, err
	}
	i := sort.Search(len(colsUsed), func(i int) bool {
		return colsUsed[i] >= colID
	})
	return i < len(colsUsed) && colsUsed[i] == colID, nil
}

// GetFamilyOfColumn returns the ColumnFamilyDescriptor for the
// the family the column is part of.
func (desc *wrapper) GetFamilyOfColumn(
	colID descpb.ColumnID,
) (*descpb.ColumnFamilyDescriptor, error) {
	for _, fam := range desc.Families {
		for _, id := range fam.ColumnIDs {
			if id == colID {
				return &fam, nil
			}
		}
	}

	return nil, errors.Newf("no column family found for column id %v", colID)
}

// PartitionNames returns a slice containing the name of every partition and
// subpartition in an arbitrary order.
func (desc *wrapper) PartitionNames() []string {
	var names []string
	for _, index := range desc.NonDropIndexes() {
		names = append(names, index.PartitionNames()...)
	}
	return names
}

// SetAuditMode configures the audit mode on the descriptor.
func (desc *Mutable) SetAuditMode(mode tree.AuditMode) (bool, error) {
	prev := desc.AuditMode
	switch mode {
	case tree.AuditModeDisable:
		desc.AuditMode = descpb.TableDescriptor_DISABLED
	case tree.AuditModeReadWrite:
		desc.AuditMode = descpb.TableDescriptor_READWRITE
	default:
		return false, pgerror.Newf(pgcode.InvalidParameterValue,
			"unknown audit mode: %s (%d)", mode, mode)
	}
	return prev != desc.AuditMode, nil
}

// FindAllReferences returns all the references from a table.
func (desc *wrapper) FindAllReferences() (map[descpb.ID]struct{}, error) {
	refs := map[descpb.ID]struct{}{}
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		refs[fk.ReferencedTableID] = struct{}{}
	}
	for i := range desc.InboundFKs {
		fk := &desc.InboundFKs[i]
		refs[fk.OriginTableID] = struct{}{}
	}
	for _, index := range desc.NonDropIndexes() {
		for i := 0; i < index.NumInterleaveAncestors(); i++ {
			refs[index.GetInterleaveAncestor(i).TableID] = struct{}{}
		}
		for i := 0; i < index.NumInterleavedBy(); i++ {
			refs[index.GetInterleavedBy(i).Table] = struct{}{}
		}
	}

	for _, c := range desc.NonDropColumns() {
		for i := 0; i < c.NumUsesSequences(); i++ {
			id := c.GetUsesSequenceID(i)
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
func (desc *immutable) ActiveChecks() []descpb.TableDescriptor_CheckConstraint {
	return desc.allChecks
}

// IsShardColumn returns true if col corresponds to a non-dropped hash sharded
// index. This method assumes that col is currently a member of desc.
func (desc *Mutable) IsShardColumn(col *descpb.ColumnDescriptor) bool {
	return nil != catalog.FindNonDropIndex(desc, func(idx catalog.Index) bool {
		return idx.IsSharded() && idx.GetShardColumnName() == col.Name
	})
}

// TableDesc implements the TableDescriptor interface.
func (desc *wrapper) TableDesc() *descpb.TableDescriptor {
	return &desc.TableDescriptor
}

// GenerateUniqueConstraintName attempts to generate a unique constraint name
// with the given prefix.
// It will first try prefix by itself, then it will subsequently try
// adding numeric digits at the end, starting from 1.
func GenerateUniqueConstraintName(prefix string, nameExistsFunc func(name string) bool) string {
	name := prefix
	for i := 1; nameExistsFunc(name); i++ {
		name = fmt.Sprintf("%s_%d", prefix, i)
	}
	return name
}

// SetParentSchemaID sets the SchemaID of the table.
func (desc *Mutable) SetParentSchemaID(schemaID descpb.ID) {
	desc.UnexposedParentSchemaID = schemaID
}

// AddDrainingName adds a draining name to the TableDescriptor's slice of
// draining names.
func (desc *Mutable) AddDrainingName(name descpb.NameInfo) {
	desc.DrainingNames = append(desc.DrainingNames, name)
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

// IsLocalityRegionalByRow returns whether or not the table is REGIONAL BY ROW
// table.
func (desc *wrapper) IsLocalityRegionalByRow() bool {
	return desc.LocalityConfig.GetRegionalByRow() != nil
}

// IsLocalityRegionalByTable returns whether or not the table is REGIONAL BY
// TABLE table.
func (desc *wrapper) IsLocalityRegionalByTable() bool {
	return desc.LocalityConfig.GetRegionalByTable() != nil
}

// IsLocalityGlobal returns whether or not the table is GLOBAL table.
func (desc *wrapper) IsLocalityGlobal() bool {
	return desc.LocalityConfig.GetGlobal() != nil
}

// GetRegionalTableRegion returns the region a REGIONAL BY TABLE table is
// homed in.
func (desc *wrapper) GetRegionalByTableRegion() (descpb.RegionName, error) {
	if !desc.IsLocalityRegionalByTable() {
		return "", errors.AssertionFailedf("%s is not REGIONAL BY TABLE", desc.Name)
	}
	region := desc.LocalityConfig.GetRegionalByTable().Region
	if region == nil {
		return descpb.RegionName(tree.PrimaryRegionNotSpecifiedName), nil
	}
	return *region, nil
}

// GetRegionalByRowTableRegionColumnName returns the region column name of a
// REGIONAL BY ROW table.
func (desc *wrapper) GetRegionalByRowTableRegionColumnName() (tree.Name, error) {
	if !desc.IsLocalityRegionalByRow() {
		return "", errors.AssertionFailedf("%q is not a REGIONAL BY ROW table", desc.Name)
	}
	colName := desc.LocalityConfig.GetRegionalByRow().As
	if colName == nil {
		return tree.RegionalByRowRegionDefaultColName, nil
	}
	return tree.Name(*colName), nil
}

// GetMultiRegionEnumDependency returns true if the given table has an "implicit"
// dependency on the multi-region enum. An implicit dependency exists for
// REGIONAL BY TABLE table's which are homed in an explicit region
// (i.e non-primary region). Even though these tables don't have a column
// denoting their locality, their region config uses a value from the
// multi-region enum. As such, any drop validation or locality switches must
// honor this implicit dependency.
func (desc *wrapper) GetMultiRegionEnumDependencyIfExists() bool {
	if desc.IsLocalityRegionalByTable() {
		regionName, _ := desc.GetRegionalByTableRegion()
		return regionName != descpb.RegionName(tree.PrimaryRegionNotSpecifiedName)
	}
	return false
}

// SetTableLocalityRegionalByTable sets the descriptor's locality config to
// regional at the table level in the supplied region. An empty region name
// (or its alias PrimaryRegionNotSpecifiedName) denotes that the table is homed in
// the primary region.
// SetTableLocalityRegionalByTable doesn't account for the locality config that
// was previously set on the descriptor. Instead, you may want to use:
// (planner) alterTableDescLocalityToRegionalByTable.
func (desc *Mutable) SetTableLocalityRegionalByTable(region tree.Name) {
	lc := LocalityConfigRegionalByTable(region)
	desc.LocalityConfig = &lc
}

// LocalityConfigRegionalByTable returns a config for a REGIONAL BY TABLE table.
func LocalityConfigRegionalByTable(region tree.Name) descpb.TableDescriptor_LocalityConfig {
	l := &descpb.TableDescriptor_LocalityConfig_RegionalByTable_{
		RegionalByTable: &descpb.TableDescriptor_LocalityConfig_RegionalByTable{},
	}
	if region != tree.PrimaryRegionNotSpecifiedName {
		regionName := descpb.RegionName(region)
		l.RegionalByTable.Region = &regionName
	}
	return descpb.TableDescriptor_LocalityConfig{Locality: l}
}

// SetTableLocalityRegionalByRow sets the descriptor's locality config to
// regional at the row level. An empty regionColName denotes the default
// crdb_region partitioning column.
// SetTableLocalityRegionalByRow doesn't account for the locality config that
// was previously set on the descriptor, and the dependency unlinking that it
// entails.
func (desc *Mutable) SetTableLocalityRegionalByRow(regionColName tree.Name) {
	lc := LocalityConfigRegionalByRow(regionColName)
	desc.LocalityConfig = &lc
}

// LocalityConfigRegionalByRow returns a config for a REGIONAL BY ROW table.
func LocalityConfigRegionalByRow(regionColName tree.Name) descpb.TableDescriptor_LocalityConfig {
	rbr := &descpb.TableDescriptor_LocalityConfig_RegionalByRow{}
	if regionColName != tree.RegionalByRowRegionNotSpecifiedName {
		rbr.As = proto.String(string(regionColName))
	}
	return descpb.TableDescriptor_LocalityConfig{
		Locality: &descpb.TableDescriptor_LocalityConfig_RegionalByRow_{
			RegionalByRow: rbr,
		},
	}
}

// SetTableLocalityGlobal sets the descriptor's locality config to a global
// table.
// SetLocalityGlobal doesn't account for the locality config that was previously
// set on the descriptor. Instead, you may want to use
// (planner) alterTableDescLocalityToGlobal.
func (desc *Mutable) SetTableLocalityGlobal() {
	lc := LocalityConfigGlobal()
	desc.LocalityConfig = &lc
}

// LocalityConfigGlobal returns a config for a GLOBAL table.
func LocalityConfigGlobal() descpb.TableDescriptor_LocalityConfig {
	return descpb.TableDescriptor_LocalityConfig{
		Locality: &descpb.TableDescriptor_LocalityConfig_Global_{
			Global: &descpb.TableDescriptor_LocalityConfig_Global{},
		},
	}
}
