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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// Mutable is a custom type for TableDescriptors
// going through schema mutations.
type Mutable struct {
	wrapper

	// ClusterVersion represents the version of the table descriptor read from the store.
	ClusterVersion descpb.TableDescriptor
}

const (
	// LegacyPrimaryKeyIndexName is the pre 22.1 default PRIMARY KEY index name.
	LegacyPrimaryKeyIndexName = "primary"
	// SequenceColumnID is the ID of the sole column in a sequence.
	SequenceColumnID = 1
	// SequenceColumnName is the name of the sole column in a sequence.
	SequenceColumnName = "value"
)

// ErrMissingColumns indicates a table with no columns.
var ErrMissingColumns = errors.New("table must contain at least 1 column")

// ErrMissingPrimaryKey indicates a table with no primary key.
var ErrMissingPrimaryKey = errors.New("table must contain a primary key")

// UseMVCCCompliantIndexCreation controls whether index additions will
// use the MVCC compliant scheme which requires both temporary indexes
// and a different initial state.
var UseMVCCCompliantIndexCreation = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.mvcc_compliant_index_creation.enabled",
	"if true, schema changes will use the an index backfiller designed for MVCC-compliant bulk operations",
	true,
)

// DescriptorType returns the type of this descriptor.
func (desc *wrapper) DescriptorType() catalog.DescriptorType {
	return catalog.Table
}

// SetName implements the DescriptorProto interface.
func (desc *Mutable) SetName(name string) {
	desc.Name = name
}

// IsPartitionAllBy implements the TableDescriptor interface.
func (desc *wrapper) IsPartitionAllBy() bool {
	return desc.PartitionAllBy
}

// GetParentSchemaID returns the ParentSchemaID if the descriptor has
// one. If the descriptor was created before the field was added, then the
// descriptor belongs to a table under the `public` physical schema. The static
// public schema ID is returned in that case.
func (desc *wrapper) GetParentSchemaID() descpb.ID {
	parentSchemaID := desc.GetUnexposedParentSchemaID()
	// TODO(richardjcai): Remove this case in 22.1.
	if parentSchemaID == descpb.InvalidID {
		parentSchemaID = keys.PublicSchemaID
	}
	return parentSchemaID
}

// IndexKeysPerRow implements the TableDescriptor interface.
func (desc *wrapper) IndexKeysPerRow(idx catalog.Index) int {
	if desc.PrimaryIndex.ID == idx.GetID() {
		return len(desc.Families)
	}
	if idx.NumSecondaryStoredColumns() == 0 || len(desc.Families) == 1 {
		return 1
	}
	// Calculate the number of column families used by the secondary index. We
	// only need to look at the stored columns because column families are only
	// applicable to the value part of the KV.
	//
	// 0th family is always present.
	numUsedFamilies := 1
	storedColumnIDs := idx.CollectSecondaryStoredColumnIDs()
	for _, family := range desc.Families[1:] {
		for _, columnID := range family.ColumnIDs {
			if storedColumnIDs.Contains(columnID) {
				numUsedFamilies++
				break
			}
		}
	}
	return numUsedFamilies
}

// BuildIndexName returns an index name that is not equal to any
// of tableDesc's indexes, roughly following Postgres's conventions for naming
// anonymous indexes. For example:
//
//   CREATE INDEX ON t (a)
//   => t_a_idx
//
//   CREATE UNIQUE INDEX ON t (a, b)
//   => t_a_b_key
//
//   CREATE INDEX ON t ((a + b), c, lower(d))
//   => t_expr_c_expr1_idx
//
func BuildIndexName(tableDesc *Mutable, idx *descpb.IndexDescriptor) (string, error) {
	// An index name has a segment for the table name, each key column, and a
	// final word (either "idx" or "key").
	segments := make([]string, 0, len(idx.KeyColumnNames)+2)

	// Add the table name segment.
	segments = append(segments, tableDesc.Name)

	// Add the key column segments. For inaccessible columns, use "expr" as the
	// segment. If there are multiple inaccessible columns, add an incrementing
	// integer suffix.
	exprCount := 0
	for i, n := idx.ExplicitColumnStartIdx(), len(idx.KeyColumnNames); i < n; i++ {
		var segmentName string
		col, err := tableDesc.FindColumnWithName(tree.Name(idx.KeyColumnNames[i]))
		if err != nil {
			return "", err
		}
		if col.IsExpressionIndexColumn() {
			if exprCount == 0 {
				segmentName = "expr"
			} else {
				segmentName = fmt.Sprintf("expr%d", exprCount)
			}
			exprCount++
		} else {
			segmentName = idx.KeyColumnNames[i]
		}
		segments = append(segments, segmentName)
	}

	// Add a segment for delete preserving indexes so that
	// temporary indexes used by the index backfiller are easily
	// identifiable and so that we don't cause too many changes in
	// the index names generated by a series of operations.
	if idx.UseDeletePreservingEncoding {
		segments = append(segments, "crdb_internal_dpe")
	}

	// Add the final segment.
	if idx.Unique {
		segments = append(segments, "key")
	} else {
		segments = append(segments, "idx")
	}

	// Append digits to the index name to make it unique, if necessary.
	baseName := strings.Join(segments, "_")
	name := baseName
	for i := 1; ; i++ {
		foundIndex, _ := tableDesc.FindIndexWithName(name)
		if foundIndex == nil {
			break
		}
		name = fmt.Sprintf("%s%d", baseName, i)
	}

	return name, nil
}

// AllActiveAndInactiveChecks implements the TableDescriptor interface.
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

// AllActiveAndInactiveUniqueWithoutIndexConstraints implements the
// TableDescriptor interface.
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

// AllActiveAndInactiveForeignKeys implements the TableDescriptor interface.
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

// ForeachDependedOnBy implements the TableDescriptor interface.
func (desc *wrapper) ForeachDependedOnBy(
	f func(dep *descpb.TableDescriptor_Reference) error,
) error {
	for i := range desc.DependedOnBy {
		if err := f(&desc.DependedOnBy[i]); err != nil {
			if iterutil.Done(err) {
				return nil
			}
			return err
		}
	}
	return nil
}

// ForeachOutboundFK implements the TableDescriptor interface.
func (desc *wrapper) ForeachOutboundFK(
	f func(constraint *descpb.ForeignKeyConstraint) error,
) error {
	for i := range desc.OutboundFKs {
		if err := f(&desc.OutboundFKs[i]); err != nil {
			if iterutil.Done(err) {
				return nil
			}
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
			if iterutil.Done(err) {
				return nil
			}
			return err
		}
	}
	return nil
}

// NumFamilies implements the TableDescriptor interface.
func (desc *wrapper) NumFamilies() int {
	return len(desc.Families)
}

// ForeachFamily implements the TableDescriptor interface.
func (desc *wrapper) ForeachFamily(f func(family *descpb.ColumnFamilyDescriptor) error) error {
	for i := range desc.Families {
		if err := f(&desc.Families[i]); err != nil {
			if iterutil.Done(err) {
				return nil
			}
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
		if c.HasOnUpdate() {
			if err := f(c.OnUpdateExpr); err != nil {
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

// GetAllReferencedTypeIDs implements the TableDescriptor interface.
func (desc *wrapper) GetAllReferencedTypeIDs(
	dbDesc catalog.DatabaseDescriptor, getType func(descpb.ID) (catalog.TypeDescriptor, error),
) (referencedAnywhere, referencedInColumns descpb.IDs, _ error) {
	ids, err := desc.getAllReferencedTypesInTableColumns(getType)
	if err != nil {
		return nil, nil, err
	}
	referencedInColumns = make(descpb.IDs, 0, len(ids))
	for id := range ids {
		referencedInColumns = append(referencedInColumns, id)
	}
	sort.Sort(referencedInColumns)

	// REGIONAL BY TABLE tables may have a dependency with the multi-region enum.
	exists := desc.GetMultiRegionEnumDependencyIfExists()
	if exists {
		regionEnumID, err := dbDesc.MultiRegionEnumID()
		if err != nil {
			return nil, nil, err
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
	return result, referencedInColumns, nil
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
		uid, err := typedesc.UserDefinedTypeOIDToID(id)
		if err != nil {
			return nil, err
		}
		typDesc, err := getType(uid)
		if err != nil {
			return nil, err
		}
		children, err := typDesc.GetIDClosure()
		if err != nil {
			return nil, err
		}
		for child := range children {
			ids[child] = struct{}{}
		}
	}

	// Now add all of the column types in the table.
	addIDsInColumn := func(c *descpb.ColumnDescriptor) error {
		children, err := typedesc.GetTypeDescriptorClosure(c.Type)
		if err != nil {
			return err
		}
		for id := range children {
			ids[id] = struct{}{}
		}
		return nil
	}
	for i := range desc.Columns {
		if err := addIDsInColumn(&desc.Columns[i]); err != nil {
			return nil, err
		}
	}
	for _, mut := range desc.Mutations {
		if c := mut.GetColumn(); c != nil {
			if err := addIDsInColumn(c); err != nil {
				return nil, err
			}
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
	if desc.NextConstraintID == 0 {
		desc.NextConstraintID = 1
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
// or index which has an ID of 0. It's the same as AllocateIDsWithoutValidation,
// but does validation on the table elements.
func (desc *Mutable) AllocateIDs(ctx context.Context, version clusterversion.ClusterVersion) error {
	if err := desc.AllocateIDsWithoutValidation(ctx); err != nil {
		return err
	}

	// This is sort of ugly. If the descriptor does not have an ID, we hack one in
	// to pass the table ID check. We use a non-reserved ID, reserved ones being set
	// before AllocateIDs.
	savedID := desc.ID
	if desc.ID == 0 {
		desc.ID = keys.SystemDatabaseID
	}
	err := validate.Self(version, desc)
	desc.ID = savedID

	return err
}

// AllocateIDsWithoutValidation allocates column, family, and index ids for any
// column, family, or index which has an ID of 0.
func (desc *Mutable) AllocateIDsWithoutValidation(ctx context.Context) error {
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

	return nil
}

func (desc *Mutable) ensurePrimaryKey() error {
	if len(desc.PrimaryIndex.KeyColumnNames) == 0 && desc.IsPhysicalTable() {
		// Ensure a Primary Key exists.
		nameExists := func(name string) bool {
			_, err := desc.FindColumnWithName(tree.Name(name))
			return err == nil
		}
		s := "unique_rowid()"
		col := &descpb.ColumnDescriptor{
			Name:        GenerateUniqueName("rowid", nameExists),
			Type:        types.Int,
			DefaultExpr: &s,
			Hidden:      true,
			Nullable:    false,
		}
		desc.AddColumn(col)
		idx := descpb.IndexDescriptor{
			Unique:              true,
			KeyColumnNames:      []string{col.Name},
			KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
		}
		if err := desc.AddPrimaryIndex(idx); err != nil {
			return err
		}
	}
	return nil
}

func (desc *Mutable) allocateIndexIDs(columnNames map[string]descpb.ColumnID) error {
	if desc.NextIndexID == 0 {
		desc.NextIndexID = 1
	}
	if desc.NextConstraintID == 0 {
		desc.NextConstraintID = 1
	}

	// Assign names to unnamed indexes.
	err := catalog.ForEachNonPrimaryIndex(desc, func(idx catalog.Index) error {
		if len(idx.GetName()) == 0 {
			name, err := BuildIndexName(desc, idx.IndexDesc())
			if err != nil {
				return err
			}
			idx.IndexDesc().Name = name
		}
		if idx.GetConstraintID() == 0 && idx.IsUnique() {
			idx.IndexDesc().ConstraintID = desc.NextConstraintID
			desc.NextConstraintID++
		}
		return nil
	})
	if err != nil {
		return err
	}

	var compositeColIDs catalog.TableColSet
	for i := range desc.Columns {
		col := &desc.Columns[i]
		if colinfo.CanHaveCompositeKeyEncoding(col.Type) {
			compositeColIDs.Add(col.ID)
		}
	}

	// Populate IDs.
	primaryColIDs := desc.GetPrimaryIndex().CollectKeyColumnIDs()
	for _, idx := range desc.AllIndexes() {
		if !idx.Primary() {
			maybeUpgradeSecondaryIndexFormatVersion(idx.IndexDesc())
		}
		if idx.Primary() && idx.GetConstraintID() == 0 {
			idx.IndexDesc().ConstraintID = desc.NextConstraintID
			desc.NextConstraintID++
		}
		if idx.GetID() == 0 {
			idx.IndexDesc().ID = desc.NextIndexID
			desc.NextIndexID++
		} else if !idx.Primary() {
			// Nothing to do for this secondary index.
			continue
		} else if !idx.CollectPrimaryStoredColumnIDs().Contains(0) {
			// Nothing to do for this primary index.
			continue
		}

		// Populate KeyColumnIDs to match KeyColumnNames.
		for j, colName := range idx.IndexDesc().KeyColumnNames {
			if len(idx.IndexDesc().KeyColumnIDs) <= j {
				idx.IndexDesc().KeyColumnIDs = append(idx.IndexDesc().KeyColumnIDs, 0)
			}
			if idx.IndexDesc().KeyColumnIDs[j] == 0 {
				idx.IndexDesc().KeyColumnIDs[j] = columnNames[colName]
			}
		}

		// Rebuild KeySuffixColumnIDs, StoreColumnIDs and CompositeColumnIDs.
		indexHasOldStoredColumns := idx.HasOldStoredColumns()
		idx.IndexDesc().KeySuffixColumnIDs = nil
		idx.IndexDesc().StoreColumnIDs = nil
		idx.IndexDesc().CompositeColumnIDs = nil

		// KeySuffixColumnIDs is only populated for indexes using the secondary
		// index encoding. It is the set difference of the primary key minus the
		// index's key.
		colIDs := idx.CollectKeyColumnIDs()
		var extraColumnIDs []descpb.ColumnID
		for _, primaryColID := range desc.PrimaryIndex.KeyColumnIDs {
			if !colIDs.Contains(primaryColID) {
				extraColumnIDs = append(extraColumnIDs, primaryColID)
				colIDs.Add(primaryColID)
			}
		}
		if idx.GetEncodingType() == descpb.SecondaryIndexEncoding {
			idx.IndexDesc().KeySuffixColumnIDs = extraColumnIDs
		} else {
			colIDs = idx.CollectKeyColumnIDs()
		}

		// StoreColumnIDs are derived from StoreColumnNames just like KeyColumnIDs
		// derives from KeyColumnNames.
		// For primary indexes this set of columns is typically defined as the set
		// difference of non-virtual columns minus the primary key.
		// In the case of secondary indexes, these columns are defined explicitly at
		// index creation via the STORING clause. We do some validation checks here
		// presumably to guard against user input errors.
		//
		// TODO(postamar): AllocateIDs should not do user input validation.
		// The only errors it should return should be assertion failures.
		for _, colName := range idx.IndexDesc().StoreColumnNames {
			col, err := desc.FindColumnWithName(tree.Name(colName))
			if err != nil {
				return err
			}
			if primaryColIDs.Contains(col.GetID()) && idx.GetEncodingType() == descpb.SecondaryIndexEncoding {
				// If the primary index contains a stored column, we don't need to
				// store it - it's already part of the index.
				err = pgerror.Newf(pgcode.DuplicateColumn,
					"index %q already contains column %q", idx.GetName(), col.GetName())
				err = errors.WithDetailf(err,
					"column %q is part of the primary index and therefore implicit in all indexes", col.GetName())
				return err
			}
			if colIDs.Contains(col.GetID()) {
				return pgerror.Newf(
					pgcode.DuplicateColumn,
					"index %q already contains column %q", idx.GetName(), col.GetName())
			}
			if indexHasOldStoredColumns {
				idx.IndexDesc().KeySuffixColumnIDs = append(idx.IndexDesc().KeySuffixColumnIDs, col.GetID())
			} else {
				idx.IndexDesc().StoreColumnIDs = append(idx.IndexDesc().StoreColumnIDs, col.GetID())
			}
			colIDs.Add(col.GetID())
		}

		// CompositeColumnIDs is defined as the subset of columns in the index key
		// or in the primary key whose type has a composite encoding, like DECIMAL
		// for instance.
		for _, colID := range idx.IndexDesc().KeyColumnIDs {
			if compositeColIDs.Contains(colID) {
				idx.IndexDesc().CompositeColumnIDs = append(idx.IndexDesc().CompositeColumnIDs, colID)
			}
		}
		for _, colID := range idx.IndexDesc().KeySuffixColumnIDs {
			if compositeColIDs.Contains(colID) {
				idx.IndexDesc().CompositeColumnIDs = append(idx.IndexDesc().CompositeColumnIDs, colID)
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
	for _, colID := range desc.PrimaryIndex.KeyColumnIDs {
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
						"see the documentation for more information about inverted indexes: "+docs.URL("inverted-indexes.html"),
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
						"see the documentation for more information about inverted indexes: "+docs.URL("inverted-indexes.html"),
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

// AddPrimaryIndex adds a primary index to a mutable table descriptor, assuming
// that none has yet been set, and performs some sanity checks.
func (desc *Mutable) AddPrimaryIndex(idx descpb.IndexDescriptor) error {
	if idx.Type == descpb.IndexDescriptor_INVERTED {
		return fmt.Errorf("primary index cannot be inverted")
	}
	if err := checkColumnsValidForIndex(desc, idx.KeyColumnNames); err != nil {
		return err
	}
	if desc.PrimaryIndex.Name != "" {
		return fmt.Errorf("multiple primary keys for table %q are not allowed", desc.Name)
	}
	if idx.Name == "" {
		// Only override the index name if it hasn't been set by the user.
		idx.Name = PrimaryKeyIndexName(desc.Name)
	}
	idx.EncodingType = descpb.PrimaryIndexEncoding
	if idx.Version < descpb.PrimaryIndexWithStoredColumnsVersion {
		idx.Version = descpb.PrimaryIndexWithStoredColumnsVersion
		// Populate store columns.
		names := make(map[string]struct{})
		for _, name := range idx.KeyColumnNames {
			names[name] = struct{}{}
		}
		cols := desc.DeletableColumns()
		idx.StoreColumnNames = make([]string, 0, len(cols))
		for _, col := range cols {
			if _, found := names[col.GetName()]; found || col.IsVirtual() {
				continue
			}
			names[col.GetName()] = struct{}{}
			idx.StoreColumnNames = append(idx.StoreColumnNames, col.GetName())
		}
		if len(idx.StoreColumnNames) == 0 {
			idx.StoreColumnNames = nil
		}
	}
	desc.SetPrimaryIndex(idx)
	return nil
}

// AddSecondaryIndex adds a secondary index to a mutable table descriptor.
func (desc *Mutable) AddSecondaryIndex(idx descpb.IndexDescriptor) error {
	if idx.Type == descpb.IndexDescriptor_FORWARD {
		if err := checkColumnsValidForIndex(desc, idx.KeyColumnNames); err != nil {
			return err
		}
	} else {
		if err := checkColumnsValidForInvertedIndex(desc, idx.KeyColumnNames); err != nil {
			return err
		}
	}
	desc.AddPublicNonPrimaryIndex(idx)
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

// RemoveColumnFromFamilyAndPrimaryIndex removes a colID from the family it's
// assigned to, and from the stored column references in the primary index.
func (desc *Mutable) RemoveColumnFromFamilyAndPrimaryIndex(colID descpb.ColumnID) {
	desc.removeColumnFromFamily(colID)
	idx := desc.GetPrimaryIndex().IndexDescDeepCopy()
	for i, id := range idx.StoreColumnIDs {
		if id == colID {
			idx.StoreColumnIDs = append(idx.StoreColumnIDs[:i], idx.StoreColumnIDs[i+1:]...)
			idx.StoreColumnNames = append(idx.StoreColumnNames[:i], idx.StoreColumnNames[i+1:]...)
			desc.SetPrimaryIndex(idx)
			return
		}
	}
}

func (desc *Mutable) removeColumnFromFamily(colID descpb.ColumnID) {
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
func (desc *Mutable) RenameColumnDescriptor(column catalog.Column, newColName string) {
	colID := column.GetID()
	column.ColumnDesc().Name = newColName

	for i := range desc.Families {
		for j := range desc.Families[i].ColumnIDs {
			if desc.Families[i].ColumnIDs[j] == colID {
				desc.Families[i].ColumnNames[j] = newColName
			}
		}
	}

	for _, idx := range desc.AllIndexes() {
		idxDesc := idx.IndexDesc()
		for i, id := range idxDesc.KeyColumnIDs {
			if id == colID {
				idxDesc.KeyColumnNames[i] = newColName
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
		if col.ColName() == name &&
			((col.Public()) ||
				(col.Adding() && col.MutationID() == currentMutationID)) {
			return col, nil
		}
	}
	return nil, colinfo.NewUndefinedColumnError(string(name))
}

// ContainsUserDefinedTypes implements the TableDescriptor interface.
func (desc *wrapper) ContainsUserDefinedTypes() bool {
	return len(desc.UserDefinedTypeColumns()) > 0
}

// FindFamilyByID implements the TableDescriptor interface.
func (desc *wrapper) FindFamilyByID(id descpb.FamilyID) (*descpb.ColumnFamilyDescriptor, error) {
	for i := range desc.Families {
		family := &desc.Families[i]
		if family.ID == id {
			return family, nil
		}
	}
	return nil, fmt.Errorf("family-id \"%d\" does not exist", id)
}

// NamesForColumnIDs implements the TableDescriptor interface.
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
		idx, err := desc.FindIndexWithID(detail.Index.ID)
		if err != nil {
			return err
		}
		idx.IndexDesc().Name = newName
		return nil

	case descpb.ConstraintTypeUnique:
		if detail.Index != nil {
			for _, tableRef := range desc.DependedOnBy {
				if tableRef.IndexID != detail.Index.ID {
					continue
				}
				return dependentViewRenameError("index", tableRef.ID)
			}
			idx, err := desc.FindIndexWithID(detail.Index.ID)
			if err != nil {
				return err
			}
			idx.IndexDesc().Name = newName
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

// GetIndexMutationCapabilities implements the TableDescriptor interface.
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

// IsPrimaryIndexDefaultRowID returns whether or not the table's primary
// index is the default primary key on the hidden rowid column.
func (desc *wrapper) IsPrimaryIndexDefaultRowID() bool {
	if len(desc.PrimaryIndex.KeyColumnIDs) != 1 {
		return false
	}
	col, err := desc.FindColumnWithID(desc.PrimaryIndex.KeyColumnIDs[0])
	if err != nil {
		// Should never be in this case.
		panic(err)
	}
	if !col.IsHidden() {
		return false
	}
	if !strings.HasPrefix(col.GetName(), "rowid") {
		return false
	}
	if !col.GetType().Equal(types.Int) {
		return false
	}
	if !col.HasDefault() {
		return false
	}
	return col.GetDefaultExpr() == "unique_rowid()"
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
			if err := desc.AddSecondaryIndex(*t.Index); err != nil {
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
						break
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
			primaryIndexCopy := desc.GetPrimaryIndex().IndexDescDeepCopy()
			// Move the old primary index from the table descriptor into the mutations queue
			// to schedule it for deletion.
			if err := desc.AddDropIndexMutation(&primaryIndexCopy); err != nil {
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
					primaryIndex.Name = PrimaryKeyIndexName(desc.Name)
				} else {
					primaryIndex.Name = args.NewPrimaryIndexName
				}
				// This is needed for `ALTER PRIMARY KEY`. Because the new primary index
				// is initially created as a secondary index before being promoted as a
				// real primary index here. `StoreColumnNames` and `StoreColumnIDs` are
				// both filled when the index is first created. So just need to promote
				// the version number here.
				// TODO (Chengxiong): this is not needed in 22.2 since all indexes are
				// created with PrimaryIndexWithStoredColumnsVersion since 22.1.
				if primaryIndex.Version == descpb.StrictIndexColumnIDGuaranteesVersion {
					primaryIndex.Version = descpb.PrimaryIndexWithStoredColumnsVersion
				}
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
				if err := desc.AddDropIndexMutation(oldIndexCopy); err != nil {
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
			desc.RemoveColumnFromFamilyAndPrimaryIndex(t.Column.ID)
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

	uniqueName := GenerateUniqueName(newCol.GetName(), nameExists)

	// Remember the name of oldCol, because newCol will take it.
	oldColName := oldCol.GetName()

	// Rename old column to this new name, and rename newCol to oldCol's name.
	desc.RenameColumnDescriptor(oldCol, uniqueName)
	desc.RenameColumnDescriptor(newCol, oldColName)

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
	constraintID descpb.ConstraintID,
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
		ConstraintID:        constraintID,
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

// AddModifyRowLevelTTLMutation adds a row-level TTL mutation to descs.Mutations.
func (desc *Mutable) AddModifyRowLevelTTLMutation(
	ttl *descpb.ModifyRowLevelTTL, direction descpb.DescriptorMutation_Direction,
) {
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_ModifyRowLevelTTL{ModifyRowLevelTTL: ttl},
		Direction:   direction,
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

// AddDropIndexMutation adds a a dropping index mutation for the given
// index descriptor.
func (desc *Mutable) AddDropIndexMutation(idx *descpb.IndexDescriptor) error {
	if err := desc.checkValidIndex(idx); err != nil {
		return err
	}
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Index{Index: idx},
		Direction:   descpb.DescriptorMutation_DROP,
	}
	desc.addMutation(m)
	return nil
}

// AddIndexMutation adds an index mutation to desc.Mutations.
func (desc *Mutable) AddIndexMutation(
	ctx context.Context,
	idx *descpb.IndexDescriptor,
	direction descpb.DescriptorMutation_Direction,
	settings *cluster.Settings,
) error {
	if !settings.Version.IsActive(ctx, clusterversion.MVCCIndexBackfiller) || !UseMVCCCompliantIndexCreation.Get(&settings.SV) {
		return desc.DeprecatedAddIndexMutation(idx, direction)
	}

	if err := desc.checkValidIndex(idx); err != nil {
		return err
	}
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Index{Index: idx},
		Direction:   direction,
	}
	desc.addMutation(m)
	return nil
}

// DeprecatedAddIndexMutation adds an index mutation to desc.Mutations that
// assumes that the first state an added index should be placed into
// is DELETE_ONLY rather than BACKFILLING.
func (desc *Mutable) DeprecatedAddIndexMutation(
	idx *descpb.IndexDescriptor, direction descpb.DescriptorMutation_Direction,
) error {
	if err := desc.checkValidIndex(idx); err != nil {
		return err
	}
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Index{Index: idx},
		Direction:   direction,
	}
	desc.deprecatedAddMutation(m)
	return nil
}

func (desc *Mutable) checkValidIndex(idx *descpb.IndexDescriptor) error {
	switch idx.Type {
	case descpb.IndexDescriptor_FORWARD:
		if err := checkColumnsValidForIndex(desc, idx.KeyColumnNames); err != nil {
			return err
		}
	case descpb.IndexDescriptor_INVERTED:
		if err := checkColumnsValidForInvertedIndex(desc, idx.KeyColumnNames); err != nil {
			return err
		}
	}
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
		switch m.Descriptor_.(type) {
		case *descpb.DescriptorMutation_Index:
			m.State = descpb.DescriptorMutation_BACKFILLING
		default:
			m.State = descpb.DescriptorMutation_DELETE_ONLY
		}
	case descpb.DescriptorMutation_DROP:
		m.State = descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
	}
	desc.addMutationWithNextID(m)
	// If we are adding an index, we add another mutation for the
	// temporary index used by the index backfiller.
	//
	// The index backfiller code currently assumes that it can
	// always find the temporary indexes in the Mutations array,
	// in same order as the adding indexes.
	if idxMut, ok := m.Descriptor_.(*descpb.DescriptorMutation_Index); ok {
		if m.Direction == descpb.DescriptorMutation_ADD {
			tempIndex := *protoutil.Clone(idxMut.Index).(*descpb.IndexDescriptor)
			tempIndex.UseDeletePreservingEncoding = true
			tempIndex.ID = 0
			tempIndex.Name = ""
			m2 := descpb.DescriptorMutation{
				Descriptor_: &descpb.DescriptorMutation_Index{Index: &tempIndex},
				Direction:   descpb.DescriptorMutation_ADD,
				State:       descpb.DescriptorMutation_DELETE_ONLY,
			}
			desc.addMutationWithNextID(m2)
		}
	}
}

// deprecatedAddMutation assumes that new indexes are added in the
// DELETE_ONLY state.
func (desc *Mutable) deprecatedAddMutation(m descpb.DescriptorMutation) {
	switch m.Direction {
	case descpb.DescriptorMutation_ADD:
		m.State = descpb.DescriptorMutation_DELETE_ONLY
	case descpb.DescriptorMutation_DROP:
		m.State = descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
	}
	desc.addMutationWithNextID(m)
}

func (desc *Mutable) addMutationWithNextID(m descpb.DescriptorMutation) {
	// For tables created in the same transaction the next mutation ID will
	// not have been allocated and the added mutation will use an invalid ID.
	// This is fine because the mutation will be processed immediately.
	m.MutationID = desc.ClusterVersion.NextMutationID
	desc.NextMutationID = desc.ClusterVersion.NextMutationID + 1
	desc.Mutations = append(desc.Mutations, m)
}

// MakeFirstMutationPublic implements the TableDescriptor interface.
func (desc *wrapper) MakeFirstMutationPublic(
	includeConstraints catalog.MutationPublicationFilter,
) (catalog.TableDescriptor, error) {
	// Clone the ImmutableTable descriptor because we want to create an ImmutableCopy one.
	table := desc.NewBuilder().(TableDescriptorBuilder).BuildExistingMutableTable()
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

// MakePublic implements the TableDescriptor interface.
func (desc *wrapper) MakePublic() catalog.TableDescriptor {
	// Clone the ImmutableTable descriptor because we want to create an ImmutableCopy one.
	table := desc.NewBuilder().(TableDescriptorBuilder).BuildExistingMutableTable()
	table.State = descpb.DescriptorState_PUBLIC
	table.Version++
	return table
}

// HasPrimaryKey implements the TableDescriptor interface.
func (desc *wrapper) HasPrimaryKey() bool {
	return !desc.PrimaryIndex.Disabled
}

// HasColumnBackfillMutation implements the TableDescriptor interface.
func (desc *wrapper) HasColumnBackfillMutation() bool {
	for _, m := range desc.AllMutations() {
		if col := m.AsColumn(); col != nil {
			if catalog.ColumnNeedsBackfill(col) {
				return true
			}
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

// AllIndexSpans implements the TableDescriptor interface.
func (desc *wrapper) AllIndexSpans(codec keys.SQLCodec) roachpb.Spans {
	var spans roachpb.Spans
	for _, index := range desc.NonDropIndexes() {
		spans = append(spans, desc.IndexSpan(codec, index.GetID()))
	}
	return spans
}

// PrimaryIndexSpan implements the TableDescriptor interface.
func (desc *wrapper) PrimaryIndexSpan(codec keys.SQLCodec) roachpb.Span {
	return desc.IndexSpan(codec, desc.PrimaryIndex.ID)
}

// IndexSpan implements the TableDescriptor interface.
func (desc *wrapper) IndexSpan(codec keys.SQLCodec, indexID descpb.IndexID) roachpb.Span {
	prefix := roachpb.Key(rowenc.MakeIndexKeyPrefix(codec, desc.GetID(), indexID))
	return roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
}

// TableSpan implements the TableDescriptor interface.
func (desc *wrapper) TableSpan(codec keys.SQLCodec) roachpb.Span {
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

// CheckConstraintUsesColumn implements the TableDescriptor interface.
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

// ActiveChecks implements the TableDescriptor interface.
func (desc *immutable) ActiveChecks() []descpb.TableDescriptor_CheckConstraint {
	return desc.allChecks
}

// IsShardColumn implements the TableDescriptor interface.
func (desc *wrapper) IsShardColumn(col catalog.Column) bool {
	return nil != catalog.FindNonDropIndex(desc, func(idx catalog.Index) bool {
		return idx.IsSharded() && idx.GetShardColumnName() == col.GetName()
	})
}

// TableDesc implements the TableDescriptor interface.
func (desc *wrapper) TableDesc() *descpb.TableDescriptor {
	return &desc.TableDescriptor
}

// GenerateUniqueName attempts to generate a unique name with the given prefix.
// It will first try prefix by itself, then it will subsequently try adding
// numeric digits at the end, starting from 1.
func GenerateUniqueName(prefix string, nameExistsFunc func(name string) bool) string {
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
//
// Deprecated: Do not use.
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

// IsLocalityRegionalByRow implements the TableDescriptor interface.
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

// GetRegionalByTableRegion implements the TableDescriptor interface.
func (desc *wrapper) GetRegionalByTableRegion() (catpb.RegionName, error) {
	if !desc.IsLocalityRegionalByTable() {
		return "", errors.AssertionFailedf("%s is not REGIONAL BY TABLE", desc.Name)
	}
	region := desc.LocalityConfig.GetRegionalByTable().Region
	if region == nil {
		return catpb.RegionName(tree.PrimaryRegionNotSpecifiedName), nil
	}
	return *region, nil
}

// GetRegionalByRowTableRegionColumnName implements the TableDescriptor interface.
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

// GetRowLevelTTL implements the TableDescriptor interface.
func (desc *wrapper) GetRowLevelTTL() *catpb.RowLevelTTL {
	return desc.RowLevelTTL
}

// HasRowLevelTTL implements the TableDescriptor interface.
func (desc *wrapper) HasRowLevelTTL() bool {
	return desc.RowLevelTTL != nil
}

// GetExcludeDataFromBackup implements the TableDescriptor interface.
func (desc *wrapper) GetExcludeDataFromBackup() bool {
	return desc.ExcludeDataFromBackup
}

// GetStorageParams implements the TableDescriptor interface.
func (desc *wrapper) GetStorageParams(spaceBetweenEqual bool) []string {
	var storageParams []string
	var spacing string
	if spaceBetweenEqual {
		spacing = ` `
	}
	appendStorageParam := func(key, value string) {
		storageParams = append(storageParams, key+spacing+`=`+spacing+value)
	}
	if ttl := desc.GetRowLevelTTL(); ttl != nil {
		appendStorageParam(`ttl`, `'on'`)
		appendStorageParam(`ttl_automatic_column`, `'on'`)
		appendStorageParam(`ttl_expire_after`, string(ttl.DurationExpr))
		if bs := ttl.SelectBatchSize; bs != 0 {
			appendStorageParam(`ttl_select_batch_size`, fmt.Sprintf(`%d`, bs))
		}
		if bs := ttl.DeleteBatchSize; bs != 0 {
			appendStorageParam(`ttl_delete_batch_size`, fmt.Sprintf(`%d`, bs))
		}
		if cron := ttl.DeletionCron; cron != "" {
			appendStorageParam(`ttl_job_cron`, fmt.Sprintf(`'%s'`, cron))
		}
		if rc := ttl.RangeConcurrency; rc != 0 {
			appendStorageParam(`ttl_range_concurrency`, fmt.Sprintf(`%d`, rc))
		}
		if rl := ttl.DeleteRateLimit; rl != 0 {
			appendStorageParam(`ttl_delete_rate_limit`, fmt.Sprintf(`%d`, rl))
		}
		if pause := ttl.Pause; pause {
			appendStorageParam(`ttl_pause`, fmt.Sprintf(`%t`, pause))
		}
		if p := ttl.RowStatsPollInterval; p != 0 {
			appendStorageParam(`ttl_row_stats_poll_interval`, fmt.Sprintf(`'%s'`, p.String()))
		}
	}
	if exclude := desc.GetExcludeDataFromBackup(); exclude {
		appendStorageParam(`exclude_data_from_backup`, `true`)
	}
	return storageParams
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
		return regionName != catpb.RegionName(tree.PrimaryRegionNotSpecifiedName)
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
func LocalityConfigRegionalByTable(region tree.Name) catpb.LocalityConfig {
	l := &catpb.LocalityConfig_RegionalByTable_{
		RegionalByTable: &catpb.LocalityConfig_RegionalByTable{},
	}
	if region != tree.PrimaryRegionNotSpecifiedName {
		regionName := catpb.RegionName(region)
		l.RegionalByTable.Region = &regionName
	}
	return catpb.LocalityConfig{Locality: l}
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
func LocalityConfigRegionalByRow(regionColName tree.Name) catpb.LocalityConfig {
	rbr := &catpb.LocalityConfig_RegionalByRow{}
	if regionColName != tree.RegionalByRowRegionNotSpecifiedName {
		rbr.As = (*string)(&regionColName)
	}
	return catpb.LocalityConfig{
		Locality: &catpb.LocalityConfig_RegionalByRow_{
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

// SetDeclarativeSchemaChangerState is part of the catalog.MutableDescriptor
// interface.
func (desc *Mutable) SetDeclarativeSchemaChangerState(state *scpb.DescriptorState) {
	desc.DeclarativeSchemaChangerState = state
}

// LocalityConfigGlobal returns a config for a GLOBAL table.
func LocalityConfigGlobal() catpb.LocalityConfig {
	return catpb.LocalityConfig{
		Locality: &catpb.LocalityConfig_Global_{
			Global: &catpb.LocalityConfig_Global{},
		},
	}
}

// PrimaryKeyIndexName returns an appropriate PrimaryKey index name for the
// given table.
func PrimaryKeyIndexName(tableName string) string {
	return tableName + "_pkey"
}

// UpdateColumnsDependedOnBy creates, updates or deletes a depended-on-by column
// reference by ID.
func (desc *Mutable) UpdateColumnsDependedOnBy(id descpb.ID, colIDs catalog.TableColSet) {
	ref := descpb.TableDescriptor_Reference{
		ID:        id,
		ColumnIDs: colIDs.Ordered(),
		ByID:      true,
	}
	for i := range desc.DependedOnBy {
		by := &desc.DependedOnBy[i]
		if by.ID == id {
			if colIDs.Empty() {
				desc.DependedOnBy = append(desc.DependedOnBy[:i], desc.DependedOnBy[i+1:]...)
				return
			}
			*by = ref
			return
		}
	}
	desc.DependedOnBy = append(desc.DependedOnBy, ref)
}
