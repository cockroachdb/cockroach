// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tabledesc

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// Mutable is a custom type for TableDescriptors
// going through schema mutations.
type Mutable struct {
	wrapper

	// original represents the version of the table descriptor read from the
	// store.
	original *immutable
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
	if desc.PrimaryIndex.ID == idx.GetID() || idx.GetEncodingType() == catenumpb.PrimaryIndexEncoding {
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
//	CREATE INDEX ON t (a)
//	=> t_a_idx
//
//	CREATE UNIQUE INDEX ON t (a, b)
//	=> t_a_b_key
//
//	CREATE INDEX ON t ((a + b), c, lower(d))
//	=> t_expr_c_expr1_idx
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
		col, err := catalog.MustFindColumnByName(tableDesc, idx.KeyColumnNames[i])
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
		foundIndex := catalog.FindIndexByName(tableDesc, name)
		if foundIndex == nil {
			break
		}
		name = fmt.Sprintf("%s%d", baseName, i)
	}

	return name, nil
}

// ForeachDependedOnBy implements the TableDescriptor interface.
func (desc *wrapper) ForeachDependedOnBy(
	f func(dep *descpb.TableDescriptor_Reference) error,
) error {
	for i := range desc.DependedOnBy {
		if err := f(&desc.DependedOnBy[i]); err != nil {
			return iterutil.Map(err)
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
			return iterutil.Map(err)
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
// it can mutate the TableDescriptor if desired. It also takes SerializedExprTyp
// to indicate the type of the expression.
func ForEachExprStringInTableDesc(
	descI catalog.TableDescriptor, f func(expr *string, typ catalog.DescExprType) error,
) error {
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
			if err := f(c.DefaultExpr, catalog.SQLExpr); err != nil {
				return err
			}
		}
		if c.IsComputed() {
			if err := f(c.ComputeExpr, catalog.SQLExpr); err != nil {
				return err
			}
		}
		if c.HasOnUpdate() {
			if err := f(c.OnUpdateExpr, catalog.SQLExpr); err != nil {
				return err
			}
		}
		return nil
	}
	doIndex := func(i catalog.Index) error {
		if i.IsPartial() {
			return f(&i.IndexDesc().Predicate, catalog.SQLExpr)
		}
		return nil
	}
	doCheck := func(c *descpb.TableDescriptor_CheckConstraint) error {
		return f(&c.Expr, catalog.SQLExpr)
	}
	doUwi := func(uwi *descpb.UniqueWithoutIndexConstraint) error {
		if uwi.Predicate != "" {
			return f(&uwi.Predicate, catalog.SQLExpr)
		}
		return nil
	}
	doTrigger := func(t *descpb.TriggerDescriptor) error {
		if t.WhenExpr != "" {
			if err := f(&t.WhenExpr, catalog.SQLExpr); err != nil {
				return err
			}
		}
		if t.FuncBody == "" {
			panic(errors.AssertionFailedf("expected non-empty trigger function body"))
		}
		return f(&t.FuncBody, catalog.PLpgSQLStmt)
	}
	doPolicy := func(p *descpb.PolicyDescriptor) error {
		if p.UsingExpr != "" {
			if err := f(&p.UsingExpr, catalog.SQLExpr); err != nil {
				return err
			}
		}
		if p.WithCheckExpr != "" {
			if err := f(&p.WithCheckExpr, catalog.SQLExpr); err != nil {
				return err
			}
		}
		return nil
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

	// Process uwis.
	for i := range desc.UniqueWithoutIndexConstraints {
		if err := doUwi(&desc.UniqueWithoutIndexConstraints[i]); err != nil {
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
		if c := mut.GetConstraint(); c != nil &&
			c.ConstraintType == descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX {
			if err := doUwi(&c.UniqueWithoutIndexConstraint); err != nil {
				return err
			}
		}
	}

	// Process all triggers.
	for i := range desc.Triggers {
		if err := doTrigger(&desc.Triggers[i]); err != nil {
			return err
		}
	}

	// Process all policies.
	for i := range desc.Policies {
		if err := doPolicy(&desc.Policies[i]); err != nil {
			return err
		}
	}
	return nil
}

// GetAllReferencedTableIDs implements the TableDescriptor interface.
func (desc *wrapper) GetAllReferencedTableIDs() descpb.IDs {
	var ids catalog.DescriptorIDSet

	// Collect referenced table IDs in foreign keys.
	for _, fk := range desc.OutboundForeignKeys() {
		ids.Add(fk.GetReferencedTableID())
	}
	for _, fk := range desc.InboundForeignKeys() {
		ids.Add(fk.GetOriginTableID())
	}
	// Add trigger dependencies.
	for i := range desc.Triggers {
		ids = ids.Union(catalog.MakeDescriptorIDSet(desc.Triggers[i].DependsOn...))
	}
	// Add policy dependencies.
	for i := range desc.Policies {
		ids = ids.Union(catalog.MakeDescriptorIDSet(desc.Policies[i].DependsOnRelations...))
	}
	// Add view dependencies.
	ids = ids.Union(catalog.MakeDescriptorIDSet(desc.DependsOn...))

	return ids.Ordered()
}

// GetAllReferencedTypeIDs implements the TableDescriptor interface.
func (desc *wrapper) GetAllReferencedTypeIDs(
	dbDesc catalog.DatabaseDescriptor, getType func(descpb.ID) (catalog.TypeDescriptor, error),
) (referencedAnywhere, referencedInColumns descpb.IDs, _ error) {
	ids, err := desc.getAllReferencedTypesInTableColumns(getType)
	if err != nil {
		return nil, nil, err
	}
	referencedInColumns = ids.Ordered()

	// REGIONAL BY TABLE tables may have a dependency with the multi-region enum.
	exists := desc.GetMultiRegionEnumDependencyIfExists()
	if exists {
		regionEnumID, err := dbDesc.MultiRegionEnumID()
		if err != nil {
			return nil, nil, err
		}
		ids.Add(regionEnumID)
	}

	// Add type dependencies from triggers.
	for i := range desc.Triggers {
		ids = ids.Union(catalog.MakeDescriptorIDSet(desc.Triggers[i].DependsOnTypes...))
	}

	// Add type dependencies from policies.
	for i := range desc.Policies {
		ids = ids.Union(catalog.MakeDescriptorIDSet(desc.Policies[i].DependsOnTypes...))
	}

	// Add any other type dependencies that are not
	// used in a column (specifically for views).
	for _, id := range desc.DependsOnTypes {
		ids.Add(id)
	}

	return ids.Ordered(), referencedInColumns, nil
}

// GetAllReferencedFunctionIDs implements the TableDescriptor interface.
func (desc *wrapper) GetAllReferencedFunctionIDs() (catalog.DescriptorIDSet, error) {
	var ret catalog.DescriptorIDSet
	for _, c := range desc.AllConstraints() {
		ids, err := desc.GetAllReferencedFunctionIDsInConstraint(c.GetConstraintID())
		if err != nil {
			return catalog.DescriptorIDSet{}, err
		}
		ret = ret.Union(ids)
	}
	for _, c := range desc.AllColumns() {
		for _, id := range c.ColumnDesc().UsesFunctionIds {
			ret.Add(id)
		}
	}
	// Add routine dependencies from triggers.
	for i := range desc.Triggers {
		ret = ret.Union(catalog.MakeDescriptorIDSet(desc.Triggers[i].DependsOnRoutines...))
	}
	// Add deps from policies
	for i := range desc.Policies {
		ret = ret.Union(catalog.MakeDescriptorIDSet(desc.Policies[i].DependsOnFunctions...))
	}
	// TODO(chengxiong): add logic to extract references from indexes when UDFs
	// are allowed in them.
	return ret.Union(catalog.MakeDescriptorIDSet(desc.DependsOnFunctions...)), nil
}

// GetAllReferencedFunctionIDsInConstraint implements the TableDescriptor
// interface.
func (desc *wrapper) GetAllReferencedFunctionIDsInConstraint(
	cstID descpb.ConstraintID,
) (fnIDs catalog.DescriptorIDSet, err error) {
	c := catalog.FindConstraintByID(desc, cstID)
	ck := c.AsCheck()
	if ck == nil {
		return catalog.DescriptorIDSet{}, nil
	}
	ret, err := schemaexpr.GetUDFIDsFromExprStr(ck.GetExpr())
	if err != nil {
		return catalog.DescriptorIDSet{}, err
	}
	return ret, nil
}

// GetAllReferencedFunctionIDsInTrigger implements the TableDescriptor
// interface.
func (desc *wrapper) GetAllReferencedFunctionIDsInTrigger(
	triggerID descpb.TriggerID,
) (fnIDs catalog.DescriptorIDSet) {
	t := catalog.FindTriggerByID(desc, triggerID)
	for _, id := range t.DependsOnRoutines {
		fnIDs.Add(id)
	}
	return fnIDs
}

// GetAllReferencedFunctionIDsInPolicy implements the TableDescriptor interface.
func (desc *wrapper) GetAllReferencedFunctionIDsInPolicy(
	policyID descpb.PolicyID,
) (fnIDs catalog.DescriptorIDSet) {
	t := catalog.FindPolicyByID(desc, policyID)
	for _, id := range t.DependsOnFunctions {
		fnIDs.Add(id)
	}
	return fnIDs
}

// GetAllReferencedFunctionIDsInColumnExprs implements the TableDescriptor
// interface.
func (desc *wrapper) GetAllReferencedFunctionIDsInColumnExprs(
	colID descpb.ColumnID,
) (fnIDs catalog.DescriptorIDSet, err error) {
	col := catalog.FindColumnByID(desc, colID)
	if col == nil {
		return catalog.DescriptorIDSet{}, nil
	}

	var ret catalog.DescriptorIDSet
	// TODO(chengxiong): add support for computed columns when UDFs are allowed in
	// them.
	if !col.IsComputed() {
		if col.HasDefault() {
			ids, err := schemaexpr.GetUDFIDsFromExprStr(col.GetDefaultExpr())
			if err != nil {
				return catalog.DescriptorIDSet{}, err
			}
			ret = ret.Union(ids)
		}
		// TODO(chengxiong): add support for ON UPDATE expressions when UDFs are
		// allowed in them.
	}

	return ret, nil
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
// GetAllReferencedTypeIDs accounts for this dependency.
func (desc *wrapper) getAllReferencedTypesInTableColumns(
	getType func(descpb.ID) (catalog.TypeDescriptor, error),
) (ret catalog.DescriptorIDSet, _ error) {
	// All serialized expressions within a table descriptor are serialized
	// with type annotations as ID's, so this visitor will collect them all.
	visitor := &tree.TypeCollectorVisitor{
		OIDs: make(map[oid.Oid]struct{}),
	}

	addOIDsInExpr := func(exprStr *string, typ catalog.DescExprType) error {
		if typ != catalog.SQLExpr {
			// Skip trigger function bodies.
			return nil
		}
		expr, err := parser.ParseExpr(*exprStr)
		if err != nil {
			return err
		}
		tree.WalkExpr(visitor, expr)
		return nil
	}

	if err := ForEachExprStringInTableDesc(desc, addOIDsInExpr); err != nil {
		return ret, err
	}

	// For each of the collected type IDs in the table descriptor expressions,
	// collect the closure of IDs referenced.
	for id := range visitor.OIDs {
		uid := typedesc.UserDefinedTypeOIDToID(id)
		typDesc, err := getType(uid)
		if err != nil {
			return ret, err
		}
		typDesc.GetIDClosure().ForEach(ret.Add)
	}

	// Now add all of the column types in the table.
	addIDsInColumn := func(c *descpb.ColumnDescriptor) {
		typedesc.GetTypeDescriptorClosure(c.Type).ForEach(ret.Add)
	}
	for i := range desc.Columns {
		addIDsInColumn(&desc.Columns[i])
	}
	for _, mut := range desc.Mutations {
		if c := mut.GetColumn(); c != nil {
			addIDsInColumn(c)
		}
	}

	return ret, nil
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
	if err := desc.AllocateIDsWithoutValidation(ctx, true /*createMissingPrimaryKey*/); err != nil {
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
func (desc *Mutable) AllocateIDsWithoutValidation(
	ctx context.Context, createMissingPrimaryKey bool,
) error {
	// Only tables with physical data can have / need a primary key. In the
	// declarative schema changer the primary key is always created explicitly,
	// so we don't need to create it here.
	if desc.IsPhysicalTable() && createMissingPrimaryKey {
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
			return catalog.FindColumnByName(desc, name) != nil
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
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
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
		// non-inverted columns in the index's key.
		colIDs := idx.CollectKeyColumnIDs()
		isInverted := idx.GetType() == descpb.IndexDescriptor_INVERTED
		invID := catid.ColumnID(0)
		if isInverted {
			invID = idx.InvertedColumnID()
		}
		var extraColumnIDs []descpb.ColumnID
		for _, primaryColID := range desc.PrimaryIndex.KeyColumnIDs {
			if !colIDs.Contains(primaryColID) {
				extraColumnIDs = append(extraColumnIDs, primaryColID)
				colIDs.Add(primaryColID)
			} else if invID == primaryColID {
				// In an inverted index, the inverted column's value is not equal to the
				// actual data in the row for that column. As a result, if the inverted
				// column happens to also be in the primary key, it's crucial that
				// the index key still be suffixed with that full primary key value to
				// preserve the index semantics.
				// extraColumnIDs = append(extraColumnIDs, primaryColID)
				// However, this functionality is not supported by the execution engine,
				// so prevent it by returning an error.
				col, err := catalog.MustFindColumnByID(desc, primaryColID)
				if err != nil {
					return err
				}
				return unimplemented.NewWithIssuef(84405,
					"primary key column %s cannot be present in an inverted index",
					col.GetName(),
				)
			}
		}
		if idx.GetEncodingType() == catenumpb.SecondaryIndexEncoding {
			idx.IndexDesc().KeySuffixColumnIDs = extraColumnIDs
		} else {
			colIDs = idx.CollectKeyColumnIDs()
		}

		// Inverted indexes don't store composite values in the individual
		// paths present. The composite values will be encoded in
		// the primary index itself.
		compositeColIDsLocal := compositeColIDs.Copy()
		if isInverted {
			compositeColIDsLocal.Remove(invID)
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
			col, err := catalog.MustFindColumnByName(desc, colName)
			if err != nil {
				return err
			}
			if primaryColIDs.Contains(col.GetID()) && idx.GetEncodingType() == catenumpb.SecondaryIndexEncoding {
				// If the primary index contains a stored column, we don't need to
				// store it - it's already part of the index.
				err = errors.WithDetailf(
					sqlerrors.NewColumnAlreadyExistsInIndexError(idx.GetName(), col.GetName()),
					"column %q is part of the primary index and therefore implicit in all indexes", col.GetName())
				return err
			}
			if colIDs.Contains(col.GetID()) {
				return sqlerrors.NewColumnAlreadyExistsInIndexError(idx.GetName(), col.GetName())
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
			if compositeColIDsLocal.Contains(colID) {
				idx.IndexDesc().CompositeColumnIDs = append(idx.IndexDesc().CompositeColumnIDs, colID)
			}
		}
		for _, colID := range idx.IndexDesc().KeySuffixColumnIDs {
			if compositeColIDsLocal.Contains(colID) {
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
//   - Put all columns in family 0.
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
	if desc.Version == desc.ClusterVersion().Version+1 || desc.ClusterVersion().Version == 0 {
		return
	}
	desc.Version++

	// Starting in 19.2 we use a zero-valued ModificationTime when incrementing
	// the version, and then, upon reading, use the MVCC timestamp to populate
	// the ModificationTime.
	desc.ResetModificationTime()
}

// ResetModificationTime implements the catalog.MutableDescriptor interface.
func (desc *Mutable) ResetModificationTime() {
	desc.ModificationTime = hlc.Timestamp{}
}

// OriginalName implements the MutableDescriptor interface.
func (desc *Mutable) OriginalName() string {
	return desc.ClusterVersion().Name
}

// OriginalID implements the MutableDescriptor interface.
func (desc *Mutable) OriginalID() descpb.ID {
	return desc.ClusterVersion().ID
}

// OriginalVersion implements the MutableDescriptor interface.
func (desc *Mutable) OriginalVersion() descpb.DescriptorVersion {
	return desc.ClusterVersion().Version
}

// ClusterVersion returns the version of the table descriptor read from the
// store, if any.
//
// TODO(ajwerner): Make this deal in catalog.TableDescriptor instead.
func (desc *Mutable) ClusterVersion() descpb.TableDescriptor {
	if desc.original == nil {
		return descpb.TableDescriptor{}
	}
	return desc.original.TableDescriptor
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

func checkColumnsValidForInvertedIndex(
	tableDesc *Mutable, indexColNames []string, colDirs []catenumpb.IndexColumn_Direction,
) error {
	lastCol := len(indexColNames) - 1
	for i, indexCol := range indexColNames {
		for _, col := range tableDesc.NonDropColumns() {
			if col.GetName() == indexCol {
				// The last column indexed by an inverted index must be
				// inverted indexable.
				if i == lastCol && !colinfo.ColumnTypeIsInvertedIndexable(col.GetType()) {
					return NewInvalidInvertedColumnError(col.GetName(), col.GetType().String())
				}
				if i == lastCol && colDirs[i] == catenumpb.IndexColumn_DESC {
					return pgerror.New(pgcode.FeatureNotSupported,
						"the last column in an inverted index cannot have the DESC option")
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

// NewInvalidInvertedColumnError returns an error for a column that's not
// inverted indexable.
func NewInvalidInvertedColumnError(colName, colType string) error {
	return errors.WithHint(
		pgerror.Newf(
			pgcode.FeatureNotSupported,
			"column %s of type %s is not allowed as the last column in an inverted index",
			colName, colType,
		),
		"see the documentation for more information about inverted indexes: "+docs.URL("inverted-indexes.html"),
	)
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
	idx.EncodingType = catenumpb.PrimaryIndexEncoding
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
		if err := checkColumnsValidForInvertedIndex(
			desc, idx.KeyColumnNames, idx.KeyColumnDirections,
		); err != nil {
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
	currentMutationID := desc.ClusterVersion().NextMutationID
	for _, col := range desc.DeletableColumns() {
		if col.ColName() == name &&
			((col.Public()) ||
				(col.Adding() && col.MutationID() == currentMutationID)) {
			return col, nil
		}
	}
	return nil, colinfo.NewUndefinedColumnError(string(name))
}

// DropConstraint drops a constraint, either by removing it from the table
// descriptor or by queuing a mutation for a schema change.
func (desc *Mutable) DropConstraint(
	constraint catalog.Constraint,
	removeFKBackRef func(catalog.ForeignKeyConstraint) error,
	removeFnBackRef func(*descpb.TableDescriptor_CheckConstraint) error,
) error {
	if u := constraint.AsUniqueWithIndex(); u != nil {
		if u.Primary() {
			primaryIndex := u.IndexDescDeepCopy()
			primaryIndex.Disabled = true
			desc.SetPrimaryIndex(primaryIndex)
			return nil
		}
		return unimplemented.NewWithIssueDetailf(42840, "drop-constraint-unique",
			"cannot drop UNIQUE constraint %q using ALTER TABLE DROP CONSTRAINT, use DROP INDEX CASCADE instead",
			tree.ErrNameString(u.GetName()))
	}
	if constraint.Adding() {
		return unimplemented.NewWithIssuef(42844,
			"constraint %q in the middle of being added, try again later", constraint.GetName())
	}
	if constraint.Dropped() {
		return unimplemented.NewWithIssuef(42844,
			"constraint %q in the middle of being dropped", constraint.GetName())
	}

	if uwoi := constraint.AsUniqueWithoutIndex(); uwoi != nil {
		// Search through the descriptor's unique constraints and delete the
		// one that we're supposed to be deleting.
		for i := range desc.UniqueWithoutIndexConstraints {
			ref := &desc.UniqueWithoutIndexConstraints[i]
			if ref.Name == uwoi.GetName() {
				// If the constraint is unvalidated, there's no assumption that it must
				// hold for all rows, so it can be dropped immediately.
				if uwoi.IsConstraintUnvalidated() {
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
	} else if ck := constraint.AsCheck(); ck != nil {
		for i, c := range desc.Checks {
			if c.Name == ck.GetName() {
				// If the constraint is unvalidated, there's no assumption that it must
				// hold for all rows, so it can be dropped immediately.
				// We also drop the constraint immediately instead of queuing a mutation
				// unless the cluster is fully upgraded to 19.2, for backward
				// compatibility.
				if ck.IsConstraintUnvalidated() {
					if err := removeFnBackRef(c); err != nil {
						return err
					}
					desc.Checks = append(desc.Checks[:i], desc.Checks[i+1:]...)
					return nil
				}
				c.Validity = descpb.ConstraintValidity_Dropping
				desc.AddCheckMutation(c, descpb.DescriptorMutation_DROP)
				return nil
			}
		}
	} else if fk := constraint.AsForeignKey(); fk != nil {
		// Search through the descriptor's foreign key constraints and delete the
		// one that we're supposed to be deleting.
		for i := range desc.OutboundFKs {
			ref := &desc.OutboundFKs[i]
			if ref.Name == fk.GetName() {
				// If the constraint is unvalidated, there's no assumption that it must
				// hold for all rows, so it can be dropped immediately.
				if fk.IsConstraintUnvalidated() {
					// Remove the backreference.
					if err := removeFKBackRef(fk); err != nil {
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
	}
	return errors.AssertionFailedf("constraint %q not found on table %q", constraint.GetName(), desc.Name)
}

// RenameConstraint renames a constraint.
func (desc *Mutable) RenameConstraint(
	constraint catalog.Constraint,
	newName string,
	dependentViewRenameError func(string, descpb.ID) error,
	renameFK func(*Mutable, catalog.ForeignKeyConstraint, string) error,
) error {
	if u := constraint.AsUniqueWithIndex(); u != nil {
		for _, tableRef := range desc.DependedOnBy {
			if tableRef.IndexID != u.GetID() {
				continue
			}
			return dependentViewRenameError("index", tableRef.ID)
		}
		u.IndexDesc().Name = newName
		return nil
	}
	switch constraint.GetConstraintValidity() {
	case descpb.ConstraintValidity_Validating:
		return unimplemented.NewWithIssuef(42844,
			"constraint %q in the middle of being added, try again later",
			tree.ErrNameString(constraint.GetName()))
	}

	if ck := constraint.AsCheck(); ck != nil {
		ck.CheckDesc().Name = newName
	} else if fk := constraint.AsForeignKey(); fk != nil {
		// Update the name on the referenced table descriptor.
		if err := renameFK(desc, fk, newName); err != nil {
			return err
		}
		fk.ForeignKeyDesc().Name = newName
	} else if uwoi := constraint.AsUniqueWithoutIndex(); uwoi != nil {
		uwoi.UniqueWithoutIndexDesc().Name = newName
	} else {
		return unimplemented.Newf(fmt.Sprintf("rename-constraint-%T", constraint),
			"constraint %q has unsupported type", tree.ErrNameString(constraint.GetName()))
	}
	return nil
}

// GetIndexMutationCapabilities implements the TableDescriptor interface.
func (desc *wrapper) GetIndexMutationCapabilities(id descpb.IndexID) (bool, bool) {
	for _, mutation := range desc.Mutations {
		if mutationIndex := mutation.GetIndex(); mutationIndex != nil {
			if mutationIndex.ID == id {
				return true,
					mutation.State == descpb.DescriptorMutation_WRITE_ONLY
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
	col, err := catalog.MustFindColumnByID(desc, desc.PrimaryIndex.KeyColumnIDs[0])
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
//
//	never be the case for a validated constraint to enter this
//	method.
//
// Validating  - The constraint has already been added, and just needs to be
//
//	marked as validated.
//
// Unvalidated - The constraint has not yet been added, and needs to be added
//
//	for the first time.
func (desc *Mutable) MakeMutationComplete(m descpb.DescriptorMutation) error {
	switch m.Direction {
	case descpb.DescriptorMutation_ADD:
		switch t := m.Descriptor_.(type) {
		case *descpb.DescriptorMutation_Column:
			desc.AddColumn(t.Column)

		case *descpb.DescriptorMutation_Index:
			// If a primary index is being made public, then we only need set the
			// index inside the descriptor directly. Only the declarative schema
			// changer will use index mutations like this.
			isPrimaryIndexToPublic := desc.IsPrimaryKeySwapMutation(&m)
			if isPrimaryIndexToPublic {
				desc.SetPrimaryIndex(*t.Index)
			} else {
				// Otherwise, we need to add this index as a secondary index.
				if err := desc.AddSecondaryIndex(*t.Index); err != nil {
					return err
				}
			}

		case *descpb.DescriptorMutation_Constraint:
			switch t.Constraint.ConstraintType {
			case descpb.ConstraintToUpdate_CHECK:
				switch t.Constraint.Check.Validity {
				case descpb.ConstraintValidity_Validating:
					// Constraint already added, just mark it as Validated.
					for _, c := range desc.Checks {
						if c.ConstraintID == t.Constraint.Check.ConstraintID {
							c.Validity = descpb.ConstraintValidity_Validated
							break
						}
					}
				case descpb.ConstraintValidity_Unvalidated, descpb.ConstraintValidity_Validated:
					// add the constraint to the list of check constraints on the table
					// descriptor.
					//
					// The "validated" validity seems strange -- why would we ever complete
					// a constraint mutation whose validity is already "validated"?
					// This is because of how legacy schema changer is implemented and will
					// occur for the following case:
					// `ALTER TABLE .. ADD CONSTRAINT .. NOT VALID, VALIDATE CONSTRAINT ..`
					// where the constraint mutation's validity is changed by `VALIDATE CONSTRAINT`
					// to "validated", and in the job of `ADD CONSTRAINT .. NOT VALID` we
					// came to mark the constraint mutation as completed.
					// The same is true for FK and UWI constraints.
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
						if fk.ConstraintID == t.Constraint.ForeignKey.ConstraintID {
							fk.Validity = descpb.ConstraintValidity_Validated
							break
						}
					}
				case descpb.ConstraintValidity_Unvalidated, descpb.ConstraintValidity_Validated:
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
						if uc.ConstraintID == t.Constraint.UniqueWithoutIndexConstraint.ConstraintID {
							uc.Validity = descpb.ConstraintValidity_Validated
							break
						}
					}
				case descpb.ConstraintValidity_Unvalidated, descpb.ConstraintValidity_Validated:
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
					if c.ConstraintID == t.Constraint.Check.ConstraintID {
						desc.Checks = append(desc.Checks[:i], desc.Checks[i+1:]...)
						break
					}
				}
				col, err := catalog.MustFindColumnByID(desc, t.Constraint.NotNullColumn)
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
			newIndex, err := catalog.MustFindIndexByID(desc, args.NewPrimaryIndexId)
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
				newIndex, err := catalog.MustFindIndexByID(desc, newID)
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
	oldCol, err := catalog.MustFindColumnByID(desc, swap.OldColumnId)
	if err != nil {
		return err
	}
	newCol, err := catalog.MustFindColumnByID(desc, swap.NewColumnId)
	if err != nil {
		return err
	}

	// Mark newCol as no longer a computed column.
	newCol.ColumnDesc().ComputeExpr = nil

	// Make the oldCol a computed column by setting its computed expression.
	oldCol.ColumnDesc().ComputeExpr = &swap.InverseExpr

	// Swap the onUpdate/default expressions. These expressions can only be stored
	// for non-computed columns, so they need to be swapped at the same time as
	// the computed expression. Additionally, we have already validated in
	// AlterColumnType that these expressions can be applied to the new column
	// type with an automatic cast. Therefore, there is no need to validate the
	// cast here.
	newCol.ColumnDesc().OnUpdateExpr = oldCol.ColumnDesc().OnUpdateExpr
	oldCol.ColumnDesc().OnUpdateExpr = nil
	newCol.ColumnDesc().DefaultExpr = oldCol.ColumnDesc().DefaultExpr
	oldCol.ColumnDesc().DefaultExpr = nil

	// Generate unique name for old column.
	nameExists := func(name string) bool {
		return catalog.FindColumnByName(desc, name) != nil
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
	desc.addIndexMutationMaybeWithTempIndex(m)
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
	desc.addIndexMutationMaybeWithTempIndex(m)
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
	desc.addIndexMutationMaybeWithTempIndex(m)
}

// MakeNotNullCheckConstraint creates a dummy check constraint equivalent to a
// NOT NULL constraint on a column, so that NOT NULL constraints can be added
// and dropped correctly in the schema changer. This function mutates inuseNames
// to add the new constraint name.
// TODO(mgartner): Move this to schemaexpr.CheckConstraintBuilder.
func MakeNotNullCheckConstraint(
	tbl catalog.TableDescriptor,
	col catalog.Column,
	validity descpb.ConstraintValidity,
	constraintID descpb.ConstraintID,
) *descpb.TableDescriptor_CheckConstraint {
	name := fmt.Sprintf("%s_auto_not_null", col.GetName())
	// If generated name isn't unique, attempt to add a number to the end to
	// get a unique name, as in generateNameForCheckConstraint().
	inuseNames := make(map[string]struct{})
	for _, c := range tbl.AllConstraints() {
		inuseNames[c.GetName()] = struct{}{}
	}
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

	expr := &tree.IsNotNullExpr{
		Expr: &tree.ColumnItem{ColumnName: tree.Name(col.GetName())},
	}

	return &descpb.TableDescriptor_CheckConstraint{
		Name:                name,
		Expr:                tree.Serialize(expr),
		Validity:            validity,
		ColumnIDs:           []descpb.ColumnID{col.GetID()},
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
	desc.addIndexMutationMaybeWithTempIndex(m)
}

// AddModifyRowLevelTTLMutation adds a row-level TTL mutation to descs.Mutations.
func (desc *Mutable) AddModifyRowLevelTTLMutation(
	ttl *descpb.ModifyRowLevelTTL, direction descpb.DescriptorMutation_Direction,
) {
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_ModifyRowLevelTTL{ModifyRowLevelTTL: ttl},
		Direction:   direction,
	}
	desc.addIndexMutationMaybeWithTempIndex(m)
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
	desc.addIndexMutationMaybeWithTempIndex(m)
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
	desc.addIndexMutationMaybeWithTempIndex(m)
	return nil
}

// AddIndexMutationMaybeWithTempIndex adds an index mutation to desc.Mutations
// for the provided index, and also synthesize and add the temp index mutation.
func (desc *Mutable) AddIndexMutationMaybeWithTempIndex(
	idx *descpb.IndexDescriptor, direction descpb.DescriptorMutation_Direction,
) error {
	if err := desc.checkValidIndex(idx); err != nil {
		return err
	}
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Index{Index: idx},
		Direction:   direction,
	}
	desc.addIndexMutationMaybeWithTempIndex(m)
	return nil
}

// AddIndexMutation adds an index mutation to desc.Mutations that
// with the provided initial state.
func (desc *Mutable) AddIndexMutation(
	idx *descpb.IndexDescriptor,
	direction descpb.DescriptorMutation_Direction,
	state descpb.DescriptorMutation_State,
) error {
	if err := desc.checkValidIndex(idx); err != nil {
		return err
	}
	stateIsValid := func() bool {
		switch direction {
		case descpb.DescriptorMutation_ADD:
			return state == descpb.DescriptorMutation_BACKFILLING ||
				state == descpb.DescriptorMutation_DELETE_ONLY
		case descpb.DescriptorMutation_DROP:
			return state == descpb.DescriptorMutation_WRITE_ONLY
		default:
			return false
		}
	}
	if !stateIsValid() {
		return errors.AssertionFailedf(
			"unexpected initial state %v for %v index mutation",
			state, direction,
		)
	}
	desc.addMutation(descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Index{Index: idx},
		Direction:   direction,
	}, state)
	return nil
}

func (desc *Mutable) checkValidIndex(idx *descpb.IndexDescriptor) error {
	switch idx.Type {
	case descpb.IndexDescriptor_FORWARD:
		if err := checkColumnsValidForIndex(desc, idx.KeyColumnNames); err != nil {
			return err
		}
	case descpb.IndexDescriptor_INVERTED:
		if err := checkColumnsValidForInvertedIndex(
			desc, idx.KeyColumnNames, idx.KeyColumnDirections,
		); err != nil {
			return err
		}
	}
	return nil
}

// AddPrimaryKeySwapMutation adds a PrimaryKeySwap mutation to the table descriptor.
func (desc *Mutable) AddPrimaryKeySwapMutation(swap *descpb.PrimaryKeySwap) {
	desc.addMutation(descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_PrimaryKeySwap{PrimaryKeySwap: swap},
		Direction:   descpb.DescriptorMutation_ADD,
	}, descpb.DescriptorMutation_DELETE_ONLY)
}

// AddMaterializedViewRefreshMutation adds a MaterializedViewRefreshMutation to
// the table descriptor.
func (desc *Mutable) AddMaterializedViewRefreshMutation(refresh *descpb.MaterializedViewRefresh) {
	desc.addMutation(descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_MaterializedViewRefresh{MaterializedViewRefresh: refresh},
		Direction:   descpb.DescriptorMutation_ADD,
	}, descpb.DescriptorMutation_DELETE_ONLY)
}

// AddComputedColumnSwapMutation adds a ComputedColumnSwap mutation to the table descriptor.
func (desc *Mutable) AddComputedColumnSwapMutation(swap *descpb.ComputedColumnSwap) {
	desc.addMutation(descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_ComputedColumnSwap{ComputedColumnSwap: swap},
		Direction:   descpb.DescriptorMutation_ADD,
	}, descpb.DescriptorMutation_DELETE_ONLY)
}

func (desc *Mutable) addIndexMutationMaybeWithTempIndex(m descpb.DescriptorMutation) {
	switch m.Direction {
	case descpb.DescriptorMutation_ADD:
		switch m.Descriptor_.(type) {
		case *descpb.DescriptorMutation_Index:
			m.State = descpb.DescriptorMutation_BACKFILLING
		default:
			m.State = descpb.DescriptorMutation_DELETE_ONLY
		}
	case descpb.DescriptorMutation_DROP:
		m.State = descpb.DescriptorMutation_WRITE_ONLY
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
			tempIndex.ConstraintID = 0
			m2 := descpb.DescriptorMutation{
				Descriptor_: &descpb.DescriptorMutation_Index{Index: &tempIndex},
				Direction:   descpb.DescriptorMutation_ADD,
				State:       descpb.DescriptorMutation_DELETE_ONLY,
			}
			desc.addMutationWithNextID(m2)
		}
	}
}

// addMutation assumes that new indexes are added in the
// DELETE_ONLY state.
func (desc *Mutable) addMutation(
	m descpb.DescriptorMutation, state descpb.DescriptorMutation_State,
) {
	m.State = state
	desc.addMutationWithNextID(m)
}

func (desc *Mutable) addMutationWithNextID(m descpb.DescriptorMutation) {
	// For tables created in the same transaction the next mutation ID will
	// not have been allocated and the added mutation will use an invalid ID.
	// This is fine because the mutation will be processed immediately.
	m.MutationID = desc.ClusterVersion().NextMutationID
	desc.NextMutationID = desc.ClusterVersion().NextMutationID + 1
	desc.Mutations = append(desc.Mutations, m)
}

// MakeFirstMutationPublic implements the TableDescriptor interface.
func (desc *wrapper) MakeFirstMutationPublic(
	filters ...catalog.MutationPublicationFilter,
) (catalog.TableDescriptor, error) {
	// Clone the ImmutableTable descriptor because we want to create an ImmutableCopy one.
	table := desc.NewBuilder().(TableDescriptorBuilder).BuildExistingMutableTable()
	mutationID := desc.Mutations[0].MutationID
	i := 0
	policy := makeMutationPublicationPolicy(filters...)
	var clone []descpb.DescriptorMutation
	for _, mutation := range desc.Mutations {
		if mutation.MutationID != mutationID {
			// Mutations are applied in a FIFO order. Only apply the first set
			// of mutations if they have the mutation ID we're looking for.
			break
		}
		i++
		switch {
		case policy.shouldSkip(desc, &mutation):
			// Don't add to clone.
		case policy.shouldRetain(desc, &mutation):
			mutation.Direction = descpb.DescriptorMutation_ADD
			fallthrough
		default:
			if err := table.MakeMutationComplete(mutation); err != nil {
				return nil, err
			}
		}
	}
	table.Mutations = append(clone, table.Mutations[i:]...)
	table.Version++
	return table, nil
}

type mutationPublicationPolicy struct {
	policy intsets.Fast
}

func makeMutationPublicationPolicy(
	filters ...catalog.MutationPublicationFilter,
) mutationPublicationPolicy {
	var p mutationPublicationPolicy
	for _, f := range filters {
		p.policy.Add(int(f))
	}
	return p
}

func (p mutationPublicationPolicy) includes(f catalog.MutationPublicationFilter) bool {
	return p.policy.Contains(int(f))
}

func (p mutationPublicationPolicy) shouldSkip(
	desc catalog.TableDescriptor, m *descpb.DescriptorMutation,
) bool {
	switch {
	case desc.IsPrimaryKeySwapMutation(m):
		return p.includes(catalog.IgnorePKSwaps)
	case m.GetConstraint() != nil:
		return p.includes(catalog.IgnoreConstraints)
	default:
		return false
	}
}

func (p mutationPublicationPolicy) shouldRetain(
	desc catalog.TableDescriptor, m *descpb.DescriptorMutation,
) bool {
	switch {
	case m.GetColumn() != nil && m.Direction == descpb.DescriptorMutation_DROP:
		return p.includes(catalog.RetainDroppingColumns)
	default:
		return false
	}
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
	return desc.ClusterVersion().ID == descpb.InvalidID
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
	if desc.External != nil {
		panic(errors.AssertionFailedf("%s uses external row data", desc.Name))
	}
	prefix := roachpb.Key(rowenc.MakeIndexKeyPrefix(codec, desc.GetID(), indexID))
	return roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
}

// IndexSpanAllowingExternalRowData implements the TableDescriptor interface.
func (desc *wrapper) IndexSpanAllowingExternalRowData(
	codec keys.SQLCodec, indexID descpb.IndexID,
) roachpb.Span {
	tableID := desc.GetID()
	if desc.External != nil {
		codec = keys.MakeSQLCodec(desc.External.TenantID)
		tableID = desc.External.TableID
	}
	prefix := roachpb.Key(rowenc.MakeIndexKeyPrefix(codec, tableID, indexID))
	return roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
}

// TableSpan implements the TableDescriptor interface.
func (desc *wrapper) TableSpan(codec keys.SQLCodec) roachpb.Span {
	if desc.External != nil {
		panic(errors.AssertionFailedf("%s uses external row data", desc.Name))
	}
	prefix := codec.TablePrefix(uint32(desc.ID))
	return roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
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

func (desc *Mutable) SetExternalRowData(ext *descpb.ExternalRowData) {
	// Do not set materialized views for sequences, they will
	// handle this on their own.
	desc.IsMaterializedView = !desc.IsSequence()
	desc.External = ext
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
	if desc.HasRowLevelTTL() {
		ttl := desc.GetRowLevelTTL()
		appendStorageParam(`ttl`, `'on'`)
		if ttl.HasDurationExpr() {
			appendStorageParam(`ttl_expire_after`, string(ttl.DurationExpr))
		}
		if ttl.HasExpirationExpr() {
			escapedTTLExpirationExpression := lexbase.EscapeSQLString(string(ttl.ExpirationExpr))
			appendStorageParam(`ttl_expiration_expression`, escapedTTLExpirationExpression)
		}
		if ttl.DeletionCron != "" {
			appendStorageParam(`ttl_job_cron`, fmt.Sprintf(`'%s'`, ttl.DeletionCronOrDefault()))
		}
		if bs := ttl.SelectBatchSize; bs != 0 {
			appendStorageParam(`ttl_select_batch_size`, fmt.Sprintf(`%d`, bs))
		}
		if bs := ttl.DeleteBatchSize; bs != 0 {
			appendStorageParam(`ttl_delete_batch_size`, fmt.Sprintf(`%d`, bs))
		}
		if rl := ttl.SelectRateLimit; rl != 0 {
			appendStorageParam(`ttl_select_rate_limit`, fmt.Sprintf(`%d`, rl))
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
		if labelMetrics := ttl.LabelMetrics; labelMetrics {
			appendStorageParam(`ttl_label_metrics`, fmt.Sprintf(`%t`, labelMetrics))
		}
		if ttl.DisableChangefeedReplication {
			appendStorageParam(`ttl_disable_changefeed_replication`, fmt.Sprintf("%t", ttl.DisableChangefeedReplication))
		}
	}
	if exclude := desc.GetExcludeDataFromBackup(); exclude {
		appendStorageParam(`exclude_data_from_backup`, `true`)
	}
	if settings := desc.AutoStatsSettings; settings != nil {
		if settings.Enabled != nil {
			value := *settings.Enabled
			appendStorageParam(catpb.AutoStatsEnabledTableSettingName,
				fmt.Sprintf("%v", value))
		}
		if settings.MinStaleRows != nil {
			value := *settings.MinStaleRows
			appendStorageParam(catpb.AutoStatsMinStaleTableSettingName,
				fmt.Sprintf("%d", value))
		}
		if settings.FractionStaleRows != nil {
			value := *settings.FractionStaleRows
			appendStorageParam(catpb.AutoStatsFractionStaleTableSettingName,
				fmt.Sprintf("%g", value))
		}
		if settings.PartialEnabled != nil {
			value := *settings.PartialEnabled
			appendStorageParam(catpb.AutoPartialStatsEnabledTableSettingName,
				fmt.Sprintf("%v", value))
		}
		if settings.PartialMinStaleRows != nil {
			value := *settings.PartialMinStaleRows
			appendStorageParam(catpb.AutoPartialStatsMinStaleTableSettingName,
				fmt.Sprintf("%d", value))
		}
		if settings.PartialFractionStaleRows != nil {
			value := *settings.PartialFractionStaleRows
			appendStorageParam(catpb.AutoPartialStatsFractionStaleTableSettingName,
				fmt.Sprintf("%g", value))
		}
	}
	if enabled, ok := desc.ForecastStatsEnabled(); ok {
		appendStorageParam(`sql_stats_forecasts_enabled`, strconv.FormatBool(enabled))
	}
	if count, ok := desc.HistogramSamplesCount(); ok {
		appendStorageParam(`sql_stats_histogram_samples_count`, fmt.Sprintf("%d", count))
	}
	if count, ok := desc.HistogramBucketsCount(); ok {
		appendStorageParam(`sql_stats_histogram_buckets_count`, fmt.Sprintf("%d", count))
	}
	if desc.IsSchemaLocked() {
		appendStorageParam(`schema_locked`, `true`)
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

// NoAutoStatsSettingsOverrides implements the TableDescriptor interface.
func (desc *wrapper) NoAutoStatsSettingsOverrides() bool {
	if desc.AutoStatsSettings == nil {
		return true
	}
	return desc.AutoStatsSettings.NoAutoStatsSettingsOverrides()
}

// AutoStatsCollectionEnabled implements the TableDescriptor interface.
func (desc *wrapper) AutoStatsCollectionEnabled() catpb.AutoStatsCollectionStatus {
	if desc.AutoStatsSettings == nil {
		return catpb.AutoStatsCollectionNotSet
	}
	return desc.AutoStatsSettings.AutoStatsCollectionEnabled()
}

func (desc *wrapper) AutoPartialStatsCollectionEnabled() catpb.AutoPartialStatsCollectionStatus {
	if desc.AutoStatsSettings == nil {
		return catpb.AutoPartialStatsCollectionNotSet
	}
	return desc.AutoStatsSettings.AutoPartialStatsCollectionEnabled()
}

// AutoStatsMinStaleRows implements the TableDescriptor interface.
func (desc *wrapper) AutoStatsMinStaleRows() (minStaleRows int64, ok bool) {
	if desc.AutoStatsSettings == nil {
		return 0, false
	}
	return desc.AutoStatsSettings.AutoStatsMinStaleRows()
}

// AutoStatsFractionStaleRows implements the TableDescriptor interface.
func (desc *wrapper) AutoStatsFractionStaleRows() (fractionStaleRows float64, ok bool) {
	if desc.AutoStatsSettings == nil {
		return 0, false
	}
	return desc.AutoStatsSettings.AutoStatsFractionStaleRows()
}

// GetAutoStatsSettings implements the TableDescriptor interface.
func (desc *wrapper) GetAutoStatsSettings() *catpb.AutoStatsSettings {
	return desc.AutoStatsSettings
}

// ForecastStatsEnabled implements the TableDescriptor interface.
func (desc *wrapper) ForecastStatsEnabled() (enabled bool, ok bool) {
	if desc.ForecastStats == nil {
		return false, false
	}
	return *desc.ForecastStats, true
}

// HistogramSamplesCount implements the TableDescriptor interface.
func (desc *wrapper) HistogramSamplesCount() (histogramSamplesCount uint32, ok bool) {
	if desc.HistogramSamples == nil {
		return 0, false
	}
	return *desc.HistogramSamples, true
}

// HistogramBucketsCount implements the TableDescriptor interface.
func (desc *wrapper) HistogramBucketsCount() (histogramBucketsCount uint32, ok bool) {
	if desc.HistogramBuckets == nil {
		return 0, false
	}
	return *desc.HistogramBuckets, true
}

// GetReplicatedPCRVersion is a part of the catalog.Descriptor
func (desc *wrapper) GetReplicatedPCRVersion() descpb.DescriptorVersion {
	return desc.ReplicatedPCRVersion
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

// BumpExternalAsOf increases the timestamp for external data row tables.
func (desc *Mutable) BumpExternalAsOf(timestamp hlc.Timestamp) error {
	if desc.External == nil {
		return errors.AssertionFailedf("cannot advanced timestamp on a real table (%d)", desc.GetID())
	}
	if timestamp.Less(desc.External.AsOf) {
		return errors.AssertionFailedf("new timestamp (%s) is less than the existing as of timestamp (%s)",
			timestamp,
			desc.External.AsOf)
	}
	desc.External.AsOf = timestamp
	return nil
}

// ForceModificationTime allows the modification time to be set externally.
// Note: This API is only used by the leasing code to adjust modification time
// since the builder will not populate it for offline descriptors.
func (desc *Mutable) ForceModificationTime(modificationTime hlc.Timestamp) {
	desc.ModificationTime = modificationTime
}
