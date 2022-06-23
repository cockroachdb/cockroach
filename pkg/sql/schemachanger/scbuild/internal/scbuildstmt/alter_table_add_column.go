// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

func alterTableAddColumn(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, t *tree.AlterTableAddColumn,
) {
	b.IncrementSchemaChangeAlterCounter("table", "add_column")
	d := t.ColumnDef
	// Check column non-existence.
	{
		elts := b.ResolveColumn(tbl.TableID, d.Name, ResolveParams{
			IsExistenceOptional: true,
			RequiredPrivilege:   privilege.CREATE,
		})
		_, _, col := scpb.FindColumn(elts)
		if col != nil {
			if t.IfNotExists {
				return
			}
			if col.IsSystemColumn {
				panic(pgerror.Newf(pgcode.DuplicateColumn,
					"column name %q conflicts with a system column name",
					d.Name))
			}
			panic(sqlerrors.NewColumnAlreadyExistsError(string(d.Name), tn.Object()))
		}
	}
	if d.IsSerial {
		panic(scerrors.NotImplementedErrorf(d, "contains serial data type"))
	}
	if d.GeneratedIdentity.IsGeneratedAsIdentity {
		panic(scerrors.NotImplementedErrorf(d, "contains generated identity type"))
	}
	// Unique without an index is unsupported.
	if d.Unique.WithoutIndex {
		// TODO(rytaft): add support for this in the future if we want to expose
		// UNIQUE WITHOUT INDEX to users.
		panic(errors.WithHint(
			pgerror.New(
				pgcode.FeatureNotSupported,
				"adding a column marked as UNIQUE WITHOUT INDEX is unsupported",
			),
			"add the column first, then run ALTER TABLE ... ADD CONSTRAINT to add a "+
				"UNIQUE WITHOUT INDEX constraint on the column",
		))
	}
	if d.PrimaryKey.IsPrimaryKey {
		publicTargets := b.QueryByID(tbl.TableID).Filter(
			func(_ scpb.Status, target scpb.TargetStatus, _ scpb.Element) bool {
				return target == scpb.ToPublic
			},
		)
		_, _, primaryIdx := scpb.FindPrimaryIndex(publicTargets)
		// TODO(#82735): support when primary key is implicit
		if primaryIdx != nil {
			panic(pgerror.Newf(pgcode.InvalidColumnDefinition,
				"multiple primary keys for table %q are not allowed", tn.Object()))
		}
	}
	if d.IsComputed() {
		d.Computed.Expr = schemaexpr.MaybeRewriteComputedColumn(d.Computed.Expr, b.SessionData())
	}
	cdd, err := tabledesc.MakeColumnDefDescs(b, d, b.SemaCtx(), b.EvalCtx())
	if err != nil {
		panic(err)
	}
	desc := cdd.ColumnDescriptor
	desc.ID = b.NextTableColumnID(tbl)
	spec := addColumnSpec{
		tbl: tbl,
		col: &scpb.Column{
			TableID:                 tbl.TableID,
			ColumnID:                desc.ID,
			IsHidden:                desc.Hidden,
			IsInaccessible:          desc.Inaccessible,
			GeneratedAsIdentityType: desc.GeneratedAsIdentityType,
			PgAttributeNum:          desc.GetPGAttributeNum(),
		},
	}
	if ptr := desc.GeneratedAsIdentitySequenceOption; ptr != nil {
		spec.col.GeneratedAsIdentitySequenceOption = *ptr
	}
	spec.name = &scpb.ColumnName{
		TableID:  tbl.TableID,
		ColumnID: spec.col.ColumnID,
		Name:     string(d.Name),
	}
	spec.colType = &scpb.ColumnType{
		TableID:    tbl.TableID,
		ColumnID:   spec.col.ColumnID,
		IsNullable: desc.Nullable,
		IsVirtual:  desc.Virtual,
	}

	spec.colType.TypeT = b.ResolveTypeRef(d.Type)
	if spec.colType.TypeT.Type.UserDefined() {
		typeID, err := typedesc.UserDefinedTypeOIDToID(spec.colType.TypeT.Type.Oid())
		if err != nil {
			panic(err)
		}
		_, _, tableNamespace := scpb.FindNamespace(b.QueryByID(tbl.TableID))
		_, _, typeNamespace := scpb.FindNamespace(b.QueryByID(typeID))
		if typeNamespace.DatabaseID != tableNamespace.DatabaseID {
			typeName := tree.MakeTypeNameWithPrefix(b.NamePrefix(typeNamespace), typeNamespace.Name)
			panic(pgerror.Newf(
				pgcode.FeatureNotSupported,
				"cross database type references are not supported: %s",
				typeName.String()))
		}
	}
	// Block unsupported types.
	switch spec.colType.Type.Oid() {
	case oid.T_int2vector, oid.T_oidvector:
		panic(pgerror.Newf(
			pgcode.FeatureNotSupported,
			"VECTOR column types are unsupported",
		))
	}
	if desc.IsComputed() {
		expr := b.ComputedColumnExpression(tbl, d)
		spec.colType.ComputeExpr = b.WrapExpression(tbl.TableID, expr)
		if desc.Virtual {
			b.IncrementSchemaChangeAddColumnQualificationCounter("virtual")
		} else {
			b.IncrementSchemaChangeAddColumnQualificationCounter("computed")
		}
	}
	if d.HasColumnFamily() {
		elts := b.QueryByID(tbl.TableID)
		var found bool
		scpb.ForEachColumnFamily(elts, func(_ scpb.Status, target scpb.TargetStatus, cf *scpb.ColumnFamily) {
			if target == scpb.ToPublic && cf.Name == string(d.Family.Name) {
				spec.colType.FamilyID = cf.FamilyID
				found = true
			}
		})
		if !found {
			if !d.Family.Create {
				panic(errors.Errorf("unknown family %q", d.Family.Name))
			}
			spec.fam = &scpb.ColumnFamily{
				TableID:  tbl.TableID,
				FamilyID: b.NextColumnFamilyID(tbl),
				Name:     string(d.Family.Name),
			}
			spec.colType.FamilyID = spec.fam.FamilyID
		} else if d.Family.Create && !d.Family.IfNotExists {
			panic(errors.Errorf("family %q already exists", d.Family.Name))
		}
	}
	if desc.HasDefault() {
		expression := b.WrapExpression(tbl.TableID, cdd.DefaultExpr)
		spec.def = &scpb.ColumnDefaultExpression{
			TableID:    tbl.TableID,
			ColumnID:   spec.col.ColumnID,
			Expression: *expression,
		}
		b.IncrementSchemaChangeAddColumnQualificationCounter("default_expr")
	}
	// We're checking to see if a user is trying add a non-nullable column without a default to a
	// non-empty table by scanning the primary index span with a limit of 1 to see if any key exists.
	if !desc.Nullable && !desc.HasDefault() && !desc.IsComputed() && !b.IsTableEmpty(tbl) {
		panic(sqlerrors.NewNonNullViolationError(d.Name.String()))
	}
	if desc.HasOnUpdate() {
		spec.onUpdate = &scpb.ColumnOnUpdateExpression{
			TableID:    tbl.TableID,
			ColumnID:   spec.col.ColumnID,
			Expression: *b.WrapExpression(tbl.TableID, cdd.OnUpdateExpr),
		}
		b.IncrementSchemaChangeAddColumnQualificationCounter("on_update")
	}
	// Add secondary indexes for this column.
	var primaryIdx *scpb.PrimaryIndex
	var indexIDsForZoneConfig []catid.IndexID

	if newPrimary := addColumn(b, spec); newPrimary != nil {
		primaryIdx = newPrimary
		indexIDsForZoneConfig = append(indexIDsForZoneConfig, newPrimary.IndexID)
	} else {
		publicTargets := b.QueryByID(tbl.TableID).Filter(
			func(_ scpb.Status, target scpb.TargetStatus, _ scpb.Element) bool {
				return target == scpb.ToPublic
			},
		)
		_, _, primaryIdx = scpb.FindPrimaryIndex(publicTargets)
	}
	if idx := cdd.PrimaryKeyOrUniqueIndexDescriptor; idx != nil {
		idx.ID = b.NextTableIndexID(tbl)
		{
			tableElts := b.QueryByID(tbl.TableID)
			namesToIDs := make(map[string]descpb.ColumnID)
			scpb.ForEachColumnName(tableElts, func(current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnName) {
				if target == scpb.ToPublic {
					namesToIDs[e.Name] = e.ColumnID
				}
			})
			for _, colName := range cdd.PrimaryKeyOrUniqueIndexDescriptor.KeyColumnNames {
				cdd.PrimaryKeyOrUniqueIndexDescriptor.KeyColumnIDs = append(cdd.PrimaryKeyOrUniqueIndexDescriptor.KeyColumnIDs, namesToIDs[colName])
			}
		}
		secondaryIndexIDs := addSecondaryIndexTargetsForAddColumn(b, tbl, idx, primaryIdx)
		indexIDsForZoneConfig = append(indexIDsForZoneConfig, secondaryIndexIDs...)
	}
	if len(indexIDsForZoneConfig) > 0 {
		tableElts := b.QueryByID(tbl.TableID)
		if _, _, elem := scpb.FindTableLocalityRegionalByRow(tableElts); elem != nil {
			_, _, namespace := scpb.FindNamespace(tableElts)
			dbRegionConfig := synthesizeMultiRegionConfig(b, namespace.DatabaseID)
			prepareZoneConfigForMultiRegionTable(b,
				dbRegionConfig,
				tbl.TableID,
				applyZoneConfigForMultiRegionTableOptionNewIndexes(indexIDsForZoneConfig...))
		}
	}

	switch spec.colType.Type.Family() {
	case types.EnumFamily:
		b.IncrementEnumCounter(sqltelemetry.EnumInTable)
	default:
		b.IncrementSchemaChangeAddColumnTypeCounter(spec.colType.Type.TelemetryName())
	}
}

type addColumnSpec struct {
	tbl      *scpb.Table
	col      *scpb.Column
	fam      *scpb.ColumnFamily
	name     *scpb.ColumnName
	colType  *scpb.ColumnType
	def      *scpb.ColumnDefaultExpression
	onUpdate *scpb.ColumnOnUpdateExpression
	comment  *scpb.ColumnComment
}

// addColumn is a helper function which adds column element targets and ensures
// that the new column is backed by a primary index, which it returns.
func addColumn(b BuildCtx, spec addColumnSpec) (backing *scpb.PrimaryIndex) {
	b.Add(spec.col)
	if spec.fam != nil {
		b.Add(spec.fam)
	}
	b.Add(spec.name)
	b.Add(spec.colType)
	if spec.def != nil {
		b.Add(spec.def)
	}
	if spec.onUpdate != nil {
		b.Add(spec.onUpdate)
	}
	if spec.comment != nil {
		b.Add(spec.comment)
	}
	// Add or update primary index for non-virtual columns.
	if spec.colType.IsVirtual {
		return nil
	}
	// Check whether a target to add a new primary index already exists. If so,
	// simply add the new column to its storing columns.
	var existing, freshlyAdded *scpb.PrimaryIndex
	publicTargets := b.QueryByID(spec.tbl.TableID).Filter(
		func(_ scpb.Status, target scpb.TargetStatus, _ scpb.Element) bool {
			return target == scpb.ToPublic
		},
	)
	scpb.ForEachPrimaryIndex(publicTargets, func(status scpb.Status, _ scpb.TargetStatus, idx *scpb.PrimaryIndex) {
		existing = idx
		if status == scpb.Status_ABSENT {
			// TODO(postamar): does it matter that there could be more than one?
			freshlyAdded = idx
		}
	})
	if freshlyAdded != nil {
		var tempIndex *scpb.TemporaryIndex
		scpb.ForEachTemporaryIndex(b.QueryByID(spec.tbl.TableID), func(
			status scpb.Status, ts scpb.TargetStatus, e *scpb.TemporaryIndex,
		) {
			if ts != scpb.Transient {
				return
			}
			if e.IndexID == freshlyAdded.TemporaryIndexID {
				if tempIndex != nil {
					panic(errors.AssertionFailedf(
						"multiple temporary index elements exist with index id %d for table %d",
						freshlyAdded.TemporaryIndexID, e.TableID,
					))
				}
				tempIndex = e
			}
		})
		if tempIndex == nil {
			panic(errors.AssertionFailedf(
				"failed to find temporary index element for new primary index id %d for table %d",
				freshlyAdded.IndexID, freshlyAdded.TableID,
			))
		}
		// Exceptionally, we can edit the element directly here, by virtue of it
		// currently being in the ABSENT state we know that it was introduced as a
		// PUBLIC target by the current statement.
		freshlyAdded.StoringColumnIDs = append(freshlyAdded.StoringColumnIDs, spec.col.ColumnID)
		tempIndex.StoringColumnIDs = append(tempIndex.StoringColumnIDs, spec.col.ColumnID)
		if colinfo.CanHaveCompositeKeyEncoding(spec.colType.Type) {
			freshlyAdded.CompositeColumnIDs = append(freshlyAdded.CompositeColumnIDs, spec.col.ColumnID)
			tempIndex.CompositeColumnIDs = append(tempIndex.CompositeColumnIDs, spec.col.ColumnID)
		}
		return freshlyAdded
	}
	// Otherwise, create a new primary index target and swap it with the existing
	// primary index.
	if existing == nil {
		// TODO(postamar): can this even be possible?
		panic(pgerror.Newf(pgcode.NoPrimaryKey, "missing active primary key"))
	}
	// Drop all existing primary index elements.
	b.Drop(existing)
	var existingName *scpb.IndexName
	var existingPartitioning *scpb.IndexPartitioning
	scpb.ForEachIndexName(publicTargets, func(_ scpb.Status, _ scpb.TargetStatus, name *scpb.IndexName) {
		if name.IndexID == existing.IndexID {
			existingName = name
		}
	})
	scpb.ForEachIndexPartitioning(publicTargets, func(_ scpb.Status, _ scpb.TargetStatus, part *scpb.IndexPartitioning) {
		if part.IndexID == existing.IndexID {
			existingPartitioning = part
		}
	})
	if existingPartitioning != nil {
		b.Drop(existingPartitioning)
	}
	if existingName != nil {
		b.Drop(existingName)
	}
	// Create the new primary index element and its dependents.
	replacement := protoutil.Clone(existing).(*scpb.PrimaryIndex)
	replacement.IndexID = b.NextTableIndexID(spec.tbl)
	replacement.SourceIndexID = existing.IndexID
	replacement.StoringColumnIDs = append(replacement.StoringColumnIDs, spec.col.ColumnID)
	replacement.TemporaryIndexID = replacement.IndexID + 1
	if colinfo.CanHaveCompositeKeyEncoding(spec.colType.Type) {
		replacement.CompositeColumnIDs = append(replacement.CompositeColumnIDs, spec.col.ColumnID)
	}
	b.Add(replacement)
	if existingName != nil {
		updatedName := protoutil.Clone(existingName).(*scpb.IndexName)
		updatedName.IndexID = replacement.IndexID
		b.Add(updatedName)
	}
	if existingPartitioning != nil {
		updatedPartitioning := protoutil.Clone(existingPartitioning).(*scpb.IndexPartitioning)
		updatedPartitioning.IndexID = replacement.IndexID
		b.Add(updatedPartitioning)
	}

	temp := &scpb.TemporaryIndex{
		Index:                    protoutil.Clone(replacement).(*scpb.PrimaryIndex).Index,
		IsUsingSecondaryEncoding: false,
	}
	temp.TemporaryIndexID = 0
	temp.IndexID = b.NextTableIndexID(spec.tbl)
	b.AddTransient(temp)
	if existingPartitioning != nil {
		updatedPartitioning := protoutil.Clone(existingPartitioning).(*scpb.IndexPartitioning)
		updatedPartitioning.IndexID = temp.IndexID
		b.Add(updatedPartitioning)
	}

	return replacement
}

// getImplicitSecondaryIndexName determines the implicit name for a secondary
// index, this logic matches tabledesc.BuildIndexName.
func getImplicitSecondaryIndexName(
	b BuildCtx, tbl *scpb.Table, id descpb.IndexID, numImplicitColumns int,
) string {
	elts := b.QueryByID(tbl.TableID)
	var idx *scpb.Index
	scpb.ForEachSecondaryIndex(elts, func(current scpb.Status, target scpb.TargetStatus, e *scpb.SecondaryIndex) {
		if e.IndexID == id {
			idx = &e.Index
		}
	})
	if idx == nil {
		panic(errors.AssertionFailedf("unable to find secondary index."))
	}
	// An index name has a segment for the table name, each key column, and a
	// final word (either "idx" or "key").
	segments := make([]string, 0, len(idx.KeyColumnIDs)+2)
	// Add the table name segment.
	var tblName *scpb.Namespace
	scpb.ForEachNamespace(b, func(current scpb.Status, target scpb.TargetStatus, e *scpb.Namespace) {
		if e.DescriptorID == tbl.TableID {
			tblName = e
		}
	})
	if tblName == nil {
		panic(errors.AssertionFailedf("unable to find table name."))
	}
	segments = append(segments, tblName.Name)
	findColumnNameByID := func(colID descpb.ColumnID) ElementResultSet {
		var columnName *scpb.ColumnName
		scpb.ForEachColumnName(b, func(current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnName) {
			if e.ColumnID == colID {
				columnName = e
			}
		})
		if columnName == nil {
			panic(errors.AssertionFailedf("unable to find column name."))
		}
		return b.ResolveColumn(tbl.TableID, tree.Name(columnName.Name), ResolveParams{})
	}
	// Add the key column segments. For inaccessible columns, use "expr" as the
	// segment. If there are multiple inaccessible columns, add an incrementing
	// integer suffix.
	exprCount := 0
	for i, n := numImplicitColumns, len(idx.KeyColumnIDs); i < n; i++ {
		var segmentName string
		colElts := findColumnNameByID(idx.KeyColumnIDs[i])
		_, _, col := scpb.FindColumnType(colElts)
		if col.ComputeExpr != nil {
			if exprCount == 0 {
				segmentName = "expr"
			} else {
				segmentName = fmt.Sprintf("expr%d", exprCount)
			}
			exprCount++
		} else {
			_, _, colName := scpb.FindColumnName(colElts)
			segmentName = colName.Name
		}
		segments = append(segments, segmentName)
	}

	// Add the final segment.
	if idx.IsUnique {
		segments = append(segments, "key")
	} else {
		segments = append(segments, "idx")
	}
	// Append digits to the index name to make it unique, if necessary.
	baseName := strings.Join(segments, "_")
	name := baseName
	for i := 1; ; i++ {
		foundIndex := false
		scpb.ForEachIndexName(elts, func(current scpb.Status, target scpb.TargetStatus, e *scpb.IndexName) {
			if e.Name == name {
				foundIndex = true
			}
		})
		if !foundIndex {
			break
		}
		name = fmt.Sprintf("%s%d", baseName, i)
	}
	return name
}

func addSecondaryIndexTargetsForAddColumn(
	b BuildCtx, tbl *scpb.Table, desc *descpb.IndexDescriptor, newPrimaryIdx *scpb.PrimaryIndex,
) []catid.IndexID {
	var partitioning *catpb.PartitioningDescriptor
	index := scpb.Index{
		TableID:             tbl.TableID,
		IndexID:             desc.ID,
		KeyColumnIDs:        desc.KeyColumnIDs,
		KeyColumnDirections: make([]scpb.Index_Direction, len(desc.KeyColumnIDs)),
		KeySuffixColumnIDs:  desc.KeySuffixColumnIDs,
		StoringColumnIDs:    desc.StoreColumnIDs,
		CompositeColumnIDs:  desc.CompositeColumnIDs,
		IsUnique:            desc.Unique,
		IsInverted:          desc.Type == descpb.IndexDescriptor_INVERTED,
		SourceIndexID:       newPrimaryIdx.IndexID,
	}
	tempIndexID := index.IndexID + 1 // this is enforced below
	index.TemporaryIndexID = tempIndexID
	for i, dir := range desc.KeyColumnDirections {
		if dir == descpb.IndexDescriptor_DESC {
			index.KeyColumnDirections[i] = scpb.Index_DESC
		}
	}
	if desc.Sharded.IsSharded {
		index.Sharding = &desc.Sharded
	}
	// If necessary add suffix columns, this would normally be done inside
	// allocateIndexIDs, but we are going to do it explicitly for the declarative
	// schema changer.
	{
		publicTargets := b.QueryByID(tbl.TableID).Filter(
			func(_ scpb.Status, target scpb.TargetStatus, _ scpb.Element) bool {
				return target == scpb.ToPublic
			},
		)
		// Apply any implicit partitioning columns first, if they are missing.
		scpb.ForEachIndexPartitioning(b, func(current scpb.Status, target scpb.TargetStatus, e *scpb.IndexPartitioning) {
			if e.IndexID == newPrimaryIdx.IndexID &&
				e.TableID == newPrimaryIdx.TableID {
				partitioning = &e.PartitioningDescriptor
			}
		})
		keyColSet := catalog.TableColSet{}
		extraSuffixColumns := catalog.TableColSet{}
		for _, colID := range index.KeyColumnIDs {
			keyColSet.Add(colID)
		}
		if partitioning != nil &&
			len(desc.Partitioning.Range) == 0 && len(desc.Partitioning.List) == 0 &&
			partitioning.NumImplicitColumns > 0 {
			keyColumns := make([]descpb.ColumnID, 0, len(index.KeyColumnIDs)+int(partitioning.NumImplicitColumns))
			for _, colID := range newPrimaryIdx.KeyColumnIDs[0:partitioning.NumImplicitColumns] {
				if !keyColSet.Contains(colID) {
					keyColumns = append(keyColumns, colID)
					keyColSet.Add(colID)
				}
			}
			index.KeyColumnIDs = append(keyColumns, index.KeyColumnIDs...)
		} else if len(desc.Partitioning.Range) != 0 || len(desc.Partitioning.List) != 0 {
			partitioning = &desc.Partitioning
		}
		for _, colID := range newPrimaryIdx.KeyColumnIDs {
			if !keyColSet.Contains(colID) {
				extraSuffixColumns.Add(colID)
			}
		}
		if !extraSuffixColumns.Empty() {
			index.KeySuffixColumnIDs = append(index.KeySuffixColumnIDs, extraSuffixColumns.Ordered()...)
		}
		// Add in any composite columns at the same time.
		// CompositeColumnIDs is defined as the subset of columns in the index key
		// or in the primary key whose type has a composite encoding, like DECIMAL
		// for instance.
		compositeColIDs := catalog.TableColSet{}
		scpb.ForEachColumnType(publicTargets, func(current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnType) {
			if colinfo.CanHaveCompositeKeyEncoding(e.Type) {
				compositeColIDs.Add(e.ColumnID)
			}
		})
		for _, colID := range index.KeyColumnIDs {
			if compositeColIDs.Contains(colID) {
				index.CompositeColumnIDs = append(index.CompositeColumnIDs, colID)
			}
		}
		for _, colID := range index.KeySuffixColumnIDs {
			if compositeColIDs.Contains(colID) {
				index.CompositeColumnIDs = append(index.CompositeColumnIDs, colID)
			}
		}
	}
	sec := &scpb.SecondaryIndex{Index: index}
	b.Add(sec)
	indexName := desc.Name
	numImplicitColumns := 0
	if partitioning != nil {
		numImplicitColumns = int(partitioning.NumImplicitColumns)
	}
	if indexName == "" {
		indexName = getImplicitSecondaryIndexName(b, tbl, index.IndexID, numImplicitColumns)
	}
	b.Add(&scpb.IndexName{
		TableID: tbl.TableID,
		IndexID: index.IndexID,
		Name:    indexName,
	})
	temp := &scpb.TemporaryIndex{
		Index:                    protoutil.Clone(sec).(*scpb.SecondaryIndex).Index,
		IsUsingSecondaryEncoding: true,
	}
	temp.TemporaryIndexID = 0
	temp.IndexID = nextRelationIndexID(b, tbl)
	if temp.IndexID != tempIndexID {
		panic(errors.AssertionFailedf(
			"assumed temporary index ID %d != %d", tempIndexID, temp.IndexID,
		))
	}
	b.AddTransient(temp)
	// Add in the partitioning descriptor for the final and temporary index.
	if partitioning != nil {
		b.Add(&scpb.IndexPartitioning{
			TableID:                tbl.TableID,
			IndexID:                index.IndexID,
			PartitioningDescriptor: *protoutil.Clone(partitioning).(*catpb.PartitioningDescriptor),
		})
		b.Add(&scpb.IndexPartitioning{
			TableID:                tbl.TableID,
			IndexID:                temp.IndexID,
			PartitioningDescriptor: *protoutil.Clone(partitioning).(*catpb.PartitioningDescriptor),
		})
	}
	return []catid.IndexID{temp.IndexID, index.IndexID}
}
