// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdecomp"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

func alterTableAddColumn(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, stmt tree.Statement, t *tree.AlterTableAddColumn,
) {
	d := t.ColumnDef
	// We don't support handling zone config related properties for tables, so
	// throw an unsupported error.
	fallBackIfSubZoneConfigExists(b, t, tbl.TableID)
	fallBackIfRegionalByRowTable(b, t, tbl.TableID)

	// Check column non-existence.
	{
		elts := b.ResolveColumn(tbl.TableID, d.Name, ResolveParams{
			IsExistenceOptional: true,
			RequiredPrivilege:   privilege.CREATE,
		})
		_, colTargetStatus, col := scpb.FindColumn(elts)
		if col != nil {
			if t.IfNotExists {
				return
			}
			if col.IsSystemColumn {
				panic(pgerror.Newf(pgcode.DuplicateColumn,
					"column name %q conflicts with a system column name",
					d.Name))
			}
			if colTargetStatus != scpb.ToAbsent {
				panic(sqlerrors.NewColumnAlreadyExistsInRelationError(string(d.Name), tn.Object()))
			}
		}
	}
	if d.IsSerial {
		if b.SessionData().SerialNormalizationMode != sessiondatapb.SerialUsesRowID {
			panic(scerrors.NotImplementedErrorf(d, "contains serial data type in unsupported mode"))
		}
		d = alterTableAddColumnSerial(b, d, tn)

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
	{
		tableElts := b.QueryByID(tbl.TableID)
		if _, _, elem := scpb.FindTableLocalityRegionalByRow(tableElts); elem != nil {
			panic(scerrors.NotImplementedErrorf(d,
				"regional by row partitioning is not supported"))
		}
	}
	cdd, err := tabledesc.MakeColumnDefDescs(b, d, b.SemaCtx(), b.EvalCtx(), tree.ColumnDefaultExprInAddColumn)
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
		},
		unique:  d.Unique.IsUnique,
		notNull: !desc.Nullable,
	}
	// Only set PgAttributeNum if it differs from ColumnID.
	if pgAttNum := desc.GetPGAttributeNum(); pgAttNum != catid.PGAttributeNum(desc.ID) {
		spec.col.PgAttributeNum = pgAttNum
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
		TableID:                 tbl.TableID,
		ColumnID:                spec.col.ColumnID,
		IsNullable:              desc.Nullable,
		IsVirtual:               desc.Virtual,
		ElementCreationMetadata: scdecomp.NewElementCreationMetadata(b.EvalCtx().Settings.Version.ActiveVersion(b)),
	}

	_, _, tableNamespace := scpb.FindNamespace(b.QueryByID(tbl.TableID))
	spec.colType.TypeT = b.ResolveTypeRef(d.Type)
	if spec.colType.TypeT.Type.UserDefined() {
		typeID := typedesc.UserDefinedTypeOIDToID(spec.colType.TypeT.Type.Oid())
		maybeFailOnCrossDBTypeReference(b, typeID, tableNamespace.DatabaseID)
	}
	// Block unique indexes on unsupported types.
	if d.Unique.IsUnique &&
		!d.Unique.WithoutIndex &&
		!colinfo.ColumnTypeIsIndexable(spec.colType.Type) {
		typInfo := spec.colType.Type.DebugString()
		panic(unimplemented.NewWithIssueDetailf(35730, typInfo,
			"column %s is of type %s and thus is not indexable",
			d.Name,
			spec.colType.Type.Name()))
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
		expr := b.WrapExpression(tbl.TableID, b.ComputedColumnExpression(tbl, d))
		if spec.colType.ElementCreationMetadata.In_24_3OrLater {
			spec.compute = &scpb.ColumnComputeExpression{
				TableID:    tbl.TableID,
				ColumnID:   spec.col.ColumnID,
				Expression: *expr,
			}
		} else {
			spec.colType.ComputeExpr = expr
		}
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
	backing := addColumn(b, spec, t)
	if idx := cdd.PrimaryKeyOrUniqueIndexDescriptor; idx != nil {
		// TODO (xiang): Think it through whether this (i.e. backing is usually
		// final and sometimes old) is okay.
		idx.ID = b.NextTableIndexID(tbl.TableID)
		{
			namesToIDs := columnNamesToIDs(b, tbl)
			for _, colName := range cdd.PrimaryKeyOrUniqueIndexDescriptor.KeyColumnNames {
				idx.KeyColumnIDs = append(idx.KeyColumnIDs, namesToIDs[colName])
			}
		}
		addSecondaryIndexTargetsForAddColumn(b, tbl, idx, backing)
	}
	b.LogEventForExistingTarget(spec.col)
	switch spec.colType.Type.Family() {
	case types.EnumFamily:
		b.IncrementEnumCounter(sqltelemetry.EnumInTable)
	default:
		b.IncrementSchemaChangeAddColumnTypeCounter(spec.colType.Type.TelemetryName())
	}
}

func alterTableAddColumnSerial(
	b BuildCtx, d *tree.ColumnTableDef, tn *tree.TableName,
) *tree.ColumnTableDef {
	if err := catalog.AssertValidSerialColumnDef(d, tn); err != nil {
		panic(err)
	}

	defType, err := tree.ResolveType(b, d.Type, b.SemaCtx().GetTypeResolver())
	if err != nil {
		panic(err)
	}

	telemetry.Inc(sqltelemetry.SerialColumnNormalizationCounter(
		defType.Name(), b.SessionData().SerialNormalizationMode.String()))

	if defType.Width() < types.Int.Width() {
		b.EvalCtx().ClientNoticeSender.BufferClientNotice(
			b,
			errors.WithHintf(
				pgnotice.Newf(
					"upgrading the column %s to %s to utilize the session serial_normalization setting",
					d.Name.String(),
					types.Int.SQLString(),
				),
				"change the serial_normalization to sql_sequence or sql_sequence_cached if you wish "+
					"to use a smaller sized serial column at the cost of performance. See %s",
				docs.URL("serial.html"),
			),
		)
	}

	// Serial is an alias for a real column definition. False indicates a remapped alias.
	d.IsSerial = false

	return catalog.UseRowID(*d)
}

func columnNamesToIDs(b BuildCtx, tbl *scpb.Table) map[string]descpb.ColumnID {
	tableElts := b.QueryByID(tbl.TableID)
	namesToIDs := make(map[string]descpb.ColumnID)
	scpb.ForEachColumnName(tableElts, func(current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnName) {
		if target == scpb.ToPublic {
			namesToIDs[e.Name] = e.ColumnID
		}
	})
	return namesToIDs
}

type addColumnSpec struct {
	tbl              *scpb.Table
	col              *scpb.Column
	fam              *scpb.ColumnFamily
	name             *scpb.ColumnName
	colType          *scpb.ColumnType
	def              *scpb.ColumnDefaultExpression
	onUpdate         *scpb.ColumnOnUpdateExpression
	compute          *scpb.ColumnComputeExpression
	comment          *scpb.ColumnComment
	unique           bool
	notNull          bool
	transientCompute bool
}

// addColumn adds a column as specified in the `spec`. It delegates most of the work
// to addColumnIgnoringNotNull and handles NOT NULL constraint itself.
// It contains version gates to help ensure compatibility in mixed version state.
// Read comments in `ColumnType` message in `elements.proto` for details.
func addColumn(b BuildCtx, spec addColumnSpec, n tree.NodeFormatter) (backing *scpb.PrimaryIndex) {
	// addColumnIgnoringNotNull is a helper function which adds column element
	// targets and ensures that the new column is backed by a primary index, which
	// it returns.
	addColumnIgnoringNotNull := func(
		b BuildCtx, spec addColumnSpec, n tree.NodeFormatter,
	) (backing *scpb.PrimaryIndex) {
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
		if spec.compute != nil {
			if spec.transientCompute {
				b.AddTransient(spec.compute)
			} else {
				b.Add(spec.compute)
			}
		}
		if spec.comment != nil {
			b.Add(spec.comment)
		}
		// Don't need to modify primary indexes for virtual columns.
		if spec.colType.IsVirtual {
			return getLatestPrimaryIndex(b, spec.tbl.TableID)
		}

		inflatedChain := getInflatedPrimaryIndexChain(b, spec.tbl.TableID)
		if spec.def == nil && spec.colType.ComputeExpr == nil && spec.compute == nil {
			// Optimization opportunity: if we were to add a new column without default
			// value nor computed expression, then we can just add the column to existing
			// non-nil primary indexes without actually backfilling any data. This is
			// achieved by inflating the chain of primary indexes and add this column
			// to *all* four primary indexes. Later, in the de-duplication step, we'd
			// recognize this and drop redundant primary indexes appropriately.
			addStoredColumnToPrimaryIndexTargeting(b, spec.tbl.TableID, inflatedChain.oldSpec.primary, spec.col, scpb.ToPublic)
		}
		addStoredColumnToPrimaryIndexTargeting(b, spec.tbl.TableID, inflatedChain.inter1Spec.primary, spec.col, scpb.Transient)
		addStoredColumnToPrimaryIndexTargeting(b, spec.tbl.TableID, inflatedChain.inter2Spec.primary, spec.col, scpb.Transient)
		addStoredColumnToPrimaryIndexTargeting(b, spec.tbl.TableID, inflatedChain.finalSpec.primary, spec.col, scpb.ToPublic)
		return inflatedChain.finalSpec.primary
	}

	backing = addColumnIgnoringNotNull(b, spec, n)
	if backing == nil {
		panic(errors.AssertionFailedf("programming error: backing primary index is nil; it" +
			"should at least be the existing primary index (aka `old`)"))
	}
	if spec.notNull {
		cnne := scpb.ColumnNotNull{
			TableID:  spec.tbl.TableID,
			ColumnID: spec.col.ColumnID,
		}
		cnne.IndexIDForValidation = backing.IndexID
		b.Add(&cnne)
	}
	return backing
}

// addStoredColumnToPrimaryIndexTargeting adds a stored column `col` to primary
// index `idx` and its associated temporary index.
// The column in primary index is targeting `target` (either ToPublic or Transient),
// and the column in its temporary index is always targeting Transient.
func addStoredColumnToPrimaryIndexTargeting(
	b BuildCtx,
	tableID catid.DescID,
	idx *scpb.PrimaryIndex,
	col *scpb.Column,
	target scpb.TargetStatus,
) {
	addIndexColumnToInternal(b, tableID, idx.IndexID, col.ColumnID, scpb.IndexColumn_STORED, target)
	addIndexColumnToInternal(b, tableID, idx.TemporaryIndexID, col.ColumnID, scpb.IndexColumn_STORED, scpb.Transient)
}

func addIndexColumnToInternal(
	b BuildCtx,
	tableID catid.DescID,
	indexID catid.IndexID,
	columnID catid.ColumnID,
	kind scpb.IndexColumn_Kind,
	target scpb.TargetStatus,
) {
	if indexID == 0 {
		return
	}

	var exist bool
	for _, toStoredCol := range getIndexColumns(b.QueryByID(tableID), indexID, kind) {
		if toStoredCol.ColumnID == columnID {
			exist = true
			break
		}
	}
	if exist {
		panic(errors.AssertionFailedf("programming error: attempt to add column %v to primary"+
			"index %v's storing columns in table %v, but this column already exists there.",
			columnID, exist, tableID))
	}

	indexCol := scpb.IndexColumn{
		TableID:       tableID,
		IndexID:       indexID,
		ColumnID:      columnID,
		OrdinalInKind: getNextStoredIndexColumnOrdinal(b.QueryByID(tableID), indexID),
		Kind:          scpb.IndexColumn_STORED,
	}
	switch target {
	case scpb.ToPublic:
		b.Add(&indexCol)
	case scpb.Transient:
		b.AddTransient(&indexCol)
	default:
		panic(errors.AssertionFailedf("programming error: add index column element "+
			"should only target PUBLIC or TRANSIENT; get %v", target.Status()))
	}
}

func getNextStoredIndexColumnOrdinal(allTargets ElementResultSet, indexID catid.IndexID) uint32 {
	max := -1
	scpb.ForEachIndexColumn(allTargets.Filter(notFilter(ghostElementFilter)), func(
		status scpb.Status, target scpb.TargetStatus, e *scpb.IndexColumn,
	) {
		if e.IndexID == indexID && e.Kind == scpb.IndexColumn_STORED &&
			int(e.OrdinalInKind) > max {
			max = int(e.OrdinalInKind)
		}
	})
	return uint32(max + 1)
}

// getImplicitSecondaryIndexName determines the implicit name for a secondary
// index, this logic matches tabledesc.BuildIndexName.
func getImplicitSecondaryIndexName(
	b BuildCtx, descID descpb.ID, indexID descpb.IndexID, numImplicitColumns int,
) string {
	elts := b.QueryByID(descID).Filter(notFilter(absentTargetFilter))
	var idx *scpb.Index
	scpb.ForEachSecondaryIndex(elts, func(current scpb.Status, target scpb.TargetStatus, e *scpb.SecondaryIndex) {
		if e.IndexID == indexID {
			idx = &e.Index
		}
	})
	if idx == nil {
		panic(errors.AssertionFailedf("unable to find secondary index."))
	}
	keyColumns := getIndexColumns(elts, indexID, scpb.IndexColumn_KEY)
	// An index name has a segment for the table name, each key column, and a
	// final word (either "idx" or "key").
	segments := make([]string, 0, len(keyColumns)+2)
	// Add the table name segment.
	_, _, tblName := scpb.FindNamespace(elts)
	if tblName == nil {
		panic(errors.AssertionFailedf("unable to find table name."))
	}
	segments = append(segments, tblName.Name)
	// Add the key column segments. For inaccessible columns, use "expr" as the
	// segment. If there are multiple inaccessible columns, add an incrementing
	// integer suffix.
	exprCount := 0
	for i, n := numImplicitColumns, len(keyColumns); i < n; i++ {
		colID := keyColumns[i].ColumnID
		colElts := elts.Filter(hasColumnIDAttrFilter(colID))
		_, _, col := scpb.FindColumn(colElts)
		if col == nil {
			panic(errors.AssertionFailedf("unable to find column %d.", colID))
		}
		var segmentName string
		if col.IsInaccessible {
			if exprCount == 0 {
				segmentName = "expr"
			} else {
				segmentName = fmt.Sprintf("expr%d", exprCount)
			}
			exprCount++
		} else {
			_, _, colName := scpb.FindColumnName(colElts)
			if idx.Sharding != nil && colName.Name == idx.Sharding.Name {
				continue
			}
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

func getIndexColumns(
	elts ElementResultSet, id descpb.IndexID, kind scpb.IndexColumn_Kind,
) []*scpb.IndexColumn {
	var keyColumns []*scpb.IndexColumn
	scpb.ForEachIndexColumn(elts.Filter(notFilter(ghostElementFilter)), func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexColumn,
	) {
		if e.IndexID == id && e.Kind == kind {
			keyColumns = append(keyColumns, e)
		}
	})
	sort.Slice(keyColumns, func(i, j int) bool {
		return keyColumns[i].OrdinalInKind < keyColumns[j].OrdinalInKind
	})
	return keyColumns
}

func addSecondaryIndexTargetsForAddColumn(
	b BuildCtx, tbl *scpb.Table, desc *descpb.IndexDescriptor, newPrimaryIdx *scpb.PrimaryIndex,
) {
	var partitioning *catpb.PartitioningDescriptor
	index := scpb.Index{
		TableID:       tbl.TableID,
		IndexID:       desc.ID,
		IsUnique:      desc.Unique,
		IsInverted:    desc.Type == descpb.IndexDescriptor_INVERTED,
		SourceIndexID: newPrimaryIdx.IndexID,
		IsNotVisible:  desc.NotVisible,
		Invisibility:  desc.Invisibility,
	}
	tempIndexID := index.IndexID + 1 // this is enforced below
	index.TemporaryIndexID = tempIndexID
	if desc.Sharded.IsSharded {
		index.Sharding = &desc.Sharded
	}

	if desc.Unique {
		index.ConstraintID = b.NextTableConstraintID(tbl.TableID)
	}

	// If necessary add suffix columns, this would normally be done inside
	// allocateIndexIDs, but we are going to do it explicitly for the declarative
	// schema changer.
	{
		// Apply any implicit partitioning columns first, if they are missing.
		scpb.ForEachIndexPartitioning(
			b.QueryByID(tbl.TableID),
			func(current scpb.Status, target scpb.TargetStatus, e *scpb.IndexPartitioning) {
				if e.IndexID == newPrimaryIdx.IndexID {
					partitioning = &e.PartitioningDescriptor
				}
			},
		)
		keyColSet := catalog.TableColSet{}
		extraSuffixColumns := catalog.TableColSet{}
		for _, colID := range desc.KeyColumnIDs {
			keyColSet.Add(colID)
		}
		newPrimaryIdxKeyColumns := getIndexColumns(
			b.QueryByID(tbl.TableID), newPrimaryIdx.IndexID, scpb.IndexColumn_KEY,
		)
		if partitioning != nil && len(desc.Partitioning.Range) == 0 &&
			len(desc.Partitioning.List) == 0 &&
			partitioning.NumImplicitColumns > 0 {

			keyColumnIDs := make(
				[]descpb.ColumnID, 0,
				len(desc.KeyColumnIDs)+int(partitioning.NumImplicitColumns),
			)
			keyColumnDirs := make(
				[]catenumpb.IndexColumn_Direction, 0,
				len(desc.KeyColumnIDs)+int(partitioning.NumImplicitColumns),
			)
			for _, c := range newPrimaryIdxKeyColumns[0:partitioning.NumImplicitColumns] {
				if !keyColSet.Contains(c.ColumnID) {
					keyColumnIDs = append(keyColumnIDs, c.ColumnID)
					keyColumnDirs = append(keyColumnDirs, c.Direction)
					keyColSet.Add(c.ColumnID)
				}
			}
			desc.KeyColumnIDs = append(keyColumnIDs, desc.KeyColumnIDs...)
			desc.KeyColumnDirections = append(keyColumnDirs, desc.KeyColumnDirections...)
		} else if len(desc.Partitioning.Range) != 0 || len(desc.Partitioning.List) != 0 {
			partitioning = &desc.Partitioning
		}
		for _, c := range newPrimaryIdxKeyColumns {
			if !keyColSet.Contains(c.ColumnID) {
				extraSuffixColumns.Add(c.ColumnID)
			}
		}
		if !extraSuffixColumns.Empty() {
			desc.KeySuffixColumnIDs = append(
				desc.KeySuffixColumnIDs, extraSuffixColumns.Ordered()...,
			)
		}
	}
	sec := &scpb.SecondaryIndex{Index: index}
	for i, dir := range desc.KeyColumnDirections {
		b.Add(&scpb.IndexColumn{
			TableID:       tbl.TableID,
			IndexID:       index.IndexID,
			ColumnID:      desc.KeyColumnIDs[i],
			OrdinalInKind: uint32(i),
			Kind:          scpb.IndexColumn_KEY,
			Direction:     dir,
		})
	}
	for i, colID := range desc.KeySuffixColumnIDs {
		b.Add(&scpb.IndexColumn{
			TableID:       tbl.TableID,
			IndexID:       index.IndexID,
			ColumnID:      colID,
			OrdinalInKind: uint32(i),
			Kind:          scpb.IndexColumn_KEY_SUFFIX,
		})
	}
	for i, colID := range desc.StoreColumnIDs {
		b.Add(&scpb.IndexColumn{
			TableID:       tbl.TableID,
			IndexID:       index.IndexID,
			ColumnID:      colID,
			OrdinalInKind: uint32(i),
			Kind:          scpb.IndexColumn_STORED,
		})
	}
	b.Add(sec)
	b.Add(&scpb.IndexData{TableID: tbl.TableID, IndexID: index.IndexID})
	indexName := desc.Name
	numImplicitColumns := 0
	if partitioning != nil {
		numImplicitColumns = int(partitioning.NumImplicitColumns)
	}
	if indexName == "" {
		indexName = getImplicitSecondaryIndexName(b, tbl.TableID, index.IndexID, numImplicitColumns)
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
	temp.ConstraintID = index.ConstraintID + 1
	var tempIndexColumns []*scpb.IndexColumn
	scpb.ForEachIndexColumn(b.QueryByID(tbl.TableID), func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexColumn,
	) {
		if e.IndexID != index.IndexID {
			return
		}
		c := protoutil.Clone(e).(*scpb.IndexColumn)
		c.IndexID = tempIndexID
		tempIndexColumns = append(tempIndexColumns, c)
	})
	for _, c := range tempIndexColumns {
		b.AddTransient(c)
	}
	b.AddTransient(temp)
	b.AddTransient(&scpb.IndexData{TableID: temp.TableID, IndexID: temp.IndexID})
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
}

func retrieveTemporaryIndexElem(
	b BuildCtx, tableID catid.DescID, indexID catid.IndexID,
) (temporaryIndexElem *scpb.TemporaryIndex) {
	b.QueryByID(tableID).FilterTemporaryIndex().ForEach(func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.TemporaryIndex,
	) {
		if e.IndexID == indexID {
			temporaryIndexElem = e
		}
	})
	return temporaryIndexElem
}

func mustRetrieveTemporaryIndexElem(
	b BuildCtx, tableID catid.DescID, indexID catid.IndexID,
) (temporaryIndexElem *scpb.TemporaryIndex) {
	temporaryIndexElem = retrieveTemporaryIndexElem(b, tableID, indexID)
	if temporaryIndexElem == nil {
		panic(errors.AssertionFailedf("programming error: cannot find a TemporaryIndex element"+
			" of ID %v", indexID))
	}
	return temporaryIndexElem
}
