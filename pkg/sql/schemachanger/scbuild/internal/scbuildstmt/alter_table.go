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
	"math"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

type commandFunc func(BuildCtx, *tree.TableName, *scpb.Table)

func wrapCF[T tree.AlterTableCmd](
	f func(b BuildCtx, tn *tree.TableName, tbl *scpb.Table, t T), t T,
) commandFunc {
	return func(b BuildCtx, tn *tree.TableName, tbl *scpb.Table) {
		b.IncrementSchemaChangeAlterCounter("table", t.TelemetryName())
		f(b, tn, tbl, t)
		b.IncrementSubWorkID()
	}
}

// AlterTable implements ALTER TABLE.
func AlterTable(n *tree.AlterTable, activeVersion clusterversion.ClusterVersion) ProcessFunc {
	// For ALTER TABLE stmt, we will need to further check whether each
	// individual command is fully supported.
	n.HoistAddColumnConstraints(func() {
		telemetry.Inc(sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "add_column.references"))
	})
	var cfs []commandFunc
	for _, cmd := range n.Cmds {
		var cf commandFunc
		switch typedCmd := cmd.(type) {
		case *tree.AlterTableAddColumn:
			cf = wrapCF(alterTableAddColumn, typedCmd)
		case *tree.AlterTableDropColumn:
			cf = wrapCF(alterTableDropColumn, typedCmd)
		case *tree.AlterTableAlterPrimaryKey:
			if typedCmd.Sharded == nil || activeVersion.IsActive(clusterversion.V23_1) {
				cf = wrapCF(alterTableAlterPrimaryKey, typedCmd)
			}
		case *tree.AlterTableSetNotNull:
			if activeVersion.IsActive(clusterversion.V23_1) {
				cf = wrapCF(alterTableSetNotNull, typedCmd)
			}
		case *tree.AlterTableAddConstraint:
			if d, ok := typedCmd.ConstraintDef.(*tree.UniqueConstraintTableDef); ok && d.PrimaryKey && typedCmd.ValidationBehavior == tree.ValidationDefault {
				// Start supporting all other ADD CONSTRAINTs from V23_1, including
				// - ADD PRIMARY KEY NOT VALID
				// - ADD UNIQUE [NOT VALID]
				// - ADD CHECK [NOT VALID]
				// - ADD FOREIGN KEY [NOT VALID]
				// - ADD UNIQUE WITHOUT INDEX [NOT VALID]
				if activeVersion.IsActive(clusterversion.V23_1) {
					cf = wrapCF(alterTableAddConstraint, typedCmd)
				}
			}
		case *tree.AlterTableDropConstraint:
			if activeVersion.IsActive(clusterversion.V23_1) {
				cf = wrapCF(alterTableDropConstraint, typedCmd)
			}
		case *tree.AlterTableValidateConstraint:
			if activeVersion.IsActive(clusterversion.V23_1) {
				cf = wrapCF(alterTableValidateConstraint, typedCmd)
			}
		}
		if cf == nil {
			return nil
		}
		cfs = append(cfs, cf)
	}
	return func(b BuildCtx) {
		tn := n.Table.ToTableName()
		elts := b.ResolveTable(n.Table, ResolveParams{
			IsExistenceOptional: n.IfExists,
			RequiredPrivilege:   privilege.CREATE,
		})
		_, target, tbl := scpb.FindTable(elts)
		if tbl == nil {
			// Mark all table names (`tn` and others) in this ALTER TABLE stmt as non-existent.
			tree.NewFmtCtx(tree.FmtSimple, tree.FmtReformatTableNames(func(
				ctx *tree.FmtCtx, name *tree.TableName,
			) {
				b.MarkNameAsNonExistent(name)
			})).FormatNode(n)
			return
		}
		if target != scpb.ToPublic {
			panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"table %q is being dropped, try again later", n.Table.Object()))
		}
		panicIfSchemaIsLocked(elts)
		tn.ObjectNamePrefix = b.NamePrefix(tbl)
		b.SetUnresolvedNameAnnotation(n.Table, &tn)
		b.IncrementSchemaChangeAlterCounter("table")
		for _, cf := range cfs {
			cf(b, &tn, tbl)
		}
		maybeDropRedundantPrimaryIndexes(b, tbl.TableID)
		maybeRewriteTempIDsInPrimaryIndexes(b, tbl.TableID)
		disallowDroppingPrimaryIndexReferencedInUDFOrView(b, tbl.TableID, n.String())
		// TODO (xiang): Remove the following line for the next major release after v23.2,
		// be it v24.1 or v23.3.
		disableAlterTableMultipleCommandsOnV232(b, n, tbl.TableID)
	}
}

// disableAlterTableMultipleCommandsOnV232 disables declarative schema changer
// if processing this ALTER TABLE stmt requires building more than one new
// primary indexes by default on v23.2, unless the mode is unsafe or ALTER TABLE
// is forcefully turned on.
func disableAlterTableMultipleCommandsOnV232(b BuildCtx, n *tree.AlterTable, tableID catid.DescID) {
	chain := getPrimaryIndexChain(b, tableID)
	chainTyp := chain.chainType()

	// isAlterPKWithRowid returns true if the stmt is ALTER PK with rowid.
	isAlterPKWithRowid := func() bool {
		if chainTyp == twoNewPrimaryIndexesWithAlteredPKType {
			_, _, inter2StoredCols := getSortedColumnIDsInIndexByKind(b, tableID, chain.inter2Spec.primary.IndexID)
			_, _, finalStoredCols := getSortedColumnIDsInIndexByKind(b, tableID, chain.finalSpec.primary.IndexID)
			inter2StoredColsAsSet := catalog.MakeTableColSet(inter2StoredCols...)
			finalStoredColsAsSet := catalog.MakeTableColSet(finalStoredCols...)
			diffSet := inter2StoredColsAsSet.Difference(finalStoredColsAsSet)
			if diffSet.Len() == 1 {
				colName := mustRetrieveColumnNameElem(b, tableID, diffSet.Ordered()[0]).Name
				if strings.HasPrefix(colName, "rowid") {
					return true
				}
			}
		}
		return false
	}

	if chainTyp == twoNewPrimaryIndexesWithAlteredPKType ||
		chainTyp == twoNewPrimaryIndexesWithAddAndDropColumnsType ||
		chainTyp == threeNewPrimaryIndexesType {
		if isAlterPKWithRowid() {
			// This is the only exception that involves building more than one new
			// primary indexes but we would enable by default, because it was already
			// supported in v23.1.
			return
		}
		newSchemaChangerMode := GetDeclarativeSchemaChangerModeForStmt(b, n)
		if newSchemaChangerMode != sessiondatapb.UseNewSchemaChangerUnsafe &&
			newSchemaChangerMode != sessiondatapb.UseNewSchemaChangerUnsafeAlways {
			panic(scerrors.NotImplementedErrorf(n, "statement has been disabled on v23.2"))
		}
	}
}

// disallowDroppingPrimaryIndexReferencedInUDFOrView prevents dropping old (current)
// primary index that is referenced explicitly via index hinting in UDF or View body.
func disallowDroppingPrimaryIndexReferencedInUDFOrView(
	b BuildCtx, tableID catid.DescID, stmtSQLString string,
) {
	chain := getPrimaryIndexChain(b, tableID)
	if !chain.isInflatedAtAll() {
		// No new primary index needs to be added at all, which means old/current
		// primary index does not need to be dropped.
		return
	}

	toBeDroppedIndexID := chain.oldSpec.primary.IndexID
	toBeDroppedIndexName := chain.oldSpec.name.Name
	b.BackReferences(tableID).Filter(publicTargetFilter).ForEachTarget(func(target scpb.TargetStatus, e scpb.Element) {
		switch el := e.(type) {
		case *scpb.FunctionBody:
			for _, ref := range el.UsesTables {
				if ref.TableID == tableID && ref.IndexID == toBeDroppedIndexID {
					fnName := b.QueryByID(el.FunctionID).FilterFunctionName().MustGetOneElement().Name
					panic(errors.WithDetail(
						sqlerrors.NewDependentBlocksOpError("drop", "index", toBeDroppedIndexName, "function", fnName),
						sqlerrors.PrimaryIndexSwapDetail))
				}
			}
		case *scpb.View:
			for _, ref := range el.ForwardReferences {
				if ref.ToID == tableID && ref.IndexID == toBeDroppedIndexID {
					viewName := b.QueryByID(el.ViewID).FilterNamespace().MustGetOneElement().Name
					panic(errors.WithDetail(
						sqlerrors.NewDependentBlocksOpError("drop", "index", toBeDroppedIndexName, "view", viewName),
						sqlerrors.PrimaryIndexSwapDetail))
				}
			}
		}
	})
}

// maybeRewriteTempIDsInPrimaryIndexes is part of the post-processing
// invoked at the end of building each ALTER TABLE statement to replace temporary
// IDs with real, actual IDs.
func maybeRewriteTempIDsInPrimaryIndexes(b BuildCtx, tableID catid.DescID) {
	chain := getPrimaryIndexChain(b, tableID)
	for i, spec := range chain.allPrimaryIndexSpecs(nonNilPrimaryIndexSpecSelector) {
		if i == 0 {
			continue
		}
		maybeRewriteIndexAndConstraintID(b, tableID, spec.primary.IndexID, spec.primary.ConstraintID)
		tempIndexSpec := chain.mustGetIndexSpecByID(spec.primary.TemporaryIndexID)
		maybeRewriteIndexAndConstraintID(b, tableID, tempIndexSpec.temporary.IndexID, tempIndexSpec.temporary.ConstraintID)
	}
	chain.validate()
}

// maybeRewriteIndexAndConstraintID attempts to replace index which currently has
// a temporary index ID `indexID` with an actual index ID. It also updates
// all elements that references this index with the actual index ID.
func maybeRewriteIndexAndConstraintID(
	b BuildCtx, tableID catid.DescID, indexID catid.IndexID, constraintID catid.ConstraintID,
) {
	if indexID < catid.IndexID(TableTentativeIdsStart) || constraintID < catid.ConstraintID(TableTentativeIdsStart) {
		// Nothing to do if it's already an actual index ID.
		return
	}

	actualIndexID := b.NextTableIndexID(tableID)
	actualConstraintID := b.NextTableConstraintID(tableID)
	b.QueryByID(tableID).ForEach(func(
		_ scpb.Status, _ scpb.TargetStatus, e scpb.Element,
	) {
		_ = screl.WalkIndexIDs(e, func(id *catid.IndexID) error {
			if id != nil && *id == indexID {
				*id = actualIndexID
			}
			return nil
		})
		_ = screl.WalkConstraintIDs(e, func(id *catid.ConstraintID) error {
			if id != nil && *id == constraintID {
				*id = actualConstraintID
			}
			return nil
		})
	})
}

// maybeDropRedundantPrimaryIndexes is part of the post-processing invoked at
// the end of building each ALTER TABLE statement to remove possibly redundant
// primary indexes.
func maybeDropRedundantPrimaryIndexes(b BuildCtx, tableID catid.DescID) {
	chain := getPrimaryIndexChain(b, tableID)
	chain.deflate(b)
}

// TableTentativeIdsStart is the beginning of a sequence of increasing
// IDs for builder internal use for each table.
// Table IDs are uint32 and for those internal use, temporary IDs we
// start from MaxInt32, halfway of MaxUint32.
const TableTentativeIdsStart = math.MaxInt32
