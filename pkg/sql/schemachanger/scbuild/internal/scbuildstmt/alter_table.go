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
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

type supportedAlterTableCommand = supportedStatement

// supportedAlterTableStatements tracks alter table operations fully supported by
// declarative schema  changer. Operations marked as non-fully supported can
// only be with the use_declarative_schema_changer session variable.
var supportedAlterTableStatements = map[reflect.Type]supportedAlterTableCommand{
	reflect.TypeOf((*tree.AlterTableAddColumn)(nil)):          {fn: alterTableAddColumn, on: true, checks: isV222Active},
	reflect.TypeOf((*tree.AlterTableDropColumn)(nil)):         {fn: alterTableDropColumn, on: true, checks: isV222Active},
	reflect.TypeOf((*tree.AlterTableAlterPrimaryKey)(nil)):    {fn: alterTableAlterPrimaryKey, on: true, checks: alterTableAlterPrimaryKeyChecks},
	reflect.TypeOf((*tree.AlterTableSetNotNull)(nil)):         {fn: alterTableSetNotNull, on: true, checks: isV231Active},
	reflect.TypeOf((*tree.AlterTableAddConstraint)(nil)):      {fn: alterTableAddConstraint, on: true, checks: alterTableAddConstraintChecks},
	reflect.TypeOf((*tree.AlterTableDropConstraint)(nil)):     {fn: alterTableDropConstraint, on: true, checks: isV231Active},
	reflect.TypeOf((*tree.AlterTableValidateConstraint)(nil)): {fn: alterTableValidateConstraint, on: true, checks: isV231Active},
}

func init() {
	boolType := reflect.TypeOf((*bool)(nil)).Elem()
	// Check function signatures inside the supportedAlterTableStatements map.
	for statementType, statementEntry := range supportedAlterTableStatements {
		callBackType := reflect.TypeOf(statementEntry.fn)
		if callBackType.Kind() != reflect.Func {
			panic(errors.AssertionFailedf("%v entry for statement is "+
				"not a function", statementType))
		}
		if callBackType.NumIn() != 4 ||
			!callBackType.In(0).Implements(reflect.TypeOf((*BuildCtx)(nil)).Elem()) ||
			callBackType.In(1) != reflect.TypeOf((*tree.TableName)(nil)) ||
			callBackType.In(2) != reflect.TypeOf((*scpb.Table)(nil)) ||
			callBackType.In(3) != statementType {
			panic(errors.AssertionFailedf("%v entry for alter table statement "+
				"does not have a valid signature; got %v", statementType, callBackType))
		}
		if statementEntry.checks != nil {
			checks := reflect.TypeOf(statementEntry.checks)
			if checks.Kind() != reflect.Func {
				panic(errors.AssertionFailedf("%v checks for statement is "+
					"not a function", statementType))
			}
			if checks.NumIn() != 3 ||
				(checks.In(0) != statementType && !statementType.Implements(checks.In(0))) ||
				checks.In(1) != reflect.TypeOf(sessiondatapb.UseNewSchemaChangerOff) ||
				checks.In(2) != reflect.TypeOf((*clusterversion.ClusterVersion)(nil)).Elem() ||
				checks.NumOut() != 1 ||
				checks.Out(0) != boolType {
				panic(errors.AssertionFailedf("%v checks does not have a valid signature; got %v",
					statementType, checks))
			}
		}
	}
}

// alterTableChecks determines if the entire set of alter table commands
// are supported.
// One side-effect is that this function will modify `n` when it hoists
// add column constraints.
func alterTableChecks(
	n *tree.AlterTable,
	mode sessiondatapb.NewSchemaChangerMode,
	activeVersion clusterversion.ClusterVersion,
) bool {
	// For ALTER TABLE stmt, we will need to further check whether each
	// individual command is fully supported.
	n.HoistAddColumnConstraints(func() {
		telemetry.Inc(sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "add_column.references"))
	})
	for _, cmd := range n.Cmds {
		if !isFullySupportedWithFalsePositiveInternal(supportedAlterTableStatements,
			reflect.TypeOf(cmd), reflect.ValueOf(cmd), mode, activeVersion) {
			return false
		}
	}
	return true
}

func alterTableAlterPrimaryKeyChecks(
	t *tree.AlterTableAlterPrimaryKey,
	mode sessiondatapb.NewSchemaChangerMode,
	activeVersion clusterversion.ClusterVersion,
) bool {
	// Start supporting ALTER PRIMARY KEY (in general with fallback cases) from V22_2.
	if !isV222Active(t, mode, activeVersion) {
		return false
	}
	// Start supporting ALTER PRIMARY KEY USING HASH from V23_1.
	if t.Sharded != nil && !activeVersion.IsActive(clusterversion.V23_1) {
		return false
	}
	return true
}

func alterTableAddConstraintChecks(
	t *tree.AlterTableAddConstraint,
	mode sessiondatapb.NewSchemaChangerMode,
	activeVersion clusterversion.ClusterVersion,
) bool {
	// Start supporting ADD PRIMARY KEY from V22_2.
	if d, ok := t.ConstraintDef.(*tree.UniqueConstraintTableDef); ok && d.PrimaryKey && t.ValidationBehavior == tree.ValidationDefault {
		return isV222Active(t, mode, activeVersion)
	}

	// Start supporting all other ADD CONSTRAINTs from V23_1, including
	// - ADD PRIMARY KEY NOT VALID
	// - ADD UNIQUE [NOT VALID]
	// - ADD CHECK [NOT VALID]
	// - ADD FOREIGN KEY [NOT VALID]
	// - ADD UNIQUE WITHOUT INDEX [NOT VALID]
	if !isV231Active(t, mode, activeVersion) {
		return false
	}
	return true
}

// AlterTable implements ALTER TABLE.
func AlterTable(b BuildCtx, n *tree.AlterTable) {
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
	for _, cmd := range n.Cmds {
		// Invoke the callback function for each command.
		b.IncrementSchemaChangeAlterCounter("table", cmd.TelemetryName())
		info := supportedAlterTableStatements[reflect.TypeOf(cmd)]
		fn := reflect.ValueOf(info.fn)
		fn.Call([]reflect.Value{
			reflect.ValueOf(b),
			reflect.ValueOf(&tn),
			reflect.ValueOf(tbl),
			reflect.ValueOf(cmd),
		})
		b.IncrementSubWorkID()
	}
}
