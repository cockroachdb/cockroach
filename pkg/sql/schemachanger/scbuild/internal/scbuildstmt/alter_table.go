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

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// supportedAlterTableCommand tracks metadata for ALTER TABLE commands that are
// implemented by the new schema changer.
type supportedAlterTableCommand struct {
	// fn is a function to perform a schema change.
	fn interface{}
	// on indicates that this statement is on by default.
	on bool
	// extraChecks contains a function to determine whether the command is
	// supported. These extraChecks are important to be able to avoid any
	// extra round-trips to resolve the descriptor and its elements if we know
	// that we cannot process the command.
	extraChecks interface{}
}

// supportedAlterTableStatements tracks alter table operations fully supported by
// declarative schema  changer. Operations marked as non-fully supported can
// only be with the use_declarative_schema_changer session variable.
var supportedAlterTableStatements = map[reflect.Type]supportedAlterTableCommand{
	reflect.TypeOf((*tree.AlterTableAddColumn)(nil)):       {fn: alterTableAddColumn, on: true},
	reflect.TypeOf((*tree.AlterTableDropColumn)(nil)):      {fn: alterTableDropColumn, on: true},
	reflect.TypeOf((*tree.AlterTableAlterPrimaryKey)(nil)): {fn: alterTableAlterPrimaryKey, on: true},
	reflect.TypeOf((*tree.AlterTableAddConstraint)(nil)): {fn: alterTableAddConstraint, on: true, extraChecks: func(
		t *tree.AlterTableAddConstraint,
	) bool {
		d, ok := t.ConstraintDef.(*tree.UniqueConstraintTableDef)
		return ok && d.PrimaryKey && t.ValidationBehavior == tree.ValidationDefault
	}},
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
				"does not have a valid signature got %v", statementType, callBackType))
		}
		if statementEntry.extraChecks != nil {
			extraChecks := reflect.TypeOf(statementEntry.extraChecks)
			if extraChecks.Kind() != reflect.Func {
				panic(errors.AssertionFailedf("%v extra checks for statement is "+
					"not a function", statementType))
			}
			if extraChecks.NumIn() != 1 ||
				extraChecks.In(0) != statementType ||
				extraChecks.NumOut() != 1 ||
				extraChecks.Out(0) != boolType {
				panic(errors.AssertionFailedf("%v extra checks for alter table statement "+
					"does not have a valid signature got %v", statementType, callBackType))
			}
		}
	}
}

// AlterTable implements ALTER TABLE.
func AlterTable(b BuildCtx, n *tree.AlterTable) {
	// Hoist the constraints to separate clauses because other code assumes that
	// that is how the commands will look.
	n.HoistAddColumnConstraints(func() {
		telemetry.Inc(sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "add_column.references"))
	})
	// Check if an entry exists for the statement type, in which
	// case. It's either fully or partially supported. Check the commands
	// first, since we don't want to do extra work in this transaction
	// only to bail out later.
	for _, cmd := range n.Cmds {
		info, ok := supportedAlterTableStatements[reflect.TypeOf(cmd)]
		if !ok {
			panic(scerrors.NotImplementedError(cmd))
		}
		// Check if partially supported operations are allowed next. If an
		// operation is not fully supported will not allow it to be run in
		// the declarative schema changer until its fully supported.
		if !isFullySupported(
			info.on, b.EvalCtx().SessionData().NewSchemaChangerMode,
		) {
			panic(scerrors.NotImplementedError(cmd))
		}

		// Run the extraChecks to see if we should avoid even resolving the
		// descriptor.
		if info.extraChecks != nil && !reflect.ValueOf(info.extraChecks).
			Call([]reflect.Value{reflect.ValueOf(cmd)})[0].Bool() {
			panic(scerrors.NotImplementedError(cmd))
		}
	}
	tn := n.Table.ToTableName()
	elts := b.ResolveTable(n.Table, ResolveParams{
		IsExistenceOptional: n.IfExists,
		RequiredPrivilege:   privilege.CREATE,
	})
	_, target, tbl := scpb.FindTable(elts)
	if tbl == nil {
		b.MarkNameAsNonExistent(&tn)
		return
	}
	if target != scpb.ToPublic {
		panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"table %q is being dropped, try again later", n.Table.Object()))
	}
	tn.ObjectNamePrefix = b.NamePrefix(tbl)
	b.SetUnresolvedNameAnnotation(n.Table, &tn)
	b.IncrementSchemaChangeAlterCounter("table")
	for _, cmd := range n.Cmds {
		info := supportedAlterTableStatements[reflect.TypeOf(cmd)]
		// Invoke the callback function, with the concrete types.
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
