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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// supportedAlterTableStatements tracks alter table operations fully supported by
// declarative schema  changer. Operations marked as non-fully supported can
// only be with the experimental_use_new_schema_changer session variable.
var supportedAlterTableStatements = map[reflect.Type]supportedStatement{
	reflect.TypeOf((*tree.AlterTableAddColumn)(nil)): {alterTableAddColumn, false},
}

func init() {
	// Check function signatures inside the supportedAlterTableStatements map.
	for statementType, statementEntry := range supportedAlterTableStatements {
		callBackType := reflect.TypeOf(statementEntry.fn)
		if callBackType.Kind() != reflect.Func {
			panic(errors.AssertionFailedf("%v entry for statement is "+
				"not a function", statementType))
		}
		if callBackType.NumIn() != 4 ||
			!callBackType.In(0).Implements(reflect.TypeOf((*BuildCtx)(nil)).Elem()) ||
			!callBackType.In(1).Implements(reflect.TypeOf((*catalog.TableDescriptor)(nil)).Elem()) ||
			callBackType.In(2) != statementType ||
			callBackType.In(3) != reflect.TypeOf((*tree.TableName)(nil)) {
			panic(errors.AssertionFailedf("%v entry for alter table statement "+
				"does not have a valid signature got %v", statementType, callBackType))
		}
	}
}

// AlterTable implements ALTER TABLE.
func AlterTable(b BuildCtx, n *tree.AlterTable) {
	// Hoist the constraints to separate clauses because other code assumes that
	// that is how the commands will look.
	n.HoistAddColumnConstraints()
	tn := n.Table.ToTableName()
	prefix, tbl := b.ResolveTable(n.Table, ResolveParams{
		IsExistenceOptional: n.IfExists,
		RequiredPrivilege:   privilege.CREATE,
	})
	tn.ObjectNamePrefix = prefix.NamePrefix()
	b.SetUnresolvedNameAnnotation(n.Table, &tn)
	if tbl == nil {
		b.MarkNameAsNonExistent(&tn)
		return
	}
	if catalog.HasConcurrentSchemaChanges(tbl) {
		panic(scerrors.ConcurrentSchemaChangeError(tbl))
	}
	for _, cmd := range n.Cmds {
		alterTableCmd(b, tbl, cmd, &tn)
		b.IncrementSubWorkID()
	}
}

func alterTableCmd(
	b BuildCtx, table catalog.TableDescriptor, cmd tree.AlterTableCmd, tn *tree.TableName,
) {
	// Check if an entry exists for the statement type, in which
	// case its either fully or partially supported.
	info, ok := supportedAlterTableStatements[reflect.TypeOf(cmd)]
	if !ok {
		panic(scerrors.NotImplementedError(cmd))
	}
	// Check if partially supported operations are allowed next. If an
	// operation is not fully supported will not allow it to be run in
	// the declarative schema changer until its fully supported.
	if !info.IsFullySupported(b.EvalCtx().SessionData().NewSchemaChangerMode) {
		panic(scerrors.NotImplementedError(cmd))
	}

	// Next invoke the callback function, with the concrete types.
	fn := reflect.ValueOf(info.fn)
	in := []reflect.Value{reflect.ValueOf(b), reflect.ValueOf(table),
		reflect.ValueOf(cmd), reflect.ValueOf(tn)}
	fn.Call(in)
}
