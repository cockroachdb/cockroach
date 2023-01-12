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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// supportedStatement tracks metadata for statements that are
// implemented by the new schema changer.
type supportedStatement struct {
	// fn is a function to perform a schema change.
	fn interface{}
	// on indicates that this statement is on by default.
	on bool
	// extraChecks contains a function to determine whether the command is
	// supported. These extraChecks are important to be able to avoid any
	// extra round-trips to resolve the descriptor and its elements if we know
	// that we cannot process the command.
	extraChecks interface{}
	// minSupportedClusterVersion is the minimal binary version that supports this
	// statement in the declarative schema changer.
	minSupportedClusterVersion clusterversion.Key
}

// isFullySupported returns if this statement type is supported, where the
// mode of the new schema changer can force unsupported statements to be
// supported.
func isFullySupported(
	n tree.Statement, onByDefault bool, extraFn interface{}, mode sessiondatapb.NewSchemaChangerMode,
) (ret bool) {
	// If the unsafe modes of the new schema changer are used then any implemented
	// operation will be exposed.
	if mode == sessiondatapb.UseNewSchemaChangerUnsafeAlways ||
		mode == sessiondatapb.UseNewSchemaChangerUnsafe ||
		onByDefault {
		ret = true
		// If a command is labeled as fully supported, it can still have additional,
		// checks via callback function.
		if extraFn != nil {
			fn := reflect.ValueOf(extraFn)
			in := []reflect.Value{reflect.ValueOf(n), reflect.ValueOf(mode)}
			values := fn.Call(in)
			ret = values[0].Bool()
		}
		// If `n` is `ALTER TABLE`, then further check each individual command, as
		// they might have their own extra checks.
		if alterTableStmt, isAlterTable := n.(*tree.AlterTable); ret && isAlterTable {
			// Hoist the constraints to separate clauses because other code assumes that
			// that is how the commands will look.
			alterTableStmt.HoistAddColumnConstraints(func() {
				telemetry.Inc(sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "add_column.references"))
			})
			for _, cmd := range alterTableStmt.Cmds {
				info := supportedAlterTableStatements[reflect.TypeOf(cmd)]
				if info.on && info.extraChecks != nil && !reflect.ValueOf(info.extraChecks).
					Call([]reflect.Value{reflect.ValueOf(cmd)})[0].Bool() {
					panic(scerrors.NotImplementedError(cmd))
				}
			}
		}
		return ret
	}
	return ret
}

// Tracks operations which are fully supported when the declarative schema
// changer is enabled. Operations marked as non-fully supported can only be
// with the use_declarative_schema_changer session variable.
var supportedStatements = map[reflect.Type]supportedStatement{
	// Alter table will have commands individually whitelisted via the
	// supportedAlterTableStatements list, so wwe will consider it fully supported
	// here.
	reflect.TypeOf((*tree.AlterTable)(nil)):          {fn: AlterTable, on: true, extraChecks: alterTableIsSupported},
	reflect.TypeOf((*tree.CreateIndex)(nil)):         {fn: CreateIndex, on: true, minSupportedClusterVersion: clusterversion.V23_1Start},
	reflect.TypeOf((*tree.DropDatabase)(nil)):        {fn: DropDatabase, on: true, minSupportedClusterVersion: clusterversion.V22_1},
	reflect.TypeOf((*tree.DropOwnedBy)(nil)):         {fn: DropOwnedBy, on: true, minSupportedClusterVersion: clusterversion.V22_2Start},
	reflect.TypeOf((*tree.DropSchema)(nil)):          {fn: DropSchema, on: true, minSupportedClusterVersion: clusterversion.V22_1},
	reflect.TypeOf((*tree.DropSequence)(nil)):        {fn: DropSequence, on: true, minSupportedClusterVersion: clusterversion.V22_1},
	reflect.TypeOf((*tree.DropTable)(nil)):           {fn: DropTable, on: true, minSupportedClusterVersion: clusterversion.V22_1},
	reflect.TypeOf((*tree.DropType)(nil)):            {fn: DropType, on: true, minSupportedClusterVersion: clusterversion.V22_1},
	reflect.TypeOf((*tree.DropView)(nil)):            {fn: DropView, on: true, minSupportedClusterVersion: clusterversion.V22_1},
	reflect.TypeOf((*tree.CommentOnDatabase)(nil)):   {fn: CommentOnDatabase, on: true, minSupportedClusterVersion: clusterversion.V22_2Start},
	reflect.TypeOf((*tree.CommentOnSchema)(nil)):     {fn: CommentOnSchema, on: true, minSupportedClusterVersion: clusterversion.V22_2Start},
	reflect.TypeOf((*tree.CommentOnTable)(nil)):      {fn: CommentOnTable, on: true, minSupportedClusterVersion: clusterversion.V22_2Start},
	reflect.TypeOf((*tree.CommentOnColumn)(nil)):     {fn: CommentOnColumn, on: true, minSupportedClusterVersion: clusterversion.V22_2Start},
	reflect.TypeOf((*tree.CommentOnIndex)(nil)):      {fn: CommentOnIndex, on: true, minSupportedClusterVersion: clusterversion.V22_2Start},
	reflect.TypeOf((*tree.CommentOnConstraint)(nil)): {fn: CommentOnConstraint, on: true, minSupportedClusterVersion: clusterversion.V22_2Start},
	reflect.TypeOf((*tree.DropIndex)(nil)):           {fn: DropIndex, on: true, minSupportedClusterVersion: clusterversion.V23_1Start},
	reflect.TypeOf((*tree.DropFunction)(nil)):        {fn: DropFunction, on: true, minSupportedClusterVersion: clusterversion.V23_1Start},
}

func init() {
	// Check function signatures inside the supportedStatements map.
	for statementType, statementEntry := range supportedStatements {
		// Validate main callback functions.
		callBackFnType := reflect.TypeOf(statementEntry.fn)
		if callBackFnType.Kind() != reflect.Func {
			panic(errors.AssertionFailedf("%v entry for statement is "+
				"not a function", statementType))
		}
		if callBackFnType.NumIn() != 2 ||
			!callBackFnType.In(0).Implements(reflect.TypeOf((*BuildCtx)(nil)).Elem()) ||
			callBackFnType.In(1) != statementType {
			panic(errors.AssertionFailedf("%v entry for statement is "+
				"does not have a valid signature got %v", statementType, callBackFnType))
		}
		// Validate fully supported function callbacks.
		if statementEntry.extraChecks != nil {
			fullySupportedCallbackFn := reflect.TypeOf(statementEntry.extraChecks)
			if fullySupportedCallbackFn.Kind() != reflect.Func {
				panic(errors.AssertionFailedf("%v entry for statement is "+
					"not a function", statementType))
			}
			if fullySupportedCallbackFn.NumIn() != 2 ||
				fullySupportedCallbackFn.In(0) != statementType ||
				fullySupportedCallbackFn.In(1) != reflect.TypeOf(sessiondatapb.UseNewSchemaChangerOff) {
				panic(errors.AssertionFailedf("%v entry for statement is "+
					"does not have a valid signature got %v", statementType, callBackFnType))

			}
		}
	}
}

// CheckIfStmtIsSupported determines if the statement is supported by the declarative
// schema changer.
func CheckIfStmtIsSupported(n tree.Statement, mode sessiondatapb.NewSchemaChangerMode) bool {
	// Check if an entry exists for the statement type, in which
	// case it is either fully or partially supported.
	info, ok := supportedStatements[reflect.TypeOf(n)]
	if !ok {
		return false
	}
	// Check if partially supported operations are allowed next. If an
	// operation is not fully supported will not allow it to be run in
	// the declarative schema changer until its fully supported.
	return isFullySupported(n, info.on, info.extraChecks, mode)
}

// Process dispatches on the statement type to populate the BuilderState
// embedded in the BuildCtx. Any error will be panicked.
func Process(b BuildCtx, n tree.Statement) {
	// Check if an entry exists for the statement type, in which
	// case it is either fully or partially supported.
	info, ok := supportedStatements[reflect.TypeOf(n)]
	if !ok {
		panic(scerrors.NotImplementedError(n))
	}
	// Check if partially supported operations are allowed next. If an
	// operation is not fully supported will not allow it to be run in
	// the declarative schema changer until its fully supported.
	if !isFullySupported(
		n, info.on, info.extraChecks, b.EvalCtx().SessionData().NewSchemaChangerMode,
	) {
		panic(scerrors.NotImplementedError(n))
	}

	// Check if the statement is supported in the current cluster version.
	if !stmtSupportedInCurrentClusterVersion(b, n) {
		panic(scerrors.NotImplementedError(n))
	}

	// Next invoke the callback function, with the concrete types.
	fn := reflect.ValueOf(info.fn)
	in := []reflect.Value{reflect.ValueOf(b), reflect.ValueOf(n)}
	// Check if the feature flag for it is enabled.
	err := b.CheckFeature(b, tree.GetSchemaFeatureNameFromStmt(n))
	if err != nil {
		panic(err)
	}
	fn.Call(in)
}

// stmtSupportedInCurrentClusterVersion checks whether the statement is supported
// in current cluster version.
func stmtSupportedInCurrentClusterVersion(b BuildCtx, n tree.Statement) bool {
	if alterTableStmt, isAlterTable := n.(*tree.AlterTable); isAlterTable {
		return alterTableAllCmdsSupportedInCurrentClusterVersion(b, alterTableStmt)
	}
	minSupportedClusterVersion := supportedStatements[reflect.TypeOf(n)].minSupportedClusterVersion
	return b.EvalCtx().Settings.Version.IsActive(b, minSupportedClusterVersion)

}
