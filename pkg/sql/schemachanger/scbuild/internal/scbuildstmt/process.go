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

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/errors"
)

// supportedStatement tracks metadata for statements that are
// implemented by the new schema changer.
type supportedStatement struct {
	fn               interface{}
	fullySupported   bool
	fullySupportedFn interface{}
}

// IsFullySupported returns if this statement type is supported, where the
// mode of the new schema changer can force unsupported statements to be
// supported.
func (s supportedStatement) IsFullySupported(
	n tree.Statement, mode sessiondatapb.NewSchemaChangerMode,
) bool {
	// If the unsafe modes of the new schema changer are used then any implemented
	// operation will be exposed.
	if mode == sessiondatapb.UseNewSchemaChangerUnsafeAlways ||
		mode == sessiondatapb.UseNewSchemaChangerUnsafe {
		return true
	}
	// If a command is labeled as fully supported, it can still have additional,
	// checks via callback function.
	if s.fullySupported && s.fullySupportedFn != nil {
		fn := reflect.ValueOf(s.fullySupportedFn)
		in := []reflect.Value{reflect.ValueOf(n)}
		values := fn.Call(in)
		return values[0].Bool()
	}
	return s.fullySupported
}

// Tracks operations which are fully supported when the declarative schema
// changer is enabled. Operations marked as non-fully supported can only be
// with the use_declarative_schema_changer session variable.
var supportedStatements = map[reflect.Type]supportedStatement{
	// Alter table will have commands individually whitelisted via the
	// supportedAlterTableStatements list, so wwe will consider it fully supported
	// here.
	reflect.TypeOf((*tree.AlterTable)(nil)):   {AlterTable, true, AlterTableIsSupported},
	reflect.TypeOf((*tree.CreateIndex)(nil)):  {CreateIndex, false, nil},
	reflect.TypeOf((*tree.DropDatabase)(nil)): {DropDatabase, true, nil},
	reflect.TypeOf((*tree.DropSchema)(nil)):   {DropSchema, true, nil},
	reflect.TypeOf((*tree.DropSequence)(nil)): {DropSequence, true, nil},
	reflect.TypeOf((*tree.DropTable)(nil)):    {DropTable, true, nil},
	reflect.TypeOf((*tree.DropType)(nil)):     {DropType, true, nil},
	reflect.TypeOf((*tree.DropView)(nil)):     {DropView, true, nil},
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
		if statementEntry.fullySupportedFn != nil {
			fullySupportedCallbackFn := reflect.TypeOf(statementEntry.fullySupportedFn)
			if fullySupportedCallbackFn.Kind() != reflect.Func {
				panic(errors.AssertionFailedf("%v entry for statement is "+
					"not a function", statementType))
			}
			if fullySupportedCallbackFn.NumIn() != 1 ||
				fullySupportedCallbackFn.In(0) != statementType {
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
	return info.IsFullySupported(n, mode)
}

// Process dispatches on the statement type to populate the BuilderState
// embedded in the BuildCtx. Any error will be panicked.
func Process(b BuildCtx, n tree.Statement) {
	// If a statement is not supported throw a not implemented error.
	if !CheckIfStmtIsSupported(n, b.EvalCtx().SessionData().NewSchemaChangerMode) {
		panic(scerrors.NotImplementedError(n))
	}
	// Check if an entry exists for the statement type, in which
	// case it is either fully or partially supported.
	info, ok := supportedStatements[reflect.TypeOf(n)]
	if !ok {
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
