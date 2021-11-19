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

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var declarativeForSupportedAllOps = settings.RegisterBoolSetting(
	"sql.schema_changer.declarative_for_all",
	"when set to true the declarative schema changer is enabled for "+
		"buth supported/unsupported operations which are full tested.",
	false,
)

type supportedStatement struct {
	fn             interface{}
	fullySupported bool
}

// Tracks operations which are fully supported when the declarative schema
// changer is enabled. Operations marked as non-fully supported can only be
// with the sql.schema_changer.declarative_for_all toggle enabled.
var supportStatements = map[reflect.Type]supportedStatement{
	reflect.TypeOf((*tree.AlterTable)(nil)):   {AlterTable, false},
	reflect.TypeOf((*tree.CreateIndex)(nil)):  {CreateIndex, false},
	reflect.TypeOf((*tree.DropDatabase)(nil)): {DropDatabase, true},
	reflect.TypeOf((*tree.DropSchema)(nil)):   {DropSchema, true},
	reflect.TypeOf((*tree.DropSequence)(nil)): {DropSequence, true},
	reflect.TypeOf((*tree.DropTable)(nil)):    {DropTable, true},
	reflect.TypeOf((*tree.DropType)(nil)):     {DropType, true},
	reflect.TypeOf((*tree.DropView)(nil)):     {DropView, true},
}

// Process dispatches on the statement type to populate the BuilderState
// embedded in the BuildCtx. Any error will be panicked.
func Process(b BuildCtx, n tree.Statement) {
	// Check if an entry exists for the statement type, in which
	// case its either fully or partially supported.
	info, ok := supportStatements[reflect.TypeOf(n)]
	if !ok {
		panic(scerrors.NotImplementedError(n))
	}
	// Check if partially supported operations are allowed next. If an
	// operation is not fully supported will not allow it to be run in
	// the declarative schema changer until its fully supported.
	if !declarativeForSupportedAllOps.Get(&b.EvalCtx().Settings.SV) &&
		!info.fullySupported {
		panic(scerrors.NotImplementedError(n))
	}

	// Next invoke the callback function, with the concrete types.
	fn := reflect.ValueOf(info.fn)
	in := []reflect.Value{reflect.ValueOf(b), reflect.ValueOf(n)}
	fn.Call(in)
}
