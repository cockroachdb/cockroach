// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Jordan Lewis (jordan@cockroachlabs.com)

package parser

import (
	"errors"
	"fmt"

	"github.com/lib/pq/oid"
)

// This file contains builtin functions that we implement primarily for
// compatibility with Postgres.

const notUsableInfo = "Not usable; exposed only for compatibility with PostgreSQL."

// typeBuiltinsHaveUnderscore is a map to keep track of which types have i/o
// builtins with underscores in between their type name and the i/o builtin
// name, like date_in vs int8int. There seems to be no other way to
// programmatically determine whether or not this underscore is present, hence
// the existence of this map.
var typeBuiltinsHaveUnderscore = map[oid.Oid]struct{}{
	TypeAny.Oid():         {},
	TypeDate.Oid():        {},
	TypeDecimal.Oid():     {},
	TypeInterval.Oid():    {},
	TypeTimestamp.Oid():   {},
	TypeTimestampTZ.Oid(): {},
	TypeTuple.Oid():       {},
}

// PGIOBuiltinPrefix returns the string prefix to a type's IO functions. This
// is either the type's postgres display name or the type's postgres display
// name plus an underscore, depending on the type.
func PGIOBuiltinPrefix(typ Type) string {
	builtinPrefix := PGDisplayName(typ)
	if _, ok := typeBuiltinsHaveUnderscore[typ.Oid()]; ok {
		return builtinPrefix + "_"
	}
	return builtinPrefix
}

// initPGBuiltins adds all of the postgres builtins to the Builtins map.
func initPGBuiltins() {
	for k, v := range pgBuiltins {
		for i := range v {
			v[i].category = categoryCompatibility
		}
		Builtins[k] = v
	}

	// Make non-array type i/o builtins.
	for _, typ := range OidToType {
		// Skip array types. We're doing them separately below.
		if typ != TypeAny && typ != TypeIntVector && typ.Equivalent(TypeAnyArray) {
			continue
		}
		builtinPrefix := PGIOBuiltinPrefix(typ)
		for name, builtins := range makeTypeIOBuiltins(builtinPrefix, typ) {
			Builtins[name] = builtins
		}
	}
	// Make array type i/o builtins.
	for name, builtins := range makeTypeIOBuiltins("array_", TypeAnyArray) {
		Builtins[name] = builtins
	}
}

func makeTypeIOBuiltin(argTypes typeList, returnType Type) []Builtin {
	return []Builtin{
		{
			Types:      argTypes,
			ReturnType: fixedReturnType(returnType),
			fn:         func(_ *EvalContext, _ Datums) (Datum, error) { return nil, errors.New("unimplemented") },
			Info:       notUsableInfo,
		},
	}
}

// makeTypeIOBuiltins generates the 4 i/o builtins that Postgres implements for
// every type: typein, typeout, typerecv, and typsend. All 4 builtins are no-op,
// and only supported because ORMs sometimes use their names to form a map for
// client-side type encoding and decoding. See issue #12526 for more details.
func makeTypeIOBuiltins(builtinPrefix string, typ Type) map[string][]Builtin {
	typname := typ.String()
	return map[string][]Builtin{
		builtinPrefix + "send": makeTypeIOBuiltin(ArgTypes{{typname, typ}}, TypeBytes),
		// Note: PG takes type 2281 "internal" for these builtins, which we don't
		// provide. We won't implement these functions anyway, so it shouldn't
		// matter.
		builtinPrefix + "recv": makeTypeIOBuiltin(ArgTypes{{"input", TypeAny}}, typ),
		// Note: PG returns 'cstring' for these builtins, but we don't support that.
		builtinPrefix + "out": makeTypeIOBuiltin(ArgTypes{{typname, typ}}, TypeBytes),
		// Note: PG takes 'cstring' for these builtins, but we don't support that.
		builtinPrefix + "in": makeTypeIOBuiltin(ArgTypes{{"input", TypeAny}}, typ),
	}
}

var pgBuiltins = map[string][]Builtin{
	// See https://www.postgresql.org/docs/9.6/static/functions-info.html.
	"pg_backend_pid": {
		Builtin{
			Types:      ArgTypes{},
			ReturnType: fixedReturnType(TypeInt),
			fn: func(_ *EvalContext, _ Datums) (Datum, error) {
				return NewDInt(-1), nil
			},
			Info: notUsableInfo,
		},
	},

	// Postgres defines pg_get_expr as a function that "decompiles the internal form
	// of an expression", which is provided in the pg_node_tree type. In Cockroach's
	// pg_catalog implementation, we populate all pg_node_tree columns with the
	// corresponding expression as a string, which means that this function can simply
	// return the first argument directly. It also means we can ignore the second and
	// optional third argument.
	"pg_get_expr": {
		Builtin{
			Types: ArgTypes{
				{"pg_node_tree", TypeString},
				{"relation_oid", TypeOid},
			},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				return args[0], nil
			},
			Info: notUsableInfo,
		},
		Builtin{
			Types: ArgTypes{
				{"pg_node_tree", TypeString},
				{"relation_oid", TypeOid},
				{"pretty_bool", TypeBool},
			},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				return args[0], nil
			},
			Info: notUsableInfo,
		},
	},

	// pg_get_indexdef functions like SHOW CREATE INDEX would if we supported that
	// statement.
	"pg_get_indexdef": {
		Builtin{
			Types: ArgTypes{
				{"index_oid", TypeOid},
			},
			distsqlBlacklist: true,
			ReturnType:       fixedReturnType(TypeString),
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				r, err := ctx.Planner.QueryRow(
					ctx.Ctx(), "SELECT indexdef FROM pg_catalog.pg_indexes WHERE crdb_oid=$1", args[0])
				if err != nil {
					return nil, err
				}
				if len(r) == 0 {
					return nil, fmt.Errorf("unknown index (OID=%s)", args[0])
				}
				return r[0], nil
			},
			Info: notUsableInfo,
		},
		// The other overload for this function, pg_get_indexdef(index_oid,
		// column_no, pretty_bool), is unimplemented, because it isn't used by
		// supported ORMs.
	},

	"pg_typeof": {
		// TODO(knz): This is a proof-of-concept until TypeAny works
		// properly.
		Builtin{
			Types:      ArgTypes{{"val", TypeAny}},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				return NewDString(args[0].ResolvedType().String()), nil
			},
			Info: notUsableInfo,
		},
	},
	"pg_get_userbyid": {
		Builtin{
			Types: ArgTypes{
				{"role_oid", TypeOid},
			},
			distsqlBlacklist: true,
			ReturnType:       fixedReturnType(TypeString),
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				oid := args[0]
				t, err := ctx.Planner.QueryRow(
					ctx.Ctx(), "SELECT rolname FROM pg_catalog.pg_roles WHERE oid=$1", oid)
				if err != nil {
					return nil, err
				}
				if len(t) == 0 {
					return NewDString(fmt.Sprintf("unknown (OID=%s)", args[0])), nil
				}
				return t[0], nil
			},
			Info: notUsableInfo,
		},
	},
	"format_type": {
		Builtin{
			// TODO(jordan): typemod should be a Nullable TypeInt when supported.
			Types:      ArgTypes{{"type_oid", TypeOid}, {"typemod", TypeInt}},
			ReturnType: fixedReturnType(TypeString),
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				typ, ok := OidToType[oid.Oid(int(args[0].(*DOid).DInt))]
				if !ok {
					return NewDString(fmt.Sprintf("unknown (OID=%s)", args[0])), nil
				}
				return NewDString(typ.SQLName()), nil
			},
			Info: "Returns the SQL name of a data type that is " +
				"identified by its type OID and possibly a type modifier. " +
				"Currently, the type modifier is ignored.",
		},
	},
	"col_description": {
		Builtin{
			Types:      ArgTypes{{"table_oid", TypeOid}, {"column_number", TypeInt}},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, _ Datums) (Datum, error) {
				return DNull, nil
			},
			Info: notUsableInfo,
		},
	},
	"obj_description": {
		Builtin{
			Types:      ArgTypes{{"object_oid", TypeOid}},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, _ Datums) (Datum, error) {
				return DNull, nil
			},
			Info: notUsableInfo,
		},
		Builtin{
			Types:      ArgTypes{{"object_oid", TypeOid}, {"catalog_name", TypeString}},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, _ Datums) (Datum, error) {
				return DNull, nil
			},
			Info: notUsableInfo,
		},
	},
	"oid": {
		Builtin{
			Types:      ArgTypes{{"int", TypeInt}},
			ReturnType: fixedReturnType(TypeOid),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				return NewDOid(*args[0].(*DInt)), nil
			},
			Info: "Converts an integer to an OID.",
		},
	},
	"shobj_description": {
		Builtin{
			Types:      ArgTypes{{"object_oid", TypeOid}, {"catalog_name", TypeString}},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, _ Datums) (Datum, error) {
				return DNull, nil
			},
			Info: notUsableInfo,
		},
	},
	"pg_try_advisory_lock": {
		Builtin{
			Types:      ArgTypes{{"int", TypeInt}},
			ReturnType: fixedReturnType(TypeBool),
			fn: func(_ *EvalContext, _ Datums) (Datum, error) {
				return DBoolTrue, nil
			},
			Info: notUsableInfo,
		},
	},
	"pg_advisory_unlock": {
		Builtin{
			Types:      ArgTypes{{"int", TypeInt}},
			ReturnType: fixedReturnType(TypeBool),
			fn: func(_ *EvalContext, _ Datums) (Datum, error) {
				return DBoolTrue, nil
			},
			Info: notUsableInfo,
		},
	},
}
