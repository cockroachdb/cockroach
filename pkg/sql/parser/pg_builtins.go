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

package parser

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"

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
	types.Any.Oid():         {},
	types.Date.Oid():        {},
	types.Decimal.Oid():     {},
	types.Interval.Oid():    {},
	types.JSON.Oid():        {},
	types.UUID.Oid():        {},
	types.Timestamp.Oid():   {},
	types.TimestampTZ.Oid(): {},
	types.Tuple.Oid():       {},
}

// PGIOBuiltinPrefix returns the string prefix to a type's IO functions. This
// is either the type's postgres display name or the type's postgres display
// name plus an underscore, depending on the type.
func PGIOBuiltinPrefix(typ types.T) string {
	builtinPrefix := types.PGDisplayName(typ)
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
	for _, typ := range types.OidToType {
		// Skip array types. We're doing them separately below.
		if typ != types.TypeAny && typ != types.TypeIntVector && typ.Equivalent(types.TypeAnyArray) {
			continue
		}
		builtinPrefix := PGIOBuiltinPrefix(typ)
		for name, builtins := range makeTypeIOBuiltins(builtinPrefix, typ) {
			Builtins[name] = builtins
		}
	}
	// Make array type i/o builtins.
	for name, builtins := range makeTypeIOBuiltins("array_", types.TypeAnyArray) {
		Builtins[name] = builtins
	}
}

var errUnimplemented = pgerror.NewError(pgerror.CodeFeatureNotSupportedError, "unimplemented")

func makeTypeIOBuiltin(argTypes typeList, returnType types.T) []Builtin {
	return []Builtin{
		{
			Types:      argTypes,
			ReturnType: fixedReturnType(returnType),
			fn: func(_ *EvalContext, _ Datums) (Datum, error) {
				return nil, errUnimplemented
			},
			Info: notUsableInfo,
		},
	}
}

// makeTypeIOBuiltins generates the 4 i/o builtins that Postgres implements for
// every type: typein, typeout, typerecv, and typsend. All 4 builtins are no-op,
// and only supported because ORMs sometimes use their names to form a map for
// client-side type encoding and decoding. See issue #12526 for more details.
func makeTypeIOBuiltins(builtinPrefix string, typ types.T) map[string][]Builtin {
	typname := typ.String()
	return map[string][]Builtin{
		builtinPrefix + "send": makeTypeIOBuiltin(ArgTypes{{typname, typ}}, types.TypeBytes),
		// Note: PG takes type 2281 "internal" for these builtins, which we don't
		// provide. We won't implement these functions anyway, so it shouldn't
		// matter.
		builtinPrefix + "recv": makeTypeIOBuiltin(ArgTypes{{"input", types.TypeAny}}, typ),
		// Note: PG returns 'cstring' for these builtins, but we don't support that.
		builtinPrefix + "out": makeTypeIOBuiltin(ArgTypes{{typname, typ}}, types.TypeBytes),
		// Note: PG takes 'cstring' for these builtins, but we don't support that.
		builtinPrefix + "in": makeTypeIOBuiltin(ArgTypes{{"input", types.TypeAny}}, typ),
	}
}

// http://doxygen.postgresql.org/pg__wchar_8h.html#a22e0c8b9f59f6e226a5968620b4bb6a9aac3b065b882d3231ba59297524da2f23
var (
	// DatEncodingUTFId is the encoding ID for our only supported database
	// encoding, UTF8.
	DatEncodingUTFId = NewDInt(6)
	// DatEncodingEnUTF8 is the encoding name for our only supported database
	// encoding, UTF8.
	DatEncodingEnUTF8        = NewDString("en_US.utf8")
	datEncodingUTF8ShortName = NewDString("UTF8")
)

// Make a pg_get_viewdef function with the given arguments.
func makePGGetViewDef(argTypes ArgTypes) Builtin {
	return Builtin{
		Types:            argTypes,
		distsqlBlacklist: true,
		ReturnType:       fixedReturnType(types.TypeString),
		fn: func(ctx *EvalContext, args Datums) (Datum, error) {
			r, err := ctx.Planner.QueryRow(
				ctx.Ctx(), "SELECT definition FROM pg_catalog.pg_views v JOIN pg_catalog.pg_class c ON "+
					"c.relname=v.viewname WHERE oid=$1", args[0])
			if err != nil {
				return nil, err
			}
			if len(r) == 0 {
				return DNull, nil
			}
			return r[0], nil
		},
		Info: notUsableInfo,
	}
}

var pgBuiltins = map[string][]Builtin{
	// See https://www.postgresql.org/docs/9.6/static/functions-info.html.
	"pg_backend_pid": {
		Builtin{
			Types:      ArgTypes{},
			ReturnType: fixedReturnType(types.TypeInt),
			fn: func(_ *EvalContext, _ Datums) (Datum, error) {
				return NewDInt(-1), nil
			},
			Info: notUsableInfo,
		},
	},

	// See https://www.postgresql.org/docs/9.3/static/catalog-pg-database.html.
	"pg_encoding_to_char": {
		Builtin{
			Types: ArgTypes{
				{"encoding_id", types.TypeInt},
			},
			ReturnType: fixedReturnType(types.TypeString),
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				if args[0].Compare(ctx, DatEncodingUTFId) == 0 {
					return datEncodingUTF8ShortName, nil
				}
				return DNull, nil
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
				{"pg_node_tree", types.TypeString},
				{"relation_oid", types.TypeOid},
			},
			ReturnType: fixedReturnType(types.TypeString),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				return args[0], nil
			},
			Info: notUsableInfo,
		},
		Builtin{
			Types: ArgTypes{
				{"pg_node_tree", types.TypeString},
				{"relation_oid", types.TypeOid},
				{"pretty_bool", types.TypeBool},
			},
			ReturnType: fixedReturnType(types.TypeString),
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
				{"index_oid", types.TypeOid},
			},
			distsqlBlacklist: true,
			ReturnType:       fixedReturnType(types.TypeString),
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				r, err := ctx.Planner.QueryRow(
					ctx.Ctx(), "SELECT indexdef FROM pg_catalog.pg_indexes WHERE crdb_oid=$1", args[0])
				if err != nil {
					return nil, err
				}
				if len(r) == 0 {
					return nil, pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, "unknown index (OID=%s)", args[0])
				}
				return r[0], nil
			},
			Info: notUsableInfo,
		},
		// The other overload for this function, pg_get_indexdef(index_oid,
		// column_no, pretty_bool), is unimplemented, because it isn't used by
		// supported ORMs.
	},

	// pg_get_viewdef functions like SHOW CREATE VIEW but returns the same format as
	// PostgreSQL leaving out the actual 'CREATE VIEW table_name AS' portion of the statement.
	"pg_get_viewdef": {
		makePGGetViewDef(ArgTypes{{"view_oid", types.TypeOid}}),
		makePGGetViewDef(ArgTypes{{"view_oid", types.TypeOid}, {"pretty_bool", types.TypeBool}}),
	},

	"pg_typeof": {
		// TODO(knz): This is a proof-of-concept until types.TypeAny works
		// properly.
		Builtin{
			Types:      ArgTypes{{"val", types.TypeAny}},
			ReturnType: fixedReturnType(types.TypeString),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				return NewDString(args[0].ResolvedType().String()), nil
			},
			Info: notUsableInfo,
		},
	},
	"pg_get_userbyid": {
		Builtin{
			Types: ArgTypes{
				{"role_oid", types.TypeOid},
			},
			distsqlBlacklist: true,
			ReturnType:       fixedReturnType(types.TypeString),
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
			Types:        ArgTypes{{"type_oid", types.TypeOid}, {"typemod", types.TypeInt}},
			ReturnType:   fixedReturnType(types.TypeString),
			nullableArgs: true,
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				oidArg := args[0]
				if oidArg == DNull {
					return DNull, nil
				}
				typ, ok := types.OidToType[oid.Oid(int(oidArg.(*DOid).DInt))]
				if !ok {
					return NewDString(fmt.Sprintf("unknown (OID=%s)", oidArg)), nil
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
			Types:      ArgTypes{{"table_oid", types.TypeOid}, {"column_number", types.TypeInt}},
			ReturnType: fixedReturnType(types.TypeString),
			fn: func(_ *EvalContext, _ Datums) (Datum, error) {
				return DNull, nil
			},
			Info: notUsableInfo,
		},
	},
	"obj_description": {
		Builtin{
			Types:      ArgTypes{{"object_oid", types.TypeOid}},
			ReturnType: fixedReturnType(types.TypeString),
			fn: func(_ *EvalContext, _ Datums) (Datum, error) {
				return DNull, nil
			},
			Info: notUsableInfo,
		},
		Builtin{
			Types:      ArgTypes{{"object_oid", types.TypeOid}, {"catalog_name", types.TypeString}},
			ReturnType: fixedReturnType(types.TypeString),
			fn: func(_ *EvalContext, _ Datums) (Datum, error) {
				return DNull, nil
			},
			Info: notUsableInfo,
		},
	},
	"oid": {
		Builtin{
			Types:      ArgTypes{{"int", types.TypeInt}},
			ReturnType: fixedReturnType(types.TypeOid),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				return NewDOid(*args[0].(*DInt)), nil
			},
			Info: "Converts an integer to an OID.",
		},
	},
	"shobj_description": {
		Builtin{
			Types:      ArgTypes{{"object_oid", types.TypeOid}, {"catalog_name", types.TypeString}},
			ReturnType: fixedReturnType(types.TypeString),
			fn: func(_ *EvalContext, _ Datums) (Datum, error) {
				return DNull, nil
			},
			Info: notUsableInfo,
		},
	},
	"pg_try_advisory_lock": {
		Builtin{
			Types:      ArgTypes{{"int", types.TypeInt}},
			ReturnType: fixedReturnType(types.TypeBool),
			fn: func(_ *EvalContext, _ Datums) (Datum, error) {
				return DBoolTrue, nil
			},
			Info: notUsableInfo,
		},
	},
	"pg_advisory_unlock": {
		Builtin{
			Types:      ArgTypes{{"int", types.TypeInt}},
			ReturnType: fixedReturnType(types.TypeBool),
			fn: func(_ *EvalContext, _ Datums) (Datum, error) {
				return DBoolTrue, nil
			},
			Info: notUsableInfo,
		},
	},
	// pg_table_is_visible returns true if the input oid corresponds to a table
	// that is part of the databases on the search path.
	// https://www.postgresql.org/docs/9.6/static/functions-info.html
	"pg_table_is_visible": {
		Builtin{
			Types:      ArgTypes{{"oid", types.TypeOid}},
			ReturnType: fixedReturnType(types.TypeBool),
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				oid := args[0]
				t, err := ctx.Planner.QueryRow(ctx.Ctx(),
					"SELECT nspname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace=n.oid "+
						"WHERE c.oid=$1 AND nspname=ANY(current_schemas(true));", oid)
				if err != nil {
					return nil, err
				}
				return MakeDBool(DBool(t != nil)), nil
			},
			Info: notUsableInfo,
		},
	},
}
