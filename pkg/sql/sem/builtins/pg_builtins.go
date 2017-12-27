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

package builtins

import (
	"fmt"

	"github.com/lib/pq/oid"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
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
	types.Time.Oid():        {},
	types.Decimal.Oid():     {},
	types.Interval.Oid():    {},
	types.JSON.Oid():        {},
	types.UUID.Oid():        {},
	types.Timestamp.Oid():   {},
	types.TimestampTZ.Oid(): {},
	types.FamTuple.Oid():    {},
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
			v[i].Category = categoryCompatibility
		}
		Builtins[k] = v
	}

	// Make non-array type i/o builtins.
	for _, typ := range types.OidToType {
		// Skip array types. We're doing them separately below.
		if typ != types.Any && typ != types.IntVector && typ.Equivalent(types.AnyArray) {
			continue
		}
		builtinPrefix := PGIOBuiltinPrefix(typ)
		for name, builtins := range makeTypeIOBuiltins(builtinPrefix, typ) {
			Builtins[name] = builtins
		}
	}
	// Make array type i/o builtins.
	for name, builtins := range makeTypeIOBuiltins("array_", types.AnyArray) {
		Builtins[name] = builtins
	}
}

var errUnimplemented = pgerror.NewError(pgerror.CodeFeatureNotSupportedError, "unimplemented")

func makeTypeIOBuiltin(argTypes tree.TypeList, returnType types.T) []tree.Builtin {
	return []tree.Builtin{
		{
			Types:      argTypes,
			ReturnType: tree.FixedReturnType(returnType),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
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
func makeTypeIOBuiltins(builtinPrefix string, typ types.T) map[string][]tree.Builtin {
	typname := typ.String()
	return map[string][]tree.Builtin{
		builtinPrefix + "send": makeTypeIOBuiltin(tree.ArgTypes{{typname, typ}}, types.Bytes),
		// Note: PG takes type 2281 "internal" for these builtins, which we don't
		// provide. We won't implement these functions anyway, so it shouldn't
		// matter.
		builtinPrefix + "recv": makeTypeIOBuiltin(tree.ArgTypes{{"input", types.Any}}, typ),
		// Note: PG returns 'cstring' for these builtins, but we don't support that.
		builtinPrefix + "out": makeTypeIOBuiltin(tree.ArgTypes{{typname, typ}}, types.Bytes),
		// Note: PG takes 'cstring' for these builtins, but we don't support that.
		builtinPrefix + "in": makeTypeIOBuiltin(tree.ArgTypes{{"input", types.Any}}, typ),
	}
}

// http://doxygen.postgresql.org/pg__wchar_8h.html#a22e0c8b9f59f6e226a5968620b4bb6a9aac3b065b882d3231ba59297524da2f23
var (
	// DatEncodingUTFId is the encoding ID for our only supported database
	// encoding, UTF8.
	DatEncodingUTFId = tree.NewDInt(6)
	// DatEncodingEnUTF8 is the encoding name for our only supported database
	// encoding, UTF8.
	DatEncodingEnUTF8        = tree.NewDString("en_US.utf8")
	datEncodingUTF8ShortName = tree.NewDString("UTF8")
)

// Make a pg_get_viewdef function with the given arguments.
func makePGGetViewDef(argTypes tree.ArgTypes) tree.Builtin {
	return tree.Builtin{
		Types:            argTypes,
		DistsqlBlacklist: true,
		ReturnType:       tree.FixedReturnType(types.String),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			r, err := ctx.Planner.QueryRow(
				ctx.Ctx(), "SELECT definition FROM pg_catalog.pg_views v JOIN pg_catalog.pg_class c ON "+
					"c.relname=v.viewname WHERE oid=$1", args[0])
			if err != nil {
				return nil, err
			}
			if len(r) == 0 {
				return tree.DNull, nil
			}
			return r[0], nil
		},
		Info: notUsableInfo,
	}
}

// Make a pg_get_constraintdef function with the given arguments.
func makePGGetConstraintDef(argTypes tree.ArgTypes) tree.Builtin {
	return tree.Builtin{
		Types:            argTypes,
		DistsqlBlacklist: true,
		ReturnType:       tree.FixedReturnType(types.String),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			r, err := ctx.Planner.QueryRow(
				ctx.Ctx(), "SELECT condef FROM pg_catalog.pg_constraint WHERE oid=$1", args[0])
			if err != nil {
				return nil, err
			}
			if len(r) == 0 {
				return nil, pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, "unknown constraint (OID=%s)", args[0])
			}
			return r[0], nil
		},
		Info: notUsableInfo,
	}
}

var pgBuiltins = map[string][]tree.Builtin{
	// See https://www.postgresql.org/docs/9.6/static/functions-info.html.
	"pg_backend_pid": {
		tree.Builtin{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.NewDInt(-1), nil
			},
			Info: notUsableInfo,
		},
	},

	// See https://www.postgresql.org/docs/9.3/static/catalog-pg-database.html.
	"pg_encoding_to_char": {
		tree.Builtin{
			Types: tree.ArgTypes{
				{"encoding_id", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0].Compare(ctx, DatEncodingUTFId) == 0 {
					return datEncodingUTF8ShortName, nil
				}
				return tree.DNull, nil
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
		tree.Builtin{
			Types: tree.ArgTypes{
				{"pg_node_tree", types.String},
				{"relation_oid", types.Oid},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return args[0], nil
			},
			Info: notUsableInfo,
		},
		tree.Builtin{
			Types: tree.ArgTypes{
				{"pg_node_tree", types.String},
				{"relation_oid", types.Oid},
				{"pretty_bool", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return args[0], nil
			},
			Info: notUsableInfo,
		},
	},

	// pg_get_constraintdef functions like SHOW CREATE CONSTRAINT would if we
	// supported that statement.
	"pg_get_constraintdef": {
		makePGGetConstraintDef(tree.ArgTypes{
			{"constraint_oid", types.Oid}, {"pretty_bool", types.Bool}}),
		makePGGetConstraintDef(tree.ArgTypes{{"constraint_oid", types.Oid}}),
	},

	// pg_get_indexdef functions like SHOW CREATE INDEX would if we supported that
	// statement.
	"pg_get_indexdef": {
		tree.Builtin{
			Types: tree.ArgTypes{
				{"index_oid", types.Oid},
			},
			DistsqlBlacklist: true,
			ReturnType:       tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
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
		makePGGetViewDef(tree.ArgTypes{{"view_oid", types.Oid}}),
		makePGGetViewDef(tree.ArgTypes{{"view_oid", types.Oid}, {"pretty_bool", types.Bool}}),
	},

	"pg_typeof": {
		// TODO(knz): This is a proof-of-concept until types.Any works
		// properly.
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.Any}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(args[0].ResolvedType().String()), nil
			},
			Info: notUsableInfo,
		},
	},
	"pg_get_userbyid": {
		tree.Builtin{
			Types: tree.ArgTypes{
				{"role_oid", types.Oid},
			},
			DistsqlBlacklist: true,
			ReturnType:       tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				oid := args[0]
				t, err := ctx.Planner.QueryRow(
					ctx.Ctx(), "SELECT rolname FROM pg_catalog.pg_roles WHERE oid=$1", oid)
				if err != nil {
					return nil, err
				}
				if len(t) == 0 {
					return tree.NewDString(fmt.Sprintf("unknown (OID=%s)", args[0])), nil
				}
				return t[0], nil
			},
			Info: notUsableInfo,
		},
	},
	"pg_sequence_parameters": {
		// pg_sequence_parameters is an undocumented Postgres builtin that returns
		// information about a sequence given its OID. It's nevertheless used by
		// at least one UI tool, so we provide an implementation for compatibility.
		// The real implementation returns a record; we fake it by returning a
		// comma-delimited string enclosed by parentheses.
		// TODO(jordan): convert this to return a record type once we support that.
		tree.Builtin{
			Types:            tree.ArgTypes{{"sequence_oid", types.Oid}},
			DistsqlBlacklist: true,
			ReturnType:       tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				r, err := ctx.Planner.QueryRow(
					ctx.Ctx(), `SELECT seqstart, seqmin, seqmax, seqincrement, seqcycle, seqcache, seqtypid
FROM pg_catalog.pg_sequence WHERE seqrelid=$1`, args[0])
				if err != nil {
					return nil, err
				}
				if len(r) == 0 {
					return nil, pgerror.NewErrorf(pgerror.CodeUndefinedTableError, "unknown sequence (OID=%s)", args[0])
				}
				seqstart, seqmin, seqmax, seqincrement, seqcycle, seqcache, seqtypid := r[0], r[1], r[2], r[3], r[4], r[5], r[6]
				seqcycleStr := "t"
				if seqcycle.(*tree.DBool) == tree.DBoolFalse {
					seqcycleStr = "f"
				}
				return tree.NewDString(fmt.Sprintf("(%s,%s,%s,%s,%s,%s,%s)", seqstart, seqmin, seqmax, seqincrement, seqcycleStr, seqcache, seqtypid)), nil
			},
			Info: notUsableInfo,
		},
	},
	"format_type": {
		tree.Builtin{
			Types:        tree.ArgTypes{{"type_oid", types.Oid}, {"typemod", types.Int}},
			ReturnType:   tree.FixedReturnType(types.String),
			NullableArgs: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				oidArg := args[0]
				if oidArg == tree.DNull {
					return tree.DNull, nil
				}
				typ, ok := types.OidToType[oid.Oid(int(oidArg.(*tree.DOid).DInt))]
				if !ok {
					return tree.NewDString(fmt.Sprintf("unknown (OID=%s)", oidArg)), nil
				}
				return tree.NewDString(typ.SQLName()), nil
			},
			Info: "Returns the SQL name of a data type that is " +
				"identified by its type OID and possibly a type modifier. " +
				"Currently, the type modifier is ignored.",
		},
	},
	"col_description": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"table_oid", types.Oid}, {"column_number", types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.DNull, nil
			},
			Info: notUsableInfo,
		},
	},
	"obj_description": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"object_oid", types.Oid}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.DNull, nil
			},
			Info: notUsableInfo,
		},
		tree.Builtin{
			Types:      tree.ArgTypes{{"object_oid", types.Oid}, {"catalog_name", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.DNull, nil
			},
			Info: notUsableInfo,
		},
	},
	"oid": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"int", types.Int}},
			ReturnType: tree.FixedReturnType(types.Oid),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDOid(*args[0].(*tree.DInt)), nil
			},
			Info: "Converts an integer to an OID.",
		},
	},
	"shobj_description": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"object_oid", types.Oid}, {"catalog_name", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.DNull, nil
			},
			Info: notUsableInfo,
		},
	},
	"pg_try_advisory_lock": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"int", types.Int}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.DBoolTrue, nil
			},
			Info: notUsableInfo,
		},
	},
	"pg_advisory_unlock": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"int", types.Int}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.DBoolTrue, nil
			},
			Info: notUsableInfo,
		},
	},
	// pg_table_is_visible returns true if the input oid corresponds to a table
	// that is part of the databases on the search path.
	// https://www.postgresql.org/docs/9.6/static/functions-info.html
	"pg_table_is_visible": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"oid", types.Oid}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				oid := args[0]
				t, err := ctx.Planner.QueryRow(ctx.Ctx(),
					"SELECT nspname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace=n.oid "+
						"WHERE c.oid=$1 AND nspname=ANY(current_schemas(true));", oid)
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(t != nil)), nil
			},
			Info: notUsableInfo,
		},
	},

	// inet_{client,server}_{addr,port} return either an INet address or integer
	// port that corresponds to either the client or server side of the current
	// session's connection.
	//
	// They're currently trivially implemented by always returning 0, to prevent
	// tools that expect them to exist from failing. The output of these builtins
	// is used in an advisory capacity for displaying in a UI, and is therefore
	// okay to fake for the time being. Implementing these properly requires
	// plumbing these values into the EvalContext.
	//
	// See https://www.postgresql.org/docs/10/static/functions-info.html
	"inet_client_addr": {
		tree.Builtin{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDIPAddr(tree.DIPAddr{IPAddr: ipaddr.IPAddr{}}), nil
			},
			Info: notUsableInfo,
		},
	},
	"inet_client_port": {
		tree.Builtin{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.DZero, nil
			},
			Info: notUsableInfo,
		},
	},
	"inet_server_addr": {
		tree.Builtin{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDIPAddr(tree.DIPAddr{IPAddr: ipaddr.IPAddr{}}), nil
			},
			Info: notUsableInfo,
		},
	},
	"inet_server_port": {
		tree.Builtin{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.DZero, nil
			},
			Info: notUsableInfo,
		},
	},
}
