// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// This file contains builtin functions that we implement primarily for
// compatibility with Postgres.

const notUsableInfo = "Not usable; exposed only for compatibility with PostgreSQL."

// makeNotUsableFalseBuiltin creates a builtin that takes no arguments and
// always returns a boolean with the value false.
func makeNotUsableFalseBuiltin() builtinDefinition {
	return builtinDefinition{
		props: defProps(),
		overloads: []tree.Overload{
			{
				Types:      tree.ArgTypes{},
				ReturnType: tree.FixedReturnType(types.Bool),
				Fn: func(*tree.EvalContext, tree.Datums) (tree.Datum, error) {
					return tree.DBoolFalse, nil
				},
				Info:       notUsableInfo,
				Volatility: tree.VolatilityVolatile,
			},
		},
	}
}

// typeBuiltinsHaveUnderscore is a map to keep track of which types have i/o
// builtins with underscores in between their type name and the i/o builtin
// name, like date_in vs int8in. There seems to be no other way to
// programmatically determine whether or not this underscore is present, hence
// the existence of this map.
var typeBuiltinsHaveUnderscore = map[oid.Oid]struct{}{
	types.Any.Oid():         {},
	types.AnyArray.Oid():    {},
	types.Date.Oid():        {},
	types.Time.Oid():        {},
	types.TimeTZ.Oid():      {},
	types.Decimal.Oid():     {},
	types.Interval.Oid():    {},
	types.Jsonb.Oid():       {},
	types.Uuid.Oid():        {},
	types.VarBit.Oid():      {},
	types.Geometry.Oid():    {},
	types.Geography.Oid():   {},
	types.Box2D.Oid():       {},
	oid.T_bit:               {},
	types.Timestamp.Oid():   {},
	types.TimestampTZ.Oid(): {},
	types.AnyTuple.Oid():    {},
}

// UpdatableCommand matches update operations in postgres.
type UpdatableCommand tree.DInt

// The following constants are the values for UpdatableCommand enumeration.
const (
	UpdateCommand UpdatableCommand = 2 + iota
	InsertCommand
	DeleteCommand
)

var (
	nonUpdatableEvents = tree.NewDInt(0)
	allUpdatableEvents = tree.NewDInt((1 << UpdateCommand) | (1 << InsertCommand) | (1 << DeleteCommand))
)

// PGIOBuiltinPrefix returns the string prefix to a type's IO functions. This
// is either the type's postgres display name or the type's postgres display
// name plus an underscore, depending on the type.
func PGIOBuiltinPrefix(typ *types.T) string {
	builtinPrefix := typ.PGName()
	if _, ok := typeBuiltinsHaveUnderscore[typ.Oid()]; ok {
		return builtinPrefix + "_"
	}
	return builtinPrefix
}

// initPGBuiltins adds all of the postgres builtins to the Builtins map.
func initPGBuiltins() {
	for k, v := range pgBuiltins {
		if _, exists := builtins[k]; exists {
			panic("duplicate builtin: " + k)
		}
		v.props.Category = categoryCompatibility
		builtins[k] = v
	}

	// Make non-array type i/o builtins.
	for _, typ := range types.OidToType {
		// Skip most array types. We're doing them separately below.
		switch typ.Oid() {
		case oid.T_int2vector, oid.T_oidvector:
		default:
			if typ.Family() == types.ArrayFamily {
				continue
			}
		}
		builtinPrefix := PGIOBuiltinPrefix(typ)
		for name, builtin := range makeTypeIOBuiltins(builtinPrefix, typ) {
			builtins[name] = builtin
		}
	}
	// Make array type i/o builtins.
	for name, builtin := range makeTypeIOBuiltins("array_", types.AnyArray) {
		builtins[name] = builtin
	}
	for name, builtin := range makeTypeIOBuiltins("anyarray_", types.AnyArray) {
		builtins[name] = builtin
	}
	// Make enum type i/o builtins.
	for name, builtin := range makeTypeIOBuiltins("enum_", types.AnyEnum) {
		builtins[name] = builtin
	}

	// Make crdb_internal.create_regfoo builtins.
	for _, typ := range []*types.T{
		types.RegClass,
		types.RegNamespace,
		types.RegProc,
		types.RegProcedure,
		types.RegRole,
		types.RegType,
	} {
		typName := typ.SQLStandardName()
		builtins["crdb_internal.create_"+typName] = makeCreateRegDef(typ)
	}
}

var errUnimplemented = pgerror.New(pgcode.FeatureNotSupported, "unimplemented")

func makeTypeIOBuiltin(argTypes tree.TypeList, returnType *types.T) builtinDefinition {
	return builtinDefinition{
		props: tree.FunctionProperties{
			Category: categoryCompatibility,
		},
		overloads: []tree.Overload{
			{
				Types:      argTypes,
				ReturnType: tree.FixedReturnType(returnType),
				Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
					return nil, errUnimplemented
				},
				Info:       notUsableInfo,
				Volatility: tree.VolatilityVolatile,
				// Ignore validity checks for typeio builtins. We don't
				// implement these anyway, and they are very hard to special
				// case.
				IgnoreVolatilityCheck: true,
			},
		},
	}
}

// makeTypeIOBuiltins generates the 4 i/o builtins that Postgres implements for
// every type: typein, typeout, typerecv, and typsend. All 4 builtins are no-op,
// and only supported because ORMs sometimes use their names to form a map for
// client-side type encoding and decoding. See issue #12526 for more details.
func makeTypeIOBuiltins(builtinPrefix string, typ *types.T) map[string]builtinDefinition {
	typname := typ.String()
	return map[string]builtinDefinition{
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

// Make a pg_get_indexdef function with the given arguments.
func makePGGetIndexDef(argTypes tree.ArgTypes) tree.Overload {
	return tree.Overload{
		Types:      argTypes,
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			colNumber := *tree.NewDInt(0)
			if len(args) == 3 {
				// The 1 argument and 3 argument variants are equivalent when column number 0 is passed.
				colNumber = *args[1].(*tree.DInt)
			}
			r, err := ctx.Planner.QueryRowEx(
				ctx.Ctx(), "pg_get_indexdef",
				ctx.Txn,
				sessiondata.NoSessionDataOverride,
				`SELECT CASE
    				WHEN $2 = 0 THEN defs.indexdef
						-- If the column number does not exist in the index we return an empty string.
						WHEN $2 < 0 OR $2 > array_length(i.indkey, 1) THEN ''
						WHEN i.indkey[$2-1] = 0 THEN (indexprs::STRING[])[array_position(array_positions(i.indkey, 0), $2)]
						ELSE a.attname
					END as pg_get_indexdef
					FROM pg_catalog.pg_index i
					LEFT JOIN pg_attribute a ON (a.attrelid = i.indexrelid AND a.attnum = $2)
					LEFT JOIN pg_indexes defs ON ($2 = 0 AND defs.crdb_oid = i.indexrelid)
					WHERE i.indexrelid = $1`, args[0], colNumber)
			if err != nil {
				return nil, err
			}
			if len(r) == 0 {
				return tree.DNull, nil
			}
			if len(r) > 1 {
				return nil, errors.AssertionFailedf("pg_get_indexdef query has more than 1 result row: %+v", r)
			}
			return r[0], nil
		},
		Info:       notUsableInfo,
		Volatility: tree.VolatilityStable,
	}
}

// Make a pg_get_viewdef function with the given arguments.
func makePGGetViewDef(argTypes tree.ArgTypes) tree.Overload {
	return tree.Overload{
		Types:      argTypes,
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			r, err := ctx.Planner.QueryRowEx(
				ctx.Ctx(), "pg_get_viewdef",
				ctx.Txn,
				sessiondata.NoSessionDataOverride,
				"SELECT definition FROM pg_catalog.pg_views v JOIN pg_catalog.pg_class c ON "+
					"c.relname=v.viewname WHERE oid=$1", args[0])
			if err != nil {
				return nil, err
			}
			if len(r) == 0 {
				return tree.DNull, nil
			}
			return r[0], nil
		},
		Info:       notUsableInfo,
		Volatility: tree.VolatilityStable,
	}
}

// Make a pg_get_constraintdef function with the given arguments.
func makePGGetConstraintDef(argTypes tree.ArgTypes) tree.Overload {
	return tree.Overload{
		Types:      argTypes,
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			r, err := ctx.Planner.QueryRowEx(
				ctx.Ctx(), "pg_get_constraintdef",
				ctx.Txn,
				sessiondata.NoSessionDataOverride,
				"SELECT condef FROM pg_catalog.pg_constraint WHERE oid=$1", args[0])
			if err != nil {
				return nil, err
			}
			if len(r) == 0 {
				return nil, pgerror.Newf(pgcode.InvalidParameterValue, "unknown constraint (OID=%s)", args[0])
			}
			return r[0], nil
		},
		Info:       notUsableInfo,
		Volatility: tree.VolatilityStable,
	}
}

// argTypeOpts is similar to tree.ArgTypes, but represents arguments that can
// accept multiple types.
type argTypeOpts []struct {
	Name string
	Typ  []*types.T
}

var strOrOidTypes = []*types.T{types.String, types.Oid}

// makePGPrivilegeInquiryDef constructs all variations of a specific PG access
// privilege inquiry function. Each variant has a different signature.
//
// The function takes a list of "object specification" arguments options. Each
// of these options can specify one or more valid types that it can accept. This
// is *not* the full list of arguments, but is only the list of arguments that
// differs between each privilege inquiry function. It also takes an info string
// that is used to construct the full function description.
func makePGPrivilegeInquiryDef(
	infoDetail string,
	objSpecArgs argTypeOpts,
	fn func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.HasAnyPrivilegeResult, error),
) builtinDefinition {
	// Collect the different argument type variations.
	//
	// 1. variants can begin with an optional "user" argument, which if used
	//    can be specified using a STRING or an OID. Postgres also allows the
	//    'public' pseudo-role to be used, but this is not supported here. If
	//    the argument omitted, the value of current_user is assumed.
	argTypes := []tree.ArgTypes{
		{}, // no user
	}
	for _, typ := range strOrOidTypes {
		argTypes = append(argTypes, tree.ArgTypes{{"user", typ}})
	}
	// 2. variants have one or more object identification arguments, which each
	//    accept multiple types.
	for _, objSpecArg := range objSpecArgs {
		prevArgTypes := argTypes
		argTypes = make([]tree.ArgTypes, 0, len(argTypes)*len(objSpecArg.Typ))
		for _, argType := range prevArgTypes {
			for _, typ := range objSpecArg.Typ {
				argTypeVariant := append(argType, tree.ArgTypes{{objSpecArg.Name, typ}}...)
				argTypes = append(argTypes, argTypeVariant)
			}
		}
	}
	// 3. variants all end with a "privilege" argument which can only
	//    be a string. See parsePrivilegeStr for details on how this
	//    argument is parsed and used.
	for i, argType := range argTypes {
		argTypes[i] = append(argType, tree.ArgTypes{{"privilege", types.String}}...)
	}

	var variants []tree.Overload
	for _, argType := range argTypes {
		withUser := argType[0].Name == "user"

		infoFmt := "Returns whether or not the current user has privileges for %s."
		if withUser {
			infoFmt = "Returns whether or not the user has privileges for %s."
		}

		variants = append(variants, tree.Overload{
			Types:      argType,
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var user security.SQLUsername
				if withUser {
					arg := tree.UnwrapDatum(ctx, args[0])
					userS, err := getNameForArg(ctx, arg, "pg_roles", "rolname")
					if err != nil {
						return nil, err
					}
					// Note: the username in pg_roles is already normalized, so
					// we can safely turn it into a SQLUsername without
					// re-normalization.
					user = security.MakeSQLUsernameFromPreNormalizedString(userS)
					if user.Undefined() {
						if _, ok := arg.(*tree.DOid); ok {
							// Postgres returns falseifn no matching user is
							// found when given an OID.
							return tree.DBoolFalse, nil
						}
						return nil, pgerror.Newf(pgcode.UndefinedObject,
							"role %s does not exist", arg)
					}

					// Remove the first argument.
					args = args[1:]
				} else {
					if ctx.SessionData().User().Undefined() {
						// Wut... is this possible?
						return tree.DNull, nil
					}
					user = ctx.SessionData().User()
				}
				ret, err := fn(ctx, args, user)
				if err != nil {
					return nil, err
				}
				switch ret {
				case tree.HasPrivilege:
					return tree.DBoolTrue, nil
				case tree.HasNoPrivilege:
					return tree.DBoolFalse, nil
				case tree.ObjectNotFound:
					return tree.DNull, nil
				default:
					panic(fmt.Sprintf("unrecognized HasAnyPrivilegeResult %d", ret))
				}
			},
			Info:       fmt.Sprintf(infoFmt, infoDetail),
			Volatility: tree.VolatilityStable,
		})
	}
	return builtinDefinition{
		props: tree.FunctionProperties{
			DistsqlBlocklist: true,
		},
		overloads: variants,
	}
}

// getNameForArg determines the object name for the specified argument, which
// should be either an unwrapped STRING or an OID. If the object is not found,
// the returned string will be empty.
func getNameForArg(ctx *tree.EvalContext, arg tree.Datum, pgTable, pgCol string) (string, error) {
	var query string
	switch t := arg.(type) {
	case *tree.DString:
		query = fmt.Sprintf("SELECT %s FROM pg_catalog.%s WHERE %s = $1 LIMIT 1", pgCol, pgTable, pgCol)
	case *tree.DOid:
		query = fmt.Sprintf("SELECT %s FROM pg_catalog.%s WHERE oid = $1 LIMIT 1", pgCol, pgTable)
	default:
		return "", errors.AssertionFailedf("unexpected arg type %T", t)
	}
	r, err := ctx.Planner.QueryRowEx(ctx.Ctx(), "get-name-for-arg",
		ctx.Txn, sessiondata.NoSessionDataOverride, query, arg)
	if err != nil || r == nil {
		return "", err
	}
	return string(tree.MustBeDString(r[0])), nil
}

// privMap maps a privilege string to a Privilege.
type privMap map[string]privilege.Privilege

func normalizePrivilegeStr(arg tree.Datum) []string {
	argStr := string(tree.MustBeDString(arg))
	privStrs := strings.Split(argStr, ",")
	res := make([]string, len(privStrs))
	for i, privStr := range privStrs {
		// Privileges are case-insensitive.
		privStr = strings.ToUpper(privStr)
		// Extra whitespace is allowed between but not within privilege names.
		privStr = strings.TrimSpace(privStr)
		res[i] = privStr
	}
	return res
}

// parsePrivilegeStr recognizes privilege strings for has_foo_privilege
// builtins, which are known as Access Privilege Inquiry Functions.
//
// The function accept a comma-separated list of case-insensitive privilege
// names, producing a list of privileges. It is liberal about whitespace between
// items, not so much about whitespace within items. The allowed privilege names
// and their corresponding privileges are given as a privMap.
func parsePrivilegeStr(arg tree.Datum, m privMap) ([]privilege.Privilege, error) {
	privStrs := normalizePrivilegeStr(arg)
	res := make([]privilege.Privilege, len(privStrs))
	for i, privStr := range privStrs {
		// Check the privilege map.
		p, ok := m[privStr]
		if !ok {
			return nil, pgerror.Newf(pgcode.InvalidParameterValue,
				"unrecognized privilege type: %q", privStr)
		}
		res[i] = p
	}
	return res, nil
}

func makeCreateRegDef(typ *types.T) builtinDefinition {
	return makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"oid", types.Int},
				{"name", types.String},
			},
			ReturnType: tree.FixedReturnType(typ),
			Fn: func(_ *tree.EvalContext, d tree.Datums) (tree.Datum, error) {
				return tree.NewDOidWithName(tree.MustBeDInt(d[0]), typ, string(tree.MustBeDString(d[1]))), nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityImmutable,
		},
	)
}

var pgBuiltins = map[string]builtinDefinition{
	// See https://www.postgresql.org/docs/9.6/static/functions-info.html.
	"pg_backend_pid": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				pid := ctx.QueryCancelKey.GetPGBackendPID()
				return tree.NewDInt(tree.DInt(pid)), nil
			},
			Info: "Returns a numerical ID attached to this session. This ID is " +
				"part of the query cancellation key used by the wire protocol. This " +
				"function was only added for compatibility, and unlike in Postgres, the " +
				"returned value does not correspond to a real process ID.",
			Volatility: tree.VolatilityStable,
		},
	),

	// See https://www.postgresql.org/docs/9.3/static/catalog-pg-database.html.
	"pg_encoding_to_char": makeBuiltin(defProps(),
		tree.Overload{
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
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
	),

	// Here getdatabaseencoding just returns UTF8 because,
	// CockroachDB supports just UTF8 for now.
	"getdatabaseencoding": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// We only support UTF-8 right now.
				// If we allow more encodings, we must also change the virtual schema
				// entry for pg_catalog.pg_database.
				return datEncodingUTF8ShortName, nil
			},
			Info:       "Returns the current encoding name used by the database.",
			Volatility: tree.VolatilityStable,
		},
	),

	// Postgres defines pg_get_expr as a function that "decompiles the internal form
	// of an expression", which is provided in the pg_node_tree type. In Cockroach's
	// pg_catalog implementation, we populate all pg_node_tree columns with the
	// corresponding expression as a string, which means that this function can simply
	// return the first argument directly. It also means we can ignore the second and
	// optional third argument.
	"pg_get_expr": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"pg_node_tree", types.String},
				{"relation_oid", types.Oid},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return args[0], nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"pg_node_tree", types.String},
				{"relation_oid", types.Oid},
				{"pretty_bool", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return args[0], nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
	),

	// pg_get_constraintdef functions like SHOW CREATE CONSTRAINT would if we
	// supported that statement.
	"pg_get_constraintdef": makeBuiltin(tree.FunctionProperties{DistsqlBlocklist: true},
		makePGGetConstraintDef(tree.ArgTypes{
			{"constraint_oid", types.Oid}, {"pretty_bool", types.Bool}}),
		makePGGetConstraintDef(tree.ArgTypes{{"constraint_oid", types.Oid}}),
	),

	// pg_get_partkeydef is only provided for compatibility and always returns
	// NULL. It is supposed to return the PARTITION BY clause of a table's
	// CREATE statement.
	"pg_get_partkeydef": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"oid", types.Oid}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.DNull, nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
	),

	// pg_get_function_result returns the types of the result of an builtin
	// function. Multi-return builtins currently are returned as anyelement, which
	// is a known incompatibility with Postgres.
	// https://www.postgresql.org/docs/11/functions-info.html
	"pg_get_function_result": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"func_oid", types.Oid}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				funcOid := tree.MustBeDOid(args[0])
				t, err := ctx.Planner.QueryRowEx(
					ctx.Ctx(), "pg_get_function_result",
					ctx.Txn,
					sessiondata.NoSessionDataOverride,
					`SELECT prorettype::REGTYPE::TEXT FROM pg_proc WHERE oid=$1`, int(funcOid.DInt))
				if err != nil {
					return nil, err
				}
				if len(t) == 0 {
					return tree.NewDString(""), nil
				}
				return t[0], nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
	),

	// pg_get_function_identity_arguments returns the argument list necessary to
	// identify a function, in the form it would need to appear in within ALTER
	// FUNCTION, for instance. This form omits default values.
	// https://www.postgresql.org/docs/11/functions-info.html
	"pg_get_function_identity_arguments": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"func_oid", types.Oid}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				funcOid := tree.MustBeDOid(args[0])
				t, err := ctx.Planner.QueryRowEx(
					ctx.Ctx(), "pg_get_function_identity_arguments",
					ctx.Txn,
					sessiondata.NoSessionDataOverride,
					`SELECT array_agg(unnest(proargtypes)::REGTYPE::TEXT) FROM pg_proc WHERE oid=$1`, int(funcOid.DInt))
				if err != nil {
					return nil, err
				}
				if len(t) == 0 || t[0] == tree.DNull {
					return tree.NewDString(""), nil
				}
				arr := tree.MustBeDArray(t[0])
				var sb strings.Builder
				for i, elem := range arr.Array {
					if i > 0 {
						sb.WriteString(", ")
					}
					if elem == tree.DNull {
						// This shouldn't ever happen, but let's be safe about it.
						sb.WriteString("NULL")
						continue
					}
					str, ok := tree.AsDString(elem)
					if !ok {
						// This also shouldn't happen.
						sb.WriteString(elem.String())
						continue
					}
					sb.WriteString(string(str))
				}
				return tree.NewDString(sb.String()), nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
	),

	// pg_get_indexdef functions like SHOW CREATE INDEX would if we supported that
	// statement.
	"pg_get_indexdef": makeBuiltin(tree.FunctionProperties{DistsqlBlocklist: true},
		makePGGetIndexDef(tree.ArgTypes{{"index_oid", types.Oid}}),
		makePGGetIndexDef(tree.ArgTypes{{"index_oid", types.Oid}, {"column_no", types.Int}, {"pretty_bool", types.Bool}}),
	),

	// pg_get_viewdef functions like SHOW CREATE VIEW but returns the same format as
	// PostgreSQL leaving out the actual 'CREATE VIEW table_name AS' portion of the statement.
	"pg_get_viewdef": makeBuiltin(tree.FunctionProperties{DistsqlBlocklist: true},
		makePGGetViewDef(tree.ArgTypes{{"view_oid", types.Oid}}),
		makePGGetViewDef(tree.ArgTypes{{"view_oid", types.Oid}, {"pretty_bool", types.Bool}}),
	),

	"pg_get_serial_sequence": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySequences,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"table_name", types.String}, {"column_name", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				tableName := tree.MustBeDString(args[0])
				columnName := tree.MustBeDString(args[1])
				qualifiedName, err := parser.ParseQualifiedTableName(string(tableName))
				if err != nil {
					return nil, err
				}
				res, err := ctx.Sequence.GetSerialSequenceNameFromColumn(ctx.Ctx(), qualifiedName, tree.Name(columnName))
				if err != nil {
					return nil, err
				}
				if res == nil {
					return tree.DNull, nil
				}
				res.ExplicitCatalog = false
				return tree.NewDString(fmt.Sprintf(`%s.%s`, res.Schema(), res.Object())), nil
			},
			Info:       "Returns the name of the sequence used by the given column_name in the table table_name.",
			Volatility: tree.VolatilityStable,
		},
	),

	// pg_my_temp_schema returns the OID of session's temporary schema, or 0 if
	// none.
	// https://www.postgresql.org/docs/11/functions-info.html
	"pg_my_temp_schema": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Oid),
			Fn: func(ctx *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				schema := ctx.SessionData().SearchPath.GetTemporarySchemaName()
				if schema == "" {
					// The session has not yet created a temporary schema.
					return tree.NewDOid(0), nil
				}
				oid, errSafeToIgnore, err := ctx.Planner.ResolveOIDFromString(
					ctx.Ctx(), types.RegNamespace, tree.NewDString(schema))
				if err != nil {
					// If the OID lookup returns an UndefinedObject error, return 0
					// instead. We can hit this path if the session created a temporary
					// schema in one database and then changed databases.
					if errSafeToIgnore && pgerror.GetPGCode(err) == pgcode.UndefinedObject {
						return tree.NewDOid(0), nil
					}
					return nil, err
				}
				return oid, nil
			},
			Info: "Returns the OID of the current session's temporary schema, " +
				"or zero if it has none (because it has not created any temporary tables).",
			Volatility: tree.VolatilityStable,
		},
	),

	// pg_is_other_temp_schema returns true if the given OID is the OID of another
	// session's temporary schema.
	// https://www.postgresql.org/docs/11/functions-info.html
	"pg_is_other_temp_schema": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"oid", types.Oid}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				schemaArg := tree.UnwrapDatum(ctx, args[0])
				schema, err := getNameForArg(ctx, schemaArg, "pg_namespace", "nspname")
				if err != nil {
					return nil, err
				}
				if schema == "" {
					// OID does not exist.
					return tree.DBoolFalse, nil
				}
				if !strings.HasPrefix(schema, catconstants.PgTempSchemaName) {
					// OID is not a reference to a temporary schema.
					//
					// This string matching is what Postgres does too. See isAnyTempNamespace.
					return tree.DBoolFalse, nil
				}
				if schema == ctx.SessionData().SearchPath.GetTemporarySchemaName() {
					// OID is a reference to this session's temporary schema.
					return tree.DBoolFalse, nil
				}
				return tree.DBoolTrue, nil
			},
			Info:       "Returns true if the given OID is the OID of another session's temporary schema. (This can be useful, for example, to exclude other sessions' temporary tables from a catalog display.)",
			Volatility: tree.VolatilityStable,
		},
	),

	// TODO(bram): Make sure the reported type is correct for tuples. See #25523.
	"pg_typeof": makeBuiltin(tree.FunctionProperties{NullableArgs: true},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.Any}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(args[0].ResolvedType().SQLStandardName()), nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
	),

	// https://www.postgresql.org/docs/10/functions-info.html#FUNCTIONS-INFO-CATALOG-TABLE
	"pg_collation_for": makeBuiltin(
		tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{"str", types.Any}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var collation string
				switch t := args[0].(type) {
				case *tree.DString:
					collation = "default"
				case *tree.DCollatedString:
					collation = t.Locale
				default:
					return tree.DNull, pgerror.Newf(pgcode.DatatypeMismatch,
						"collations are not supported by type: %s", t.ResolvedType())
				}
				return tree.NewDString(fmt.Sprintf(`"%s"`, collation)), nil
			},
			Info:       "Returns the collation of the argument",
			Volatility: tree.VolatilityStable,
		},
	),

	"pg_get_userbyid": makeBuiltin(tree.FunctionProperties{DistsqlBlocklist: true},
		tree.Overload{
			Types: tree.ArgTypes{
				{"role_oid", types.Oid},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				oid := args[0]
				t, err := ctx.Planner.QueryRowEx(
					ctx.Ctx(), "pg_get_userbyid",
					ctx.Txn,
					sessiondata.NoSessionDataOverride,
					"SELECT rolname FROM pg_catalog.pg_roles WHERE oid=$1", oid)
				if err != nil {
					return nil, err
				}
				if len(t) == 0 {
					return tree.NewDString(fmt.Sprintf("unknown (OID=%s)", args[0])), nil
				}
				return t[0], nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
	),

	"pg_sequence_parameters": makeBuiltin(tree.FunctionProperties{DistsqlBlocklist: true},
		// pg_sequence_parameters is an undocumented Postgres builtin that returns
		// information about a sequence given its OID. It's nevertheless used by
		// at least one UI tool, so we provide an implementation for compatibility.
		// The real implementation returns a record; we fake it by returning a
		// comma-delimited string enclosed by parentheses.
		// TODO(jordan): convert this to return a record type once we support that.
		tree.Overload{
			Types:      tree.ArgTypes{{"sequence_oid", types.Oid}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				r, err := ctx.Planner.QueryRowEx(
					ctx.Ctx(), "pg_sequence_parameters",
					ctx.Txn,
					sessiondata.NoSessionDataOverride,
					`SELECT seqstart, seqmin, seqmax, seqincrement, seqcycle, seqcache, seqtypid `+
						`FROM pg_catalog.pg_sequence WHERE seqrelid=$1`, args[0])
				if err != nil {
					return nil, err
				}
				if len(r) == 0 {
					return nil, pgerror.Newf(pgcode.UndefinedTable, "unknown sequence (OID=%s)", args[0])
				}
				seqstart, seqmin, seqmax, seqincrement, seqcycle, seqcache, seqtypid := r[0], r[1], r[2], r[3], r[4], r[5], r[6]
				seqcycleStr := "t"
				if seqcycle.(*tree.DBool) == tree.DBoolFalse {
					seqcycleStr = "f"
				}
				return tree.NewDString(fmt.Sprintf("(%s,%s,%s,%s,%s,%s,%s)", seqstart, seqmin, seqmax, seqincrement, seqcycleStr, seqcache, seqtypid)), nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
	),

	"format_type": makeBuiltin(tree.FunctionProperties{NullableArgs: true, DistsqlBlocklist: true},
		tree.Overload{
			Types:      tree.ArgTypes{{"type_oid", types.Oid}, {"typemod", types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// See format_type.c in Postgres.
				oidArg := args[0]
				if oidArg == tree.DNull {
					return tree.DNull, nil
				}
				maybeTypmod := args[1]
				oid := oid.Oid(oidArg.(*tree.DOid).DInt)
				typ, ok := types.OidToType[oid]
				if !ok {
					// If the type wasn't statically known, try looking it up as a user
					// defined type.
					var err error
					typ, err = ctx.Planner.ResolveTypeByOID(ctx.Context, oid)
					if err != nil {
						// If the error is a descriptor does not exist error, then swallow it.
						unknown := tree.NewDString(fmt.Sprintf("unknown (OID=%s)", oidArg))
						switch {
						case errors.Is(err, catalog.ErrDescriptorNotFound):
							return unknown, nil
						case pgerror.GetPGCode(err) == pgcode.UndefinedObject:
							return unknown, nil
						default:
							return nil, err
						}
					}
				}
				var hasTypmod bool
				var typmod int
				if maybeTypmod != tree.DNull {
					hasTypmod = true
					typmod = int(tree.MustBeDInt(maybeTypmod))
				}
				return tree.NewDString(typ.SQLStandardNameWithTypmod(hasTypmod, typmod)), nil
			},
			Info: "Returns the SQL name of a data type that is " +
				"identified by its type OID and possibly a type modifier. " +
				"Currently, the type modifier is ignored.",
			Volatility: tree.VolatilityStable,
		},
	),

	"col_description": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"table_oid", types.Oid}, {"column_number", types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if *args[1].(*tree.DInt) == 0 {
					// column ID 0 never exists, and we don't want the query
					// below to pick up the table comment by accident.
					return tree.DNull, nil
				}
				// Note: the following is equivalent to:
				//
				// SELECT description FROM pg_catalog.pg_description
				//  WHERE objoid=$1 AND objsubid=$2 LIMIT 1
				//
				// TODO(jordanlewis): Really we'd like to query this directly
				// on pg_description and let predicate push-down do its job.
				r, err := ctx.Planner.QueryRowEx(
					ctx.Ctx(), "pg_get_coldesc",
					ctx.Txn,
					sessiondata.NoSessionDataOverride,
					`
SELECT COALESCE(c.comment, pc.comment) FROM system.comments c
FULL OUTER JOIN crdb_internal.predefined_comments pc
ON pc.object_id=c.object_id AND pc.sub_id=c.sub_id AND pc.type = c.type
WHERE c.type=$1::int AND c.object_id=$2::int AND c.sub_id=$3::int LIMIT 1
`, keys.ColumnCommentType, args[0], args[1])
				if err != nil {
					return nil, err
				}
				if len(r) == 0 {
					return tree.DNull, nil
				}
				return r[0], nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
	),

	"obj_description": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"object_oid", types.Oid}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return getPgObjDesc(ctx, "", int(args[0].(*tree.DOid).DInt))
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"object_oid", types.Oid}, {"catalog_name", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return getPgObjDesc(ctx,
					string(tree.MustBeDString(args[1])),
					int(args[0].(*tree.DOid).DInt),
				)
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
	),

	"oid": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"int", types.Int}},
			ReturnType: tree.FixedReturnType(types.Oid),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDOid(*args[0].(*tree.DInt)), nil
			},
			Info:       "Converts an integer to an OID.",
			Volatility: tree.VolatilityImmutable,
		},
	),

	"shobj_description": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"object_oid", types.Oid}, {"catalog_name", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				catalogName := string(tree.MustBeDString(args[1]))
				objOid := int(args[0].(*tree.DOid).DInt)

				classOid, ok := getCatalogOidForComments(catalogName)
				if !ok {
					// No such catalog - return null, matching pg.
					return tree.DNull, nil
				}

				r, err := ctx.Planner.QueryRowEx(
					ctx.Ctx(), "pg_get_shobjdesc", ctx.Txn,
					sessiondata.NoSessionDataOverride,
					fmt.Sprintf(`
SELECT description
  FROM pg_catalog.pg_shdescription
 WHERE objoid = %[1]d
   AND classoid = %[2]d
 LIMIT 1`,
						objOid,
						classOid,
					))
				if err != nil {
					return nil, err
				}
				if len(r) == 0 {
					return tree.DNull, nil
				}
				return r[0], nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
	),

	"pg_try_advisory_lock": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"int", types.Int}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.DBoolTrue, nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityVolatile,
		},
	),

	"pg_advisory_unlock": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"int", types.Int}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.DBoolTrue, nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityVolatile,
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-string.html
	// CockroachDB supports just UTF8 for now.
	"pg_client_encoding": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.NewDString("UTF8"), nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
	),

	// pg_function_is_visible returns true if the input oid corresponds to a
	// builtin function that is part of the databases on the search path.
	// CockroachDB doesn't have a concept of namespaced functions, so this is
	// always true if the builtin exists at all, and NULL otherwise.
	// https://www.postgresql.org/docs/9.6/static/functions-info.html
	"pg_function_is_visible": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"oid", types.Oid}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				oid := tree.MustBeDOid(args[0])
				t, err := ctx.Planner.QueryRowEx(
					ctx.Ctx(), "pg_function_is_visible",
					ctx.Txn,
					sessiondata.NoSessionDataOverride,
					"SELECT * from pg_proc WHERE oid=$1 LIMIT 1", int(oid.DInt))
				if err != nil {
					return nil, err
				}
				if t != nil {
					return tree.DBoolTrue, nil
				}
				return tree.DNull, nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
	),
	// pg_table_is_visible returns true if the input oid corresponds to a table
	// that is part of the schemas on the search path.
	// https://www.postgresql.org/docs/9.6/static/functions-info.html
	"pg_table_is_visible": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"oid", types.Oid}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				oidArg := tree.MustBeDOid(args[0])
				isVisible, exists, err := ctx.Planner.IsTableVisible(
					ctx.Context, ctx.SessionData().Database, ctx.SessionData().SearchPath, oid.Oid(oidArg.DInt),
				)
				if err != nil {
					return nil, err
				}
				if !exists {
					return tree.DNull, nil
				}
				return tree.MakeDBool(tree.DBool(isVisible)), nil
			},
			Info:       "Returns whether the table with the given OID belongs to one of the schemas on the search path.",
			Volatility: tree.VolatilityStable,
		},
	),

	// pg_type_is_visible returns true if the input oid corresponds to a type
	// that is part of the databases on the search path, or NULL if no such type
	// exists. CockroachDB doesn't support the notion of type visibility, so we
	// always return true for any type oid that we support, and NULL for those
	// that we don't.
	// https://www.postgresql.org/docs/9.6/static/functions-info.html
	"pg_type_is_visible": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"oid", types.Oid}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				oidArg := tree.MustBeDOid(args[0])
				isVisible, exists, err := ctx.Planner.IsTypeVisible(
					ctx.Context, ctx.SessionData().Database, ctx.SessionData().SearchPath, oid.Oid(oidArg.DInt),
				)
				if err != nil {
					return nil, err
				}
				if !exists {
					return tree.DNull, nil
				}
				return tree.MakeDBool(tree.DBool(isVisible)), nil
			},
			Info:       "Returns whether the type with the given OID belongs to one of the schemas on the search path.",
			Volatility: tree.VolatilityStable,
		},
	),

	"pg_relation_is_updatable": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"reloid", types.Oid}, {"include_triggers", types.Bool}},
			ReturnType: tree.FixedReturnType(types.Int4),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				oidArg := tree.MustBeDOid(args[0])
				oid := int(oidArg.DInt)
				tableDescI, err := ctx.Planner.GetImmutableTableInterfaceByID(ctx.Ctx(), oid)
				if err != nil {
					// For postgres compatibility, it is expected that rather returning
					// an error this return nonUpdatableEvents (Zero) because there could
					// be oid references on deleted tables.
					if sqlerrors.IsUndefinedRelationError(err) {
						return nonUpdatableEvents, nil
					}
					return nonUpdatableEvents, err
				}
				tableDesc := tableDescI.(catalog.TableDescriptor)
				if !tableDesc.IsTable() || tableDesc.IsVirtualTable() {
					return nonUpdatableEvents, nil
				}

				// pg_relation_is_updatable was created for compatibility. This
				// should return the update events the relation supports, but as crdb
				// does not support updatable views or foreign tables, right now this
				// basically return allEvents or none.
				return allUpdatableEvents, nil
			},
			Info:       `Returns the update events the relation supports.`,
			Volatility: tree.VolatilityStable,
		},
	),

	"pg_column_is_updatable": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"reloid", types.Oid},
				{"attnum", types.Int2},
				{"include_triggers", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				oidArg := tree.MustBeDOid(args[0])
				attNumArg := tree.MustBeDInt(args[1])
				oid := int(oidArg.DInt)
				attNum := uint32(attNumArg)
				if attNumArg < 0 {
					// System columns are not updatable.
					return tree.DBoolFalse, nil
				}
				tableDescI, err := ctx.Planner.GetImmutableTableInterfaceByID(ctx.Ctx(), oid)
				if err != nil {
					if sqlerrors.IsUndefinedRelationError(err) {
						// For postgres compatibility, it is expected that rather returning
						// an error this return nonUpdatableEvents (Zero) because there could
						// be oid references on deleted tables.
						return tree.DBoolFalse, nil
					}
					return tree.DBoolFalse, err
				}
				tableDesc := tableDescI.(catalog.TableDescriptor)
				if !tableDesc.IsTable() || tableDesc.IsVirtualTable() {
					return tree.DBoolFalse, nil
				}

				column, err := tableDesc.FindColumnWithID(descpb.ColumnID(attNum))
				if err != nil {
					if sqlerrors.IsUndefinedColumnError(err) {
						// When column does not exist postgres returns true.
						return tree.DBoolTrue, nil
					}
					return tree.DBoolFalse, err
				}

				// pg_column_is_updatable was created for compatibility. This
				// will return true if is a table (not virtual) and column is not
				// a computed column.
				return tree.MakeDBool(tree.DBool(!column.IsComputed())), nil
			},
			Info:       `Returns whether the given column can be updated.`,
			Volatility: tree.VolatilityStable,
		},
	),

	"pg_sleep": makeBuiltin(
		tree.FunctionProperties{},
		tree.Overload{
			Types:      tree.ArgTypes{{"seconds", types.Float}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				durationNanos := int64(float64(*args[0].(*tree.DFloat)) * float64(1000000000))
				dur := time.Duration(durationNanos)
				select {
				case <-ctx.Ctx().Done():
					return nil, ctx.Ctx().Err()
				case <-time.After(dur):
					return tree.DBoolTrue, nil
				}
			},
			Info: "pg_sleep makes the current session's process sleep until " +
				"seconds seconds have elapsed. seconds is a value of type " +
				"double precision, so fractional-second delays can be specified.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	// pg_is_in_recovery returns true if the Postgres database is currently in
	// recovery.  This is not applicable so this can always return false.
	// https://www.postgresql.org/docs/current/static/functions-admin.html#FUNCTIONS-RECOVERY-INFO-TABLE
	"pg_is_in_recovery": makeNotUsableFalseBuiltin(),

	// pg_is_xlog_replay_paused returns true if the Postgres database is currently
	// in recovery but that recovery is paused.  This is not applicable so this
	// can always return false.
	// https://www.postgresql.org/docs/9.6/static/functions-admin.html#FUNCTIONS-RECOVERY-CONTROL-TABLE
	// Note that this function was removed from Postgres in version 10.
	"pg_is_xlog_replay_paused": makeNotUsableFalseBuiltin(),

	// Access Privilege Inquiry Functions allow users to query object access
	// privileges programmatically. Each function has a number of variants,
	// which differ based on their function signatures. These signatures have
	// the following structure:
	// - optional "user" argument
	//   - if used, can be a STRING or an OID type
	//   - if not used, current_user is assumed
	// - series of one or more object specifier arguments
	//   - each can accept multiple types
	// - a "privilege" argument
	//   - must be a STRING
	//   - parsed as a comma-separated list of privilege
	//
	// See https://www.postgresql.org/docs/9.6/static/functions-info.html#FUNCTIONS-INFO-ACCESS-TABLE.
	"has_any_column_privilege": makePGPrivilegeInquiryDef(
		"any column of table",
		argTypeOpts{{"table", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.HasAnyPrivilegeResult, error) {
			tableArg := tree.UnwrapDatum(ctx, args[0])
			specifier, err := tableHasPrivilegeSpecifier(tableArg, false /* isSequence */)
			if err != nil {
				return tree.HasNoPrivilege, err
			}

			privs, err := parsePrivilegeStr(args[1], privMap{
				"SELECT":                       {Kind: privilege.SELECT},
				"SELECT WITH GRANT OPTION":     {Kind: privilege.SELECT, GrantOption: true},
				"INSERT":                       {Kind: privilege.INSERT},
				"INSERT WITH GRANT OPTION":     {Kind: privilege.INSERT, GrantOption: true},
				"UPDATE":                       {Kind: privilege.UPDATE},
				"UPDATE WITH GRANT OPTION":     {Kind: privilege.UPDATE, GrantOption: true},
				"REFERENCES":                   {Kind: privilege.SELECT},
				"REFERENCES WITH GRANT OPTION": {Kind: privilege.SELECT, GrantOption: true},
			})
			if err != nil {
				return tree.HasNoPrivilege, err
			}
			return ctx.Planner.HasAnyPrivilege(ctx.Context, specifier, user, privs)
		},
	),

	"has_column_privilege": makePGPrivilegeInquiryDef(
		"column",
		argTypeOpts{{"table", strOrOidTypes}, {"column", []*types.T{types.String, types.Int}}},
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.HasAnyPrivilegeResult, error) {
			tableArg := tree.UnwrapDatum(ctx, args[0])
			colArg := tree.UnwrapDatum(ctx, args[1])
			specifier, err := columnHasPrivilegeSpecifier(tableArg, colArg)
			if err != nil {
				return tree.HasNoPrivilege, err
			}

			privs, err := parsePrivilegeStr(args[2], privMap{
				"SELECT":                       {Kind: privilege.SELECT},
				"SELECT WITH GRANT OPTION":     {Kind: privilege.SELECT, GrantOption: true},
				"INSERT":                       {Kind: privilege.INSERT},
				"INSERT WITH GRANT OPTION":     {Kind: privilege.INSERT, GrantOption: true},
				"UPDATE":                       {Kind: privilege.UPDATE},
				"UPDATE WITH GRANT OPTION":     {Kind: privilege.UPDATE, GrantOption: true},
				"REFERENCES":                   {Kind: privilege.SELECT},
				"REFERENCES WITH GRANT OPTION": {Kind: privilege.SELECT, GrantOption: true},
			})
			if err != nil {
				return tree.HasNoPrivilege, err
			}
			return ctx.Planner.HasAnyPrivilege(ctx.Context, specifier, user, privs)
		},
	),

	"has_database_privilege": makePGPrivilegeInquiryDef(
		"database",
		argTypeOpts{{"database", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.HasAnyPrivilegeResult, error) {

			databaseArg := tree.UnwrapDatum(ctx, args[0])
			specifier, err := databaseHasPrivilegeSpecifier(databaseArg)
			if err != nil {
				return tree.HasNoPrivilege, err
			}

			privs, err := parsePrivilegeStr(args[1], privMap{
				"CREATE":                      {Kind: privilege.CREATE},
				"CREATE WITH GRANT OPTION":    {Kind: privilege.CREATE, GrantOption: true},
				"CONNECT":                     {Kind: privilege.CONNECT},
				"CONNECT WITH GRANT OPTION":   {Kind: privilege.CONNECT, GrantOption: true},
				"TEMPORARY":                   {Kind: privilege.CREATE},
				"TEMPORARY WITH GRANT OPTION": {Kind: privilege.CREATE, GrantOption: true},
				"TEMP":                        {Kind: privilege.CREATE},
				"TEMP WITH GRANT OPTION":      {Kind: privilege.CREATE, GrantOption: true},
			})
			if err != nil {
				return tree.HasNoPrivilege, err
			}

			return ctx.Planner.HasAnyPrivilege(ctx.Context, specifier, user, privs)
		},
	),

	"has_foreign_data_wrapper_privilege": makePGPrivilegeInquiryDef(
		"foreign-data wrapper",
		argTypeOpts{{"fdw", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.HasAnyPrivilegeResult, error) {
			fdwArg := tree.UnwrapDatum(ctx, args[0])
			fdw, err := getNameForArg(ctx, fdwArg, "pg_foreign_data_wrapper", "fdwname")
			if err != nil {
				return tree.HasNoPrivilege, err
			}
			retNull := false
			if fdw == "" {
				switch fdwArg.(type) {
				case *tree.DString:
					return tree.HasNoPrivilege, pgerror.Newf(pgcode.UndefinedObject,
						"foreign-data wrapper %s does not exist", fdwArg)
				case *tree.DOid:
					// Postgres returns NULL if no matching foreign data wrapper is found
					// when given an OID.
					retNull = true
				}
			}

			privs, err := parsePrivilegeStr(args[1], privMap{
				"USAGE":                   {Kind: privilege.USAGE},
				"USAGE WITH GRANT OPTION": {Kind: privilege.USAGE, GrantOption: true},
			})
			if err != nil {
				return tree.HasNoPrivilege, err
			}
			if retNull {
				return tree.ObjectNotFound, nil
			}
			// All users have USAGE privileges for all foreign-data wrappers.
			_ = privs
			return tree.HasPrivilege, nil
		},
	),

	"has_function_privilege": makePGPrivilegeInquiryDef(
		"function",
		argTypeOpts{{"function", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.HasAnyPrivilegeResult, error) {
			oidArg := tree.UnwrapDatum(ctx, args[0])
			// When specifying a function by a text string rather than by OID,
			// the allowed input is the same as for the regprocedure data type.
			var oid tree.Datum
			switch t := oidArg.(type) {
			case *tree.DString:
				var err error
				oid, err = tree.ParseDOid(ctx, string(*t), types.RegProcedure)
				if err != nil {
					return tree.HasNoPrivilege, err
				}
			case *tree.DOid:
				oid = t
			}

			fn, err := getNameForArg(ctx, oid, "pg_proc", "proname")
			if err != nil {
				return tree.HasNoPrivilege, err
			}
			retNull := false
			if fn == "" {
				// Postgres returns NULL if no matching function is found
				// when given an OID.
				retNull = true
			}

			privs, err := parsePrivilegeStr(args[1], privMap{
				// TODO(nvanbenschoten): this privilege is incorrect, but we don't
				// currently have an EXECUTE privilege and we aren't even checking
				// this down below, so it's fine for now.
				"EXECUTE":                   {Kind: privilege.USAGE},
				"EXECUTE WITH GRANT OPTION": {Kind: privilege.USAGE, GrantOption: true},
			})
			if err != nil {
				return tree.HasNoPrivilege, err
			}
			if retNull {
				return tree.ObjectNotFound, nil
			}
			// All users have EXECUTE privileges for all functions.
			_ = privs
			return tree.HasPrivilege, nil
		},
	),

	"has_language_privilege": makePGPrivilegeInquiryDef(
		"language",
		argTypeOpts{{"language", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.HasAnyPrivilegeResult, error) {
			langArg := tree.UnwrapDatum(ctx, args[0])
			lang, err := getNameForArg(ctx, langArg, "pg_language", "lanname")
			if err != nil {
				return tree.HasNoPrivilege, err
			}
			retNull := false
			if lang == "" {
				switch langArg.(type) {
				case *tree.DString:
					return tree.HasNoPrivilege, pgerror.Newf(pgcode.UndefinedObject,
						"language %s does not exist", langArg)
				case *tree.DOid:
					// Postgres returns NULL if no matching language is found
					// when given an OID.
					retNull = true
				}
			}

			privs, err := parsePrivilegeStr(args[1], privMap{
				"USAGE":                   {Kind: privilege.USAGE},
				"USAGE WITH GRANT OPTION": {Kind: privilege.USAGE, GrantOption: true},
			})
			if err != nil {
				return tree.HasNoPrivilege, err
			}
			if retNull {
				return tree.ObjectNotFound, nil
			}
			// All users have USAGE privileges for all languages.
			_ = privs
			return tree.HasPrivilege, nil
		},
	),

	"has_schema_privilege": makePGPrivilegeInquiryDef(
		"schema",
		argTypeOpts{{"schema", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.HasAnyPrivilegeResult, error) {
			schemaArg := tree.UnwrapDatum(ctx, args[0])
			databaseName := ctx.SessionData().Database
			specifier, err := schemaHasPrivilegeSpecifier(ctx, schemaArg, databaseName)
			if err != nil {
				return tree.HasNoPrivilege, err
			}

			privs, err := parsePrivilegeStr(args[1], privMap{
				"CREATE":                   {Kind: privilege.CREATE},
				"CREATE WITH GRANT OPTION": {Kind: privilege.CREATE, GrantOption: true},
				"USAGE":                    {Kind: privilege.USAGE},
				"USAGE WITH GRANT OPTION":  {Kind: privilege.USAGE, GrantOption: true},
			})
			if err != nil {
				return tree.HasNoPrivilege, err
			}
			if len(databaseName) == 0 {
				// If no database is set, return NULL.
				return tree.ObjectNotFound, nil
			}

			return ctx.Planner.HasAnyPrivilege(ctx.Context, specifier, user, privs)
		},
	),

	"has_sequence_privilege": makePGPrivilegeInquiryDef(
		"sequence",
		argTypeOpts{{"sequence", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.HasAnyPrivilegeResult, error) {
			seqArg := tree.UnwrapDatum(ctx, args[0])
			specifier, err := tableHasPrivilegeSpecifier(seqArg, true /* isSequence */)
			if err != nil {
				return tree.HasNoPrivilege, err
			}
			privs, err := parsePrivilegeStr(args[1], privMap{
				// Sequences and other table objects cannot be given a USAGE privilege,
				// so we check for SELECT here instead. See privilege.TablePrivileges.
				"USAGE":                    {Kind: privilege.SELECT},
				"USAGE WITH GRANT OPTION":  {Kind: privilege.SELECT, GrantOption: true},
				"SELECT":                   {Kind: privilege.SELECT},
				"SELECT WITH GRANT OPTION": {Kind: privilege.SELECT, GrantOption: true},
				"UPDATE":                   {Kind: privilege.UPDATE},
				"UPDATE WITH GRANT OPTION": {Kind: privilege.UPDATE, GrantOption: true},
			})
			if err != nil {
				return tree.HasPrivilege, err
			}
			return ctx.Planner.HasAnyPrivilege(ctx.Context, specifier, user, privs)
		},
	),

	"has_server_privilege": makePGPrivilegeInquiryDef(
		"foreign server",
		argTypeOpts{{"server", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.HasAnyPrivilegeResult, error) {
			serverArg := tree.UnwrapDatum(ctx, args[0])
			server, err := getNameForArg(ctx, serverArg, "pg_foreign_server", "srvname")
			if err != nil {
				return tree.HasNoPrivilege, err
			}
			retNull := false
			if server == "" {
				switch serverArg.(type) {
				case *tree.DString:
					return tree.HasNoPrivilege, pgerror.Newf(pgcode.UndefinedObject,
						"server %s does not exist", serverArg)
				case *tree.DOid:
					// Postgres returns NULL if no matching foreign server is found when
					// given an OID.
					retNull = true
				}
			}

			privs, err := parsePrivilegeStr(args[1], privMap{
				"USAGE":                   {Kind: privilege.USAGE},
				"USAGE WITH GRANT OPTION": {Kind: privilege.USAGE, GrantOption: true},
			})
			if err != nil {
				return tree.HasNoPrivilege, err
			}
			if retNull {
				return tree.ObjectNotFound, nil
			}
			// All users have USAGE privileges for all foreign servers.
			_ = privs
			return tree.HasPrivilege, nil
		},
	),

	"has_table_privilege": makePGPrivilegeInquiryDef(
		"table",
		argTypeOpts{{"table", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.HasAnyPrivilegeResult, error) {
			tableArg := tree.UnwrapDatum(ctx, args[0])
			specifier, err := tableHasPrivilegeSpecifier(tableArg, false /* isSequence */)
			if err != nil {
				return tree.HasNoPrivilege, err
			}

			privs, err := parsePrivilegeStr(args[1], privMap{
				"SELECT":                       {Kind: privilege.SELECT},
				"SELECT WITH GRANT OPTION":     {Kind: privilege.SELECT, GrantOption: true},
				"INSERT":                       {Kind: privilege.INSERT},
				"INSERT WITH GRANT OPTION":     {Kind: privilege.INSERT, GrantOption: true},
				"UPDATE":                       {Kind: privilege.UPDATE},
				"UPDATE WITH GRANT OPTION":     {Kind: privilege.UPDATE, GrantOption: true},
				"DELETE":                       {Kind: privilege.DELETE},
				"DELETE WITH GRANT OPTION":     {Kind: privilege.DELETE, GrantOption: true},
				"TRUNCATE":                     {Kind: privilege.DELETE},
				"TRUNCATE WITH GRANT OPTION":   {Kind: privilege.DELETE, GrantOption: true},
				"REFERENCES":                   {Kind: privilege.SELECT},
				"REFERENCES WITH GRANT OPTION": {Kind: privilege.SELECT, GrantOption: true},
				"TRIGGER":                      {Kind: privilege.CREATE},
				"TRIGGER WITH GRANT OPTION":    {Kind: privilege.CREATE, GrantOption: true},
				"RULE":                         {Kind: privilege.RULE},
				"RULE WITH GRANT OPTION":       {Kind: privilege.RULE, GrantOption: true},
			})
			if err != nil {
				return tree.HasNoPrivilege, err
			}
			return ctx.Planner.HasAnyPrivilege(ctx.Context, specifier, user, privs)
		},
	),

	"has_tablespace_privilege": makePGPrivilegeInquiryDef(
		"tablespace",
		argTypeOpts{{"tablespace", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.HasAnyPrivilegeResult, error) {
			tablespaceArg := tree.UnwrapDatum(ctx, args[0])
			tablespace, err := getNameForArg(ctx, tablespaceArg, "pg_tablespace", "spcname")
			if err != nil {
				return tree.HasNoPrivilege, err
			}
			retNull := false
			if tablespace == "" {
				switch tablespaceArg.(type) {
				case *tree.DString:
					return tree.HasNoPrivilege, pgerror.Newf(pgcode.UndefinedObject,
						"tablespace %s does not exist", tablespaceArg)
				case *tree.DOid:
					// Postgres returns NULL if no matching tablespace is found when given
					// an OID.
					retNull = true
				}
			}

			privs, err := parsePrivilegeStr(args[1], privMap{
				"CREATE":                   {Kind: privilege.CREATE},
				"CREATE WITH GRANT OPTION": {Kind: privilege.CREATE, GrantOption: true},
			})
			if err != nil {
				return tree.HasNoPrivilege, err
			}
			if retNull {
				return tree.ObjectNotFound, nil
			}
			// All users have CREATE privileges in all tablespaces.
			_ = privs
			return tree.HasPrivilege, nil
		},
	),

	"has_type_privilege": makePGPrivilegeInquiryDef(
		"type",
		argTypeOpts{{"type", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.HasAnyPrivilegeResult, error) {
			oidArg := tree.UnwrapDatum(ctx, args[0])
			// When specifying a type by a text string rather than by OID, the
			// allowed input is the same as for the regtype data type.
			var oid tree.Datum
			switch t := oidArg.(type) {
			case *tree.DString:
				var err error
				oid, err = tree.ParseDOid(ctx, string(*t), types.RegType)
				if err != nil {
					return tree.HasNoPrivilege, err
				}
			case *tree.DOid:
				oid = t
			}

			typ, err := getNameForArg(ctx, oid, "pg_type", "typname")
			if err != nil {
				return tree.HasNoPrivilege, err
			}
			retNull := false
			if typ == "" {
				// Postgres returns NULL if no matching type is found
				// when given an OID.
				retNull = true
			}

			privs, err := parsePrivilegeStr(args[1], privMap{
				"USAGE":                   {Kind: privilege.USAGE},
				"USAGE WITH GRANT OPTION": {Kind: privilege.USAGE, GrantOption: true},
			})
			if err != nil {
				return tree.HasNoPrivilege, err
			}
			if retNull {
				return tree.ObjectNotFound, nil
			}
			// All users have USAGE privileges to all types.
			_ = privs
			return tree.HasPrivilege, nil
		},
	),

	"pg_has_role": makePGPrivilegeInquiryDef(
		"role",
		argTypeOpts{{"role", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.HasAnyPrivilegeResult, error) {
			roleArg := tree.UnwrapDatum(ctx, args[0])
			roleS, err := getNameForArg(ctx, roleArg, "pg_roles", "rolname")
			if err != nil {
				return tree.HasNoPrivilege, err
			}
			// Note: the username in pg_roles is already normalized, so we can safely
			// turn it into a SQLUsername without re-normalization.
			role := security.MakeSQLUsernameFromPreNormalizedString(roleS)
			if role.Undefined() {
				switch roleArg.(type) {
				case *tree.DString:
					return tree.HasNoPrivilege, pgerror.Newf(pgcode.UndefinedObject,
						"role %s does not exist", roleArg)
				case *tree.DOid:
					// Postgres returns NULL if no matching role is found when given an
					// OID.
					return tree.ObjectNotFound, nil
				}
			}

			privStrs := normalizePrivilegeStr(args[1])
			for _, privStr := range privStrs {
				var hasAnyPrivilegeResult tree.HasAnyPrivilegeResult
				var err error
				switch privStr {
				case "USAGE":
					hasAnyPrivilegeResult, err = hasPrivsOfRole(ctx, user, role)
				case "MEMBER":
					hasAnyPrivilegeResult, err = isMemberOfRole(ctx, user, role)
				case
					"USAGE WITH GRANT OPTION",
					"USAGE WITH ADMIN OPTION",
					"MEMBER WITH GRANT OPTION",
					"MEMBER WITH ADMIN OPTION":
					hasAnyPrivilegeResult, err = isAdminOfRole(ctx, user, role)
				default:
					return tree.HasNoPrivilege, pgerror.Newf(pgcode.InvalidParameterValue,
						"unrecognized privilege type: %q", privStr)
				}
				if err != nil {
					return tree.HasNoPrivilege, err
				}
				if hasAnyPrivilegeResult == tree.HasPrivilege {
					return hasAnyPrivilegeResult, nil
				}
			}
			return tree.HasNoPrivilege, nil
		},
	),

	// See https://www.postgresql.org/docs/10/functions-admin.html#FUNCTIONS-ADMIN-SET
	"current_setting": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySystemInfo,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"setting_name", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return getSessionVar(ctx, string(tree.MustBeDString(args[0])), false /* missingOk */)
			},
			Info:       categorySystemInfo,
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"setting_name", types.String}, {"missing_ok", types.Bool}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return getSessionVar(ctx, string(tree.MustBeDString(args[0])), bool(tree.MustBeDBool(args[1])))
			},
			Info:       categorySystemInfo,
			Volatility: tree.VolatilityStable,
		},
	),

	// See https://www.postgresql.org/docs/10/functions-admin.html#FUNCTIONS-ADMIN-SET
	"set_config": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySystemInfo,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"setting_name", types.String}, {"new_value", types.String}, {"is_local", types.Bool}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				varName := string(tree.MustBeDString(args[0]))
				newValue := string(tree.MustBeDString(args[1]))
				err := setSessionVar(ctx, varName, newValue, bool(tree.MustBeDBool(args[2])))
				if err != nil {
					return nil, err
				}
				return getSessionVar(ctx, varName, false /* missingOk */)
			},
			Info:       categorySystemInfo,
			Volatility: tree.VolatilityVolatile,
		},
	),

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
	"inet_client_addr": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDIPAddr(tree.DIPAddr{IPAddr: ipaddr.IPAddr{}}), nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
	),

	"inet_client_port": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.DZero, nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
	),

	"inet_server_addr": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDIPAddr(tree.DIPAddr{IPAddr: ipaddr.IPAddr{}}), nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
	),

	"inet_server_port": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.DZero, nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
	),

	// pg_column_size(any) - number of bytes used to store a particular value
	// (possibly compressed)

	// Database Object Size Functions, see: https://www.postgresql.org/docs/9.4/functions-admin.html
	"pg_column_size": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.VariadicType{
				VarType: types.Any,
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var totalSize int
				for _, arg := range args {
					encodeTableValue, err := valueside.Encode(nil, valueside.NoColumnID, arg, nil)
					if err != nil {
						return tree.DNull, err
					}
					totalSize += len(encodeTableValue)
				}
				return tree.NewDInt(tree.DInt(totalSize)), nil
			},
			Info:       "Return size in bytes of the column provided as an argument",
			Volatility: tree.VolatilityImmutable,
		}),

	// NOTE: these two builtins could be defined as user-defined functions, like
	// they are in Postgres:
	// https://github.com/postgres/postgres/blob/master/src/backend/catalog/information_schema.sql
	//
	//  CREATE FUNCTION _pg_truetypid(pg_attribute, pg_type) RETURNS oid
	//    LANGUAGE sql
	//    IMMUTABLE
	//    PARALLEL SAFE
	//    RETURNS NULL ON NULL INPUT
	//  RETURN CASE WHEN $2.typtype = 'd' THEN $2.typbasetype ELSE $1.atttypid END;
	//
	"information_schema._pg_truetypid": pgTrueTypImpl("atttypid", "typbasetype", types.Oid),
	//
	//  CREATE FUNCTION _pg_truetypmod(pg_attribute, pg_type) RETURNS int4
	//    LANGUAGE sql
	//    IMMUTABLE
	//    PARALLEL SAFE
	//    RETURNS NULL ON NULL INPUT
	//  RETURN CASE WHEN $2.typtype = 'd' THEN $2.typtypmod ELSE $1.atttypmod END;
	//
	"information_schema._pg_truetypmod": pgTrueTypImpl("atttypmod", "typtypmod", types.Int4),

	// NOTE: this could be defined as a user-defined function, like
	// it is in Postgres:
	// https://github.com/postgres/postgres/blob/master/src/backend/catalog/information_schema.sql
	//
	//  CREATE FUNCTION _pg_char_max_length(typid oid, typmod int4) RETURNS integer
	//      LANGUAGE sql
	//      IMMUTABLE
	//      PARALLEL SAFE
	//      RETURNS NULL ON NULL INPUT
	//  RETURN
	//    CASE WHEN $2 = -1 /* default typmod */
	//         THEN null
	//         WHEN $1 IN (1042, 1043) /* char, varchar */
	//         THEN $2 - 4
	//         WHEN $1 IN (1560, 1562) /* bit, varbit */
	//         THEN $2
	//         ELSE null
	//    END;
	//
	"information_schema._pg_char_max_length": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"typid", types.Oid},
				{"typmod", types.Int4},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				typid := oid.Oid(args[0].(*tree.DOid).DInt)
				typmod := *args[1].(*tree.DInt)
				if typmod == -1 {
					return tree.DNull, nil
				} else if typid == oid.T_bpchar || typid == oid.T_varchar {
					return tree.NewDInt(typmod - 4), nil
				} else if typid == oid.T_bit || typid == oid.T_varbit {
					return tree.NewDInt(typmod), nil
				}
				return tree.DNull, nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityImmutable,
		},
	),

	// Given an index's OID and an underlying-table column number,
	// _pg_index_position return the column's position in the index
	// (or NULL if not there).
	//
	// NOTE: this could be defined as a user-defined function, like
	// it is in Postgres:
	// https://github.com/postgres/postgres/blob/master/src/backend/catalog/information_schema.sql
	//
	//  CREATE FUNCTION _pg_index_position(oid, smallint) RETURNS int
	//      LANGUAGE sql STRICT STABLE
	//  BEGIN ATOMIC
	//  SELECT (ss.a).n FROM
	//    (SELECT information_schema._pg_expandarray(indkey) AS a
	//     FROM pg_catalog.pg_index WHERE indexrelid = $1) ss
	//    WHERE (ss.a).x = $2;
	//  END;
	//
	"information_schema._pg_index_position": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"oid", types.Oid},
				{"col", types.Int2},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				r, err := ctx.Planner.QueryRowEx(
					ctx.Ctx(), "information_schema._pg_index_position",
					ctx.Txn,
					sessiondata.NoSessionDataOverride,
					`SELECT (ss.a).n FROM
					  (SELECT information_schema._pg_expandarray(indkey) AS a
					   FROM pg_catalog.pg_index WHERE indexrelid = $1) ss
            WHERE (ss.a).x = $2`,
					args[0], args[1])
				if err != nil {
					return nil, err
				}
				if len(r) == 0 {
					return tree.DNull, nil
				}
				return r[0], nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
	),

	"information_schema._pg_numeric_precision": makeBuiltin(tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types: tree.ArgTypes{
				{"typid", types.Oid},
				{"typmod", types.Int4},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				typid := oid.Oid(tree.MustBeDOid(args[0]).DInt)
				typmod := tree.MustBeDInt(args[1])
				switch typid {
				case oid.T_int2:
					return tree.NewDInt(16), nil
				case oid.T_int4:
					return tree.NewDInt(32), nil
				case oid.T_int8:
					return tree.NewDInt(64), nil
				case oid.T_numeric:
					if typmod != -1 {
						// This logics matches the postgres implementation
						// of how to calculate the precision based on the typmod
						// https://github.com/postgres/postgres/blob/d84ffffe582b8e036a14c6bc2378df29167f3a00/src/backend/catalog/information_schema.sql#L109
						return tree.NewDInt(((typmod - 4) >> 16) & 65535), nil
					}
					return tree.DNull, nil
				case oid.T_float4:
					return tree.NewDInt(24), nil
				case oid.T_float8:
					return tree.NewDInt(53), nil
				}
				return tree.DNull, nil
			},
			Info:       "Returns the precision of the given type with type modifier",
			Volatility: tree.VolatilityImmutable,
		},
	),

	"information_schema._pg_numeric_precision_radix": makeBuiltin(tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types: tree.ArgTypes{
				{"typid", types.Oid},
				{"typmod", types.Int4},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				typid := oid.Oid(tree.MustBeDOid(args[0]).DInt)
				if typid == oid.T_int2 || typid == oid.T_int4 || typid == oid.T_int8 || typid == oid.T_float4 || typid == oid.T_float8 {
					return tree.NewDInt(2), nil
				} else if typid == oid.T_numeric {
					return tree.NewDInt(10), nil
				} else {
					return tree.DNull, nil
				}
			},
			Info:       "Returns the radix of the given type with type modifier",
			Volatility: tree.VolatilityImmutable,
		},
	),

	"information_schema._pg_numeric_scale": makeBuiltin(tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types: tree.ArgTypes{
				{"typid", types.Oid},
				{"typmod", types.Int4},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				typid := oid.Oid(tree.MustBeDOid(args[0]).DInt)
				typmod := tree.MustBeDInt(args[1])
				if typid == oid.T_int2 || typid == oid.T_int4 || typid == oid.T_int8 {
					return tree.NewDInt(0), nil
				} else if typid == oid.T_numeric {
					if typmod == -1 {
						return tree.DNull, nil
					}
					// This logics matches the postgres implementation
					// of how to calculate scale based on the typmod
					// https://github.com/postgres/postgres/blob/d84ffffe582b8e036a14c6bc2378df29167f3a00/src/backend/catalog/information_schema.sql#L140
					return tree.NewDInt((typmod - 4) & 65535), nil
				}
				return tree.DNull, nil
			},
			Info:       "Returns the scale of the given type with type modifier",
			Volatility: tree.VolatilityImmutable,
		},
	),
}

func getSessionVar(ctx *tree.EvalContext, settingName string, missingOk bool) (tree.Datum, error) {
	if ctx.SessionAccessor == nil {
		return nil, errors.AssertionFailedf("session accessor not set")
	}
	ok, s, err := ctx.SessionAccessor.GetSessionVar(ctx.Context, settingName, missingOk)
	if err != nil {
		return nil, err
	}
	if !ok {
		return tree.DNull, nil
	}
	return tree.NewDString(s), nil
}

func setSessionVar(ctx *tree.EvalContext, settingName, newVal string, isLocal bool) error {
	if ctx.SessionAccessor == nil {
		return errors.AssertionFailedf("session accessor not set")
	}
	return ctx.SessionAccessor.SetSessionVar(ctx.Context, settingName, newVal, isLocal)
}

// getCatalogOidForComments returns the "catalog table oid" (the oid of a
// catalog table like pg_database, in the pg_class table) for an input catalog
// name (like pg_class or pg_database). It returns false if there is no such
// catalog table.
func getCatalogOidForComments(catalogName string) (id int, ok bool) {
	switch catalogName {
	case "pg_class":
		return catconstants.PgCatalogClassTableID, true
	case "pg_database":
		return catconstants.PgCatalogDatabaseTableID, true
	case "pg_description":
		return catconstants.PgCatalogDescriptionTableID, true
	case "pg_constraint":
		return catconstants.PgCatalogConstraintTableID, true
	default:
		// We currently only support comments on pg_class objects
		// (columns, tables) in this context.
		// see a different name, matching pg.
		return 0, false
	}
}

// getPgObjDesc queries pg_description for object comments. catalog_name, if not
// empty, provides a constraint on which "system catalog" the comment is in.
// System catalogs are things like pg_class, pg_type, pg_database, and so on.
func getPgObjDesc(ctx *tree.EvalContext, catalogName string, oid int) (tree.Datum, error) {
	classOidFilter := ""
	if catalogName != "" {
		classOid, ok := getCatalogOidForComments(catalogName)
		if !ok {
			// Return NULL for no comment if we can't find the catalog, matching pg.
			return tree.DNull, nil
		}
		classOidFilter = fmt.Sprintf("AND classoid = %d", classOid)
	}
	r, err := ctx.Planner.QueryRowEx(
		ctx.Ctx(), "pg_get_objdesc", ctx.Txn,
		sessiondata.NoSessionDataOverride,
		fmt.Sprintf(`
SELECT description
  FROM pg_catalog.pg_description
 WHERE objoid = %[1]d
   AND objsubid = 0
   %[2]s
 LIMIT 1`,
			oid,
			classOidFilter,
		))
	if err != nil {
		return nil, err
	}
	if len(r) == 0 {
		return tree.DNull, nil
	}
	return r[0], nil
}

func databaseHasPrivilegeSpecifier(databaseArg tree.Datum) (tree.HasPrivilegeSpecifier, error) {
	var specifier tree.HasPrivilegeSpecifier
	switch t := databaseArg.(type) {
	case *tree.DString:
		s := string(*t)
		specifier.DatabaseName = &s
	case *tree.DOid:
		oid := oid.Oid(t.DInt)
		specifier.DatabaseOID = &oid
	default:
		return specifier, errors.AssertionFailedf("unknown privilege specifier: %#v", databaseArg)
	}
	return specifier, nil
}

// tableHasPrivilegeSpecifier returns the HasPrivilegeSpecifier for
// the given table.
func tableHasPrivilegeSpecifier(
	tableArg tree.Datum, isSequence bool,
) (tree.HasPrivilegeSpecifier, error) {
	specifier := tree.HasPrivilegeSpecifier{
		IsSequence: &isSequence,
	}
	switch t := tableArg.(type) {
	case *tree.DString:
		s := string(*t)
		specifier.TableName = &s
	case *tree.DOid:
		oid := oid.Oid(t.DInt)
		specifier.TableOID = &oid
	default:
		return specifier, errors.AssertionFailedf("unknown privilege specifier: %#v", tableArg)
	}
	return specifier, nil
}

// Note that we only verify the column exists for has_column_privilege.
func columnHasPrivilegeSpecifier(
	tableArg tree.Datum, colArg tree.Datum,
) (tree.HasPrivilegeSpecifier, error) {
	specifier, err := tableHasPrivilegeSpecifier(tableArg, false /* isSequence */)
	if err != nil {
		return specifier, err
	}
	switch t := colArg.(type) {
	case *tree.DString:
		n := tree.Name(*t)
		specifier.ColumnName = &n
	case *tree.DInt:
		attNum := uint32(*t)
		specifier.ColumnAttNum = &attNum
	default:
		return specifier, errors.AssertionFailedf("unexpected arg type %T", t)
	}
	return specifier, nil
}

func schemaHasPrivilegeSpecifier(
	ctx *tree.EvalContext, schemaArg tree.Datum, databaseName string,
) (tree.HasPrivilegeSpecifier, error) {
	specifier := tree.HasPrivilegeSpecifier{
		SchemaDatabaseName: &databaseName,
	}
	var schemaIsRequired bool
	switch t := schemaArg.(type) {
	case *tree.DString:
		s := string(*t)
		specifier.SchemaName = &s
		schemaIsRequired = true
	case *tree.DOid:
		schemaName, err := getNameForArg(ctx, schemaArg, "pg_namespace", "nspname")
		if err != nil {
			return specifier, err
		}
		specifier.SchemaName = &schemaName
	default:
		return specifier, errors.AssertionFailedf("unknown privilege specifier: %#v", schemaArg)
	}
	specifier.SchemaIsRequired = &schemaIsRequired
	return specifier, nil
}

func pgTrueTypImpl(attrField, typField string, retType *types.T) builtinDefinition {
	return makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"pg_attribute", types.AnyTuple},
				{"pg_type", types.AnyTuple},
			},
			ReturnType: tree.FixedReturnType(retType),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// In Postgres, this builtin is statically typed to accept a
				// pg_attribute record and a pg_type record. This isn't currently
				// possible in CockroachDB, so instead, we accept any tuple and then
				// perform a bit of dynamic typing to pull out the desired fields from
				// the records.
				fieldIdx := func(t *tree.DTuple, field string) int {
					for i, label := range t.ResolvedType().TupleLabels() {
						if label == field {
							return i
						}
					}
					return -1
				}

				pgAttr, pgType := args[0].(*tree.DTuple), args[1].(*tree.DTuple)
				pgAttrFieldIdx := fieldIdx(pgAttr, attrField)
				pgTypeTypeIdx := fieldIdx(pgType, "typtype")
				pgTypeFieldIdx := fieldIdx(pgType, typField)
				if pgAttrFieldIdx == -1 || pgTypeTypeIdx == -1 || pgTypeFieldIdx == -1 {
					return nil, pgerror.Newf(pgcode.UndefinedFunction,
						"No function matches the given name and argument types.")
				}

				pgAttrField := pgAttr.D[pgAttrFieldIdx]
				pgTypeType := pgType.D[pgTypeTypeIdx].(*tree.DString)
				pgTypeField := pgType.D[pgTypeFieldIdx]

				// If this is a domain type, return the field from pg_type, otherwise,
				// return the field from pg_attribute.
				if *pgTypeType == "d" {
					return pgTypeField, nil
				}
				return pgAttrField, nil
			},
			Info:       notUsableInfo,
			Volatility: tree.VolatilityImmutable,
		},
	)
}

// hasPrivsOfRole returns whether the user has the privileges of the
// specified role (directly or indirectly).
//
// This is defined not to recurse through roles that don't have rolinherit
// set; for such roles, membership implies the ability to do SET ROLE, but
// the privileges are not available until you've done so.
//
// However, because we don't currently support NOINHERIT, a user being a
// member of a role is equivalent to a user having the privileges of that
// role, so this is currently equivalent to isMemberOfRole.
// See https://github.com/cockroachdb/cockroach/issues/69583.
func hasPrivsOfRole(
	ctx *tree.EvalContext, user, role security.SQLUsername,
) (tree.HasAnyPrivilegeResult, error) {
	return isMemberOfRole(ctx, user, role)
}

// isMemberOfRole returns whether the user is a member of the specified role
// (directly or indirectly).
//
// This is defined to recurse through roles regardless of rolinherit.
func isMemberOfRole(
	ctx *tree.EvalContext, user, role security.SQLUsername,
) (tree.HasAnyPrivilegeResult, error) {
	// Fast path for simple case.
	if user == role {
		return tree.HasPrivilege, nil
	}

	// Superusers have every privilege and are part of every role.
	if isSuper, err := ctx.Planner.UserHasAdminRole(ctx.Context, user); err != nil {
		return tree.HasNoPrivilege, err
	} else if isSuper {
		return tree.HasPrivilege, nil
	}

	allRoleMemberships, err := ctx.Planner.MemberOfWithAdminOption(ctx.Context, user)
	if err != nil {
		return tree.HasNoPrivilege, err
	}
	_, member := allRoleMemberships[role]
	if member {
		return tree.HasPrivilege, nil
	}
	return tree.HasNoPrivilege, nil
}

// isAdminOfRole returns whether the user is an admin of the specified role.
//
// That is, is member the role itself (subject to restrictions below), a
// member (directly or indirectly) WITH ADMIN OPTION, or a superuser?
func isAdminOfRole(
	ctx *tree.EvalContext, user, role security.SQLUsername,
) (tree.HasAnyPrivilegeResult, error) {
	// Superusers are an admin of every role.
	//
	// NB: this is intentionally before the user == role check here.
	if isSuper, err := ctx.Planner.UserHasAdminRole(ctx.Context, user); err != nil {
		return tree.HasNoPrivilege, err
	} else if isSuper {
		return tree.HasPrivilege, nil
	}

	// Fast path for simple case.
	if user == role {
		// From Postgres:
		//
		// > A role can admin itself when it matches the session user and we're
		// > outside any security-restricted operation, SECURITY DEFINER or
		// > similar context. SQL-standard roles cannot self-admin. However,
		// > SQL-standard users are distinct from roles, and they are not
		// > grantable like roles: PostgreSQL's role-user duality extends the
		// > standard. Checking for a session user match has the effect of
		// > letting a role self-admin only when it's conspicuously behaving
		// > like a user. Note that allowing self-admin under a mere SET ROLE
		// > would make WITH ADMIN OPTION largely irrelevant; any member could
		// > SET ROLE to issue the otherwise-forbidden command.
		// >
		// > Withholding self-admin in a security-restricted operation prevents
		// > object owners from harnessing the session user identity during
		// > administrative maintenance. Suppose Alice owns a database, has
		// > issued "GRANT alice TO bob", and runs a daily ANALYZE. Bob creates
		// > an alice-owned SECURITY DEFINER function that issues "REVOKE alice
		// > FROM carol". If he creates an expression index calling that
		// > function, Alice will attempt the REVOKE during each ANALYZE.
		// > Checking InSecurityRestrictedOperation() thwarts that attack.
		// >
		// > Withholding self-admin in SECURITY DEFINER functions makes their
		// > behavior independent of the calling user. There's no security or
		// > SQL-standard-conformance need for that restriction, though.
		// >
		// > A role cannot have actual WITH ADMIN OPTION on itself, because that
		// > would imply a membership loop. Therefore, we're done either way.
		//
		// Because CockroachDB does not have "security-restricted operation", so
		// for compatibility, we just need to check whether the user matches the
		// session user.
		if isSessionUser := user == ctx.SessionData().SessionUser(); isSessionUser {
			return tree.HasPrivilege, nil
		}
		return tree.HasNoPrivilege, nil
	}

	allRoleMemberships, err := ctx.Planner.MemberOfWithAdminOption(ctx.Context, user)
	if err != nil {
		return tree.HasNoPrivilege, err
	}
	if isAdmin := allRoleMemberships[role]; isAdmin {
		return tree.HasPrivilege, nil
	}
	return tree.HasNoPrivilege, nil
}
