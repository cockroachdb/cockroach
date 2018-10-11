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
	"strings"
	"time"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
				Info: notUsableInfo,
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
	types.Decimal.Oid():     {},
	types.Interval.Oid():    {},
	types.JSON.Oid():        {},
	types.UUID.Oid():        {},
	oid.T_varbit:            {},
	oid.T_bit:               {},
	types.Timestamp.Oid():   {},
	types.TimestampTZ.Oid(): {},
	types.FamTuple.Oid():    {},
}

// PGIOBuiltinPrefix returns the string prefix to a type's IO functions. This
// is either the type's postgres display name or the type's postgres display
// name plus an underscore, depending on the type.
func PGIOBuiltinPrefix(typ types.T) string {
	builtinPrefix := strings.ToLower(oid.TypeName[typ.Oid()])
	if _, ok := typeBuiltinsHaveUnderscore[typ.Oid()]; ok {
		return builtinPrefix + "_"
	}
	return builtinPrefix
}

// initPGBuiltins adds all of the postgres builtins to the Builtins map.
func initPGBuiltins() {
	for k, v := range pgBuiltins {
		v.props.Category = categoryCompatibility
		builtins[k] = v
	}

	// Make non-array type i/o builtins.
	for _, typ := range types.OidToType {
		// Skip array types. We're doing them separately below.
		if typ != types.Any && typ != types.IntVector && typ != types.OidVector && typ.Equivalent(types.AnyArray) {
			continue
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

	// Make crdb_internal.create_regfoo builtins.
	for _, typ := range []types.TOid{types.RegType, types.RegProc, types.RegProcedure, types.RegClass, types.RegNamespace} {
		typName := typ.SQLName()
		builtins["crdb_internal.create_"+typName] = makeCreateRegDef(typ)
	}
}

var errUnimplemented = pgerror.NewError(pgerror.CodeFeatureNotSupportedError, "unimplemented")

func makeTypeIOBuiltin(argTypes tree.TypeList, returnType types.T) builtinDefinition {
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
				Info: notUsableInfo,
			},
		},
	}
}

// makeTypeIOBuiltins generates the 4 i/o builtins that Postgres implements for
// every type: typein, typeout, typerecv, and typsend. All 4 builtins are no-op,
// and only supported because ORMs sometimes use their names to form a map for
// client-side type encoding and decoding. See issue #12526 for more details.
func makeTypeIOBuiltins(builtinPrefix string, typ types.T) map[string]builtinDefinition {
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
				colNumber = *args[1].(*tree.DInt)
			}
			r, err := ctx.InternalExecutor.QueryRow(
				ctx.Ctx(), "pg_get_indexdef",
				ctx.Txn,
				"SELECT indexdef FROM pg_catalog.pg_indexes WHERE crdb_oid = $1", args[0])
			if err != nil {
				return nil, err
			}
			// If the index does not exist we return null.
			if len(r) == 0 {
				return tree.DNull, nil
			}
			// The 1 argument and 3 argument variants are equivalent when column number 0 is passed.
			if colNumber == 0 {
				return r[0], nil
			}
			// The 3 argument variant for column number other than 0 returns the column name.
			r, err = ctx.InternalExecutor.QueryRow(
				ctx.Ctx(), "pg_get_indexdef",
				ctx.Txn,
				`SELECT ischema.column_name as pg_get_indexdef 
		               FROM information_schema.statistics AS ischema 
											INNER JOIN pg_catalog.pg_indexes AS pgindex 
													ON ischema.table_schema = pgindex.schemaname 
													AND ischema.table_name = pgindex.tablename 
													AND ischema.index_name = pgindex.indexname 
													AND pgindex.crdb_oid = $1 
													AND ischema.seq_in_index = $2`, args[0], args[1])
			if err != nil {
				return nil, err
			}
			// If the column number does not exist in the index we return an empty string.
			if len(r) == 0 {
				return tree.NewDString(""), nil
			}
			if len(r) > 1 {
				return nil, pgerror.NewAssertionErrorf("pg_get_indexdef query has more than 1 result row: %+v", r)
			}
			return r[0], nil
		},
		Info: notUsableInfo,
	}
}

// Make a pg_get_viewdef function with the given arguments.
func makePGGetViewDef(argTypes tree.ArgTypes) tree.Overload {
	return tree.Overload{
		Types:      argTypes,
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			r, err := ctx.InternalExecutor.QueryRow(
				ctx.Ctx(), "pg_get_viewdef",
				ctx.Txn,
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
		Info: notUsableInfo,
	}
}

// Make a pg_get_constraintdef function with the given arguments.
func makePGGetConstraintDef(argTypes tree.ArgTypes) tree.Overload {
	return tree.Overload{
		Types:      argTypes,
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			r, err := ctx.InternalExecutor.QueryRow(
				ctx.Ctx(), "pg_get_constraintdef",
				ctx.Txn,
				"SELECT condef FROM pg_catalog.pg_constraint WHERE oid=$1", args[0])
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

// argTypeOpts is similar to tree.ArgTypes, but represents arguments that can
// accept multiple types.
type argTypeOpts []struct {
	Name string
	Typ  []types.T
}

var strOrOidTypes = []types.T{types.String, types.Oid}

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
	fn func(ctx *tree.EvalContext, args tree.Datums, user string) (tree.Datum, error),
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
				var user string
				if withUser {
					var err error
					user, err = getNameForArg(ctx, args[0], "pg_roles", "rolname")
					if err != nil {
						return nil, err
					}
					if user == "" {
						if _, ok := args[0].(*tree.DOid); ok {
							// Postgres returns falseifn no matching user is
							// found when given an OID.
							return tree.DBoolFalse, nil
						}
						return nil, pgerror.NewErrorf(pgerror.CodeUndefinedObjectError,
							"role %s does not exist", args[0])
					}

					// Remove the first argument.
					args = args[1:]
				} else {
					if len(ctx.SessionData.User) == 0 {
						// Wut... is this possible?
						return tree.DNull, nil
					}
					user = ctx.SessionData.User
				}
				return fn(ctx, args, user)
			},
			Info: fmt.Sprintf(infoFmt, infoDetail),
		})
	}
	return builtinDefinition{
		props: tree.FunctionProperties{
			DistsqlBlacklist: true,
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
		log.Fatalf(ctx.Ctx(), "unexpected arg type %T", t)
	}
	r, err := ctx.InternalExecutor.QueryRow(ctx.Ctx(), "get-name-for-arg", ctx.Txn, query, arg)
	if err != nil || r == nil {
		return "", err
	}
	return string(tree.MustBeDString(r[0])), nil
}

// getTableNameForArg determines the qualified table name for the specified
// argument, which should be either an unwrapped STRING or an OID. If the table
// is not found, the returned pointer will be nil.
func getTableNameForArg(ctx *tree.EvalContext, arg tree.Datum) (*tree.TableName, error) {
	switch t := arg.(type) {
	case *tree.DString:
		tn, err := ctx.Planner.ParseQualifiedTableName(ctx.Ctx(), string(*t))
		if err != nil {
			return nil, err
		}
		if err := ctx.Planner.ResolveTableName(ctx.Ctx(), tn); err != nil {
			return nil, err
		}
		if ctx.SessionData.Database != "" && ctx.SessionData.Database != string(tn.CatalogName) {
			// Postgres does not allow cross-database references in these
			// functions, so we don't either.
			return nil, pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError,
				"cross-database references are not implemented: %s", tn)
		}
		return tn, nil
	case *tree.DOid:
		r, err := ctx.InternalExecutor.QueryRow(ctx.Ctx(), "get-table-name-for-arg",
			ctx.Txn,
			`SELECT n.nspname, c.relname FROM pg_class c
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			WHERE c.oid = $1`, t)
		if err != nil || r == nil {
			return nil, err
		}
		db := tree.Name(ctx.SessionData.Database)
		schema := tree.Name(tree.MustBeDString(r[0]))
		table := tree.Name(tree.MustBeDString(r[1]))
		tn := tree.MakeTableNameWithSchema(db, schema, table)
		return &tn, nil
	default:
		log.Fatalf(ctx.Ctx(), "unexpected arg type %T", t)
	}
	return nil, nil
}

// TODO(nvanbenschoten): give this a comment.
type pgPrivList map[string]func(withGrantOpt bool) (tree.Datum, error)

// parsePrivilegeStr parses the provided privilege string and calls into the
// privilege option map for each specified privilege.
func parsePrivilegeStr(arg tree.Datum, availOpts pgPrivList) (tree.Datum, error) {
	// Postgres allows WITH GRANT OPTION to be added to a privilege type to
	// test whether the privilege is held with grant option.
	for priv, fn := range availOpts {
		fn := fn
		availOpts[priv+" WITH GRANT OPTION"] = func(_ bool) (tree.Datum, error) {
			return fn(true /* withGrantOpt */) // Override withGrantOpt
		}
	}

	argStr := string(tree.MustBeDString(arg))
	privs := strings.Split(argStr, ",")
	for i, priv := range privs {
		// Privileges are case-insensitive.
		priv = strings.ToUpper(priv)
		// Extra whitespace is allowed between but not within privilege names.
		privs[i] = strings.TrimSpace(priv)
	}
	// Check that all privileges are allowed.
	for _, priv := range privs {
		if _, ok := availOpts[priv]; !ok {
			return nil, pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError,
				"unrecognized privilege type: %q", priv)
		}
	}
	// Perform all privilege checks.
	for _, priv := range privs {
		d, err := availOpts[priv](false /* withGrantOpt */)
		if err != nil {
			return nil, errors.Wrapf(err, "error checking privilege %q", priv)
		}
		switch d {
		case tree.DNull, tree.DBoolFalse:
			return d, nil
		case tree.DBoolTrue:
			continue
		default:
			panic(fmt.Sprintf("unexpected privilege check result %v", d))
		}
	}
	return tree.DBoolTrue, nil
}

// evalPrivilegeCheck performs a privilege check for the specified privilege.
// The function takes an information_schema table name for which to run a query
// against, along with an arbitrary predicate to run against the table and the
// user to perform the check on. It also takes a flag as to whether the
// privilege check should also test whether the privilege is held with grant
// option.
func evalPrivilegeCheck(
	ctx *tree.EvalContext, infoTable, user, pred string, priv privilege.Kind, withGrantOpt bool,
) (tree.Datum, error) {
	privChecks := []privilege.Kind{priv}
	if withGrantOpt {
		privChecks = append(privChecks, privilege.GRANT)
	}
	for _, p := range privChecks {
		query := fmt.Sprintf(`
			SELECT bool_or(privilege_type IN ('%s', '%s')) IS TRUE
			FROM information_schema.%s WHERE grantee IN ($1, $2) AND %s`,
			privilege.ALL, p, infoTable, pred)
		// TODO(mberhault): "public" is a constant defined in sql/sqlbase, but importing that
		// would cause a dependency cycle sqlbase -> sem/transform -> sem/builtins -> sqlbase
		r, err := ctx.InternalExecutor.QueryRow(
			ctx.Ctx(), "eval-privilege-check", ctx.Txn, query, "public", user,
		)
		if err != nil {
			return nil, err
		}
		switch r[0] {
		case tree.DBoolFalse:
			return tree.DBoolFalse, nil
		case tree.DBoolTrue:
			continue
		default:
			panic(fmt.Sprintf("unexpected privilege check result %v", r[0]))
		}
	}
	return tree.DBoolTrue, nil
}

func makeCreateRegDef(typ types.TOid) builtinDefinition {
	return makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"oid", types.Int},
				{"name", types.String},
			},
			ReturnType: tree.FixedReturnType(typ),
			Fn: func(_ *tree.EvalContext, d tree.Datums) (tree.Datum, error) {
				return tree.NewDOidWithName(tree.MustBeDInt(d[0]), coltypes.OidTypeToColType(typ), string(tree.MustBeDString(d[1]))), nil
			},
			Info: notUsableInfo,
		},
	)
}

var pgBuiltins = map[string]builtinDefinition{
	// See https://www.postgresql.org/docs/9.6/static/functions-info.html.
	"pg_backend_pid": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.NewDInt(-1), nil
			},
			Info: notUsableInfo,
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
			Info: notUsableInfo,
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
			Info: notUsableInfo,
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
			Info: notUsableInfo,
		},
	),

	// pg_get_constraintdef functions like SHOW CREATE CONSTRAINT would if we
	// supported that statement.
	"pg_get_constraintdef": makeBuiltin(tree.FunctionProperties{DistsqlBlacklist: true},
		makePGGetConstraintDef(tree.ArgTypes{
			{"constraint_oid", types.Oid}, {"pretty_bool", types.Bool}}),
		makePGGetConstraintDef(tree.ArgTypes{{"constraint_oid", types.Oid}}),
	),

	// pg_get_indexdef functions like SHOW CREATE INDEX would if we supported that
	// statement.
	"pg_get_indexdef": makeBuiltin(tree.FunctionProperties{DistsqlBlacklist: true},
		makePGGetIndexDef(tree.ArgTypes{{"index_oid", types.Oid}}),
		makePGGetIndexDef(tree.ArgTypes{{"index_oid", types.Oid}, {"column_no", types.Int}, {"pretty_bool", types.Bool}}),
	),

	// pg_get_viewdef functions like SHOW CREATE VIEW but returns the same format as
	// PostgreSQL leaving out the actual 'CREATE VIEW table_name AS' portion of the statement.
	"pg_get_viewdef": makeBuiltin(tree.FunctionProperties{DistsqlBlacklist: true},
		makePGGetViewDef(tree.ArgTypes{{"view_oid", types.Oid}}),
		makePGGetViewDef(tree.ArgTypes{{"view_oid", types.Oid}, {"pretty_bool", types.Bool}}),
	),

	// TODO(bram): Make sure the reported type is correct for tuples. See #25523.
	"pg_typeof": makeBuiltin(tree.FunctionProperties{NullableArgs: true},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.Any}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(args[0].ResolvedType().String()), nil
			},
			Info: notUsableInfo,
		},
	),

	"pg_get_userbyid": makeBuiltin(tree.FunctionProperties{DistsqlBlacklist: true},
		tree.Overload{
			Types: tree.ArgTypes{
				{"role_oid", types.Oid},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				oid := args[0]
				t, err := ctx.InternalExecutor.QueryRow(
					ctx.Ctx(), "pg_get_userbyid",
					ctx.Txn,
					"SELECT rolname FROM pg_catalog.pg_roles WHERE oid=$1", oid)
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
	),

	"pg_sequence_parameters": makeBuiltin(tree.FunctionProperties{DistsqlBlacklist: true},
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
				r, err := ctx.InternalExecutor.QueryRow(
					ctx.Ctx(), "pg_sequence_parameters",
					ctx.Txn,
					`SELECT seqstart, seqmin, seqmax, seqincrement, seqcycle, seqcache, seqtypid `+
						`FROM pg_catalog.pg_sequence WHERE seqrelid=$1`, args[0])
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
	),

	"format_type": makeBuiltin(tree.FunctionProperties{NullableArgs: true},
		tree.Overload{
			Types:      tree.ArgTypes{{"type_oid", types.Oid}, {"typemod", types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
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
	),

	"col_description": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"table_oid", types.Oid}, {"column_number", types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.DNull, nil
			},
			Info: notUsableInfo,
		},
	),

	"obj_description": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"object_oid", types.Oid}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.DNull, nil
			},
			Info: notUsableInfo,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"object_oid", types.Oid}, {"catalog_name", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.DNull, nil
			},
			Info: notUsableInfo,
		},
	),

	"oid": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"int", types.Int}},
			ReturnType: tree.FixedReturnType(types.Oid),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDOid(*args[0].(*tree.DInt)), nil
			},
			Info: "Converts an integer to an OID.",
		},
	),

	"shobj_description": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"object_oid", types.Oid}, {"catalog_name", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.DNull, nil
			},
			Info: notUsableInfo,
		},
	),

	"pg_try_advisory_lock": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"int", types.Int}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.DBoolTrue, nil
			},
			Info: notUsableInfo,
		},
	),

	"pg_advisory_unlock": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"int", types.Int}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.DBoolTrue, nil
			},
			Info: notUsableInfo,
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
			Info: notUsableInfo,
		},
	),

	// pg_table_is_visible returns true if the input oid corresponds to a table
	// that is part of the databases on the search path.
	// https://www.postgresql.org/docs/9.6/static/functions-info.html
	"pg_table_is_visible": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"oid", types.Oid}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				oid := args[0]
				t, err := ctx.InternalExecutor.QueryRow(
					ctx.Ctx(), "pg_table_is_visible",
					ctx.Txn,
					"SELECT nspname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace=n.oid "+
						"WHERE c.oid=$1 AND nspname=ANY(current_schemas(true));", oid)
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(t != nil)), nil
			},
			Info: notUsableInfo,
		},
	),

	"pg_sleep": makeBuiltin(
		tree.FunctionProperties{
			// pg_sleep is marked as impure so it doesn't get executed during
			// normalization.
			Impure: true,
		},
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
		func(ctx *tree.EvalContext, args tree.Datums, user string) (tree.Datum, error) {
			tableArg := tree.UnwrapDatum(ctx, args[0])
			tn, err := getTableNameForArg(ctx, tableArg)
			if err != nil {
				return nil, err
			}
			pred := ""
			retNull := false
			if tn == nil {
				// Postgres returns NULL if no matching table is found
				// when given an OID.
				retNull = true
			} else {
				pred = fmt.Sprintf(
					"table_catalog = '%s' AND table_schema = '%s' AND table_name = '%s'",
					tn.CatalogName, tn.SchemaName, tn.TableName)
			}

			return parsePrivilegeStr(args[1], pgPrivList{
				"SELECT": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "table_privileges",
						user, pred, privilege.SELECT, withGrantOpt)
				},
				"INSERT": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "table_privileges",
						user, pred, privilege.INSERT, withGrantOpt)
				},
				"UPDATE": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "table_privileges",
						user, pred, privilege.UPDATE, withGrantOpt)
				},
				"REFERENCES": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "table_privileges",
						user, pred, privilege.SELECT, withGrantOpt)
				},
			})
		},
	),

	"has_column_privilege": makePGPrivilegeInquiryDef(
		"column",
		argTypeOpts{{"table", strOrOidTypes}, {"column", []types.T{types.String, types.Int}}},
		func(ctx *tree.EvalContext, args tree.Datums, user string) (tree.Datum, error) {
			tableArg := tree.UnwrapDatum(ctx, args[0])
			tn, err := getTableNameForArg(ctx, tableArg)
			if err != nil {
				return nil, err
			}
			pred := ""
			retNull := false
			if tn == nil {
				// Postgres returns NULL if no matching table is found
				// when given an OID.
				retNull = true
			} else {
				pred = fmt.Sprintf(
					"table_catalog = '%s' AND table_schema = '%s' AND table_name = '%s'",
					tn.CatalogName, tn.SchemaName, tn.TableName)

				// Verify that the column exists in the table.
				var colPred string
				colArg := tree.UnwrapDatum(ctx, args[1])
				switch t := colArg.(type) {
				case *tree.DString:
					colPred = "column_name = $1"
				case *tree.DInt:
					colPred = "ordinal_position = $1"
				default:
					log.Fatalf(ctx.Ctx(), "expected arg type %T", t)
				}

				if r, err := ctx.InternalExecutor.QueryRow(
					ctx.Ctx(), "has-column-privilege",
					ctx.Txn,
					fmt.Sprintf(`
					SELECT column_name FROM information_schema.columns
					WHERE %s AND %s`, pred, colPred), colArg); err != nil {
					return nil, err
				} else if r == nil {
					return nil, pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
						"column %s of relation %s does not exist", colArg, tableArg)
				}
			}

			return parsePrivilegeStr(args[2], pgPrivList{
				"SELECT": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "table_privileges",
						user, pred, privilege.SELECT, withGrantOpt)
				},
				"INSERT": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "table_privileges",
						user, pred, privilege.INSERT, withGrantOpt)
				},
				"UPDATE": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "table_privileges",
						user, pred, privilege.UPDATE, withGrantOpt)
				},
				"REFERENCES": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "table_privileges",
						user, pred, privilege.SELECT, withGrantOpt)
				},
			})
		},
	),

	"has_database_privilege": makePGPrivilegeInquiryDef(
		"database",
		argTypeOpts{{"database", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user string) (tree.Datum, error) {
			dbArg := tree.UnwrapDatum(ctx, args[0])
			db, err := getNameForArg(ctx, dbArg, "pg_database", "datname")
			if err != nil {
				return nil, err
			}
			retNull := false
			if db == "" {
				switch dbArg.(type) {
				case *tree.DString:
					return nil, pgerror.NewErrorf(pgerror.CodeInvalidCatalogNameError,
						"database %s does not exist", dbArg)
				case *tree.DOid:
					// Postgres returns NULL if no matching language is found
					// when given an OID.
					retNull = true
				}
			}

			pred := fmt.Sprintf("table_catalog = '%s'", db)
			return parsePrivilegeStr(args[1], pgPrivList{
				"CREATE": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "schema_privileges",
						user, pred, privilege.CREATE, withGrantOpt)
				},
				"CONNECT": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					// All users have CONNECT privileges for all databases.
					return tree.DBoolTrue, nil
				},
				"TEMPORARY": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "schema_privileges",
						user, pred, privilege.CREATE, withGrantOpt)
				},
				"TEMP": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "schema_privileges",
						user, pred, privilege.CREATE, withGrantOpt)
				},
			})
		},
	),

	"has_foreign_data_wrapper_privilege": makePGPrivilegeInquiryDef(
		"foreign-data wrapper",
		argTypeOpts{{"fdw", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user string) (tree.Datum, error) {
			fdwArg := tree.UnwrapDatum(ctx, args[0])
			fdw, err := getNameForArg(ctx, fdwArg, "pg_foreign_data_wrapper", "fdwname")
			if err != nil {
				return nil, err
			}
			if fdw == "" {
				switch fdwArg.(type) {
				case *tree.DString:
					return nil, pgerror.NewErrorf(pgerror.CodeUndefinedObjectError,
						"foreign-data wrapper %s does not exist", fdwArg)
				case *tree.DOid:
					// Unlike most of the functions, Postgres does not return
					// NULL when an OID does not match.
				}
			}

			return parsePrivilegeStr(args[1], pgPrivList{
				"USAGE": func(withGrantOpt bool) (tree.Datum, error) {
					// All users have USAGE privileges for all foreign-data wrappers.
					return tree.DBoolTrue, nil
				},
			})
		},
	),

	"has_function_privilege": makePGPrivilegeInquiryDef(
		"function",
		argTypeOpts{{"function", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user string) (tree.Datum, error) {
			oidArg := tree.UnwrapDatum(ctx, args[0])
			// When specifying a function by a text string rather than by OID,
			// the allowed input is the same as for the regprocedure data type.
			var oid tree.Datum
			switch t := oidArg.(type) {
			case *tree.DString:
				var err error
				oid, err = tree.PerformCast(ctx, t, coltypes.RegProcedure)
				if err != nil {
					return nil, err
				}
			case *tree.DOid:
				oid = t
			}

			fn, err := getNameForArg(ctx, oid, "pg_proc", "proname")
			if err != nil {
				return nil, err
			}
			retNull := false
			if fn == "" {
				// Postgres returns NULL if no matching function is found
				// when given an OID.
				retNull = true
			}

			return parsePrivilegeStr(args[1], pgPrivList{
				"EXECUTE": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					// All users have access to all functions.
					return tree.DBoolTrue, nil
				},
			})
		},
	),

	"has_language_privilege": makePGPrivilegeInquiryDef(
		"language",
		argTypeOpts{{"language", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user string) (tree.Datum, error) {
			langArg := tree.UnwrapDatum(ctx, args[0])
			lang, err := getNameForArg(ctx, langArg, "pg_language", "lanname")
			if err != nil {
				return nil, err
			}
			retNull := false
			if lang == "" {
				switch langArg.(type) {
				case *tree.DString:
					return nil, pgerror.NewErrorf(pgerror.CodeUndefinedObjectError,
						"language %s does not exist", langArg)
				case *tree.DOid:
					// Postgres returns NULL if no matching language is found
					// when given an OID.
					retNull = true
				}
			}

			return parsePrivilegeStr(args[1], pgPrivList{
				"USAGE": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					// All users have access to all languages.
					return tree.DBoolTrue, nil
				},
			})
		},
	),

	"has_schema_privilege": makePGPrivilegeInquiryDef(
		"schema",
		argTypeOpts{{"schema", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user string) (tree.Datum, error) {
			schemaArg := tree.UnwrapDatum(ctx, args[0])
			schema, err := getNameForArg(ctx, schemaArg, "pg_namespace", "nspname")
			if err != nil {
				return nil, err
			}
			retNull := false
			if schema == "" {
				switch schemaArg.(type) {
				case *tree.DString:
					return nil, pgerror.NewErrorf(pgerror.CodeInvalidSchemaNameError,
						"schema %s does not exist", schemaArg)
				case *tree.DOid:
					// Postgres returns NULL if no matching schema is found
					// when given an OID.
					retNull = true
				}
			}
			if len(ctx.SessionData.Database) == 0 {
				// If no database is set, return NULL.
				retNull = true
			}

			pred := fmt.Sprintf("table_catalog = '%s' AND table_schema = '%s'",
				ctx.SessionData.Database, schema)
			return parsePrivilegeStr(args[1], pgPrivList{
				"CREATE": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "schema_privileges",
						user, pred, privilege.CREATE, withGrantOpt)
				},
				"USAGE": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "schema_privileges",
						user, pred, privilege.SELECT, withGrantOpt)
				},
			})
		},
	),

	"has_sequence_privilege": makePGPrivilegeInquiryDef(
		"sequence",
		argTypeOpts{{"sequence", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user string) (tree.Datum, error) {
			seqArg := tree.UnwrapDatum(ctx, args[0])
			tn, err := getTableNameForArg(ctx, seqArg)
			if err != nil {
				return nil, err
			}
			pred := ""
			retNull := false
			if tn == nil {
				// Postgres returns NULL if no matching table is found
				// when given an OID.
				retNull = true
			} else {
				// Verify that the table name is actually a sequence.
				if r, err := ctx.InternalExecutor.QueryRow(
					ctx.Ctx(), "has-sequence-privilege",
					ctx.Txn,
					`SELECT sequence_name FROM information_schema.sequences `+
						`WHERE sequence_catalog = $1 AND sequence_schema = $2 AND sequence_name = $3`,
					tn.CatalogName, tn.SchemaName, tn.TableName); err != nil {
					return nil, err
				} else if r == nil {
					return nil, pgerror.NewErrorf(pgerror.CodeWrongObjectTypeError,
						"%s is not a sequence", seqArg)
				}

				pred = fmt.Sprintf(
					"table_catalog = '%s' AND table_schema = '%s' AND table_name = '%s'",
					tn.CatalogName, tn.SchemaName, tn.TableName)
			}

			return parsePrivilegeStr(args[1], pgPrivList{
				"USAGE": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "table_privileges",
						user, pred, privilege.SELECT, withGrantOpt)
				},
				"SELECT": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "table_privileges",
						user, pred, privilege.SELECT, withGrantOpt)
				},
				"UPDATE": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "table_privileges",
						user, pred, privilege.UPDATE, withGrantOpt)
				},
			})
		},
	),

	"has_server_privilege": makePGPrivilegeInquiryDef(
		"foreign server",
		argTypeOpts{{"server", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user string) (tree.Datum, error) {
			serverArg := tree.UnwrapDatum(ctx, args[0])
			server, err := getNameForArg(ctx, serverArg, "pg_foreign_server", "srvname")
			if err != nil {
				return nil, err
			}
			if server == "" {
				switch serverArg.(type) {
				case *tree.DString:
					return nil, pgerror.NewErrorf(pgerror.CodeUndefinedObjectError,
						"server %s does not exist", serverArg)
				case *tree.DOid:
					// Unlike most of the functions, Postgres does not return
					// NULL when an OID does not match.
				}
			}

			return parsePrivilegeStr(args[1], pgPrivList{
				"USAGE": func(withGrantOpt bool) (tree.Datum, error) {
					// All users have USAGE privileges for all foreign servers.
					return tree.DBoolTrue, nil
				},
			})
		},
	),

	"has_table_privilege": makePGPrivilegeInquiryDef(
		"table",
		argTypeOpts{{"table", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user string) (tree.Datum, error) {
			tableArg := tree.UnwrapDatum(ctx, args[0])
			tn, err := getTableNameForArg(ctx, tableArg)
			if err != nil {
				return nil, err
			}
			pred := ""
			retNull := false
			if tn == nil {
				// Postgres returns NULL if no matching table is found
				// when given an OID.
				retNull = true
			} else {
				pred = fmt.Sprintf(
					"table_catalog = '%s' AND table_schema = '%s' AND table_name = '%s'",
					tn.CatalogName, tn.SchemaName, tn.TableName)
			}

			return parsePrivilegeStr(args[1], pgPrivList{
				"SELECT": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "table_privileges",
						user, pred, privilege.SELECT, withGrantOpt)
				},
				"INSERT": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "table_privileges",
						user, pred, privilege.INSERT, withGrantOpt)
				},
				"UPDATE": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "table_privileges",
						user, pred, privilege.UPDATE, withGrantOpt)
				},
				"DELETE": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "table_privileges",
						user, pred, privilege.DELETE, withGrantOpt)
				},
				"TRUNCATE": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "table_privileges",
						user, pred, privilege.DELETE, withGrantOpt)
				},
				"REFERENCES": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "table_privileges",
						user, pred, privilege.SELECT, withGrantOpt)
				},
				"TRIGGER": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "table_privileges",
						user, pred, privilege.CREATE, withGrantOpt)
				},
			})
		},
	),

	"has_tablespace_privilege": makePGPrivilegeInquiryDef(
		"tablespace",
		argTypeOpts{{"tablespace", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user string) (tree.Datum, error) {
			tablespaceArg := tree.UnwrapDatum(ctx, args[0])
			tablespace, err := getNameForArg(ctx, tablespaceArg, "pg_tablespace", "spcname")
			if err != nil {
				return nil, err
			}
			if tablespace == "" {
				switch tablespaceArg.(type) {
				case *tree.DString:
					return nil, pgerror.NewErrorf(pgerror.CodeUndefinedObjectError,
						"tablespace %s does not exist", tablespaceArg)
				case *tree.DOid:
					// Unlike most of the functions, Postgres does not return
					// NULL when an OID does not match.
				}
			}

			return parsePrivilegeStr(args[1], pgPrivList{
				"CREATE": func(withGrantOpt bool) (tree.Datum, error) {
					// All users have CREATE privileges in all tablespaces.
					return tree.DBoolTrue, nil
				},
			})
		},
	),

	"has_type_privilege": makePGPrivilegeInquiryDef(
		"type",
		argTypeOpts{{"type", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user string) (tree.Datum, error) {
			oidArg := tree.UnwrapDatum(ctx, args[0])
			// When specifying a type by a text string rather than by OID, the
			// allowed input is the same as for the regtype data type.
			var oid tree.Datum
			switch t := oidArg.(type) {
			case *tree.DString:
				var err error
				oid, err = tree.PerformCast(ctx, t, coltypes.RegType)
				if err != nil {
					return nil, err
				}
			case *tree.DOid:
				oid = t
			}

			typ, err := getNameForArg(ctx, oid, "pg_type", "typname")
			if err != nil {
				return nil, err
			}
			retNull := false
			if typ == "" {
				// Postgres returns NULL if no matching type is found
				// when given an OID.
				retNull = true
			}

			return parsePrivilegeStr(args[1], pgPrivList{
				"USAGE": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					// All users have access to all types.
					return tree.DBoolTrue, nil
				},
			})
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
			Info: notUsableInfo,
		},
	),

	"inet_client_port": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.DZero, nil
			},
			Info: notUsableInfo,
		},
	),

	"inet_server_addr": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDIPAddr(tree.DIPAddr{IPAddr: ipaddr.IPAddr{}}), nil
			},
			Info: notUsableInfo,
		},
	),

	"inet_server_port": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.DZero, nil
			},
			Info: notUsableInfo,
		},
	),
}
