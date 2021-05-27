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
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
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
	for _, typ := range []*types.T{types.RegType, types.RegProc, types.RegProcedure, types.RegClass, types.RegNamespace} {
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
			r, err := ctx.InternalExecutor.QueryRow(
				ctx.Ctx(), "pg_get_constraintdef",
				ctx.Txn,
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
	fn func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.Datum, error),
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
					if ctx.SessionData.User().Undefined() {
						// Wut... is this possible?
						return tree.DNull, nil
					}
					user = ctx.SessionData.User()
				}
				return fn(ctx, args, user)
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
		tn, err := parser.ParseQualifiedTableName(string(*t))
		if err != nil {
			return nil, err
		}
		if _, err := ctx.Planner.ResolveTableName(ctx.Ctx(), tn); err != nil {
			return nil, err
		}
		if ctx.SessionData.Database != "" && ctx.SessionData.Database != string(tn.CatalogName) {
			// Postgres does not allow cross-database references in these
			// functions, so we don't either.
			return nil, pgerror.Newf(pgcode.FeatureNotSupported,
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
		return nil, errors.AssertionFailedf("unexpected arg type %T", t)
	}
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
			return nil, pgerror.Newf(pgcode.InvalidParameterValue,
				"unrecognized privilege type: %q", priv)
		}
	}
	// Perform all privilege checks.
	for _, priv := range privs {
		d, err := availOpts[priv](false /* withGrantOpt */)
		if err != nil {
			return nil, err
		}
		switch d {
		case tree.DNull, tree.DBoolFalse:
			return d, nil
		case tree.DBoolTrue:
			continue
		default:
			return nil, errors.AssertionFailedf(
				"unexpected privilege check result %v", d)
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
	ctx *tree.EvalContext,
	schema string,
	infoTable string,
	user security.SQLUsername,
	pred string,
	priv privilege.Kind,
	withGrantOpt bool,
) (tree.Datum, error) {
	privChecks := []privilege.Kind{priv}
	if withGrantOpt {
		privChecks = append(privChecks, privilege.GRANT)
	}

	allRoleMemberships, err := ctx.Planner.MemberOfWithAdminOption(ctx.Context, user)
	if err != nil {
		return nil, err
	}

	// Slice containing all roles user is a direct and indirect member of.
	allRoles := []string{security.PublicRole, user.Normalized()}
	for role := range allRoleMemberships {
		allRoles = append(allRoles, role.Normalized())
	}

	for _, p := range privChecks {
		query := fmt.Sprintf(`
			SELECT bool_or(privilege_type IN ('%s', '%s')) IS TRUE
			FROM %s.%s WHERE grantee = ANY ($1) AND %s`,
			privilege.ALL, p, schema, infoTable, pred)
		r, err := ctx.InternalExecutor.QueryRow(
			ctx.Ctx(), "eval-privilege-check", ctx.Txn, query, allRoles,
		)
		if err != nil {
			return nil, err
		}
		if r == nil {
			return nil, errors.AssertionFailedf("failed to evaluate privilege check")
		}
		switch r[0] {
		case tree.DBoolFalse:
			return tree.DBoolFalse, nil
		case tree.DBoolTrue:
			continue
		default:
			return nil, errors.AssertionFailedf("unexpected privilege check result %v", r[0])
		}
	}
	return tree.DBoolTrue, nil
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
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.NewDInt(-1), nil
			},
			Info:       notUsableInfo,
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
				t, err := ctx.InternalExecutor.QueryRow(
					ctx.Ctx(), "pg_get_function_result",
					ctx.Txn,
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
				t, err := ctx.InternalExecutor.QueryRow(
					ctx.Ctx(), "pg_get_function_identity_arguments",
					ctx.Txn,
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

	// pg_my_temp_schema returns the OID of session's temporary schema, or 0 if
	// none. CockroachDB doesn't support this, so it always returns 0.
	// https://www.postgresql.org/docs/11/functions-info.html
	"pg_my_temp_schema": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Oid),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.NewDOid(0), nil
			},
			Info:       notUsableInfo,
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

	"pg_get_userbyid": makeBuiltin(tree.FunctionProperties{DistsqlBlocklist: true},
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
				r, err := ctx.InternalExecutor.QueryRow(
					ctx.Ctx(), "pg_sequence_parameters",
					ctx.Txn,
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
				oid := oid.Oid(int(oidArg.(*tree.DOid).DInt))
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
				r, err := ctx.InternalExecutor.QueryRow(
					ctx.Ctx(), "pg_get_coldesc",
					ctx.Txn,
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

				r, err := ctx.InternalExecutor.QueryRow(
					ctx.Ctx(), "pg_get_shobjdesc", ctx.Txn,
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
				t, err := ctx.InternalExecutor.QueryRow(
					ctx.Ctx(), "pg_function_is_visible",
					ctx.Txn,
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
					ctx.Context, ctx.SessionData.Database, ctx.SessionData.SearchPath, oid.Oid(oidArg.DInt),
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
					ctx.Context, ctx.SessionData.Database, ctx.SessionData.SearchPath, oid.Oid(oidArg.DInt),
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
				table, err := ctx.Planner.GetImmutableTableInterfaceByID(ctx.Ctx(), oid)
				if err != nil {
					// For postgres compatibility, it is expected that rather returning
					// an error this return nonUpdatableEvents (Zero) because there could
					// be oid references on deleted tables.
					if sqlerrors.IsUndefinedRelationError(err) {
						return nonUpdatableEvents, nil
					}
					return nonUpdatableEvents, err
				}
				tableDesc, ok := table.(catalog.TableDescriptor)
				if !ok || !tableDesc.IsTable() || tableDesc.IsVirtualTable() {
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
				table, err := ctx.Planner.GetImmutableTableInterfaceByID(ctx.Ctx(), oid)
				if err != nil {
					if sqlerrors.IsUndefinedRelationError(err) {
						// For postgres compatibility, it is expected that rather returning
						// an error this return nonUpdatableEvents (Zero) because there could
						// be oid references on deleted tables.
						return tree.DBoolFalse, nil
					}
					return tree.DBoolFalse, err
				}
				tableDesc, ok := table.(catalog.TableDescriptor)
				if !ok || !tableDesc.IsTable() || tableDesc.IsVirtualTable() {
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
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.Datum, error) {
			tableArg := tree.UnwrapDatum(ctx, args[0])
			specifier, err := tableHasPrivilegeSpecifier(tableArg)
			if err != nil {
				return nil, err
			}

			return parsePrivilegeStr(args[1], pgPrivList{
				"SELECT": func(withGrantOpt bool) (tree.Datum, error) {
					return hasPrivilege(ctx, specifier, user, privilege.SELECT, withGrantOpt)
				},
				"INSERT": func(withGrantOpt bool) (tree.Datum, error) {
					return hasPrivilege(ctx, specifier, user, privilege.INSERT, withGrantOpt)
				},
				"UPDATE": func(withGrantOpt bool) (tree.Datum, error) {
					return hasPrivilege(ctx, specifier, user, privilege.UPDATE, withGrantOpt)
				},
				"REFERENCES": func(withGrantOpt bool) (tree.Datum, error) {
					return hasPrivilege(ctx, specifier, user, privilege.SELECT, withGrantOpt)
				},
			})
		},
	),

	"has_column_privilege": makePGPrivilegeInquiryDef(
		"column",
		argTypeOpts{{"table", strOrOidTypes}, {"column", []*types.T{types.String, types.Int}}},
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.Datum, error) {
			tableArg := tree.UnwrapDatum(ctx, args[0])
			specifier, err := tableHasPrivilegeSpecifier(tableArg)
			if err != nil {
				return nil, err
			}
			// Note that we only verify the column exists for has_column_privilege.
			colArg := tree.UnwrapDatum(ctx, args[1])
			switch t := colArg.(type) {
			case *tree.DString:
				// When colArg is a string, it specifies the attribute name.
				n := tree.Name(*t)
				specifier.ColumnName = &n
			case *tree.DInt:
				// When colArg is an integer, it specifies the attribute number.
				attNum := uint32(*t)
				specifier.ColumnAttNum = &attNum
			default:
				return nil, errors.AssertionFailedf("unexpected arg type %T", t)
			}

			return parsePrivilegeStr(args[2], pgPrivList{
				"SELECT": func(withGrantOpt bool) (tree.Datum, error) {
					return hasPrivilege(ctx, specifier, user, privilege.SELECT, withGrantOpt)
				},
				"INSERT": func(withGrantOpt bool) (tree.Datum, error) {
					return hasPrivilege(ctx, specifier, user, privilege.INSERT, withGrantOpt)
				},
				"UPDATE": func(withGrantOpt bool) (tree.Datum, error) {
					return hasPrivilege(ctx, specifier, user, privilege.UPDATE, withGrantOpt)
				},
				"REFERENCES": func(withGrantOpt bool) (tree.Datum, error) {
					return hasPrivilege(ctx, specifier, user, privilege.SELECT, withGrantOpt)
				},
			})
		},
	),

	"has_database_privilege": makePGPrivilegeInquiryDef(
		"database",
		argTypeOpts{{"database", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.Datum, error) {
			dbArg := tree.UnwrapDatum(ctx, args[0])
			db, err := getNameForArg(ctx, dbArg, "pg_database", "datname")
			if err != nil {
				return nil, err
			}
			retNull := false
			if db == "" {
				switch dbArg.(type) {
				case *tree.DString:
					return nil, pgerror.Newf(pgcode.InvalidCatalogName,
						"database %s does not exist", dbArg)
				case *tree.DOid:
					// Postgres returns NULL if no matching language is found
					// when given an OID.
					retNull = true
				}
			}

			databasePrivilegePred := fmt.Sprintf("database_name = '%s'", db)
			return parsePrivilegeStr(args[1], pgPrivList{
				"CREATE": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, `"".crdb_internal`,
						"cluster_database_privileges", user, databasePrivilegePred,
						privilege.CREATE, withGrantOpt)
				},
				"CONNECT": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, `"".crdb_internal`,
						"cluster_database_privileges", user, databasePrivilegePred,
						privilege.CONNECT, withGrantOpt)
				},
				"TEMPORARY": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, `"".crdb_internal`,
						"cluster_database_privileges", user, databasePrivilegePred,
						privilege.CREATE, withGrantOpt)
				},
				"TEMP": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, `"".crdb_internal`,
						"cluster_database_privileges", user, databasePrivilegePred,
						privilege.CREATE, withGrantOpt)
				},
			})
		},
	),

	"has_foreign_data_wrapper_privilege": makePGPrivilegeInquiryDef(
		"foreign-data wrapper",
		argTypeOpts{{"fdw", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.Datum, error) {
			fdwArg := tree.UnwrapDatum(ctx, args[0])
			fdw, err := getNameForArg(ctx, fdwArg, "pg_foreign_data_wrapper", "fdwname")
			if err != nil {
				return nil, err
			}
			if fdw == "" {
				switch fdwArg.(type) {
				case *tree.DString:
					return nil, pgerror.Newf(pgcode.UndefinedObject,
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
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.Datum, error) {
			oidArg := tree.UnwrapDatum(ctx, args[0])
			// When specifying a function by a text string rather than by OID,
			// the allowed input is the same as for the regprocedure data type.
			var oid tree.Datum
			switch t := oidArg.(type) {
			case *tree.DString:
				var err error
				oid, err = tree.PerformCast(ctx, t, types.RegProcedure)
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
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.Datum, error) {
			langArg := tree.UnwrapDatum(ctx, args[0])
			lang, err := getNameForArg(ctx, langArg, "pg_language", "lanname")
			if err != nil {
				return nil, err
			}
			retNull := false
			if lang == "" {
				switch langArg.(type) {
				case *tree.DString:
					return nil, pgerror.Newf(pgcode.UndefinedObject,
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
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.Datum, error) {
			schemaArg := tree.UnwrapDatum(ctx, args[0])
			schema, err := getNameForArg(ctx, schemaArg, "pg_namespace", "nspname")
			if err != nil {
				return nil, err
			}
			retNull := false
			if schema == "" {
				switch schemaArg.(type) {
				case *tree.DString:
					return nil, pgerror.Newf(pgcode.InvalidSchemaName,
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
					return evalPrivilegeCheck(ctx, "information_schema",
						"schema_privileges", user, pred,
						privilege.CREATE, withGrantOpt)
				},
				"USAGE": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "information_schema",
						"schema_privileges", user, pred,
						privilege.USAGE, withGrantOpt)
				},
			})
		},
	),

	"has_sequence_privilege": makePGPrivilegeInquiryDef(
		"sequence",
		argTypeOpts{{"sequence", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.Datum, error) {
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
					tn.CatalogName, tn.SchemaName, tn.ObjectName); err != nil {
					return nil, err
				} else if r == nil {
					return nil, pgerror.Newf(pgcode.WrongObjectType,
						"%s is not a sequence", seqArg)
				}

				pred = fmt.Sprintf(
					"table_catalog = '%s' AND table_schema = '%s' AND table_name = '%s'",
					tn.CatalogName, tn.SchemaName, tn.ObjectName)
			}

			return parsePrivilegeStr(args[1], pgPrivList{
				"USAGE": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "information_schema",
						"table_privileges", user, pred,
						privilege.SELECT, withGrantOpt)
				},
				"SELECT": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "information_schema",
						"table_privileges", user, pred,
						privilege.SELECT, withGrantOpt)
				},
				"UPDATE": func(withGrantOpt bool) (tree.Datum, error) {
					if retNull {
						return tree.DNull, nil
					}
					return evalPrivilegeCheck(ctx, "information_schema",
						"table_privileges", user, pred,
						privilege.UPDATE, withGrantOpt)
				},
			})
		},
	),

	"has_server_privilege": makePGPrivilegeInquiryDef(
		"foreign server",
		argTypeOpts{{"server", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.Datum, error) {
			serverArg := tree.UnwrapDatum(ctx, args[0])
			server, err := getNameForArg(ctx, serverArg, "pg_foreign_server", "srvname")
			if err != nil {
				return nil, err
			}
			if server == "" {
				switch serverArg.(type) {
				case *tree.DString:
					return nil, pgerror.Newf(pgcode.UndefinedObject,
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
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.Datum, error) {
			tableArg := tree.UnwrapDatum(ctx, args[0])
			specifier, err := tableHasPrivilegeSpecifier(tableArg)
			if err != nil {
				return nil, err
			}

			return parsePrivilegeStr(args[1], pgPrivList{
				"SELECT": func(withGrantOpt bool) (tree.Datum, error) {
					return hasPrivilege(ctx, specifier, user, privilege.SELECT, withGrantOpt)
				},
				"INSERT": func(withGrantOpt bool) (tree.Datum, error) {
					return hasPrivilege(ctx, specifier, user, privilege.INSERT, withGrantOpt)
				},
				"UPDATE": func(withGrantOpt bool) (tree.Datum, error) {
					return hasPrivilege(ctx, specifier, user, privilege.UPDATE, withGrantOpt)
				},
				"DELETE": func(withGrantOpt bool) (tree.Datum, error) {
					return hasPrivilege(ctx, specifier, user, privilege.DELETE, withGrantOpt)
				},
				"TRUNCATE": func(withGrantOpt bool) (tree.Datum, error) {
					return hasPrivilege(ctx, specifier, user, privilege.DELETE, withGrantOpt)
				},
				"REFERENCES": func(withGrantOpt bool) (tree.Datum, error) {
					return hasPrivilege(ctx, specifier, user, privilege.SELECT, withGrantOpt)
				},
				"TRIGGER": func(withGrantOpt bool) (tree.Datum, error) {
					return hasPrivilege(ctx, specifier, user, privilege.CREATE, withGrantOpt)
				},
			})
		},
	),

	"has_tablespace_privilege": makePGPrivilegeInquiryDef(
		"tablespace",
		argTypeOpts{{"tablespace", strOrOidTypes}},
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.Datum, error) {
			tablespaceArg := tree.UnwrapDatum(ctx, args[0])
			tablespace, err := getNameForArg(ctx, tablespaceArg, "pg_tablespace", "spcname")
			if err != nil {
				return nil, err
			}
			if tablespace == "" {
				switch tablespaceArg.(type) {
				case *tree.DString:
					return nil, pgerror.Newf(pgcode.UndefinedObject,
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
		func(ctx *tree.EvalContext, args tree.Datums, user security.SQLUsername) (tree.Datum, error) {
			oidArg := tree.UnwrapDatum(ctx, args[0])
			// When specifying a type by a text string rather than by OID, the
			// allowed input is the same as for the regtype data type.
			var oid tree.Datum
			switch t := oidArg.(type) {
			case *tree.DString:
				var err error
				oid, err = tree.PerformCast(ctx, t, types.RegType)
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
			Info:       notUsableInfo,
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"setting_name", types.String}, {"missing_ok", types.Bool}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return getSessionVar(ctx, string(tree.MustBeDString(args[0])), bool(tree.MustBeDBool(args[1])))
			},
			Info:       notUsableInfo,
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
				err := setSessionVar(ctx, varName, string(tree.MustBeDString(args[1])), bool(tree.MustBeDBool(args[2])))
				if err != nil {
					return nil, err
				}
				return getSessionVar(ctx, varName, false /* missingOk */)
			},
			Info:       notUsableInfo,
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
					encodeTableValue, err := rowenc.EncodeTableValue(nil, descpb.ColumnID(encoding.NoColumnID), arg, nil)
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
	if isLocal {
		return unimplemented.NewWithIssuef(32562, "transaction-scoped settings are not supported")
	}
	return ctx.SessionAccessor.SetSessionVar(ctx.Context, settingName, newVal)
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
	r, err := ctx.InternalExecutor.QueryRow(
		ctx.Ctx(), "pg_get_objdesc", ctx.Txn,
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

// hasPrivilege returns whether the given specifier has the given privilege.
func hasPrivilege(
	ctx *tree.EvalContext,
	specifier tree.HasPrivilegeSpecifier,
	user security.SQLUsername,
	kind privilege.Kind,
	withGrantOpt bool,
) (tree.Datum, error) {
	ret, err := ctx.Planner.HasPrivilege(
		ctx.Context,
		specifier,
		user,
		kind,
		withGrantOpt,
	)
	if err != nil {
		// When an OID is specified and the relation is not found, we return NULL.
		if specifier.TableOID != nil && sqlerrors.IsUndefinedRelationError(err) {
			return tree.DNull, nil
		}
		return nil, err
	}
	return tree.MakeDBool(tree.DBool(ret)), nil
}

// tableHasPrivilegeSpecifier returns the HasPrivilegeSpecifier for
// the given table.
func tableHasPrivilegeSpecifier(tableArg tree.Datum) (tree.HasPrivilegeSpecifier, error) {
	var specifier tree.HasPrivilegeSpecifier
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
