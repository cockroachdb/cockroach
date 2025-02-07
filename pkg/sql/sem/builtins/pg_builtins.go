// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/oidext"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
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
	return makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(context.Context, *eval.Context, tree.Datums) (tree.Datum, error) {
				return tree.DBoolFalse, nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Volatile,
		},
	)
}

// typeBuiltinsHaveUnderscore is a map to keep track of which types have i/o
// builtins with underscores in between their type name and the i/o builtin
// name, like date_in vs int8in. There seems to be no other way to
// programmatically determine whether or not this underscore is present, hence
// the existence of this map.
var typeBuiltinsHaveUnderscore = map[oid.Oid]struct{}{
	types.AnyElement.Oid():  {},
	types.AnyArray.Oid():    {},
	types.Date.Oid():        {},
	types.Time.Oid():        {},
	types.TimeTZ.Oid():      {},
	types.Decimal.Oid():     {},
	types.Interval.Oid():    {},
	types.Json.Oid():        {},
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

// initPGBuiltins adds all of the postgres builtins to the builtins map.
func init() {
	const enforceClass = true
	for k, v := range pgBuiltins {
		v.props.Category = builtinconstants.CategoryCompatibility
		registerBuiltin(k, v, tree.NormalClass, enforceClass)
	}

	// Make non-array type i/o builtins.
	for _, typ := range types.OidToType {
		switch typ.Oid() {
		case oid.T_trigger:
			// TRIGGER is not valid in any context apart from the return-type of a
			// trigger function.
			continue
		case oid.T_int2vector, oid.T_oidvector:
			// Handled separately below.
		default:
			if typ.Family() == types.ArrayFamily {
				// Array types are handled separately below.
				continue
			}
		}
		builtinPrefix := PGIOBuiltinPrefix(typ)
		for name, builtin := range makeTypeIOBuiltins(builtinPrefix, typ) {
			registerBuiltin(name, builtin, tree.NormalClass, enforceClass)
		}
	}
	// Make array type i/o builtins.
	for name, builtin := range makeTypeIOBuiltins("array_", types.AnyArray) {
		registerBuiltin(name, builtin, tree.NormalClass, enforceClass)
	}
	for name, builtin := range makeTypeIOBuiltins("anyarray_", types.AnyArray) {
		registerBuiltin(name, builtin, tree.NormalClass, enforceClass)
	}
	// Make enum type i/o builtins.
	for name, builtin := range makeTypeIOBuiltins("enum_", types.AnyEnum) {
		registerBuiltin(name, builtin, tree.NormalClass, enforceClass)
	}

	// Make type cast builtins.
	// In postgresql, this is done at type resolution type - if a valid cast exists
	// but used as a function, make it a cast.
	// e.g. date(ts) is the same as ts::date.
	castBuiltins := make(map[oid.Oid]*builtinDefinition)
	cast.ForEachCast(func(fromOID oid.Oid, toOID oid.Oid, _ cast.Context, _ cast.ContextOrigin, v volatility.V) {
		fromTyp, ok := types.OidToType[fromOID]
		if !ok || !shouldMakeFromCastBuiltin(fromTyp) {
			return
		}
		toType, ok := types.OidToType[toOID]
		if !ok {
			return
		}
		distSQLBlockList := toType.Family() == types.OidFamily
		if _, ok := castBuiltins[toOID]; !ok {
			castBuiltins[toOID] = &builtinDefinition{
				props: tree.FunctionProperties{
					Category:         builtinconstants.CategoryCast,
					Undocumented:     true,
					DistsqlBlocklist: distSQLBlockList,
				},
			}
		}
		castBuiltins[toOID].overloads = append(
			castBuiltins[toOID].overloads,
			tree.Overload{
				Types:      tree.ParamTypes{{Name: fromTyp.String(), Typ: fromTyp}},
				ReturnType: tree.FixedReturnType(toType),
				Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
					return eval.PerformCast(ctx, evalCtx, args[0], toType)
				},
				Class:      tree.NormalClass,
				Info:       fmt.Sprintf("Cast from %s to %s.", fromTyp.SQLString(), toType.SQLString()),
				Volatility: v,
				// The one for name casts differ.
				// Since we're using the same one as cast, ignore that from now.
				IgnoreVolatilityCheck: true,
			},
		)
	})
	// Add casts between the same type in deterministic order.
	typOIDs := make([]oid.Oid, 0, len(castBuiltins))
	for typOID := range castBuiltins {
		typOIDs = append(typOIDs, typOID)
	}
	sort.Slice(typOIDs, func(i, j int) bool {
		return typOIDs[i] < typOIDs[j]
	})
	for _, typOID := range typOIDs {
		def := castBuiltins[typOID]
		typ := types.OidToType[typOID]
		if !shouldMakeFromCastBuiltin(typ) {
			continue
		}
		// Some casts already have been defined to deal with typmod coercion.
		// Do not double add them.
		if cast.OIDInCastMap(typOID, typOID) {
			continue
		}
		def.overloads = append(
			def.overloads,
			tree.Overload{
				Types:      tree.ParamTypes{{Name: typ.String(), Typ: typ}},
				ReturnType: tree.FixedReturnType(typ),
				Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
					return eval.PerformCast(ctx, evalCtx, args[0], typ)
				},
				Class:      tree.NormalClass,
				Info:       fmt.Sprintf("Cast from %s to %s.", typ.SQLString(), typ.SQLString()),
				Volatility: volatility.Immutable,
			},
		)
	}
	for toOID, def := range castBuiltins {
		n := cast.CastTypeName(types.OidToType[toOID])
		CastBuiltinNames[n] = struct{}{}
		registerBuiltin(n, *def, tree.NormalClass, enforceClass)
	}

	// Make crdb_internal.create_regfoo and to_regfoo builtins.
	for _, b := range []struct {
		toRegOverloadHelpText string
		typ                   *types.T
	}{
		{"Translates a textual relation name to its OID", types.RegClass},
		{"Translates a textual schema name to its OID", types.RegNamespace},
		{"Translates a textual function or procedure name to its OID", types.RegProc},
		{"Translates a textual function or procedure name(with argument types) to its OID", types.RegProcedure},
		{"Translates a textual role name to its OID", types.RegRole},
		{"Translates a textual type name to its OID", types.RegType},
	} {
		typName := b.typ.SQLStandardName()
		registerBuiltin("crdb_internal.create_"+typName, makeCreateRegDef(b.typ), tree.NormalClass, enforceClass)
		registerBuiltin("to_"+typName, makeToRegOverload(b.typ, b.toRegOverloadHelpText), tree.NormalClass, enforceClass)
	}
}

// CastBuiltinNames contains all cast builtin names.
var CastBuiltinNames = make(map[string]struct{})

// CastBuiltinOIDs maps casts from tgt oid to src family to OIDs.
// We base the second on family as casts are only defined once per family
// in order to make type resolution non-ambiguous.
var CastBuiltinOIDs = make(map[oid.Oid]map[types.Family]oid.Oid)

func shouldMakeFromCastBuiltin(in *types.T) bool {
	// Since type resolutions are based on families, prevent ambiguity where
	// possible by using the "preferred" type for the family.
	switch {
	case in.Family() == types.OidFamily && in.Oid() != oid.T_oid:
		return false
	case in.Family() == types.BitFamily && in.Oid() != oid.T_bit:
		return false
	case in.Family() == types.StringFamily && in.Oid() != oid.T_text:
		return false
	case in.Family() == types.IntFamily && in.Oid() != oid.T_int8:
		return false
	case in.Family() == types.FloatFamily && in.Oid() != oid.T_float8:
		return false
	case in.Family() == types.TriggerFamily:
		// TRIGGER is not a valid cast target.
		return false
	}
	return true
}

var errUnimplemented = pgerror.New(pgcode.FeatureNotSupported, "unimplemented")

func makeTypeIOBuiltin(paramTypes tree.TypeList, returnType *types.T) builtinDefinition {
	return makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryCompatibility,
		},
		tree.Overload{
			Types:      paramTypes,
			ReturnType: tree.FixedReturnType(returnType),
			Fn: func(_ context.Context, _ *eval.Context, _ tree.Datums) (tree.Datum, error) {
				return nil, errUnimplemented
			},
			Info:       notUsableInfo,
			Volatility: volatility.Stable,
			// Ignore validity checks for typeio builtins. We don't
			// implement these anyway, and they are very hard to special
			// case.
			IgnoreVolatilityCheck: true,
		},
	)
}

// makeTypeIOBuiltins generates the 4 i/o builtins that Postgres implements for
// every type: typein, typeout, typerecv, and typsend. All 4 builtins are no-op,
// and only supported because ORMs sometimes use their names to form a map for
// client-side type encoding and decoding. See issue #12526 for more details.
func makeTypeIOBuiltins(builtinPrefix string, typ *types.T) map[string]builtinDefinition {
	typname := typ.String()
	return map[string]builtinDefinition{
		builtinPrefix + "send": makeTypeIOBuiltin(tree.ParamTypes{{Name: typname, Typ: typ}}, types.Bytes),
		// Note: PG takes type 2281 "internal" for these builtins, which we don't
		// provide. We won't implement these functions anyway, so it shouldn't
		// matter.
		builtinPrefix + "recv": makeTypeIOBuiltin(tree.ParamTypes{{Name: "input", Typ: types.AnyElement}}, typ),
		// Note: PG returns 'cstring' for these builtins, but we don't support that.
		builtinPrefix + "out": makeTypeIOBuiltin(tree.ParamTypes{{Name: typname, Typ: typ}}, types.Bytes),
		// Note: PG takes 'cstring' for these builtins, but we don't support that.
		builtinPrefix + "in": makeTypeIOBuiltin(tree.ParamTypes{{Name: "input", Typ: types.AnyElement}}, typ),
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
func makePGGetViewDef(paramTypes tree.ParamTypes) tree.Overload {
	return tree.Overload{
		Types:      paramTypes,
		ReturnType: tree.FixedReturnType(types.String),
		Body: `SELECT definition
		FROM pg_catalog.pg_views v
		JOIN pg_catalog.pg_class c ON c.relname=v.viewname
		WHERE c.oid=$1
		UNION ALL
		SELECT definition
		FROM pg_catalog.pg_matviews v
		JOIN pg_catalog.pg_class c ON c.relname=v.matviewname
		WHERE c.oid=$1`,
		Info:       "Returns the CREATE statement for an existing view.",
		Volatility: volatility.Stable,
		Language:   tree.RoutineLangSQL,
	}
}

// Make a pg_get_constraintdef function with the given arguments.
func makePGGetConstraintDef(paramTypes tree.ParamTypes) tree.Overload {
	return tree.Overload{
		Types:      paramTypes,
		ReturnType: tree.FixedReturnType(types.String),
		Body:       `SELECT condef FROM pg_catalog.pg_constraint WHERE oid=$1 LIMIT 1`,
		Info:       notUsableInfo,
		Volatility: volatility.Stable,
		Language:   tree.RoutineLangSQL,
	}
}

// paramTypeOpts is similar to tree.ParamTypes, but represents parameters that
// can accept multiple types.
type paramTypeOpts []struct {
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
	objSpecArgs paramTypeOpts,
	fn func(ctx context.Context, evalCtx *eval.Context, args tree.Datums, user username.SQLUsername) (eval.HasAnyPrivilegeResult, error),
) builtinDefinition {
	// Collect the different argument type variations.
	//
	// 1. variants can begin with an optional "user" argument, which if used
	//    can be specified using a STRING or an OID. Postgres also allows the
	//    'public' pseudo-role to be used, but this is not supported here. If
	//    the argument omitted, the value of current_user is assumed.
	paramTypes := []tree.ParamTypes{
		{}, // no user
	}
	for _, typ := range strOrOidTypes {
		paramTypes = append(paramTypes, tree.ParamTypes{{Name: "user", Typ: typ}})
	}
	// 2. variants have one or more object identification arguments, which each
	//    accept multiple types.
	for _, objSpecArg := range objSpecArgs {
		prevParamTypes := paramTypes
		paramTypes = make([]tree.ParamTypes, 0, len(paramTypes)*len(objSpecArg.Typ))
		for _, paramType := range prevParamTypes {
			for _, typ := range objSpecArg.Typ {
				paramTypeVariant := append(paramType, tree.ParamTypes{{Name: objSpecArg.Name, Typ: typ}}...)
				paramTypes = append(paramTypes, paramTypeVariant)
			}
		}
	}
	// 3. variants all end with a "privilege" argument which can only
	//    be a string. See parsePrivilegeStr for details on how this
	//    argument is parsed and used.
	for i, paramType := range paramTypes {
		paramTypes[i] = append(paramType, tree.ParamTypes{{Name: "privilege", Typ: types.String}}...)
	}

	var variants []tree.Overload
	for _, paramType := range paramTypes {
		withUser := paramType[0].Name == "user"

		infoFmt := "Returns whether or not the current user has privileges for %s."
		if withUser {
			infoFmt = "Returns whether or not the user has privileges for %s."
		}

		variants = append(variants, tree.Overload{
			Types:      paramType,
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				var user username.SQLUsername
				if withUser {
					arg := eval.UnwrapDatum(ctx, evalCtx, args[0])
					userS, err := getNameForArg(ctx, evalCtx, arg, "pg_roles", "rolname")

					if err != nil {
						return nil, err
					}
					// Note: the username in pg_roles is already normalized, so
					// we can safely turn it into a SQLUsername without
					// re-normalization.
					user = username.MakeSQLUsernameFromPreNormalizedString(userS)
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
					if evalCtx.SessionData().User().Undefined() {
						// Wut... is this possible?
						return tree.DNull, nil
					}
					user = evalCtx.SessionData().User()
				}
				ret, err := fn(ctx, evalCtx, args, user)
				if err != nil {
					return nil, err
				}
				switch ret {
				case eval.HasPrivilege:
					return tree.DBoolTrue, nil
				case eval.HasNoPrivilege:
					return tree.DBoolFalse, nil
				case eval.ObjectNotFound:
					return tree.DNull, nil
				default:
					panic(fmt.Sprintf("unrecognized HasAnyPrivilegeResult %d", ret))
				}
			},
			Info:       fmt.Sprintf(infoFmt, infoDetail),
			Volatility: volatility.Stable,
		})
	}
	return makeBuiltin(
		tree.FunctionProperties{
			DistsqlBlocklist: true,
		},
		variants...,
	)
}

// getNameForArg determines the object name for the specified argument, which
// should be either an unwrapped STRING or an OID. If the object is not found,
// the returned string will be empty.
func getNameForArg(
	ctx context.Context, evalCtx *eval.Context, arg tree.Datum, pgTable, pgCol string,
) (string, error) {
	var query string
	switch t := arg.(type) {
	case *tree.DString:
		u, err := username.MakeSQLUsernameFromUserInput(string(*t), username.PurposeValidation)
		if err != nil {
			return "", err
		}
		if u == username.PublicRoleName() {
			return username.PublicRole, nil
		}
		arg = tree.NewDString(u.Normalized())
		query = fmt.Sprintf("SELECT %s FROM pg_catalog.%s WHERE %s = $1 LIMIT 1", pgCol, pgTable, pgCol)
	case *tree.DOid:
		if t.Oid == username.PublicRoleID {
			return username.PublicRole, nil
		}
		query = fmt.Sprintf("SELECT %s FROM pg_catalog.%s WHERE oid = $1 LIMIT 1", pgCol, pgTable)
	default:
		return "", errors.AssertionFailedf("unexpected arg type %T", t)
	}
	r, err := evalCtx.Planner.QueryRowEx(ctx, "get-name-for-arg",
		sessiondata.NoSessionDataOverride, query, arg)
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
			Types: tree.ParamTypes{
				{Name: "oid", Typ: types.Oid},
				{Name: "name", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(typ),
			Fn: func(_ context.Context, _ *eval.Context, d tree.Datums) (tree.Datum, error) {
				return tree.NewDOidWithTypeAndName(
					tree.MustBeDOid(d[0]).Oid, typ, string(tree.MustBeDString(d[1])),
				), nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Immutable,
		},
	)
}

func makeToRegOverload(typ *types.T, helpText string) builtinDefinition {
	return makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "text", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.RegType),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				typName := tree.MustBeDString(args[0])
				_, err := strconv.Atoi(strings.TrimSpace(string(typName)))
				if err == nil {
					// If a number was passed in, return NULL.
					return tree.DNull, nil
				}
				typOid, err := eval.ParseDOid(ctx, evalCtx, string(typName), typ)
				if err != nil {
					//nolint:returnerrcheck
					return tree.DNull, nil
				}

				return typOid, nil
			},
			Info:       helpText,
			Volatility: volatility.Stable,
		},
	)
}

// Format the array {type,othertype} as type, othertype.
// If there are no args, output the empty string.
const getFunctionArgStringQuery = `
SELECT COALESCE(
    (SELECT trim('{}' FROM replace(
        (
            SELECT array_agg(unnested::REGTYPE::TEXT)
            FROM unnest(proargtypes) AS unnested
        )::TEXT, ',', ', '))
    ), '')
FROM pg_catalog.pg_proc WHERE oid=$1 GROUP BY oid, proargtypes LIMIT 1
`

var pgBuiltins = map[string]builtinDefinition{
	// See https://www.postgresql.org/docs/9.6/static/functions-info.html.
	"pg_backend_pid": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, _ tree.Datums) (tree.Datum, error) {
				pid := evalCtx.QueryCancelKey.GetPGBackendPID()
				return tree.NewDInt(tree.DInt(pid)), nil
			},
			Info: "Returns a numerical ID attached to this session. This ID is " +
				"part of the query cancellation key used by the wire protocol. This " +
				"function was only added for compatibility, and unlike in Postgres, the " +
				"returned value does not correspond to a real process ID.",
			Volatility: volatility.Stable,
		},
	),

	// See https://www.postgresql.org/docs/9.3/static/catalog-pg-database.html.
	"pg_encoding_to_char": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "encoding_id", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if cmp, err := args[0].Compare(ctx, evalCtx, DatEncodingUTFId); err != nil {
					return tree.DNull, err
				} else if cmp == 0 {
					return datEncodingUTF8ShortName, nil
				}
				return tree.DNull, nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Stable,
		},
	),

	// Here getdatabaseencoding just returns UTF8 because,
	// CockroachDB supports just UTF8 for now.
	"getdatabaseencoding": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				// We only support UTF-8 right now.
				// If we allow more encodings, we must also change the virtual schema
				// entry for pg_catalog.pg_database.
				return datEncodingUTF8ShortName, nil
			},
			Info:       "Returns the current encoding name used by the database.",
			Volatility: volatility.Stable,
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
			Types: tree.ParamTypes{

				{Name: "pg_node_tree", Typ: types.String},
				{Name: "relation_oid", Typ: types.Oid},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return args[0], nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Stable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "pg_node_tree", Typ: types.String},
				{Name: "relation_oid", Typ: types.Oid},
				{Name: "pretty_bool", Typ: types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return args[0], nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Stable,
		},
	),

	// pg_get_constraintdef functions like SHOW CREATE CONSTRAINT would if we
	// supported that statement.
	"pg_get_constraintdef": makeBuiltin(tree.FunctionProperties{DistsqlBlocklist: true},
		makePGGetConstraintDef(tree.ParamTypes{
			{Name: "constraint_oid", Typ: types.Oid}, {Name: "pretty_bool", Typ: types.Bool}}),
		makePGGetConstraintDef(tree.ParamTypes{{Name: "constraint_oid", Typ: types.Oid}}),
	),

	// pg_get_partkeydef is only provided for compatibility and always returns
	// NULL. It is supposed to return the PARTITION BY clause of a table's
	// CREATE statement.
	"pg_get_partkeydef": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "oid", Typ: types.Oid}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.DNull, nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Stable,
		},
	),

	"pg_get_functiondef": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "func_oid", Typ: types.Oid}},
			ReturnType: tree.FixedReturnType(types.String),
			Body: fmt.Sprintf(
				`SELECT COALESCE(create_statement, prosrc)
             FROM pg_catalog.pg_proc
             LEFT JOIN crdb_internal.create_function_statements
             ON schema_id=pronamespace
             AND function_id=oid::int-%d
             WHERE oid=$1
             LIMIT 1`, oidext.CockroachPredefinedOIDMax),
			Info: "For user-defined functions, returns the definition of the specified function. " +
				"For builtin functions, returns the name of the function.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
			Language:          tree.RoutineLangSQL,
		},
	),

	"pg_get_function_arguments": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "func_oid", Typ: types.Oid}},
			ReturnType: tree.FixedReturnType(types.String),
			Body:       getFunctionArgStringQuery,
			Info: "Returns the argument list (with defaults) necessary to identify a function, " +
				"in the form it would need to appear in within CREATE FUNCTION.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
			Language:          tree.RoutineLangSQL,
		},
	),

	"pg_get_function_arg_default": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "func_oid", Typ: types.Oid}, {Name: "arg_num", Typ: types.Int4}},
			ReturnType: tree.FixedReturnType(types.String),
			Body:       "SELECT NULL",
			Info: "Get textual representation of a function argument's default value. " +
				"The second argument of this function is the argument number among all " +
				"arguments (i.e. proallargtypes, *not* proargtypes), starting with 1, " +
				"because that's how information_schema.sql uses it. Currently, this " +
				"always returns NULL, since CockroachDB does not support default values.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
			Language:          tree.RoutineLangSQL,
		},
	),

	// pg_get_function_result returns the types of the result of a builtin
	// function. Multi-return builtins currently are returned as anyelement, which
	// is a known incompatibility with Postgres.
	// https://www.postgresql.org/docs/11/functions-info.html
	"pg_get_function_result": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "func_oid", Typ: types.Oid}},
			ReturnType: tree.FixedReturnType(types.String),
			Body: `SELECT t.typname
             FROM pg_catalog.pg_proc p
             JOIN pg_catalog.pg_type t
             ON prorettype=t.oid
             WHERE p.oid=$1 LIMIT 1`,
			Info:              "Returns the types of the result of the specified function.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
			Language:          tree.RoutineLangSQL,
		},
	),

	// pg_get_function_identity_arguments returns the argument list necessary to
	// identify a function, in the form it would need to appear in within ALTER
	// FUNCTION, for instance. This form omits default values.
	// https://www.postgresql.org/docs/11/functions-info.html
	"pg_get_function_identity_arguments": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "func_oid", Typ: types.Oid}},
			ReturnType: tree.FixedReturnType(types.String),
			Body:       getFunctionArgStringQuery,
			Info: "Returns the argument list (without defaults) necessary to identify a function, " +
				"in the form it would need to appear in within ALTER FUNCTION, for instance.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
			Language:          tree.RoutineLangSQL,
		},
	),

	// pg_get_indexdef functions like SHOW CREATE INDEX would if we supported that
	// statement.
	"pg_get_indexdef": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo, DistsqlBlocklist: true},
		tree.Overload{
			Types:             tree.ParamTypes{{Name: "index_oid", Typ: types.Oid}},
			ReturnType:        tree.FixedReturnType(types.String),
			Body:              `SELECT indexdef FROM pg_catalog.pg_indexes WHERE crdb_oid = $1`,
			Info:              "Gets the CREATE INDEX command for index",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
			Language:          tree.RoutineLangSQL,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "index_oid", Typ: types.Oid}, {Name: "column_no", Typ: types.Int}, {Name: "pretty_bool", Typ: types.Bool}},
			ReturnType: tree.FixedReturnType(types.String),
			Body: `SELECT CASE
    				WHEN $2 = 0 THEN defs.indexdef
						WHEN $2 < 0 OR $2 > array_length(i.indkey, 1) THEN ''
						-- array_positions(i.indkey, 0) returns the 1-based indexes of the indkey elements that are 0.
    				-- array_position(arr, $2) returns the 1-based index of the value $2 in arr.
    				-- indkey is an int2vector, which is accessed with 0-based indexes.
    				-- indexprs is a string[], which is accessed with 1-based indexes.
    				-- To put this all together, for the k-th 0 value inside of indkey, this will find the k-th indexpr.
						WHEN i.indkey[$2-1] = 0 THEN (indexprs::STRING[])[array_position(array_positions(i.indkey, 0), $2)]
						ELSE a.attname
					END as pg_get_indexdef
					FROM pg_catalog.pg_index i
					LEFT JOIN pg_catalog.pg_attribute a ON (a.attrelid = i.indexrelid AND a.attnum = $2)
					LEFT JOIN pg_catalog.pg_indexes defs ON ($2 = 0 AND defs.crdb_oid = i.indexrelid)
					WHERE i.indexrelid = $1`,
			Info:       "Gets the CREATE INDEX command for index, or definition of just one index column when given a non-zero column number",
			Volatility: volatility.Stable,
			Language:   tree.RoutineLangSQL,
		},
	),

	// pg_get_viewdef functions like SHOW CREATE VIEW but returns the same format as
	// PostgreSQL leaving out the actual 'CREATE VIEW table_name AS' portion of the statement.
	"pg_get_viewdef": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo, DistsqlBlocklist: true},
		makePGGetViewDef(tree.ParamTypes{{Name: "view_oid", Typ: types.Oid}}),
		makePGGetViewDef(tree.ParamTypes{{Name: "view_oid", Typ: types.Oid}, {Name: "pretty_bool", Typ: types.Bool}}),
	),

	"pg_get_serial_sequence": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySequences,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "table_name", Typ: types.String}, {Name: "column_name", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				tableName := tree.MustBeDString(args[0])
				columnName := tree.MustBeDString(args[1])
				qualifiedName, err := parser.ParseQualifiedTableName(string(tableName))
				if err != nil {
					return nil, err
				}
				res, err := evalCtx.Sequence.GetSerialSequenceNameFromColumn(ctx, qualifiedName, tree.Name(columnName))
				if err != nil {
					return nil, err
				}
				if res == nil {
					return tree.DNull, nil
				}
				res.ExplicitCatalog = false
				return tree.NewDString(fmt.Sprintf(`%s.%s`, res.SchemaName.String(), res.ObjectName.String())), nil
			},
			Info:       "Returns the name of the sequence used by the given column_name in the table table_name.",
			Volatility: volatility.Stable,
		},
	),

	"pg_sequence_last_value": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySequences,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "sequence_oid", Typ: types.Oid}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				seqOid := tree.MustBeDOid(args[0])

				value, wasCalled, err := evalCtx.Sequence.GetLastSequenceValueByID(ctx, uint32(seqOid.Oid))
				if err != nil {
					return nil, err
				}
				if !wasCalled {
					return tree.DNull, nil
				}
				return tree.NewDInt(tree.DInt(value)), nil
			},
			Info:       "Returns the last value generated by a sequence, or NULL if the sequence has not been used yet.",
			Volatility: volatility.Volatile,
		},
	),

	// pg_my_temp_schema returns the OID of session's temporary schema, or 0 if
	// none.
	// https://www.postgresql.org/docs/11/functions-info.html
	"pg_my_temp_schema": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Oid),
			Fn: func(ctx context.Context, evalCtx *eval.Context, _ tree.Datums) (tree.Datum, error) {
				schema := evalCtx.SessionData().SearchPath.GetTemporarySchemaName()
				if schema == "" {
					// The session has not yet created a temporary schema.
					return tree.NewDOid(0), nil
				}
				oid, errSafeToIgnore, err := evalCtx.Planner.ResolveOIDFromString(
					ctx, types.RegNamespace, tree.NewDString(schema))
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
			Volatility: volatility.Stable,
		},
	),

	// pg_is_other_temp_schema returns true if the given OID is the OID of another
	// session's temporary schema.
	// https://www.postgresql.org/docs/11/functions-info.html
	"pg_is_other_temp_schema": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "oid", Typ: types.Oid}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				schemaArg := eval.UnwrapDatum(ctx, evalCtx, args[0])
				schema, err := getNameForArg(ctx, evalCtx, schemaArg, "pg_namespace", "nspname")
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
				if schema == evalCtx.SessionData().SearchPath.GetTemporarySchemaName() {
					// OID is a reference to this session's temporary schema.
					return tree.DBoolFalse, nil
				}
				return tree.DBoolTrue, nil
			},
			Info:       "Returns true if the given OID is the OID of another session's temporary schema. (This can be useful, for example, to exclude other sessions' temporary tables from a catalog display.)",
			Volatility: volatility.Stable,
		},
	),

	// TODO(bram): Make sure the reported type is correct for tuples. See #25523.
	"pg_typeof": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.AnyElement}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(args[0].ResolvedType().SQLStandardName()), nil
			},
			Info:              notUsableInfo,
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
		},
	),

	// https://www.postgresql.org/docs/10/functions-info.html#FUNCTIONS-INFO-CATALOG-TABLE
	"pg_collation_for": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryString},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "str", Typ: types.AnyElement}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Stable,
		},
	),

	"pg_get_userbyid": makeBuiltin(tree.FunctionProperties{DistsqlBlocklist: true},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "role_oid", Typ: types.Oid},
			},
			ReturnType:        tree.FixedReturnType(types.String),
			Body:              `SELECT COALESCE((SELECT rolname FROM pg_catalog.pg_roles WHERE oid=$1 LIMIT 1), 'unknown (OID=' || $1 || ')')`,
			Info:              notUsableInfo,
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
			Language:          tree.RoutineLangSQL,
		},
	),

	"pg_sequence_parameters": makeBuiltin(tree.FunctionProperties{DistsqlBlocklist: true},
		// pg_sequence_parameters is an undocumented Postgres builtin that returns
		// information about a sequence given its OID. It's nevertheless used by
		// at least one UI tool, so we provide an implementation for compatibility.
		// The real implementation returns a record; we fake it by returning a
		// comma-delimited string enclosed by parentheses.
		tree.Overload{
			Types: tree.ParamTypes{{Name: "sequence_oid", Typ: types.Oid}},
			ReturnType: tree.FixedReturnType(types.MakeLabeledTuple(
				[]*types.T{types.Int, types.Int, types.Int, types.Int, types.Bool, types.Int, types.Oid},
				[]string{"start_value", "minimum_value", "maxmimum_value", "increment", "cycle_option", "cache_size", "data_type"},
			)),
			Body: `SELECT COALESCE ((SELECT (seqstart, seqmin, seqmax, seqincrement, seqcycle, seqcache, seqtypid)
             FROM pg_catalog.pg_sequence WHERE seqrelid=$1 LIMIT 1),
             CASE WHEN crdb_internal.force_error('42P01', 'relation with OID ' || $1 || ' does not exist') > 0 THEN NULL ELSE NULL END)`,
			Info:              notUsableInfo,
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
			Language:          tree.RoutineLangSQL,
		},
	),

	"format_type": makeBuiltin(tree.FunctionProperties{DistsqlBlocklist: true},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "type_oid", Typ: types.Oid}, {Name: "typemod", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				// See format_type.c in Postgres.
				oidArg := args[0]
				if oidArg == tree.DNull {
					return tree.DNull, nil
				}
				maybeTypmod := args[1]
				oid := oidArg.(*tree.DOid).Oid
				typ, ok := types.OidToType[oid]
				if !ok {
					// If the type wasn't statically known, try looking it up as a user
					// defined type.
					var err error
					typ, err = evalCtx.Planner.ResolveTypeByOID(ctx, oid)
					if err != nil {
						// If the error is a descriptor does not exist error, then swallow it.
						switch {
						case sqlerrors.IsMissingDescriptorError(err),
							errors.Is(err, catalog.ErrDescriptorNotFound):
							return tree.NewDString(fmt.Sprintf("unknown (OID=%s)", oidArg)), nil
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
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
		},
	),

	"col_description": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "table_oid", Typ: types.Oid}, {Name: "column_number", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			// Note: the following is equivalent to:
			//
			// SELECT description FROM pg_catalog.pg_description
			//  WHERE objoid=$1 AND objsubid=$2 LIMIT 1
			//
			// TODO(jordanlewis): Really we'd like to query this directly
			// on pg_description and let predicate push-down do its job.
			Body: fmt.Sprintf(
				`SELECT comment
				 FROM system.public.comments c
				 WHERE c.type=%[1]d
				 AND c.object_id=$1::int
				 AND c.sub_id=$2::int
				 AND $1 < %[2]d /* Virtual table columns do not have descriptions. */
				 AND $2 != 0 /* Column ID 0 never exists, and we don't want the query
					              to pick up the table comment by accident. */
				 LIMIT 1`,
				catalogkeys.ColumnCommentType,
				catconstants.MinVirtualID),
			Info: "Returns the comment for a table column, which is specified by the OID of its table and its column number. " +
				"(obj_description cannot be used for table columns, since columns do not have OIDs of their own.)",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
			Language:          tree.RoutineLangSQL,
		},
	),

	"obj_description": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "object_oid", Typ: types.Oid}},
			ReturnType: tree.FixedReturnType(types.String),
			Body: `SELECT description
						 FROM pg_catalog.pg_description
						 WHERE objoid = $1
						 AND objsubid = 0
						 LIMIT 1`,
			Info: "Returns the comment for a database object specified by its OID alone. " +
				"This is deprecated since there is no guarantee that OIDs are unique across different system catalogs; " +
				"therefore, the wrong comment might be returned.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
			Language:          tree.RoutineLangSQL,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "object_oid", Typ: types.Oid}, {Name: "catalog_name", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Body: `SELECT d.description
							FROM pg_catalog.pg_description d
							JOIN pg_catalog.pg_class c
							ON d.classoid = c.oid
							JOIN pg_catalog.pg_namespace n
							ON c.relnamespace = n.oid
							WHERE d.objoid = $1
							AND c.relname = $2
							AND n.nspname = 'pg_catalog'
							AND d.objsubid = 0
							LIMIT 1`,
			Info: "Returns the comment for a database object specified by its OID and the name of the containing system catalog. " +
				"For example, obj_description(123456, 'pg_class') would retrieve the comment for the table with OID 123456.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
			Language:          tree.RoutineLangSQL,
		},
	),

	"shobj_description": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "object_oid", Typ: types.Oid}, {Name: "catalog_name", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Body: `SELECT d.description
							FROM pg_catalog.pg_shdescription d
							JOIN pg_catalog.pg_class c
							ON d.classoid = c.oid
							JOIN pg_catalog.pg_namespace n
							ON c.relnamespace = n.oid
							WHERE d.objoid = $1
							AND c.relname = $2
							AND n.nspname = 'pg_catalog'
							LIMIT 1`,
			Info: "Returns the comment for a shared database object specified by its OID and the name of the containing system catalog. " +
				"This is just like obj_description except that it is used for retrieving comments on shared objects (e.g. databases). ",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
			Language:          tree.RoutineLangSQL,
		},
	),

	"pg_try_advisory_lock": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "int", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ context.Context, _ *eval.Context, _ tree.Datums) (tree.Datum, error) {
				return tree.DBoolTrue, nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Volatile,
		},
	),

	"pg_advisory_unlock": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "key", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ context.Context, _ *eval.Context, _ tree.Datums) (tree.Datum, error) {
				return tree.DBoolTrue, nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "key1", Typ: types.Int4}, {Name: "key2", Typ: types.Int4}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ context.Context, _ *eval.Context, _ tree.Datums) (tree.Datum, error) {
				return tree.DBoolTrue, nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Volatile,
		},
	),

	"pg_advisory_unlock_shared": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "key", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ context.Context, _ *eval.Context, _ tree.Datums) (tree.Datum, error) {
				return tree.DBoolTrue, nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "key1", Typ: types.Int4}, {Name: "key2", Typ: types.Int4}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ context.Context, _ *eval.Context, _ tree.Datums) (tree.Datum, error) {
				return tree.DBoolTrue, nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Volatile,
		},
	),

	"pg_advisory_unlock_all": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Void),
			Fn: func(_ context.Context, _ *eval.Context, _ tree.Datums) (tree.Datum, error) {
				return tree.DVoidDatum, nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Volatile,
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-string.html
	// CockroachDB supports just UTF8 for now.
	"pg_client_encoding": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, _ tree.Datums) (tree.Datum, error) {
				return tree.NewDString("UTF8"), nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Stable,
		},
	),

	// pg_function_is_visible returns true if the input oid corresponds to a
	// builtin function that is part of the databases on the search path.
	// https://www.postgresql.org/docs/9.6/static/functions-info.html
	"pg_function_is_visible": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "oid", Typ: types.Oid}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Body: `SELECT n.nspname = any current_schemas(true)
             FROM pg_catalog.pg_proc p
             INNER LOOKUP JOIN pg_catalog.pg_namespace n
             ON p.pronamespace = n.oid
             WHERE p.oid=$1 LIMIT 1`,
			CalledOnNullInput: true,
			Info:              "Returns whether the function with the given OID belongs to one of the schemas on the search path.",
			Volatility:        volatility.Stable,
			Language:          tree.RoutineLangSQL,
		},
	),
	// pg_table_is_visible returns true if the input oid corresponds to a table
	// that is part of the schemas on the search path.
	// https://www.postgresql.org/docs/9.6/static/functions-info.html
	"pg_table_is_visible": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "oid", Typ: types.Oid}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Body: `SELECT n.nspname = any current_schemas(true)
             FROM pg_catalog.pg_class c
             INNER LOOKUP JOIN pg_catalog.pg_namespace n
             ON c.relnamespace = n.oid
             WHERE c.oid=$1 LIMIT 1`,
			CalledOnNullInput: true,
			Info:              "Returns whether the table with the given OID belongs to one of the schemas on the search path.",
			Volatility:        volatility.Stable,
			Language:          tree.RoutineLangSQL,
		},
	),

	// pg_type_is_visible returns true if the input oid corresponds to a type
	// that is part of the databases on the search path, or NULL if no such type
	// exists. CockroachDB doesn't support the notion of type visibility for
	// builtin types, so we  always return true for those. For user-defined types,
	// we consult pg_type.
	// https://www.postgresql.org/docs/9.6/static/functions-info.html
	"pg_type_is_visible": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "oid", Typ: types.Oid}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Body: `SELECT n.nspname = any current_schemas(true)
             FROM pg_catalog.pg_type t
             INNER LOOKUP JOIN pg_catalog.pg_namespace n
             ON t.typnamespace = n.oid
             WHERE t.oid=$1 LIMIT 1`,
			CalledOnNullInput: true,
			Info:              "Returns whether the type with the given OID belongs to one of the schemas on the search path.",
			Volatility:        volatility.Stable,
			Language:          tree.RoutineLangSQL,
		},
	),

	"pg_relation_is_updatable": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "reloid", Typ: types.Oid}, {Name: "include_triggers", Typ: types.Bool}},
			ReturnType: tree.FixedReturnType(types.Int4),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				ret, err := evalCtx.CatalogBuiltins.PGRelationIsUpdatable(ctx, tree.MustBeDOid(args[0]))
				if err != nil {
					return nil, err
				}
				return ret, nil
			},
			Info:       `Returns the update events the relation supports.`,
			Volatility: volatility.Stable,
		},
	),

	"pg_column_is_updatable": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "reloid", Typ: types.Oid},
				{Name: "attnum", Typ: types.Int2},
				{Name: "include_triggers", Typ: types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				ret, err := evalCtx.CatalogBuiltins.PGColumnIsUpdatable(ctx, tree.MustBeDOid(args[0]), tree.MustBeDInt(args[1]))
				if err != nil {
					return nil, err
				}
				return ret, nil
			},
			Info:       `Returns whether the given column can be updated.`,
			Volatility: volatility.Stable,
		},
	),

	"pg_sleep": makeBuiltin(
		tree.FunctionProperties{},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "seconds", Typ: types.Float}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				durationNanos := int64(float64(*args[0].(*tree.DFloat)) * float64(1000000000))
				dur := time.Duration(durationNanos)
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(dur):
					return tree.DBoolTrue, nil
				}
			},
			Info: "pg_sleep makes the current session's process sleep until " +
				"seconds seconds have elapsed. seconds is a value of type " +
				"double precision, so fractional-second delays can be specified.",
			Volatility: volatility.Volatile,
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

	// pg_encoding_max_length returns the maximum length of a given encoding. For CRDB's use case,
	// we only support UTF8; so, this will return the max_length of UTF8 - which is 4.
	// https://github.com/postgres/postgres/blob/master/src/common/wchar.c
	"pg_encoding_max_length": makeBuiltin(
		tree.FunctionProperties{},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "encoding", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if cmp, err := args[0].Compare(ctx, evalCtx, DatEncodingUTFId); err != nil {
					return tree.DNull, err
				} else if cmp == 0 {
					return tree.NewDInt(4), nil
				}
				return tree.DNull, nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Immutable,
		},
	),

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
		paramTypeOpts{{"table", strOrOidTypes}},
		func(ctx context.Context, evalCtx *eval.Context, args tree.Datums, user username.SQLUsername) (eval.HasAnyPrivilegeResult, error) {
			tableArg := eval.UnwrapDatum(ctx, evalCtx, args[0])
			specifier, err := tableHasPrivilegeSpecifier(tableArg, false /* isSequence */)
			if err != nil {
				return eval.HasNoPrivilege, err
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
				return eval.HasNoPrivilege, err
			}
			return evalCtx.Planner.HasAnyPrivilegeForSpecifier(ctx, specifier, user, privs)
		},
	),

	"has_column_privilege": makePGPrivilegeInquiryDef(
		"column",
		paramTypeOpts{{"table", strOrOidTypes}, {"column", []*types.T{types.String, types.Int}}},
		func(ctx context.Context, evalCtx *eval.Context, args tree.Datums, user username.SQLUsername) (eval.HasAnyPrivilegeResult, error) {
			tableArg := eval.UnwrapDatum(ctx, evalCtx, args[0])
			colArg := eval.UnwrapDatum(ctx, evalCtx, args[1])
			specifier, err := columnHasPrivilegeSpecifier(tableArg, colArg)
			if err != nil {
				return eval.HasNoPrivilege, err
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
				return eval.HasNoPrivilege, err
			}
			return evalCtx.Planner.HasAnyPrivilegeForSpecifier(ctx, specifier, user, privs)
		},
	),

	"has_database_privilege": makePGPrivilegeInquiryDef(
		"database",
		paramTypeOpts{{"database", strOrOidTypes}},
		func(ctx context.Context, evalCtx *eval.Context, args tree.Datums, user username.SQLUsername) (eval.HasAnyPrivilegeResult, error) {

			databaseArg := eval.UnwrapDatum(ctx, evalCtx, args[0])
			specifier, err := databaseHasPrivilegeSpecifier(databaseArg)
			if err != nil {
				return eval.HasNoPrivilege, err
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
				return eval.HasNoPrivilege, err
			}

			return evalCtx.Planner.HasAnyPrivilegeForSpecifier(ctx, specifier, user, privs)
		},
	),

	"has_foreign_data_wrapper_privilege": makePGPrivilegeInquiryDef(
		"foreign-data wrapper",
		paramTypeOpts{{"fdw", strOrOidTypes}},
		func(ctx context.Context, evalCtx *eval.Context, args tree.Datums, user username.SQLUsername) (eval.HasAnyPrivilegeResult, error) {
			fdwArg := eval.UnwrapDatum(ctx, evalCtx, args[0])
			fdw, err := getNameForArg(ctx, evalCtx, fdwArg, "pg_foreign_data_wrapper", "fdwname")
			if err != nil {
				return eval.HasNoPrivilege, err
			}
			retNull := false
			if fdw == "" {
				switch fdwArg.(type) {
				case *tree.DString:
					return eval.HasNoPrivilege, pgerror.Newf(pgcode.UndefinedObject,
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
				return eval.HasNoPrivilege, err
			}
			if retNull {
				return eval.ObjectNotFound, nil
			}
			// All users have USAGE privileges for all foreign-data wrappers.
			_ = privs
			return eval.HasPrivilege, nil
		},
	),

	"has_function_privilege": makePGPrivilegeInquiryDef(
		"function",
		paramTypeOpts{{"function", strOrOidTypes}},
		func(ctx context.Context, evalCtx *eval.Context, args tree.Datums, user username.SQLUsername) (eval.HasAnyPrivilegeResult, error) {
			oidArg := eval.UnwrapDatum(ctx, evalCtx, args[0])
			// When specifying a function by a text string rather than by OID,
			// the allowed input is the same as for the regprocedure data type.
			var oid tree.Datum
			switch t := oidArg.(type) {
			case *tree.DString:
				var err error
				oid, err = eval.ParseDOid(ctx, evalCtx, string(*t), types.RegProcedure)
				if err != nil {
					return eval.HasNoPrivilege, err
				}
			case *tree.DOid:
				oid = t
			}

			// Check if the function OID actually exists.
			_, _, err := evalCtx.Planner.ResolveFunctionByOID(ctx, oid.(*tree.DOid).Oid)
			if err != nil {
				if errors.Is(err, tree.ErrRoutineUndefined) {
					return eval.ObjectNotFound, nil
				}
				return eval.HasNoPrivilege, err
			}

			specifier := eval.HasPrivilegeSpecifier{FunctionOID: &oid.(*tree.DOid).Oid}
			privs, err := parsePrivilegeStr(args[1], privMap{
				"EXECUTE":                   {Kind: privilege.EXECUTE},
				"EXECUTE WITH GRANT OPTION": {Kind: privilege.EXECUTE, GrantOption: true},
			})
			if err != nil {
				return eval.HasNoPrivilege, err
			}

			// For user-defined function, utilize the descriptor based way.
			if catid.IsOIDUserDefined(oid.(*tree.DOid).Oid) {
				return evalCtx.Planner.HasAnyPrivilegeForSpecifier(ctx, specifier, evalCtx.SessionData().User(), privs)
			}

			// For builtin functions, all users should have `EXECUTE` privilege, but
			// no one can grant on them.
			for _, priv := range privs {
				if !priv.GrantOption {
					return eval.HasPrivilege, nil
				}
			}
			return eval.HasNoPrivilege, nil
		},
	),

	"has_language_privilege": makePGPrivilegeInquiryDef(
		"language",
		paramTypeOpts{{"language", strOrOidTypes}},
		func(ctx context.Context, evalCtx *eval.Context, args tree.Datums, user username.SQLUsername) (eval.HasAnyPrivilegeResult, error) {
			langArg := eval.UnwrapDatum(ctx, evalCtx, args[0])
			lang, err := getNameForArg(ctx, evalCtx, langArg, "pg_language", "lanname")
			if err != nil {
				return eval.HasNoPrivilege, err
			}
			retNull := false
			if lang == "" {
				switch langArg.(type) {
				case *tree.DString:
					return eval.HasNoPrivilege, pgerror.Newf(pgcode.UndefinedObject,
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
				return eval.HasNoPrivilege, err
			}
			if retNull {
				return eval.ObjectNotFound, nil
			}
			// All users have USAGE privileges for all languages.
			_ = privs
			return eval.HasPrivilege, nil
		},
	),

	"has_schema_privilege": makePGPrivilegeInquiryDef(
		"schema",
		paramTypeOpts{{"schema", strOrOidTypes}},
		func(ctx context.Context, evalCtx *eval.Context, args tree.Datums, user username.SQLUsername) (eval.HasAnyPrivilegeResult, error) {
			schemaArg := eval.UnwrapDatum(ctx, evalCtx, args[0])
			databaseName := evalCtx.SessionData().Database
			specifier, err := schemaHasPrivilegeSpecifier(ctx, evalCtx, schemaArg, databaseName)
			if err != nil {
				return eval.HasNoPrivilege, err
			}

			privs, err := parsePrivilegeStr(args[1], privMap{
				"CREATE":                   {Kind: privilege.CREATE},
				"CREATE WITH GRANT OPTION": {Kind: privilege.CREATE, GrantOption: true},
				"USAGE":                    {Kind: privilege.USAGE},
				"USAGE WITH GRANT OPTION":  {Kind: privilege.USAGE, GrantOption: true},
			})
			if err != nil {
				return eval.HasNoPrivilege, err
			}
			if len(databaseName) == 0 {
				// If no database is set, return NULL.
				return eval.ObjectNotFound, nil
			}

			return evalCtx.Planner.HasAnyPrivilegeForSpecifier(ctx, specifier, user, privs)
		},
	),

	"has_sequence_privilege": makePGPrivilegeInquiryDef(
		"sequence",
		paramTypeOpts{{"sequence", strOrOidTypes}},
		func(ctx context.Context, evalCtx *eval.Context, args tree.Datums, user username.SQLUsername) (eval.HasAnyPrivilegeResult, error) {
			seqArg := eval.UnwrapDatum(ctx, evalCtx, args[0])
			specifier, err := tableHasPrivilegeSpecifier(seqArg, true /* isSequence */)
			if err != nil {
				return eval.HasNoPrivilege, err
			}
			privs, err := parsePrivilegeStr(args[1], privMap{
				// Sequences and other table objects cannot be given a USAGE privilege,
				// so we check for SELECT here instead. See privilege.TablePrivileges.
				"USAGE":                    {Kind: privilege.USAGE},
				"USAGE WITH GRANT OPTION":  {Kind: privilege.USAGE, GrantOption: true},
				"SELECT":                   {Kind: privilege.SELECT},
				"SELECT WITH GRANT OPTION": {Kind: privilege.SELECT, GrantOption: true},
				"UPDATE":                   {Kind: privilege.UPDATE},
				"UPDATE WITH GRANT OPTION": {Kind: privilege.UPDATE, GrantOption: true},
			})
			if err != nil {
				return eval.HasPrivilege, err
			}
			return evalCtx.Planner.HasAnyPrivilegeForSpecifier(ctx, specifier, user, privs)
		},
	),

	"has_server_privilege": makePGPrivilegeInquiryDef(
		"foreign server",
		paramTypeOpts{{"server", strOrOidTypes}},
		func(ctx context.Context, evalCtx *eval.Context, args tree.Datums, user username.SQLUsername) (eval.HasAnyPrivilegeResult, error) {
			serverArg := eval.UnwrapDatum(ctx, evalCtx, args[0])
			server, err := getNameForArg(ctx, evalCtx, serverArg, "pg_foreign_server", "srvname")
			if err != nil {
				return eval.HasNoPrivilege, err
			}
			retNull := false
			if server == "" {
				switch serverArg.(type) {
				case *tree.DString:
					return eval.HasNoPrivilege, pgerror.Newf(pgcode.UndefinedObject,
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
				return eval.HasNoPrivilege, err
			}
			if retNull {
				return eval.ObjectNotFound, nil
			}
			// All users have USAGE privileges for all foreign servers.
			_ = privs
			return eval.HasPrivilege, nil
		},
	),

	"has_table_privilege": makePGPrivilegeInquiryDef(
		"table",
		paramTypeOpts{{"table", strOrOidTypes}},
		func(ctx context.Context, evalCtx *eval.Context, args tree.Datums, user username.SQLUsername) (eval.HasAnyPrivilegeResult, error) {
			tableArg := eval.UnwrapDatum(ctx, evalCtx, args[0])
			specifier, err := tableHasPrivilegeSpecifier(tableArg, false /* isSequence */)
			if err != nil {
				return eval.HasNoPrivilege, err
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
				return eval.HasNoPrivilege, err
			}
			return evalCtx.Planner.HasAnyPrivilegeForSpecifier(ctx, specifier, user, privs)
		},
	),

	"has_tablespace_privilege": makePGPrivilegeInquiryDef(
		"tablespace",
		paramTypeOpts{{"tablespace", strOrOidTypes}},
		func(ctx context.Context, evalCtx *eval.Context, args tree.Datums, user username.SQLUsername) (eval.HasAnyPrivilegeResult, error) {
			tablespaceArg := eval.UnwrapDatum(ctx, evalCtx, args[0])
			tablespace, err := getNameForArg(ctx, evalCtx, tablespaceArg, "pg_tablespace", "spcname")
			if err != nil {
				return eval.HasNoPrivilege, err
			}
			retNull := false
			if tablespace == "" {
				switch tablespaceArg.(type) {
				case *tree.DString:
					return eval.HasNoPrivilege, pgerror.Newf(pgcode.UndefinedObject,
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
				return eval.HasNoPrivilege, err
			}
			if retNull {
				return eval.ObjectNotFound, nil
			}
			// All users have CREATE privileges in all tablespaces.
			_ = privs
			return eval.HasPrivilege, nil
		},
	),

	"has_type_privilege": makePGPrivilegeInquiryDef(
		"type",
		paramTypeOpts{{"type", strOrOidTypes}},
		func(ctx context.Context, evalCtx *eval.Context, args tree.Datums, user username.SQLUsername) (eval.HasAnyPrivilegeResult, error) {
			oidArg := eval.UnwrapDatum(ctx, evalCtx, args[0])
			// When specifying a type by a text string rather than by OID, the
			// allowed input is the same as for the regtype data type.
			var oid tree.Datum
			switch t := oidArg.(type) {
			case *tree.DString:
				var err error
				oid, err = eval.ParseDOid(ctx, evalCtx, string(*t), types.RegType)
				if err != nil {
					return eval.HasNoPrivilege, err
				}
			case *tree.DOid:
				oid = t
			}

			typ, err := getNameForArg(ctx, evalCtx, oid, "pg_type", "typname")
			if err != nil {
				return eval.HasNoPrivilege, err
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
				return eval.HasNoPrivilege, err
			}
			if retNull {
				return eval.ObjectNotFound, nil
			}
			// All users have USAGE privileges to all types.
			_ = privs
			return eval.HasPrivilege, nil
		},
	),

	"pg_has_role": makePGPrivilegeInquiryDef(
		"role",
		paramTypeOpts{{"role", strOrOidTypes}},
		func(ctx context.Context, evalCtx *eval.Context, args tree.Datums, user username.SQLUsername) (eval.HasAnyPrivilegeResult, error) {
			roleArg := eval.UnwrapDatum(ctx, evalCtx, args[0])
			roleS, err := getNameForArg(ctx, evalCtx, roleArg, "pg_roles", "rolname")
			if err != nil {
				return eval.HasNoPrivilege, err
			}
			// Note: the username in pg_roles is already normalized, so we can safely
			// turn it into a SQLUsername without re-normalization.
			role := username.MakeSQLUsernameFromPreNormalizedString(roleS)
			if role.Undefined() {
				switch roleArg.(type) {
				case *tree.DString:
					return eval.HasNoPrivilege, pgerror.Newf(pgcode.UndefinedObject,
						"role %s does not exist", roleArg)
				case *tree.DOid:
					// Postgres returns NULL if no matching role is found when given an
					// OID.
					return eval.ObjectNotFound, nil
				}
			}

			privStrs := normalizePrivilegeStr(args[1])
			for _, privStr := range privStrs {
				var hasAnyPrivilegeResult eval.HasAnyPrivilegeResult
				var err error
				switch privStr {
				case "USAGE":
					hasAnyPrivilegeResult, err = hasPrivsOfRole(ctx, evalCtx, user, role)
				case "MEMBER":
					hasAnyPrivilegeResult, err = isMemberOfRole(ctx, evalCtx, user, role)
				case
					"USAGE WITH GRANT OPTION",
					"USAGE WITH ADMIN OPTION",
					"MEMBER WITH GRANT OPTION",
					"MEMBER WITH ADMIN OPTION":
					hasAnyPrivilegeResult, err = isAdminOfRole(ctx, evalCtx, user, role)
				default:
					return eval.HasNoPrivilege, pgerror.Newf(pgcode.InvalidParameterValue,
						"unrecognized privilege type: %q", privStr)
				}
				if err != nil {
					return eval.HasNoPrivilege, err
				}
				if hasAnyPrivilegeResult == eval.HasPrivilege {
					return hasAnyPrivilegeResult, nil
				}
			}
			return eval.HasNoPrivilege, nil
		},
	),

	// See https://www.postgresql.org/docs/10/functions-admin.html#FUNCTIONS-ADMIN-SET
	"current_setting": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemInfo,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "setting_name", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return getSessionVar(ctx, evalCtx, string(tree.MustBeDString(args[0])), false /* missingOk */)
			},
			Info:       builtinconstants.CategorySystemInfo,
			Volatility: volatility.Stable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "setting_name", Typ: types.String}, {Name: "missing_ok", Typ: types.Bool}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return getSessionVar(ctx, evalCtx, string(tree.MustBeDString(args[0])), bool(tree.MustBeDBool(args[1])))
			},
			Info:       builtinconstants.CategorySystemInfo,
			Volatility: volatility.Stable,
		},
	),

	// See https://www.postgresql.org/docs/10/functions-admin.html#FUNCTIONS-ADMIN-SET
	"set_config": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemInfo,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "setting_name", Typ: types.String}, {Name: "new_value", Typ: types.String}, {Name: "is_local", Typ: types.Bool}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				varName := string(tree.MustBeDString(args[0]))
				newValue := string(tree.MustBeDString(args[1]))
				err := setSessionVar(ctx, evalCtx, varName, newValue, bool(tree.MustBeDBool(args[2])))
				if err != nil {
					return nil, err
				}
				return getSessionVar(ctx, evalCtx, varName, false /* missingOk */)
			},
			Info:       builtinconstants.CategorySystemInfo,
			Volatility: volatility.Volatile,
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
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.NewDIPAddr(tree.DIPAddr{IPAddr: ipaddr.IPAddr{}}), nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Stable,
		},
	),

	"inet_client_port": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.DZero, nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Stable,
		},
	),

	"inet_server_addr": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.NewDIPAddr(tree.DIPAddr{IPAddr: ipaddr.IPAddr{}}), nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Stable,
		},
	),

	"inet_server_port": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.DZero, nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Stable,
		},
	),

	"pg_blocking_pids": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.IntArray),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.NewDArray(types.Int), nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Stable,
		},
	),

	// pg_column_size(any) - number of bytes used to store a particular value
	// (possibly compressed)

	// Database Object Size Functions, see: https://www.postgresql.org/docs/9.4/functions-admin.html
	"pg_column_size": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.VariadicType{
				VarType: types.AnyElement,
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				var totalSize int
				for _, arg := range args {
					encodeTableValue, err := valueside.Encode(nil, valueside.NoColumnID, arg)
					if err != nil {
						return tree.DNull, err
					}
					totalSize += len(encodeTableValue)
				}
				return tree.NewDInt(tree.DInt(totalSize)), nil
			},
			Info:       "Return size in bytes of the column provided as an argument",
			Volatility: volatility.Immutable,
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
			Types: tree.ParamTypes{
				{Name: "typid", Typ: types.Oid},
				{Name: "typmod", Typ: types.Int4},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				typid := args[0].(*tree.DOid).Oid
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
			Volatility: volatility.Immutable,
		},
	),

	// Given an index's OID and an underlying-table column number,
	// _pg_index_position return the column's position in the index
	// (or NULL if not there).
	//
	// NOTE: this is defined as a UDF, same as in Postgres:
	// https://github.com/postgres/postgres/blob/master/src/backend/catalog/information_schema.sql
	"information_schema._pg_index_position": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "oid", Typ: types.Oid},
				{Name: "col", Typ: types.Int2},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Body: `SELECT (ss.a).n FROM
		         (SELECT information_schema._pg_expandarray(indkey) AS a
			        FROM pg_catalog.pg_index WHERE indexrelid = $1) ss
			       WHERE (ss.a).x = $2`,
			Info:       notUsableInfo,
			Volatility: volatility.Stable,
			Language:   tree.RoutineLangSQL,
		},
	),

	"information_schema._pg_numeric_precision": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "typid", Typ: types.Oid},
				{Name: "typmod", Typ: types.Int4},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				typid := tree.MustBeDOid(args[0]).Oid
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
						// This logic matches the postgres implementation
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
			Volatility: volatility.Immutable,
		},
	),

	"information_schema._pg_numeric_precision_radix": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "typid", Typ: types.Oid},
				{Name: "typmod", Typ: types.Int4},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				typid := tree.MustBeDOid(args[0]).Oid
				if typid == oid.T_int2 || typid == oid.T_int4 || typid == oid.T_int8 || typid == oid.T_float4 || typid == oid.T_float8 {
					return tree.NewDInt(2), nil
				} else if typid == oid.T_numeric {
					return tree.NewDInt(10), nil
				} else {
					return tree.DNull, nil
				}
			},
			Info:       "Returns the radix of the given type with type modifier",
			Volatility: volatility.Immutable,
		},
	),

	"information_schema._pg_numeric_scale": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "typid", Typ: types.Oid},
				{Name: "typmod", Typ: types.Int4},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				typid := tree.MustBeDOid(args[0]).Oid
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
			Volatility: volatility.Immutable,
		},
	),

	// https://github.com/postgres/postgres/blob/master/src/backend/catalog/information_schema.sql
	"information_schema._pg_char_octet_length": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "typid", Typ: types.Oid},
				{Name: "typmod", Typ: types.Int4},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Body: `SELECT
						 CASE WHEN $1 IN (25, 1042, 1043) /* text, char, varchar */
			            THEN CASE WHEN $2 = -1 /* default typmod */
														THEN CAST(2^30 AS integer)
			                    	ELSE information_schema._pg_char_max_length($1, $2) *
			                           pg_catalog.pg_encoding_max_length((SELECT encoding FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()))
			                 END
			            ELSE null
			       END`,
			Info:              notUsableInfo,
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
			Language:          tree.RoutineLangSQL,
		},
	),

	// NOTE: this could be defined as a user-defined function, like
	// it is in Postgres:
	// https://github.com/postgres/postgres/blob/master/src/backend/catalog/information_schema.sql
	// CREATE FUNCTION _pg_datetime_precision(typid oid, typmod int4) RETURNS integer
	//     LANGUAGE sql
	//     IMMUTABLE
	//     PARALLEL SAFE
	//     RETURNS NULL ON NULL INPUT
	// RETURN
	//   CASE WHEN $1 IN (1082) /* date */
	// 						THEN 0
	// 	 			WHEN $1 IN (1083, 1114, 1184, 1266) /* time, timestamp, same + tz */
	// 						THEN CASE WHEN $2 < 0 THEN 6 ELSE $2 END
	// 				WHEN $1 IN (1186) /* interval */
	// 						THEN CASE WHEN $2 < 0 OR $2 & 0xFFFF = 0xFFFF THEN 6 ELSE $2 & 0xFFFF END
	// 				ELSE null
	// END;
	"information_schema._pg_datetime_precision": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "typid", Typ: types.Oid},
				{Name: "typmod", Typ: types.Int4},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				typid := args[0].(*tree.DOid).Oid
				typmod := *args[1].(*tree.DInt)
				if typid == oid.T_date {
					return tree.DZero, nil
				} else if typid == oid.T_time || typid == oid.T_timestamp || typid == oid.T_timestamptz || typid == oid.T_timetz {
					if typmod < 0 {
						return tree.NewDInt(6), nil
					}
					return tree.NewDInt(typmod), nil
				} else if typid == oid.T_interval {
					if typmod < 0 || (typmod&0xFFFF) == 0xFFFF {
						return tree.NewDInt(6), nil
					}
					return tree.NewDInt(typmod & 0xFFFF), nil
				}
				return tree.DNull, nil
			},
			Info:       notUsableInfo,
			Volatility: volatility.Immutable,
		},
	),

	// https://github.com/postgres/postgres/blob/master/src/backend/catalog/information_schema.sql
	"information_schema._pg_interval_type": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "typid", Typ: types.Oid},
				{Name: "typmod", Typ: types.Int4},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Body: `SELECT
						 CASE WHEN $1 IN (1186) /* interval */
			 								THEN pg_catalog.upper(substring(pg_catalog.format_type($1, $2), 'interval[()0-9]* #"%#"', '#')) 
			        		ELSE null
  			     END`,
			Info:              notUsableInfo,
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
			Language:          tree.RoutineLangSQL,
		},
	),

	"nameconcatoid": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "name", Typ: types.String}, {Name: "oid", Typ: types.Oid}},
			ReturnType: tree.FixedReturnType(types.Name),
			Body: `
SELECT
  CASE WHEN length($1::text || '_' || $2::text) > 63
	THEN (substring($1 from 1 for 63 - length($2::text) - 1) || '_' || $2::text)::name
	ELSE ($1::text || '_' || $2::text)::name
	END
`,
			Info: "Used in the information_schema to produce specific_name " +
				"columns, which are supposed to be unique per schema. " +
				"The result is the same as ($1::text || '_' || $2::text)::name " +
				"except that, if it would not fit in 63 characters, we make it do so " +
				"by truncating the name input (not the oid).",
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
			Language:          tree.RoutineLangSQL,
		},
	),
}

func getSessionVar(
	ctx context.Context, evalCtx *eval.Context, settingName string, missingOk bool,
) (tree.Datum, error) {
	if evalCtx.SessionAccessor == nil {
		return nil, errors.AssertionFailedf("session accessor not set")
	}
	ok, s, err := evalCtx.SessionAccessor.GetSessionVar(ctx, settingName, missingOk)
	if err != nil {
		return nil, err
	}
	if !ok {
		return tree.DNull, nil
	}
	return tree.NewDString(s), nil
}

func setSessionVar(
	ctx context.Context, evalCtx *eval.Context, settingName, newVal string, isLocal bool,
) error {
	if evalCtx.SessionAccessor == nil {
		return errors.AssertionFailedf("session accessor not set")
	}
	return evalCtx.SessionAccessor.SetSessionVar(ctx, settingName, newVal, isLocal)
}

func databaseHasPrivilegeSpecifier(databaseArg tree.Datum) (eval.HasPrivilegeSpecifier, error) {
	var specifier eval.HasPrivilegeSpecifier
	switch t := databaseArg.(type) {
	case *tree.DString:
		s := string(*t)
		specifier.DatabaseName = &s
	case *tree.DOid:
		oidVal := t.Oid
		specifier.DatabaseOID = &oidVal
	default:
		return specifier, errors.AssertionFailedf("unknown privilege specifier: %#v", databaseArg)
	}
	return specifier, nil
}

// tableHasPrivilegeSpecifier returns the HasPrivilegeSpecifier for
// the given table.
func tableHasPrivilegeSpecifier(
	tableArg tree.Datum, isSequence bool,
) (eval.HasPrivilegeSpecifier, error) {
	specifier := eval.HasPrivilegeSpecifier{
		IsSequence: &isSequence,
	}
	switch t := tableArg.(type) {
	case *tree.DString:
		s := string(*t)
		specifier.TableName = &s
	case *tree.DOid:
		oidVal := t.Oid
		specifier.TableOID = &oidVal
	default:
		return specifier, errors.AssertionFailedf("unknown privilege specifier: %#v", tableArg)
	}
	return specifier, nil
}

// Note that we only verify the column exists for has_column_privilege.
func columnHasPrivilegeSpecifier(
	tableArg tree.Datum, colArg tree.Datum,
) (eval.HasPrivilegeSpecifier, error) {
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
	ctx context.Context, evalCtx *eval.Context, schemaArg tree.Datum, databaseName string,
) (eval.HasPrivilegeSpecifier, error) {
	specifier := eval.HasPrivilegeSpecifier{
		SchemaDatabaseName: &databaseName,
	}
	var schemaIsRequired bool
	switch t := schemaArg.(type) {
	case *tree.DString:
		s := string(*t)
		specifier.SchemaName = &s
		schemaIsRequired = true
	case *tree.DOid:
		schemaName, err := getNameForArg(ctx, evalCtx, schemaArg, "pg_namespace", "nspname")
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
			Types: tree.ParamTypes{
				{Name: "pg_attribute", Typ: types.AnyTuple},
				{Name: "pg_type", Typ: types.AnyTuple},
			},
			ReturnType: tree.FixedReturnType(retType),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
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
	ctx context.Context, evalCtx *eval.Context, user, role username.SQLUsername,
) (eval.HasAnyPrivilegeResult, error) {
	return isMemberOfRole(ctx, evalCtx, user, role)
}

// isMemberOfRole returns whether the user is a member of the specified role
// (directly or indirectly).
//
// This is defined to recurse through roles regardless of rolinherit.
func isMemberOfRole(
	ctx context.Context, evalCtx *eval.Context, user, role username.SQLUsername,
) (eval.HasAnyPrivilegeResult, error) {
	// Fast path for simple case.
	if user == role {
		return eval.HasPrivilege, nil
	}

	// Superusers have every privilege and are part of every role.
	if isSuper, err := evalCtx.Planner.UserHasAdminRole(ctx, user); err != nil {
		return eval.HasNoPrivilege, err
	} else if isSuper {
		return eval.HasPrivilege, nil
	}

	allRoleMemberships, err := evalCtx.Planner.MemberOfWithAdminOption(ctx, user)
	if err != nil {
		return eval.HasNoPrivilege, err
	}
	_, member := allRoleMemberships[role]
	if member {
		return eval.HasPrivilege, nil
	}
	return eval.HasNoPrivilege, nil
}

// isAdminOfRole returns whether the user is an admin of the specified role.
//
// That is, is member the role itself (subject to restrictions below), a
// member (directly or indirectly) WITH ADMIN OPTION, or a superuser?
func isAdminOfRole(
	ctx context.Context, evalCtx *eval.Context, user, role username.SQLUsername,
) (eval.HasAnyPrivilegeResult, error) {
	// Superusers are an admin of every role.
	//
	// NB: this is intentionally before the user == role check here.
	if isSuper, err := evalCtx.Planner.UserHasAdminRole(ctx, user); err != nil {
		return eval.HasNoPrivilege, err
	} else if isSuper {
		return eval.HasPrivilege, nil
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
		if isSessionUser := user == evalCtx.SessionData().SessionUser(); isSessionUser {
			return eval.HasPrivilege, nil
		}
		return eval.HasNoPrivilege, nil
	}

	allRoleMemberships, err := evalCtx.Planner.MemberOfWithAdminOption(ctx, user)
	if err != nil {
		return eval.HasNoPrivilege, err
	}
	if isAdmin := allRoleMemberships[role]; isAdmin {
		return eval.HasPrivilege, nil
	}
	return eval.HasNoPrivilege, nil
}
