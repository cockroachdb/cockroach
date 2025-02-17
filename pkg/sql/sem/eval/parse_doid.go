// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// pgSignatureRegexp matches a Postgres function type signature, capturing the
// name of the function into group 1.
// e.g. function(a, b, c) or function( a )
var pgSignatureRegexp = regexp.MustCompile(`^\s*([\w\."]+)\s*\((?:(?:\s*[\w"]+\s*,)*\s*[\w"]+)?\s*\)\s*$`)

// ParseDOid parses and returns an Oid family datum.
func ParseDOid(ctx context.Context, evalCtx *Context, s string, t *types.T) (*tree.DOid, error) {
	if t.Oid() != oid.T_oid && s == tree.UnknownOidName {
		return tree.NewDOidWithType(tree.UnknownOidValue, t), nil
	}

	// If it is an integer in string form, convert it as an int.
	if _, err := tree.ParseDInt(strings.TrimSpace(s)); err == nil {
		tmpOid, err := tree.ParseDOidAsInt(s)
		if err != nil {
			return nil, err
		}
		oidRes, errSafeToIgnore, err := evalCtx.Planner.ResolveOIDFromOID(ctx, t, tmpOid)
		if err != nil {
			if !errSafeToIgnore {
				return nil, err
			}
			oidRes = tmpOid
			*oidRes = tree.MakeDOid(tmpOid.Oid, t)
		}
		return oidRes, nil
	}

	switch t.Oid() {
	case oid.T_regproc:
		// To be compatible with postgres, we always treat the trimmed input string
		// as a function name.
		substrs, err := splitIdentifierList(strings.TrimSpace(s))
		if err != nil {
			return nil, err
		}

		if len(substrs) > 3 {
			// A fully qualified function name in pg's dialect can contain
			// at most 3 parts: db.schema.funname.
			// For example mydb.pg_catalog.max().
			// Anything longer is always invalid.
			return nil, pgerror.Newf(pgcode.Syntax,
				"invalid function name: %s", s)
		}
		name := tree.UnresolvedName{NumParts: len(substrs)}
		for i := 0; i < len(substrs); i++ {
			name.Parts[i] = substrs[len(substrs)-1-i]
		}
		funcDef, err := evalCtx.Planner.ResolveFunction(
			ctx, tree.MakeUnresolvedFunctionName(&name), &evalCtx.SessionData().SearchPath,
		)
		if err != nil {
			return nil, err
		}
		if len(funcDef.Overloads) > 1 {
			return nil, pgerror.Newf(pgcode.AmbiguousAlias,
				"more than one function named '%s'", funcDef.Name)
		}
		if funcDef.UnsupportedWithIssue != 0 {
			return nil, funcDef.MakeUnsupportedError()
		}
		overload := funcDef.Overloads[0]
		return tree.NewDOidWithTypeAndName(overload.Oid, t, funcDef.Name), nil
	case oid.T_regprocedure:
		// Fake a ALTER FUNCTION statement to extract the function signature.
		// We're kinda being lazy here to rely on the parser to determine if the
		// function signature syntax is sane from grammar perspective. We may
		// match postgres' implementation of `parseNameAndArgTypes` to return
		// more detailed errors like "expected a left parenthesis".
		stmt, err := parser.ParseOne("ALTER FUNCTION " + strings.TrimSpace(s) + " IMMUTABLE")
		if err != nil {
			return nil, errors.Wrapf(err, "invalid function signature: %s", s)
		}
		fn := stmt.AST.(*tree.AlterFunctionOptions).Function
		if fn.Params == nil {
			// Always require the full function signature.
			return nil, errors.Newf("invalid function signature: %s", s)
		}

		un := fn.FuncName.ToUnresolvedObjectName().ToUnresolvedName()
		fd, err := evalCtx.Planner.ResolveFunction(
			ctx, tree.MakeUnresolvedFunctionName(un), &evalCtx.SessionData().SearchPath,
		)
		if err != nil {
			return nil, err
		}

		if len(fd.Overloads) == 1 {
			// This is a hack to be compatible with some ORMs which depends on some
			// builtin function not implemented in CRDB. We just use `AnyElement` as the arg
			// type while some ORMs sends more meaningful function signatures whose
			// arg type list mismatch with `AnyElement` type. For this case we just
			// short-circuit it to return the oid. For example, `array_in` is defined
			// to take in a `AnyElement` type, but some ORM sends
			// `'array_in(cstring,oid,integer)'::REGPROCEDURE` for introspection.
			ol := fd.Overloads[0]
			if !catid.IsOIDUserDefined(ol.Oid) &&
				ol.Types.Length() == 1 &&
				ol.Types.GetAt(0).Identical(types.AnyElement) {
				return tree.NewDOidWithTypeAndName(ol.Oid, t, fd.Name), nil
			}
		}

		ol, err := fd.MatchOverload(
			ctx,
			evalCtx.Planner,
			&fn,
			&evalCtx.SessionData().SearchPath,
			tree.BuiltinRoutine|tree.UDFRoutine|tree.ProcedureRoutine,
			false, /* inDropContext */
			false, /* tryDefaultExprs */
		)
		if err != nil {
			return nil, err
		}
		return tree.NewDOidWithTypeAndName(ol.Oid, t, fd.Name), nil
	case oid.T_regtype:
		parsedTyp, err := evalCtx.Planner.GetTypeFromValidSQLSyntax(ctx, s)
		if err == nil {
			return tree.NewDOidWithTypeAndName(
				parsedTyp.Oid(), t, parsedTyp.SQLStandardName(),
			), nil
		}

		// Fall back to searching pg_type, since we don't provide syntax for
		// every postgres type that we understand OIDs for.
		// Note this section does *not* work if there is a schema in front of the
		// type, e.g. "pg_catalog"."int4" (if int4 was not defined).

		// Trim whitespace and unwrap outer quotes if necessary.
		// This is required to mimic postgres.
		s = strings.TrimSpace(s)
		if len(s) > 1 && s[0] == '"' && s[len(s)-1] == '"' {
			s = s[1 : len(s)-1]
		}
		// Trim type modifiers, e.g. `numeric(10,3)` becomes `numeric`.
		s = pgSignatureRegexp.ReplaceAllString(s, "$1")

		dOid, errSafeToIgnore, missingTypeErr := evalCtx.Planner.ResolveOIDFromString(
			ctx, t, tree.NewDString(tree.Name(s).Normalize()),
		)
		if missingTypeErr == nil {
			return dOid, nil
		} else if !errSafeToIgnore {
			return nil, missingTypeErr
		}
		// Fall back to some special cases that we support for compatibility
		// only. Client use syntax like 'sometype'::regtype to produce the oid
		// for a type that they want to search a catalog table for. Since we
		// don't support that type, we return an artificial OID that will never
		// match anything.
		switch s {
		// We don't support triggers, but some tools search for them
		// specifically.
		case "trigger":
		default:
			return nil, missingTypeErr
		}
		// Types we don't support get OID 0, so they won't match anything
		// in catalogs.
		return tree.NewDOidWithTypeAndName(0, t, s), nil

	case oid.T_regclass:
		tn, err := castStringToRegClassTableName(s)
		if err != nil {
			return nil, err
		}
		if id, err := evalCtx.Planner.ResolveTableName(ctx, &tn); err == nil {
			// tree.ID is a uint32, so this type conversion is safe.
			return tree.NewDOidWithTypeAndName(oid.Oid(id), t, tn.ObjectName.String()), nil
		} else if pgerror.GetPGCode(err) != pgcode.UndefinedTable {
			return nil, err
		}
		// If the above resulted in an UndefinedTable error, then we can try
		// searching for an index with this name,
		oidRes, err := indexNameToOID(ctx, evalCtx, tn)
		if err != nil {
			return nil, err
		}
		return tree.NewDOidWithTypeAndName(oidRes, t, tn.ObjectName.String()), nil

	default:
		d, _ /* errSafeToIgnore */, err := evalCtx.Planner.ResolveOIDFromString(ctx, t, tree.NewDString(s))
		return d, err
	}
}

// indexNameToOID finds the OID for the given index. If the name is qualified,
// then the index must belong to that schema. Otherwise, the schemas are
// searched in order of the search_path.
func indexNameToOID(ctx context.Context, evalCtx *Context, tn tree.TableName) (oid.Oid, error) {
	query := `SELECT c.oid FROM %[1]spg_catalog.pg_class AS c
             JOIN %[1]spg_catalog.pg_namespace AS n ON c.relnamespace = n.oid
             WHERE c.relname = $1
             AND n.nspname = $2
             LIMIT 1`
	args := []interface{}{tn.Object(), tn.Schema()}
	if !tn.ExplicitSchema {
		// If there is no explicit schema, then we need a different query that
		// looks for the object name for each schema in the search_path. Choose
		// the first match in the order of the search_path array. There is an
		// unused $2 placeholder in the query so that the call to QueryRow can
		// be consolidated.
		query = `WITH
			  current_schemas AS (
		      SELECT * FROM unnest(current_schemas(true)) WITH ORDINALITY AS scname
		    )
			SELECT c.oid
			FROM %[1]spg_catalog.pg_class AS c
		  JOIN %[1]spg_catalog.pg_namespace AS n ON c.relnamespace = n.oid
		  JOIN current_schemas AS cs ON cs.scname = n.nspname
			WHERE c.relname = $1
			ORDER BY cs.ordinality ASC
			LIMIT 1`
		args = []interface{}{tn.Object()}
	}
	catalogPrefix := ""
	if tn.ExplicitCatalog {
		catalogPrefix = tn.CatalogName.String() + "."
	}
	row, err := evalCtx.Planner.QueryRowEx(
		ctx,
		"regclass-cast",
		sessiondata.NoSessionDataOverride,
		fmt.Sprintf(query, catalogPrefix),
		args...,
	)
	if err != nil {
		return 0, err
	}
	if row == nil {
		return 0, sqlerrors.NewUndefinedRelationError(&tn)
	}
	return tree.MustBeDOid(row[0]).Oid, nil
}

// castStringToRegClassTableName normalizes a TableName from a string.
func castStringToRegClassTableName(s string) (tree.TableName, error) {
	components, err := splitIdentifierList(s)
	if err != nil {
		return tree.TableName{}, err
	}

	if len(components) > 3 {
		return tree.TableName{}, pgerror.Newf(
			pgcode.InvalidName,
			"too many components: %s",
			s,
		)
	}
	var retComponents [3]string
	for i := 0; i < len(components); i++ {
		retComponents[len(components)-1-i] = components[i]
	}
	u, err := tree.NewUnresolvedObjectName(
		len(components),
		retComponents,
		0,
	)
	if err != nil {
		return tree.TableName{}, err
	}
	return u.ToTableName(), nil
}

// splitIdentifierList splits identifiers to individual components, lower
// casing non-quoted identifiers and escaping quoted identifiers as appropriate.
// It is based on PostgreSQL's SplitIdentifier.
func splitIdentifierList(in string) ([]string, error) {
	var pos int
	var ret []string
	const separator = '.'

	for pos < len(in) {
		if isWhitespace(in[pos]) {
			pos++
			continue
		}
		if in[pos] == '"' {
			var b strings.Builder
			// Attempt to find the ending quote. If the quote is double "",
			// fold it into a " character for the str (e.g. "a""" means a").
			for {
				pos++
				endIdx := strings.IndexByte(in[pos:], '"')
				if endIdx == -1 {
					return nil, pgerror.Newf(
						pgcode.InvalidName,
						`invalid name: unclosed ": %s`,
						in,
					)
				}
				b.WriteString(in[pos : pos+endIdx])
				pos += endIdx + 1
				// If we reached the end, or the following character is not ",
				// we can break and assume this is one identifier.
				// There are checks below to ensure EOF or whitespace comes
				// afterward.
				if pos == len(in) || in[pos] != '"' {
					break
				}
				b.WriteByte('"')
			}
			ret = append(ret, b.String())
		} else {
			var b strings.Builder
			for pos < len(in) && in[pos] != separator && !isWhitespace(in[pos]) {
				b.WriteByte(in[pos])
				pos++
			}
			// Anything with no quotations should be lowered.
			ret = append(ret, strings.ToLower(b.String()))
		}

		// Further ignore all white space.
		for pos < len(in) && isWhitespace(in[pos]) {
			pos++
		}

		// At this stage, we expect separator or end of string.
		if pos == len(in) {
			break
		}

		if in[pos] != separator {
			return nil, pgerror.Newf(
				pgcode.InvalidName,
				"invalid name: expected separator %c: %s",
				separator,
				in,
			)
		}

		pos++
	}

	return ret, nil
}

// isWhitespace returns true if the given character is a space.
// This must match parser.SkipWhitespace above.
func isWhitespace(ch byte) bool {
	switch ch {
	case ' ', '\t', '\r', '\f', '\n':
		return true
	}
	return false
}
