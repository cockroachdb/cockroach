// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eval

import (
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/lib/pq/oid"
)

// pgSignatureRegexp matches a Postgres function type signature, capturing the
// name of the function into group 1.
// e.g. function(a, b, c) or function( a )
var pgSignatureRegexp = regexp.MustCompile(`^\s*([\w\."]+)\s*\((?:(?:\s*[\w"]+\s*,)*\s*[\w"]+)?\s*\)\s*$`)

// ParseDOid parses and returns an Oid family datum.
func ParseDOid(ctx *Context, s string, t *types.T) (*tree.DOid, error) {
	// If it is an integer in string form, convert it as an int.
	if _, err := tree.ParseDInt(strings.TrimSpace(s)); err == nil {
		tmpOid, err := tree.ParseDOidAsInt(s)
		if err != nil {
			return nil, err
		}
		oidRes, errSafeToIgnore, err := ctx.Planner.ResolveOIDFromOID(ctx.Ctx(), t, tmpOid)
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
	case oid.T_regproc, oid.T_regprocedure:
		// Trim procedure type parameters, e.g. `max(int)` becomes `max`.
		// Postgres only does this when the cast is ::regprocedure, but we're
		// going to always do it.
		// We additionally do not yet implement disambiguation based on type
		// parameters: we return the match iff there is exactly one.
		s = pgSignatureRegexp.ReplaceAllString(s, "$1")

		substrs, err := splitIdentifierList(s)
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
		funcDef, err := ctx.Planner.ResolveFunction(ctx.Ctx(), &name, &ctx.SessionData().SearchPath)
		if err != nil {
			return nil, err
		}
		if len(funcDef.Overloads) > 1 {
			return nil, pgerror.Newf(pgcode.AmbiguousAlias,
				"more than one function named '%s'", funcDef.Name)
		}
		overload := funcDef.Overloads[0]
		return tree.NewDOidWithTypeAndName(overload.Oid, t, funcDef.Name), nil
	case oid.T_regtype:
		parsedTyp, err := ctx.Planner.GetTypeFromValidSQLSyntax(s)
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

		dOid, errSafeToIgnore, missingTypeErr := ctx.Planner.ResolveOIDFromString(
			ctx.Ctx(), t, tree.NewDString(tree.Name(s).Normalize()),
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
		id, err := ctx.Planner.ResolveTableName(ctx.Ctx(), &tn)
		if err != nil {
			return nil, err
		}
		// tree.ID is a uint32, so this type conversion is safe.
		return tree.NewDOidWithTypeAndName(oid.Oid(id), t, tn.ObjectName.String()), nil

	default:
		d, _ /* errSafeToIgnore */, err := ctx.Planner.ResolveOIDFromString(ctx.Ctx(), t, tree.NewDString(s))
		return d, err
	}
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
