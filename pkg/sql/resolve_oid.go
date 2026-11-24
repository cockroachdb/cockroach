// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// ResolveOIDFromString is part of tree.TypeResolver.
func (p *planner) ResolveOIDFromString(
	ctx context.Context, resultType *types.T, toResolve *tree.DString,
) (_ *tree.DOid, errSafeToIgnore bool, _ error) {
	return resolveOID(
		ctx, p.Txn(),
		p.InternalSQLTxn(),
		resultType, toResolve,
	)
}

// ResolveOIDFromOID is part of tree.TypeResolver.
func (p *planner) ResolveOIDFromOID(
	ctx context.Context, resultType *types.T, toResolve *tree.DOid,
) (_ *tree.DOid, errSafeToIgnore bool, _ error) {
	return resolveOID(
		ctx, p.Txn(),
		p.InternalSQLTxn(),
		resultType, toResolve,
	)
}

func resolveOID(
	ctx context.Context, txn *kv.Txn, ie isql.Executor, resultType *types.T, toResolve tree.Datum,
) (_ *tree.DOid, errSafeToIgnore bool, _ error) {
	info, ok := regTypeInfos[resultType.Oid()]
	if !ok {
		return nil, true, pgerror.Newf(
			pgcode.InvalidTextRepresentation,
			"invalid input syntax for type %s: %q",
			resultType,
			tree.AsStringWithFlags(toResolve, tree.FmtBareStrings),
		)
	}
	queryCol := info.nameCol
	if _, isOid := toResolve.(*tree.DOid); isOid {
		queryCol = "oid"
	}

	results, err := ie.QueryRowEx(ctx, "queryOid", txn,
		sessiondata.NoSessionDataOverride, info.query(queryCol), toResolve)
	if err != nil {
		if catalog.HasInactiveDescriptorError(err) {
			// Descriptor is either dropped or offline, so
			// the OID does not exist.
			return nil, true, pgerror.Newf(info.errType,
				"%s %s does not exist", info.objName, toResolve)
		} else if errors.HasType(err, (*tree.MultipleResultsError)(nil)) {
			return nil, false, pgerror.Newf(pgcode.AmbiguousAlias,
				"more than one %s named %s", info.objName, toResolve)
		} else if errors.Is(err, errInvalidDbPrefix) {
			// The internal executor has no current database, so it cannot
			// read pg_catalog tables. Callers (qualifiedDOidFromOID and the
			// OID-to-reg* cast path) treat this as benign and fall back to
			// the unqualified name they already have.
			return nil, true, err
		}
		return nil, false, err
	}
	if results.Len() == 0 {
		return nil, true, pgerror.Newf(info.errType,
			"%s %s does not exist", info.objName, toResolve)
	}

	resultOid := results[0].(*tree.DOid).Oid
	resultName := string(tree.MustBeDString(results[1]))
	if info.namespaceCol != "" {
		// Postgres's reg*out emits a schema prefix iff the bare name does
		// not resolve back to this OID through the current search_path.
		nspname := string(tree.MustBeDString(results[2]))
		visible := false
		if v, ok := results[3].(*tree.DBool); ok {
			visible = bool(*v)
		}
		resultName = qualifiedRegName(nspname, resultName, visible)
	}
	return tree.NewDOidWithTypeAndName(resultOid, resultType, resultName), true, nil
}

// qualifiedRegName returns the SQL-identifier form of an object name. Each
// component is double-quoted only when necessary (matching PG's
// quote_qualified_identifier). The schema prefix is included when the bare
// name is not visible via the current search_path. This mirrors Postgres's
// reg*out behavior used by regclassout / regprocout / regtypeout etc.
func qualifiedRegName(nspname, name string, visible bool) string {
	var buf bytes.Buffer
	if !visible {
		lexbase.EncodeRestrictedSQLIdent(&buf, nspname, lexbase.EncNoFlags)
		buf.WriteByte('.')
	}
	lexbase.EncodeRestrictedSQLIdent(&buf, name, lexbase.EncNoFlags)
	return buf.String()
}

// regTypeInfo contains details on a pg_catalog table that has a reg* type.
type regTypeInfo struct {
	tableName string
	// nameCol is the column that contains the entity's local name.
	nameCol string
	// namespaceCol is the column on tableName that joins to pg_namespace.oid,
	// or "" if the entity has no enclosing schema (regnamespace, regrole).
	// When non-empty, resolveOID also fetches nspname and a visibility bit so
	// the name returned to the caller can be schema-qualified per Postgres
	// reg*out semantics.
	namespaceCol string
	// signatureCol, when non-empty, is an additional column whose equality
	// the visibility subquery requires — used to match Postgres's
	// signature-aware FunctionIsVisible (pg_proc.proargtypes), so two
	// functions sharing a proname but with disjoint signatures do not
	// shadow each other.
	signatureCol string
	// objName is a human-readable name describing the objects in the table.
	objName string
	// errType is the pg error code in case the object does not exist.
	errType pgcode.Code
}

// query builds the SELECT used to look up an OID for this regtype.
//
// When namespaceCol is empty, the result has two columns (oid, name). When
// namespaceCol is set, the result has four columns (oid, name, nspname,
// visible). visible is true iff the entity's own schema is the first one in
// the current search_path containing any same-named entity (and, when
// signatureCol is set, of matching signature). This mirrors Postgres's
// reg*out emission rule used by RelationIsVisible / TypeIsVisible /
// FunctionIsVisible.
func (info regTypeInfo) query(queryCol string) string {
	if info.namespaceCol == "" {
		return fmt.Sprintf(
			`SELECT %[1]s.oid, %[2]s FROM pg_catalog.%[1]s WHERE %[3]s = $1`,
			info.tableName, info.nameCol, queryCol,
		)
	}
	var sigPredicate string
	if info.signatureCol != "" {
		sigPredicate = fmt.Sprintf(`AND t2.%[1]s = t.%[1]s`, info.signatureCol)
	}
	return fmt.Sprintf(
		`SELECT t.oid, t.%[2]s, n.nspname,
                (SELECT n2.nspname
                 FROM pg_catalog.%[1]s t2
                 JOIN pg_catalog.pg_namespace n2 ON t2.%[3]s = n2.oid
                 WHERE t2.%[2]s = t.%[2]s
                   %[5]s
                   AND n2.nspname = ANY current_schemas(true)
                 ORDER BY array_position(current_schemas(true), n2.nspname)
                 LIMIT 1) IS NOT DISTINCT FROM n.nspname AS visible
         FROM pg_catalog.%[1]s t
         JOIN pg_catalog.pg_namespace n ON t.%[3]s = n.oid
         WHERE t.%[4]s = $1`,
		info.tableName, info.nameCol, info.namespaceCol, queryCol, sigPredicate,
	)
}

// regTypeInfos maps an oid.Oid to a regTypeInfo that describes the pg_catalog
// table that contains the entities of the type of the key.
var regTypeInfos = map[oid.Oid]regTypeInfo{
	oid.T_regclass:     {"pg_class", "relname", "relnamespace", "", "relation", pgcode.UndefinedTable},
	oid.T_regnamespace: {"pg_namespace", "nspname", "", "", "namespace", pgcode.UndefinedObject},
	oid.T_regproc:      {"pg_proc", "proname", "pronamespace", "proargtypes", "function", pgcode.UndefinedFunction},
	oid.T_regprocedure: {"pg_proc", "proname", "pronamespace", "proargtypes", "function", pgcode.UndefinedFunction},
	oid.T_regrole:      {"pg_authid", "rolname", "", "", "role", pgcode.UndefinedObject},
	oid.T_regtype:      {"pg_type", "typname", "typnamespace", "", "type", pgcode.UndefinedObject},
}
