// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
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
	q := fmt.Sprintf(
		"SELECT %s.oid, %s FROM pg_catalog.%s WHERE %s = $1",
		info.tableName, info.nameCol, info.tableName, queryCol,
	)

	results, err := ie.QueryRowEx(ctx, "queryOid", txn,
		sessiondata.NoSessionDataOverride, q, toResolve)
	if err != nil {
		if catalog.HasInactiveDescriptorError(err) {
			// Descriptor is either dropped or offline, so
			// the OID does not exist.
			return nil, true, pgerror.Newf(info.errType,
				"%s %s does not exist", info.objName, toResolve)
		} else if errors.HasType(err, (*tree.MultipleResultsError)(nil)) {
			return nil, false, pgerror.Newf(pgcode.AmbiguousAlias,
				"more than one %s named %s", info.objName, toResolve)
		}
		return nil, false, err
	}
	if results.Len() == 0 {
		return nil, true, pgerror.Newf(info.errType,
			"%s %s does not exist", info.objName, toResolve)
	}
	return tree.NewDOidWithTypeAndName(
		results[0].(*tree.DOid).Oid,
		resultType,
		tree.AsStringWithFlags(results[1], tree.FmtBareStrings),
	), true, nil
}

// regTypeInfo contains details on a pg_catalog table that has a reg* type.
type regTypeInfo struct {
	tableName string
	// nameCol is the name of the column that contains the table's entity name.
	nameCol string
	// objName is a human-readable name describing the objects in the table.
	objName string
	// errType is the pg error code in case the object does not exist.
	errType pgcode.Code
}

// regTypeInfos maps an oid.Oid to a regTypeInfo that describes the pg_catalog
// table that contains the entities of the type of the key.
var regTypeInfos = map[oid.Oid]regTypeInfo{
	oid.T_regclass:     {"pg_class", "relname", "relation", pgcode.UndefinedTable},
	oid.T_regnamespace: {"pg_namespace", "nspname", "namespace", pgcode.UndefinedObject},
	oid.T_regproc:      {"pg_proc", "proname", "function", pgcode.UndefinedFunction},
	oid.T_regprocedure: {"pg_proc", "proname", "function", pgcode.UndefinedFunction},
	oid.T_regrole:      {"pg_authid", "rolname", "role", pgcode.UndefinedObject},
	oid.T_regtype:      {"pg_type", "typname", "type", pgcode.UndefinedObject},
}
