// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// ResolveOIDFromString is part of tree.TypeResolver.
func (p *planner) ResolveOIDFromString(
	ctx context.Context, resultType *types.T, toResolve *tree.DString,
) (*tree.DOid, error) {
	return resolveOID(
		ctx, p.Txn(),
		p.extendedEvalCtx.InternalExecutor.(sqlutil.InternalExecutor),
		resultType, toResolve,
	)
}

// ResolveOIDFromOID is part of tree.TypeResolver.
func (p *planner) ResolveOIDFromOID(
	ctx context.Context, resultType *types.T, toResolve *tree.DOid,
) (*tree.DOid, error) {
	return resolveOID(
		ctx, p.Txn(),
		p.extendedEvalCtx.InternalExecutor.(sqlutil.InternalExecutor),
		resultType, toResolve,
	)
}

func resolveOID(
	ctx context.Context,
	txn *kv.Txn,
	ie sqlutil.InternalExecutor,
	resultType *types.T,
	toResolve tree.Datum,
) (*tree.DOid, error) {
	info, ok := regTypeInfos[resultType.Oid()]
	if !ok {
		return nil, errors.AssertionFailedf("illegal oid type %v", resultType)
	}
	queryCol := info.nameCol
	if _, isOid := toResolve.(*tree.DOid); isOid {
		queryCol = "oid"
	}
	q := fmt.Sprintf(
		"SELECT %s.oid, %s FROM pg_catalog.%s WHERE %s = $1",
		info.tableName, info.nameCol, info.tableName, queryCol,
	)
	results, err := ie.QueryRow(ctx, "queryOid", txn, q, toResolve)
	if err != nil {
		if errors.HasType(err, (*tree.MultipleResultsError)(nil)) {
			return nil, pgerror.Newf(pgcode.AmbiguousAlias,
				"more than one %s named %s", info.objName, toResolve)
		}
		return nil, err
	}
	if results.Len() == 0 {
		return nil, pgerror.Newf(info.errType,
			"%s %s does not exist", info.objName, toResolve)
	}
	return tree.NewDOidWithName(
		results[0].(*tree.DOid).DInt,
		resultType,
		tree.AsStringWithFlags(results[1], tree.FmtBareStrings),
	), nil
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
	oid.T_regtype:      {"pg_type", "typname", "type", pgcode.UndefinedObject},
	oid.T_regproc:      {"pg_proc", "proname", "function", pgcode.UndefinedFunction},
	oid.T_regprocedure: {"pg_proc", "proname", "function", pgcode.UndefinedFunction},
	oid.T_regnamespace: {"pg_namespace", "nspname", "namespace", pgcode.UndefinedObject},
}
