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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// ResolveOIDFromStringOrOID is part of tree.EvalPlanner.
func (p *planner) ResolveOIDFromStringOrOID(
	ctx context.Context, oidTyp *types.T, d tree.Datum,
) (*tree.DOid, error) {
	info := regTypeInfos[oidTyp.Oid()]
	var queryCol string
	switch d.(type) {
	case *tree.DOid:
		queryCol = "oid"
	case *tree.DString:
		queryCol = info.nameCol
	default:
		return nil, errors.AssertionFailedf("invalid argument to OID cast: %s", d)
	}
	results, err := p.EvalContext().InternalExecutor.QueryRow(
		ctx, "queryOid",
		p.Txn(),
		fmt.Sprintf("SELECT %s.oid, %s FROM pg_catalog.%s WHERE %s = $1",
			info.tableName, info.nameCol, info.tableName, queryCol),
		d)
	if err != nil {
		if errors.HasType(err, (*tree.MultipleResultsError)(nil)) {
			return nil, pgerror.Newf(pgcode.AmbiguousAlias,
				"more than one %s named %s", info.objName, d)
		}
		return nil, err
	}
	if results.Len() == 0 {
		return nil, pgerror.Newf(info.errType, "%s %s does not exist", info.objName, d)
	}
	return tree.NewDOidWithName(
		results[0].(*tree.DOid).DInt,
		oidTyp,
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
