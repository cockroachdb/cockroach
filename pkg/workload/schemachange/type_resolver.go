// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemachange

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/jackc/pgx"
	"github.com/lib/pq/oid"
)

// txTypeResolver is a minimal type resolver to support writing enum values to
// columns.
type txTypeResolver struct {
	tx *pgx.Tx
}

func (t txTypeResolver) ResolveType(
	ctx context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	schemaClause := ""
	if name.HasExplicitSchema() {
		schemaClause = fmt.Sprintf("AND nspname = '%s'", name.Schema())
	}
	rows, err := t.tx.Query(fmt.Sprintf(`
  SELECT enumlabel, enumsortorder, pgt.oid::int
    FROM pg_enum AS pge, pg_type AS pgt, pg_namespace AS pgn
   WHERE (pgt.typnamespace = pgn.oid AND pgt.oid = pge.enumtypid)
         AND typcategory = 'E'
         AND typname = $1
				 %s
ORDER BY enumsortorder`, schemaClause), name.Object())
	if err != nil {
		return nil, err
	}
	var logicalReps []string
	var physicalReps [][]byte
	var readOnly []bool
	var objectID oid.Oid
	for rows.Next() {
		var logicalRep string
		var order int64
		if err := rows.Scan(&logicalRep, &order, &objectID); err != nil {
			return nil, err
		}
		logicalReps = append(logicalReps, logicalRep)
		physicalReps = append(physicalReps, encoding.EncodeUntaggedIntValue(nil, order))
		readOnly = append(readOnly, false)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// TODO(ajwerner): Fill in some more fields here to generate better errors
	// down the line.
	n := types.UserDefinedTypeName{Name: name.Object()}
	if name.HasExplicitSchema() {
		n.Schema = name.Schema()
		n.ExplicitSchema = true
	}
	return &types.T{
		InternalType: types.InternalType{
			Family: types.EnumFamily,
			Oid:    objectID,
		},
		TypeMeta: types.UserDefinedTypeMetadata{
			Name: &n,
			EnumData: &types.EnumMetadata{
				LogicalRepresentations:  logicalReps,
				PhysicalRepresentations: physicalReps,
				IsMemberReadOnly:        readOnly,
			},
		},
	}, nil
}

func (t txTypeResolver) ResolveTypeByOID(ctx context.Context, oid oid.Oid) (*types.T, error) {
	return nil, pgerror.Newf(pgcode.UndefinedObject, "type %d does not exist", oid)
}

var _ tree.TypeReferenceResolver = (*txTypeResolver)(nil)
