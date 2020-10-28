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
	rows, err := t.tx.Query(`
  SELECT enumlabel, enumsortorder
    FROM pg_enum AS pge, pg_type AS pgt, pg_namespace AS pgn
   WHERE (pgt.typnamespace = pgn.oid AND pgt.oid = pge.enumtypid)
         AND typcategory = 'E'
         AND typname = $1
ORDER BY enumsortorder`, name.Object())
	if err != nil {
		return nil, err
	}
	var logicalReps []string
	var physicalReps [][]byte
	var readOnly []bool
	for rows.Next() {
		var logicalRep string
		var order int64
		if err := rows.Scan(&logicalRep, &order); err != nil {
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
	return &types.T{
		InternalType: types.InternalType{
			Family: types.EnumFamily,
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
