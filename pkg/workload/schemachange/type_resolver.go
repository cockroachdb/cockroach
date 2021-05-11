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
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx"
	"github.com/lib/pq/oid"
)

// txTypeResolver is a minimal type resolver to support writing enum values to
// columns.
type txTypeResolver struct {
	tx *pgx.Tx
}

// ResolveType implements the TypeReferenceResolver interface.
// Note: If the name has an explicit schema, it will be resolved as
// a user defined enum.
func (t txTypeResolver) ResolveType(
	ctx context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {

	if name.HasExplicitSchema() {
		rows, err := t.tx.Query(`
  SELECT enumlabel, enumsortorder, pgt.oid::int
    FROM pg_enum AS pge, pg_type AS pgt, pg_namespace AS pgn
   WHERE (pgt.typnamespace = pgn.oid AND pgt.oid = pge.enumtypid)
         AND typcategory = 'E'
         AND typname = $1
				 AND nspname = $2
ORDER BY enumsortorder`, name.Object(), name.Schema())
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
		n.Schema = name.Schema()
		n.ExplicitSchema = true
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

	// Since the type is not a user defined schema type, it is a primitive type.
	var objectID oid.Oid
	if err := t.tx.QueryRow(`
  SELECT oid::int
    FROM pg_type
   WHERE typname = $1
  `, name.Object(),
	).Scan(&objectID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			err = errors.Errorf("failed to resolve primitive type %s", name)
		}
		return nil, err
	}

	if _, exists := types.OidToType[objectID]; !exists {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "type %s with oid %s does not exist", name.Object(), objectID)
	}
	// Special case CHAR to have the right width.
	if objectID == oid.T_char || objectID == oid.T_bpchar {
		t := *types.OidToType[objectID]
		t.InternalType.Width = 1
		return &t, nil
	}
	return types.OidToType[objectID], nil
}

func (t txTypeResolver) ResolveTypeByOID(ctx context.Context, oid oid.Oid) (*types.T, error) {
	return nil, pgerror.Newf(pgcode.UndefinedObject, "type %d does not exist", oid)
}

var _ tree.TypeReferenceResolver = (*txTypeResolver)(nil)
