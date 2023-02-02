// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const deleteDroppedFunctionQuery = `
WITH to_json AS (
    SELECT 
      id,
      crdb_internal.pb_to_json(
        'cockroach.sql.sqlbase.Descriptor',
        descriptor,
        false
      ) AS d
    FROM 
      system.descriptor
),
to_delete AS (
    SELECT id
    FROM to_json
    WHERE 
      d->'function' IS NOT NULL
      AND d->'function'->>'declarativeSchemaChangerState' IS NULL
      AND d->'function'->>'state' = 'DROP'
)
SELECT crdb_internal.unsafe_delete_descriptor(id, false)
FROM to_delete
WHERE id >= $1
AND id < $2;
`

func deleteDescriptorsOfDroppedFunctions(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	if err := d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		row, err := txn.QueryRow(
			ctx,
			"upgrade-delete-dropped-function-descriptors-get-max-descriptor-id",
			txn.KV(),
			`SELECT max(id) FROM system.descriptor`,
		)

		if err != nil {
			return err
		}

		maxID := int(tree.MustBeDInt(row[0]))
		const batchSize = 50

		for curID := 1; curID <= maxID; curID += batchSize {
			_, err := txn.Exec(
				ctx,
				"upgrade-delete-dropped-function-descriptors", /* opName */
				txn.KV(),
				deleteDroppedFunctionQuery,
				curID,
				curID+batchSize,
			)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}
