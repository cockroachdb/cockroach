// Copyright 2021 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

const query = `
WITH
	cte1
		AS (
			SELECT
				crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', descriptor, false) AS d
			FROM
				system.descriptor
			ORDER BY
				id ASC
		),
	cte2 AS (SELECT COALESCE(d->'table', d->'database', d->'schema', d->'type') AS d FROM cte1)
SELECT
	(d->'id')::INT8 AS id, d->>'name' AS name
FROM
	cte2
WHERE
	COALESCE(json_array_length(d->'drainingNames'), 0) > 0
LIMIT 1;
`

// ensureNoDrainingNames waits until every descriptor has no draining names.
func ensureNoDrainingNames(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	retryOpts := retry.Options{
		MaxBackoff: 10 * time.Second,
	}
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		rows, err := d.InternalExecutor.QueryBufferedEx(
			ctx,
			"ensure-no-draining-names",
			nil, /* txn */
			sessiondata.InternalExecutorOverride{User: username.RootUserName()},
			query,
		)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		datums := rows[0]
		id := descpb.ID(*datums[0].(*tree.DInt))
		name := string(*datums[1].(*tree.DString))
		log.Infof(ctx, "descriptor with ID %d and name %q still has draining names", id, name)
	}
	return ctx.Err()
}
