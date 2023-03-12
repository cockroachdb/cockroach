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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/errors"
)

const backfillJobInfoTableWithPayloadStmt = `
INSERT INTO system.job_info (job_id, info_key, value)
SELECT id, $1, payload FROM system.jobs
`

const backfillJobInfoTableWithProgressStmt = `
INSERT INTO system.job_info (job_id, info_key, value)
SELECT id, $1, progress FROM system.jobs
`

func backfillJobInfoTable(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	return d.DB.KV().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := d.InternalExecutor.ExecEx(ctx, "backfill-job-info-payload", txn,
			sessiondata.NodeUserSessionDataOverride, backfillJobInfoTableWithPayloadStmt, jobs.GetLegacyPayloadKey())
		if err != nil {
			return errors.Wrap(err, "failed to backfill legacy payload")
		}

		_, err = d.InternalExecutor.ExecEx(ctx, "backfill-job-info-progress", txn,
			sessiondata.NodeUserSessionDataOverride, backfillJobInfoTableWithProgressStmt, jobs.GetLegacyProgressKey())
		return errors.Wrap(err, "failed to backfill legacy progress")
	})
}
