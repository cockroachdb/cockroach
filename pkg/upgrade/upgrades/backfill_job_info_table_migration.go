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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var jobInfoBackfillBatchSize = envutil.EnvOrDefaultInt("COCKROACH_UPGRADE_JOB_BACKFILL_BATCH", 100)

const (
	backfillJobInfoSharedPrefix = `WITH inserted AS (
		INSERT INTO system.job_info (job_id, info_key, value) 
	SELECT id, '`

	backfillJobInfoSharedSuffix = ` FROM system.jobs 
	WHERE jobs.id > $1
	ORDER BY jobs.id ASC
	LIMIT $2
	RETURNING job_id) SELECT job_id FROM inserted ORDER BY job_id DESC LIMIT 1`

	backfillJobInfoPayloadStmt  = backfillJobInfoSharedPrefix + jobs.LegacyPayloadKey + `', payload` + backfillJobInfoSharedSuffix
	backfillJobInfoProgressStmt = backfillJobInfoSharedPrefix + jobs.LegacyProgressKey + `', progress` + backfillJobInfoSharedSuffix
)

func backfillJobInfoTable(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {

	for step, stmt := range []string{backfillJobInfoPayloadStmt, backfillJobInfoProgressStmt} {
		var resumeAfter int
		for batch, done := 0, false; !done; batch++ {
			if err := d.DB.KV().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				last, err := d.InternalExecutor.QueryBufferedEx(
					ctx,
					fmt.Sprintf("backfill-job-info-step%d-batch%d", step, batch),
					txn,
					sessiondata.NodeUserSessionDataOverride,
					stmt,
					resumeAfter,
					jobInfoBackfillBatchSize,
				)

				if err != nil {
					return errors.Wrap(err, "failed to backfill")
				}
				resumeAfter = 0
				if len(last) == 1 && len(last[0]) == 1 && last[0][0] != tree.DNull {
					resumeAfter = int(tree.MustBeDInt(last[0][0]))
				} else {
					done = true
				}
				log.Infof(ctx, "backfilling job_info, step%d, batch%d done; resume after %d, done %v", step, batch, resumeAfter, done)
				return nil
			}); err != nil {
				return err
			}
			if done {
				break
			}
		}
	}
	return nil
}
