// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var debugJobCleanupInfoRows = &cobra.Command{
	Use:   "job-cleanup-job-info",
	Short: "cleans up system.job_info rows with no related system.jobs entry",
	RunE:  clierrorplus.MaybeDecorateError(runDebugJobInfoCleanup),
}

var jobCleanupInfoRowOpts = struct {
	PageSize int
	Age      time.Duration
}{
	PageSize: 500,
	Age:      12 * time.Hour,
}

const abandonedJobInfoRowsCleanupQuery = `
	DELETE
FROM system.job_info
WHERE written < $1 AND job_id NOT IN (SELECT id FROM system.jobs)
LIMIT $2`

func runDebugJobInfoCleanup(_ *cobra.Command, args []string) (resErr error) {
	ctx := context.Background()
	sqlConn, err := makeSQLClient(ctx, "cockroach debug job-cleanup-job-info", useSystemDb)
	if err != nil {
		return errors.Wrap(err, "could not establish connection to cluster")
	}
	defer func() { resErr = errors.CombineErrors(resErr, sqlConn.Close()) }()

	totalDeleted := 0
	defer func() {
		if totalDeleted > 0 {
			telemetry.Inc(jobs.AbandonedInfoRowsFound)
		}
	}()

	for {
		rowsAffected, err := sqlConn.ExecWithRowsAffected(ctx,
			abandonedJobInfoRowsCleanupQuery,
			timeutil.Now().Add(-1*jobCleanupInfoRowOpts.Age),
			jobCleanupInfoRowOpts.PageSize)
		if err != nil {
			return err
		}
		if rowsAffected == 0 {
			break
		}
		fmt.Printf("deleted %d system.job_info rows\n", rowsAffected)
		totalDeleted += int(rowsAffected)
	}

	fmt.Printf("%d total rows deleted\n", totalDeleted)
	return nil
}
