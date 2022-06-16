// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// Target schema changes in the system.statement_diagnostics_requests table,
// adding a column and updating the secondary index to store those columns.
const (
	addSamplingProbColToStmtDiagReqs = `
ALTER TABLE system.statement_diagnostics_requests
  ADD COLUMN sampling_probability FLOAT NULL FAMILY "primary"`

	createCompletedIdxV3 = `
CREATE INDEX completed_idx ON system.statement_diagnostics_requests (completed, ID)
  STORING (statement_fingerprint, sampling_probability, min_execution_latency, expires_at)`

	dropCompletedIdxV2 = `DROP INDEX IF EXISTS system.statement_diagnostics_requests@completed_idx_v2`
)

// sampledStmtDiagReqsMigration changes the schema of the
// system.statement_diagnostics_requests table to support probabilistic bundle
// collection.
func sampledStmtDiagReqsMigration(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	for _, op := range []operation{
		{
			name:           "add-stmt-diag-reqs-sampling-probability-column",
			schemaList:     []string{"sampling_probability"},
			query:          addSamplingProbColToStmtDiagReqs,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "create-stmt-diag-reqs-v3-index",
			schemaList:     []string{"completed_idx"},
			query:          createCompletedIdxV3,
			schemaExistsFn: hasIndex,
		},
		{
			name:       "drop-stmt-diag-reqs-v2-index",
			schemaList: []string{"completed_idx_v2"},
			query:      dropCompletedIdxV2,
			schemaExistsFn: func(catalog.TableDescriptor, catalog.TableDescriptor, string) (bool, error) {
				return false, nil
			},
		},
	} {
		if err := migrateTable(ctx, cs, d, op, keys.StatementDiagnosticsRequestsTableID,
			systemschema.StatementDiagnosticsRequestsTable); err != nil {
			return err
		}
	}
	return nil
}
