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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// Target schema changes in the system.statement_diagnostics_requests table,
// adding two columns and updating the secondary index to store those columns.
const (
	addColsToStmtDiagReqs = `
ALTER TABLE system.statement_diagnostics_requests
  ADD COLUMN min_execution_latency INTERVAL NULL FAMILY "primary",
  ADD COLUMN expires_at TIMESTAMPTZ NULL FAMILY "primary"`

	createCompletedIdxV2 = `
CREATE INDEX completed_idx_v2 ON system.statement_diagnostics_requests (completed, ID)
  STORING (statement_fingerprint, min_execution_latency, expires_at)`

	dropCompletedIdx = `DROP INDEX IF EXISTS system.statement_diagnostics_requests@completed_idx`
)

// alterSystemStmtDiagReqs changes the schema of the
// system.statement_diagnostics_requests table. It adds two columns which are
// then included into STORING clause of the secondary index.
func alterSystemStmtDiagReqs(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	for _, op := range []operation{
		{
			name:           "add-stmt-diag-reqs-conditional-columns",
			schemaList:     []string{"min_execution_latency", "expires_at"},
			query:          addColsToStmtDiagReqs,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "create-stmt-diag-reqs-new-index",
			schemaList:     []string{"completed_idx_v2"},
			query:          createCompletedIdxV2,
			schemaExistsFn: hasIndex,
		},
		{
			name:       "drop-stmt-diag-reqs-old-index",
			schemaList: []string{"completed_idx"},
			query:      dropCompletedIdx,
			schemaExistsFn: func(catalog.TableDescriptor, catalog.TableDescriptor, string) (bool, error) {
				return false, nil
			},
		},
	} {
		if err := migrateTable(ctx, cs, d, op, keys.StatementDiagnosticsRequestsTableID, systemschema.StatementDiagnosticsRequestsTable); err != nil {
			return err
		}
	}
	return nil
}
