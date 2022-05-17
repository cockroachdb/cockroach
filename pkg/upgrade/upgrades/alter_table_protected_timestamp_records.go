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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const addTargetCol = `
ALTER TABLE system.protected_ts_records
ADD COLUMN IF NOT EXISTS target BYTES FAMILY "primary"
`

func alterTableProtectedTimestampRecords(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	op := operation{
		name:           "add-table-pts-records-target-col",
		schemaList:     []string{"target"},
		query:          addTargetCol,
		schemaExistsFn: hasColumn,
	}
	if err := migrateTable(ctx, cs, d, op,
		keys.ProtectedTimestampsRecordsTableID,
		systemschema.ProtectedTimestampsRecordsTable); err != nil {
		return err
	}
	return nil
}
