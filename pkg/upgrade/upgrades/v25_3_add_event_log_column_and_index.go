// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const (
	addEventLogPayloadColumn = `
ALTER TABLE system.eventlog
	ADD COLUMN IF NOT EXISTS payload JSONB
	CREATE IF NOT EXISTS FAMILY "fam_7_payload"
`

	addEventLogIndex = `
CREATE INDEX IF NOT EXISTS event_type_idx ON system.eventlog ("eventType", timestamp DESC)`
)

func eventLogTableMigration(
	ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	for _, op := range []operation{
		{
			name:           "add-payload-column-to-eventlog",
			schemaList:     []string{"payload"},
			query:          addEventLogPayloadColumn,
			schemaExistsFn: hasColumn,
		},
		{
			name:           "add-event_type_idx-index-to-eventlog",
			schemaList:     []string{"event_type_idx"},
			query:          addEventLogIndex,
			schemaExistsFn: hasColumn,
		},
	} {
		if err := migrateTable(ctx, version, deps, op, keys.EventLogTableID,
			systemschema.EventLogTable); err != nil {
			return err
		}
	}
	return nil
}
