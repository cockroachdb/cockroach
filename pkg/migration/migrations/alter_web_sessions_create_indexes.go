// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
)

// Target schema changes in the system.web_sessions table, adding indexes on two columns
// for deleting old web sessions.
const (
	addIdxRevokedAtStmt = `
CREATE INDEX IF NOT EXISTS "web_sessions_revokedAt_idx" 
ON system.web_sessions ("revokedAt")
`
	addIdxLastUsedAtStmt = `
CREATE INDEX IF NOT EXISTS "web_sessions_lastUsedAt_idx" 
ON system.web_sessions ("lastUsedAt")
`
)

// alterSystemWebSessionsCreateIndexes changes the schema of the system.web_sessions table.
// It adds two indexes.
func alterSystemWebSessionsCreateIndexes(
	ctx context.Context, cs clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
) error {
	for _, op := range []operation{
		{
			name:           "add-web-sessions-revokedAt-idx",
			schemaList:     []string{"web_sessions_revokedAt_idx"},
			query:          addIdxRevokedAtStmt,
			schemaExistsFn: hasIndex,
		},
		{
			name:           "add-web-sessions-lastUsedAt-idx",
			schemaList:     []string{"web_sessions_lastUsedAt_idx"},
			query:          addIdxLastUsedAtStmt,
			schemaExistsFn: hasIndex,
		},
	} {
		if err := migrateTable(ctx, cs, d, op, keys.WebSessionsTableID, systemschema.WebSessionsTable); err != nil {
			return err
		}
	}
	return nil
}
