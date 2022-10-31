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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const alterSystemCommentsPKey = `
ALTER TABLE system.comments ALTER PRIMARY KEY USING COLUMNS (object_id, type, sub_id);
`

const dropOldUniqueKey = `
DROP INDEX system.public.comments@comments_type_object_id_sub_id_key CASCADE;
`

const renameSystemCommentsPKey = `
ALTER TABLE system.comments RENAME CONSTRAINT comments_pkey TO "primary";
`

func alterSystemCommentsPrimaryKey(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	ops := []operation{
		{
			name:           "alter-system-comments-primary-key",
			schemaList:     []string{"primary"},
			query:          alterSystemCommentsPKey,
			schemaExistsFn: hasPrimaryKey,
		},
		{
			name:           "drop-system-comments-old-unique-index",
			schemaList:     []string{"comments_type_object_id_sub_id_key"},
			query:          dropOldUniqueKey,
			schemaExistsFn: doesNotHaveIndex,
		},
		{
			name:           "rename-primary-key-to-be-primary",
			schemaList:     []string{"primary"},
			query:          renameSystemCommentsPKey,
			schemaExistsFn: hasIndex,
		},
	}

	for _, op := range ops {
		if err := migrateTable(ctx, cs, d, op, keys.CommentsTableID, systemschema.CommentsTable); err != nil {
			return err
		}
	}

	return nil
}
