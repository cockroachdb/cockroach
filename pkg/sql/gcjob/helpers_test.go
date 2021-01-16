// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gcjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
)

// GcTenant is a wrapper around the internal function that gc-s a tenant.
func GcTenant(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	tenID uint64,
	progress *jobspb.SchemaChangeGCProgress,
) error {
	return gcTenant(ctx, execCfg, tenID, progress)
}
