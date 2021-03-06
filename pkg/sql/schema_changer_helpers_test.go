// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// TestingDistIndexBackfill exposes the index backfill functionality for
// testing.
func (sc *SchemaChanger) TestingDistIndexBackfill(
	ctx context.Context,
	version descpb.DescriptorVersion,
	targetSpans []roachpb.Span,
	addedIndexes []descpb.IndexID,
	filter backfill.MutationFilter,
) error {
	return sc.distIndexBackfill(ctx, version, targetSpans, addedIndexes, filter)
}

// SetJob sets the job.
func (sc *SchemaChanger) SetJob(job *jobs.Job) {
	sc.job = job
}
