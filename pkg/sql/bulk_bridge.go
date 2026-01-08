// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type bulkMergeFunc func(
	ctx context.Context,
	execCtx JobExecContext,
	ssts []execinfrapb.BulkMergeSpec_SST,
	spans []roachpb.Span,
	genOutputURIAndRecordPrefix func(base.SQLInstanceID) (string, error),
	iteration int,
	maxIterations int,
	writeTS *hlc.Timestamp,
) ([]execinfrapb.BulkMergeSpec_SST, error)

var registeredBulkMerge bulkMergeFunc

type bulkIngestFunc func(
	ctx context.Context,
	execCtx JobExecContext,
	spans []roachpb.Span,
	ssts []execinfrapb.BulkMergeSpec_SST,
) error

// RegisterBulkMerge installs the distributed merge implementation so other
// packages can invoke it without introducing an import cycle.
func RegisterBulkMerge(fn bulkMergeFunc) {
	registeredBulkMerge = fn
}

// RegisterBulkIngest installs the distributed ingest implementation.
func RegisterBulkIngest(fn bulkIngestFunc) {
	// TODO(159374): remove ingest processor
}

func invokeBulkMerge(
	ctx context.Context,
	execCtx JobExecContext,
	ssts []execinfrapb.BulkMergeSpec_SST,
	spans []roachpb.Span,
	genOutputURIAndRecordPrefix func(base.SQLInstanceID) (string, error),
	iteration int,
	maxIterations int,
	writeTS *hlc.Timestamp,
) ([]execinfrapb.BulkMergeSpec_SST, error) {
	if registeredBulkMerge == nil {
		return nil, errors.AssertionFailedf("bulk merge implementation not registered")
	}
	return registeredBulkMerge(ctx, execCtx, ssts, spans, genOutputURIAndRecordPrefix, iteration, maxIterations, writeTS)
}
