// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/internal/scbuildstmt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var _ scbuildstmt.IndexPartitioner = buildCtx{}

// CreatePartitioningDescriptor implements the scbuildstmt.IndexPartitioner
// interface.
func (b buildCtx) CreatePartitioningDescriptor(
	ctx context.Context,
	columnLookupFn func(tree.Name) (catalog.Column, error),
	oldNumImplicitColumns int,
	oldKeyColumnNames []string,
	partBy *tree.PartitionBy,
	allowedNewColumnNames []tree.Name,
	allowImplicitPartitioning bool,
) (newImplicitCols []catalog.Column, newPartitioning catpb.PartitioningDescriptor) {
	callback := b.Dependencies.IndexPartitioningCCLCallback()
	newImplicitCols, newPartitioning, err := callback(
		ctx,
		b.ClusterSettings(),
		b.EvalCtx(),
		columnLookupFn,
		oldNumImplicitColumns,
		oldKeyColumnNames,
		partBy,
		allowedNewColumnNames,
		allowImplicitPartitioning,
	)
	if err != nil {
		panic(err)
	}
	return newImplicitCols, newPartitioning
}
