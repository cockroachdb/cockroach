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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type schemaChangerCCLCallbacks struct {
	settings    *cluster.Settings
	evalContext *tree.EvalContext
}

func (s *schemaChangerCCLCallbacks) CreatePartitioning(
	ctx context.Context,
	tableDesc *tabledesc.Mutable,
	indexDesc descpb.IndexDescriptor,
	partBy *tree.PartitionBy,
	allowedNewColumnNames []tree.Name,
	allowImplicitPartitioning bool,
) (newImplicitCols []catalog.Column, newPartitioning descpb.PartitioningDescriptor, err error) {
	if s.settings == nil ||
		s.evalContext == nil {
		panic("unimplemented when settings or evalContext are omitted")
	}
	return CreatePartitioningCCL(ctx,
		s.settings,
		s.evalContext,
		tableDesc,
		indexDesc,
		partBy,
		allowedNewColumnNames,
		allowImplicitPartitioning)
}

// MakeCCLCallbacks makes callbacks needed for the new schema
// changer.
func MakeCCLCallbacks(
	settings *cluster.Settings, evalContext *tree.EvalContext,
) scexec.Partitioner {
	return &schemaChangerCCLCallbacks{
		settings:    settings,
		evalContext: evalContext,
	}
}
