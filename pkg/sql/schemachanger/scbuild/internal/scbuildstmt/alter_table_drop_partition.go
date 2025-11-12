// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func alterTableDropPartition(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableDropPartition,
) {
	partitionName := string(t.Partition)

	// Find all partition entries for this table.
	var foundPartition *scpb.IndexPartitionEntry
	var foundTarget scpb.TargetStatus

	b.QueryByID(tbl.TableID).ForEach(func(
		_ scpb.Status, target scpb.TargetStatus, e scpb.Element,
	) {
		if partEntry, ok := e.(*scpb.IndexPartitionEntry); ok {
			// Check if this partition entry matches the partition name we're looking for.
			// The partition_path is a slice representing the hierarchical path.
			// For a top-level partition, we check if the last element equals the partition name.
			if len(partEntry.PartitionPath) > 0 &&
				partEntry.PartitionPath[len(partEntry.PartitionPath)-1] == partitionName {
				foundPartition = partEntry
				foundTarget = target
			}
		}
	})

	// Handle IF EXISTS case: if partition not found and IF EXISTS is specified.
	if foundPartition == nil {
		if t.IfExists {
			b.EvalCtx().ClientNoticeSender.BufferClientNotice(b, pgnotice.Newf(
				"partition %q of relation %q does not exist, skipping", partitionName, tn.Table()))
			return
		}
		// Partition not found and IF EXISTS not specified.
		panic(pgerror.Newf(pgcode.UndefinedObject,
			"partition %q of relation %q does not exist", partitionName, tn.Table()))
	}

	// Mark the partition for dropping if it's targeting ToPublic.
	// We also need to handle any related zone configurations.
	if foundTarget == scpb.ToPublic {
		b.Drop(foundPartition)
	} else {
		// Partition is already being dropped or in some other state.
		panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"partition %q is not in a valid state to be dropped", partitionName))
	}
}
