// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func alterTableAddPartition(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	stmt tree.Statement,
	t *tree.AlterTableAddPartition,
) {
	// Get the primary index for the table.
	primaryIndexID := getCurrentPrimaryIndexID(b, tbl.TableID)

	// Query for existing partitions on the primary index and check for duplicates.
	// Use Normalize() for case-insensitive comparison.
	newPartitionName := t.Partition.Name.Normalize()
	b.QueryByID(tbl.TableID).FilterIndexPartitioning().ForEach(
		func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexPartitioning) {
			if e.IndexID != primaryIndexID {
				return
			}
			// Check for duplicate partition names.
			partition := tabledesc.NewPartitioning(&e.PartitioningDescriptor)
			_ = partition.ForEachPartitionName(func(name string) error {
				if name == newPartitionName {
					panic(pgerror.Newf(pgcode.InvalidObjectDefinition,
						"PARTITION %s: name must be unique (used twice in index %q)",
						t.Partition.Name, "primary"))
				}
				return nil
			})
		})
}
