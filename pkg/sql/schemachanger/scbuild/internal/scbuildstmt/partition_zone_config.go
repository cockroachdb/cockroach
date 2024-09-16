// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// partitionZoneConfigObj is used to represent a table-specific zone configuration
// object.
type partitionZoneConfigObj struct {
	indexZoneConfigObj
	partitionSubzone *zonepb.Subzone
	partitionName    string
	seqNum           uint32
}

// panicIfNoPartitionExistsOnIdx panics if the partition referenced in a
// ALTER PARTITION ... OF TABLE does not exist on the provided index.
func (pzo *partitionZoneConfigObj) panicIfNoPartitionExistsOnIdx(
	b BuildCtx, n *tree.SetZoneConfig,
) {
	zs := n.ZoneSpecifier
	if zs.TargetsPartition() && len(zs.TableOrIndex.Index) != 0 && !n.AllIndexes {
		partitionName := string(zs.Partition)
		var indexes []scpb.IndexPartitioning
		idxPart := b.QueryByID(pzo.tableID).FilterIndexPartitioning()
		idxPart.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexPartitioning) {
			if e.IndexID != pzo.indexID {
				return
			}
			partition := tabledesc.NewPartitioning(&e.PartitioningDescriptor)
			partition = partition.FindPartitionByName(partitionName)
			if partition != nil {
				indexes = append(indexes, *e)
			}
		})

		indexName := string(zs.TableOrIndex.Index)
		switch len(indexes) {
		case 0:
			panic(fmt.Errorf("partition %q does not exist on index %q", partitionName, indexName))
		case 1:
			return
		default:
			panic(errors.AssertionFailedf("partition %q exists multiple times on index %q",
				partitionName, indexName))
		}
	}
}
