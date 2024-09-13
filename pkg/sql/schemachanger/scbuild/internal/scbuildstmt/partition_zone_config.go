// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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

// panicIfBadPartitionReference panics if the partition referenced in a
// ALTER PARTITION ... OF TABLE does not exist or if it exists on multiple
// indexes. Otherwise, we find the existing index and save in to our AST.
// In cases where we find the partition existing on two indexes, we check
// to ensure that we are not in a backfill case before panicking.
func (pzo *partitionZoneConfigObj) panicIfBadPartitionReference(b BuildCtx, n *tree.SetZoneConfig) {
	zs := &n.ZoneSpecifier
	// Backward compatibility for ALTER PARTITION ... OF TABLE. Determine which
	// index has the specified partition.
	if zs.TargetsPartition() && len(zs.TableOrIndex.Index) == 0 && !n.AllIndexes {
		partitionName := string(zs.Partition)

		// TODO(before merge): [reviewer callout] can we guarantee that this will
		// be in the same order as NonDropIndexes()? Mainly for case 2 in the switch
		// case below. My understanding is yes, but I want to confirm.
		var indexes []scpb.IndexPartitioning
		idxPart := b.QueryByID(pzo.tableID).FilterIndexPartitioning()
		idxPart.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexPartitioning) {
			partition := tabledesc.NewPartitioning(&e.PartitioningDescriptor)
			partition = partition.FindPartitionByName(partitionName)
			if partition != nil {
				indexes = append(indexes, *e)
			}
		})

		tableName := n.TableOrIndex.Table.String()
		switch len(indexes) {
		case 0:
			panic(fmt.Errorf("partition %q does not exist on table %q", partitionName, tableName))
		case 1:
			// We found the partition on a single index. Fill this index out in our
			// AST.
			idx := indexes[0]
			idxName := mustRetrieveIndexNameElem(b, pzo.tableID, idx.IndexID)
			zs.TableOrIndex.Index = tree.UnrestrictedName(idxName.Name)
		case 2:
			// Perhaps our target index is a part of a backfill. In this case, we
			// temporary indexes created during backfill should always share the same
			// zone configs as the corresponding new index.
			idx := indexes[0]
			maybeTempIdx := indexes[1]
			if isCorrespondingTemporaryIndex(b, pzo.tableID, maybeTempIdx.IndexID, idx.IndexID) {
				idxName := mustRetrieveIndexNameElem(b, pzo.tableID, idx.IndexID)
				zs.TableOrIndex.Index = tree.UnrestrictedName(idxName.Name)
				break
			}
			// We are not in a backfill case -- the partition we are referencing is on
			// multiple indexes. Fallthrough.
			fallthrough
		default:
			err := fmt.Errorf(
				"partition %q exists on multiple indexes of table %q", partitionName, tableName)
			err = pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
			err = errors.WithHint(err, "try ALTER PARTITION ... OF INDEX ...")
			panic(err)
		}
	}
}
