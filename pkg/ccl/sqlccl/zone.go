// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlccl

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/intervalccl"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// GenerateSubzoneSpans constructs from a TableDescriptor the entries mapping
// zone config spans to subzones for use in the SubzoneSpans field of
// config.ZoneConfig. SubzoneSpans controls which splits are created, so only
// the spans corresponding to entries in subzones are returned.
//
// Zone configs target indexes and partitions via `subzones`, which are attached
// to a table-scoped row in `system.zones`. Each subzone represents one index
// (primary or secondary) or one partition (or subpartition) and contains the
// usual zone config constraints. They are saved to `system.zones` sparsely
// (only when set by a user) and are the most specific entry in the normal
// cluster-default/database/table/subzone config hierarchy.
//
// Each non-interleaved index and partition can be mapped to spans in the
// keyspace. Indexes and range partitions each map to one span, while each list
// partition maps to one or more spans. Each partition span is contained by some
// index span and each subpartition span is contained by one of its parent
// partition's spans. The spans for a given level of a range partitioning
// (corresponding to one `PARTITION BY` in sql or one `PartitionDescriptor`) are
// disjoint, but the spans for a given level of a list partitioning may overlap
// if DEFAULT is used. A list partitioning which includes both (1, DEFAULT) and
// (1, 2) will overlap with the latter getting precedence in the zone config
// hierarchy. NB: In a valid PartitionDescriptor, no partitions with the same
// number of DEFAULTs will overlap (this property is used by
// `indexCoveringsForPartitioning`).
//
// These subzone spans are kept denormalized to the relevant `system.zone` row
// for performance. Given a TableDescriptor, the spans for every
// index/partition/subpartition are created, filtered out if they don't have a
// config set for them, and precedence applied (via `OverlapCoveringMerge`) to
// produce a set of non-overlapping spans, which each map to a subzone. There
// may be "holes" (uncovered spans) in this set.
//
// The returned spans are returned in exactly the format required by
// `system.zones`. They must be sorted and non-overlapping. Each contains an
// IndexID, which maps to one of the input `subzones` by indexing into the
// slice. As space optimizations, all `Key`s and `EndKey`s of `SubzoneSpan` omit
// the common prefix (the encoded table ID) and if `EndKey` is equal to
// `Key.PrefixEnd()` it is omitted.
func GenerateSubzoneSpans(
	tableDesc *sqlbase.TableDescriptor, subzones []config.Subzone,
) ([]config.SubzoneSpan, error) {
	a := &sqlbase.DatumAlloc{}

	subzoneIndexByIndexID := make(map[sqlbase.IndexID]int32)
	subzoneIndexByPartition := make(map[string]int32)
	for i, subzone := range subzones {
		if len(subzone.PartitionName) > 0 {
			subzoneIndexByPartition[subzone.PartitionName] = int32(i)
		} else {
			subzoneIndexByIndexID[sqlbase.IndexID(subzone.IndexID)] = int32(i)
		}
	}

	var indexCovering intervalccl.Covering
	var partitionCoverings []intervalccl.Covering
	if err := tableDesc.ForeachNonDropIndex(func(idxDesc *sqlbase.IndexDescriptor) error {
		_, indexSubzoneExists := subzoneIndexByIndexID[idxDesc.ID]
		if indexSubzoneExists {
			idxSpan := tableDesc.IndexSpan(idxDesc.ID)
			// Each index starts with a unique prefix, so (from a precedence
			// perspective) it's safe to append them all together.
			indexCovering = append(indexCovering, intervalccl.Range{
				Start: idxSpan.Key, End: idxSpan.EndKey,
				Payload: config.Subzone{IndexID: uint32(idxDesc.ID)},
			})
		}

		var emptyPrefix []tree.Datum
		indexPartitionCoverings, err := indexCoveringsForPartitioning(
			a, tableDesc, idxDesc, &idxDesc.Partitioning, subzoneIndexByPartition, emptyPrefix)
		if err != nil {
			return err
		}
		// The returned indexPartitionCoverings are sorted with highest
		// precedence first. They all start with the index prefix, so cannot
		// overlap with the partition coverings for any other index, so (from a
		// precedence perspective) it's safe to append them all together.
		partitionCoverings = append(partitionCoverings, indexPartitionCoverings...)

		return nil
	}); err != nil {
		return nil, err
	}

	// OverlapCoveringMerge returns the payloads for any coverings that overlap
	// in the same order they were input. So, we require that they be ordered
	// with highest precedence first, so the first payload of each range is the
	// one we need.
	ranges := intervalccl.OverlapCoveringMerge(append(partitionCoverings, indexCovering))

	// NB: This assumes that none of the indexes are interleaved, which is
	// checked in PartitionDescriptor validation.
	sharedPrefix := keys.MakeTablePrefix(uint32(tableDesc.ID))

	var subzoneSpans []config.SubzoneSpan
	for _, r := range ranges {
		payloads := r.Payload.([]interface{})
		if len(payloads) == 0 {
			continue
		}
		subzoneSpan := config.SubzoneSpan{
			Key:    bytes.TrimPrefix(r.Start, sharedPrefix),
			EndKey: bytes.TrimPrefix(r.End, sharedPrefix),
		}
		var ok bool
		if subzone := payloads[0].(config.Subzone); len(subzone.PartitionName) > 0 {
			subzoneSpan.SubzoneIndex, ok = subzoneIndexByPartition[subzone.PartitionName]
		} else {
			subzoneSpan.SubzoneIndex, ok = subzoneIndexByIndexID[sqlbase.IndexID(subzone.IndexID)]
		}
		if !ok {
			continue
		}
		if bytes.Equal(subzoneSpan.Key.PrefixEnd(), subzoneSpan.EndKey) {
			subzoneSpan.EndKey = nil
		}
		subzoneSpans = append(subzoneSpans, subzoneSpan)
	}
	return subzoneSpans, nil
}

// indexCoveringsForPartitioning returns span coverings representing the
// partitions in partDesc (including subpartitions). They are sorted with
// highest precedence first and the intervalccl.Range payloads are each a
// `config.Subzone` with the PartitionName set.
func indexCoveringsForPartitioning(
	a *sqlbase.DatumAlloc,
	tableDesc *sqlbase.TableDescriptor,
	idxDesc *sqlbase.IndexDescriptor,
	partDesc *sqlbase.PartitioningDescriptor,
	relevantPartitions map[string]int32,
	prefixDatums []tree.Datum,
) ([]intervalccl.Covering, error) {
	if partDesc.NumColumns == 0 {
		return nil, nil
	}

	var coverings []intervalccl.Covering
	var descendentCoverings []intervalccl.Covering

	if len(partDesc.List) > 0 {
		// The returned spans are required to be ordered with highest precedence
		// first. The span for (1, DEFAULT) overlaps with (1, 2) and needs to be
		// returned at a lower precedence. Luckily, because of the partitioning
		// validation, we're guaranteed that all entries in a list partitioning
		// with the same number of DEFAULTs are non-overlapping. So, bucket the
		// `intervalccl.Range`s by the number of non-DEFAULT columns and return
		// them ordered from least # of DEFAULTs to most.
		listCoverings := make([]intervalccl.Covering, int(partDesc.NumColumns)+1)
		for _, p := range partDesc.List {
			for _, valueEncBuf := range p.Values {
				t, keyPrefix, err := sqlbase.DecodePartitionTuple(
					a, tableDesc, idxDesc, partDesc, valueEncBuf, prefixDatums)
				if err != nil {
					return nil, err
				}
				if _, ok := relevantPartitions[p.Name]; ok {
					listCoverings[len(t.Datums)] = append(listCoverings[len(t.Datums)], intervalccl.Range{
						Start: keyPrefix, End: roachpb.Key(keyPrefix).PrefixEnd(),
						Payload: config.Subzone{PartitionName: p.Name},
					})
				}
				newPrefixDatums := append(prefixDatums, t.Datums...)
				subpartitionCoverings, err := indexCoveringsForPartitioning(
					a, tableDesc, idxDesc, &p.Subpartitioning, relevantPartitions, newPrefixDatums)
				if err != nil {
					return nil, err
				}
				descendentCoverings = append(descendentCoverings, subpartitionCoverings...)
			}
		}
		for i := range listCoverings {
			if covering := listCoverings[len(listCoverings)-i-1]; len(covering) > 0 {
				coverings = append(coverings, covering)
			}
		}
	}

	if len(partDesc.Range) > 0 {
		for _, p := range partDesc.Range {
			if _, ok := relevantPartitions[p.Name]; !ok {
				continue
			}
			_, fromKey, err := sqlbase.DecodePartitionTuple(
				a, tableDesc, idxDesc, partDesc, p.FromInclusive, prefixDatums)
			if err != nil {
				return nil, err
			}
			_, toKey, err := sqlbase.DecodePartitionTuple(
				a, tableDesc, idxDesc, partDesc, p.ToExclusive, prefixDatums)
			if err != nil {
				return nil, err
			}
			if _, ok := relevantPartitions[p.Name]; ok {
				coverings = append(coverings, intervalccl.Covering{{
					Start: fromKey, End: toKey,
					Payload: config.Subzone{PartitionName: p.Name},
				}})
			}
		}
	}

	// descendentCoverings are from subpartitions and so get precedence; append
	// them to the front.
	return append(descendentCoverings, coverings...), nil
}

func init() {
	sql.GenerateSubzoneSpans = GenerateSubzoneSpans
}
