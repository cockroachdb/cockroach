// Copyright 2017 The Cockroach Authors.
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
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// GenerateSubzoneSpans constructs from a TableDescriptor the entries mapping
// zone config spans to subzones for use in the SubzoneSpans field of
// zonepb.ZoneConfig. SubzoneSpans controls which splits are created, so only
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
//
// This function has tests in the partitionccl package.
//
// TODO(benesch): remove the hasNewSubzones parameter when a statement to clear
// all subzones at once is introduced.
func GenerateSubzoneSpans(
	st *cluster.Settings,
	clusterID uuid.UUID,
	codec keys.SQLCodec,
	tableDesc catalog.TableDescriptor,
	subzones []zonepb.Subzone,
	hasNewSubzones bool,
) ([]zonepb.SubzoneSpan, error) {
	// Removing zone configs does not require a valid license.
	if hasNewSubzones {
		org := ClusterOrganization.Get(&st.SV)
		if err := base.CheckEnterpriseEnabled(st, clusterID, org,
			"replication zones on indexes or partitions"); err != nil {
			return nil, err
		}
	}

	// We already completely avoid creating subzone spans for dropped indexes.
	// Whether this was intentional is a different story, but it turns out to be
	// pretty sane. Dropped elements may refer to dropped types and we aren't
	// necessarily in a position to deal with those dropped types. Add a special
	// case to avoid generating any subzone spans in the face of being dropped.
	if tableDesc.Dropped() {
		return nil, nil
	}

	a := &rowenc.DatumAlloc{}

	subzoneIndexByIndexID := make(map[descpb.IndexID]int32)
	subzoneIndexByPartition := make(map[string]int32)
	for i, subzone := range subzones {
		if len(subzone.PartitionName) > 0 {
			subzoneIndexByPartition[subzone.PartitionName] = int32(i)
		} else {
			subzoneIndexByIndexID[descpb.IndexID(subzone.IndexID)] = int32(i)
		}
	}

	var indexCovering covering.Covering
	var partitionCoverings []covering.Covering
	if err := catalog.ForEachIndex(tableDesc, catalog.IndexOpts{
		AddMutations: true,
	}, func(idx catalog.Index) error {
		_, indexSubzoneExists := subzoneIndexByIndexID[idx.GetID()]
		if indexSubzoneExists {
			idxSpan := tableDesc.IndexSpan(codec, idx.GetID())
			// Each index starts with a unique prefix, so (from a precedence
			// perspective) it's safe to append them all together.
			indexCovering = append(indexCovering, covering.Range{
				Start: idxSpan.Key, End: idxSpan.EndKey,
				Payload: zonepb.Subzone{IndexID: uint32(idx.GetID())},
			})
		}

		var emptyPrefix []tree.Datum
		indexPartitionCoverings, err := indexCoveringsForPartitioning(
			a, codec, tableDesc, idx, idx.GetPartitioning(), subzoneIndexByPartition, emptyPrefix)
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
	ranges := covering.OverlapCoveringMerge(append(partitionCoverings, indexCovering))

	// NB: This assumes that none of the indexes are interleaved, which is
	// checked in PartitionDescriptor validation.
	sharedPrefix := codec.TablePrefix(uint32(tableDesc.GetID()))

	var subzoneSpans []zonepb.SubzoneSpan
	for _, r := range ranges {
		payloads := r.Payload.([]interface{})
		if len(payloads) == 0 {
			continue
		}
		subzoneSpan := zonepb.SubzoneSpan{
			Key:    bytes.TrimPrefix(r.Start, sharedPrefix),
			EndKey: bytes.TrimPrefix(r.End, sharedPrefix),
		}
		var ok bool
		if subzone := payloads[0].(zonepb.Subzone); len(subzone.PartitionName) > 0 {
			subzoneSpan.SubzoneIndex, ok = subzoneIndexByPartition[subzone.PartitionName]
		} else {
			subzoneSpan.SubzoneIndex, ok = subzoneIndexByIndexID[descpb.IndexID(subzone.IndexID)]
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
// highest precedence first and the interval.Range payloads are each a
// `zonepb.Subzone` with the PartitionName set.
func indexCoveringsForPartitioning(
	a *rowenc.DatumAlloc,
	codec keys.SQLCodec,
	tableDesc catalog.TableDescriptor,
	idx catalog.Index,
	part catalog.Partitioning,
	relevantPartitions map[string]int32,
	prefixDatums []tree.Datum,
) ([]covering.Covering, error) {
	if part.NumColumns() == 0 {
		return nil, nil
	}

	var coverings []covering.Covering
	var descendentCoverings []covering.Covering

	if part.NumLists() > 0 {
		// The returned spans are required to be ordered with highest precedence
		// first. The span for (1, DEFAULT) overlaps with (1, 2) and needs to be
		// returned at a lower precedence. Luckily, because of the partitioning
		// validation, we're guaranteed that all entries in a list partitioning
		// with the same number of DEFAULTs are non-overlapping. So, bucket the
		// `interval.Range`s by the number of non-DEFAULT columns and return
		// them ordered from least # of DEFAULTs to most.
		listCoverings := make([]covering.Covering, part.NumColumns()+1)
		err := part.ForEachList(func(name string, values [][]byte, subPartitioning catalog.Partitioning) error {
			for _, valueEncBuf := range values {
				t, keyPrefix, err := rowenc.DecodePartitionTuple(
					a, codec, tableDesc, idx, part, valueEncBuf, prefixDatums)
				if err != nil {
					return err
				}
				if _, ok := relevantPartitions[name]; ok {
					listCoverings[len(t.Datums)] = append(listCoverings[len(t.Datums)], covering.Range{
						Start: keyPrefix, End: roachpb.Key(keyPrefix).PrefixEnd(),
						Payload: zonepb.Subzone{PartitionName: name},
					})
				}
				newPrefixDatums := append(prefixDatums, t.Datums...)
				subpartitionCoverings, err := indexCoveringsForPartitioning(
					a, codec, tableDesc, idx, subPartitioning, relevantPartitions, newPrefixDatums)
				if err != nil {
					return err
				}
				descendentCoverings = append(descendentCoverings, subpartitionCoverings...)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		for i := range listCoverings {
			if covering := listCoverings[len(listCoverings)-i-1]; len(covering) > 0 {
				coverings = append(coverings, covering)
			}
		}
	}

	if part.NumRanges() > 0 {
		err := part.ForEachRange(func(name string, from, to []byte) error {
			if _, ok := relevantPartitions[name]; !ok {
				return nil
			}
			_, fromKey, err := rowenc.DecodePartitionTuple(
				a, codec, tableDesc, idx, part, from, prefixDatums)
			if err != nil {
				return err
			}
			_, toKey, err := rowenc.DecodePartitionTuple(
				a, codec, tableDesc, idx, part, to, prefixDatums)
			if err != nil {
				return err
			}
			if _, ok := relevantPartitions[name]; ok {
				coverings = append(coverings, covering.Covering{{
					Start: fromKey, End: toKey,
					Payload: zonepb.Subzone{PartitionName: name},
				}})
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	// descendentCoverings are from subpartitions and so get precedence; append
	// them to the front.
	return append(descendentCoverings, coverings...), nil
}
