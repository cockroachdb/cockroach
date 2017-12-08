// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlccl

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/intervalccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type repartitioningSide string

const (
	repartitioningBefore repartitioningSide = "old"
	repartitioningAfter  repartitioningSide = "new"
)

// partitionLeafSpans returns the spans covered by all partitions which are not
// subpartitioned, which we call a "leaf covering".
//
// TODO(dan): We have to do the same "sort by num DEFAULTS" trick that
// indexCoveringsForPartitioning does, but there's currently no test that fails
// if we don't.
func partitionLeafSpans(
	a *sqlbase.DatumAlloc,
	tableDesc *sqlbase.TableDescriptor,
	idxDesc *sqlbase.IndexDescriptor,
	partDesc *sqlbase.PartitioningDescriptor,
	prefixDatums []tree.Datum,
	payload interface{},
) ([]intervalccl.Covering, error) {
	var coverings []intervalccl.Covering

	if len(partDesc.List) > 0 {
		// The span for `(1, DEFAULT)` overlaps with `(1, 2)` and
		// intervalccl.Covering is required to be non-overlapping so we have be
		// tricky here. Because of the partitioning validation, we're guaranteed
		// that all entries in a list partitioning with the same number of
		// DEFAULTs are non-overlapping. So, bucket the `intervalccl.Range`s by
		// the number of non-DEFAULT columns.
		listCoverings := make([]intervalccl.Covering, int(partDesc.NumColumns)+1)
		for _, p := range partDesc.List {
			for _, valueEncBuf := range p.Values {
				datums, keyPrefix, err := sqlbase.TranslateValueEncodingToSpan(
					a, tableDesc, idxDesc, partDesc, valueEncBuf, prefixDatums)
				if err != nil {
					return nil, err
				}
				newPrefixDatums := append(prefixDatums, datums...)
				if p.Subpartitioning.NumColumns > 0 {
					descendentCoverings, err := partitionLeafSpans(
						a, tableDesc, idxDesc, &p.Subpartitioning, newPrefixDatums, payload)
					if err != nil {
						return nil, err
					}
					if len(descendentCoverings) > 0 {
						coverings = append(coverings, descendentCoverings...)
						continue
					}
				}
				listCoverings[len(datums)] = append(listCoverings[len(datums)], intervalccl.Range{
					Start: keyPrefix, End: roachpb.Key(keyPrefix).PrefixEnd(), Payload: payload,
				})
			}
		}
		for _, covering := range listCoverings {
			if len(covering) > 0 {
				coverings = append(coverings, covering)
			}
		}
	} else if len(partDesc.Range) > 0 {
		lastEndKey := sqlbase.MakeIndexKeyPrefix(tableDesc, idxDesc.ID)
		if len(prefixDatums) > 0 {
			colMap := make(map[sqlbase.ColumnID]int, len(prefixDatums))
			for i := range prefixDatums {
				colMap[idxDesc.ColumnIDs[i]] = i
			}

			var err error
			lastEndKey, _, err = sqlbase.EncodePartialIndexKey(
				tableDesc, idxDesc, len(prefixDatums), colMap, prefixDatums, lastEndKey)
			if err != nil {
				return nil, err
			}
		}

		var covering intervalccl.Covering
		for _, p := range partDesc.Range {
			_, endKey, err := sqlbase.TranslateValueEncodingToSpan(
				a, tableDesc, idxDesc, partDesc, p.UpperBound, prefixDatums)
			if err != nil {
				return nil, err
			}

			covering = append(covering, intervalccl.Range{
				Start: lastEndKey, End: endKey, Payload: payload,
			})
			lastEndKey = endKey
		}
		coverings = append(coverings, covering)
	} else {
		// An unpartitioned index essentially has one anonymous DEFAULT
		// partition covering the whole thing.
		span := tableDesc.IndexSpan(idxDesc.ID)
		coverings = append(coverings, intervalccl.Covering{{
			Start: span.Key, End: span.EndKey, Payload: payload,
		}})
	}

	return coverings, nil
}

// RepartitioningFastPathAvailable returns true when the schema change to
// validate existing data can be skipped.
//
// Each partitioned index guarantees that all rows in that index belong to one
// of its partitions. Certain repartitionings (removing partitioning entirely, a
// LIST partitioning that is a superset of the previous partitioning, etc) can
// assume the the existing data meets this guarantee without checking it via a
// sort of "inductive proof" logic.
func RepartitioningFastPathAvailable(
	oldTableDesc, newTableDesc *sqlbase.TableDescriptor,
) (bool, error) {
	a := &sqlbase.DatumAlloc{}
	var emptyPrefix []tree.Datum
	var coverings []intervalccl.Covering

	if err := oldTableDesc.ForeachNonDropIndex(func(idxDesc *sqlbase.IndexDescriptor) error {
		partitionCoverings, err := partitionLeafSpans(
			a, newTableDesc, idxDesc, &idxDesc.Partitioning, emptyPrefix, repartitioningBefore)
		coverings = append(coverings, partitionCoverings...)
		return err
	}); err != nil {
		return false, err
	}

	if err := newTableDesc.ForeachNonDropIndex(func(idxDesc *sqlbase.IndexDescriptor) error {
		partitionCoverings, err := partitionLeafSpans(
			a, newTableDesc, idxDesc, &idxDesc.Partitioning, emptyPrefix, repartitioningAfter)
		coverings = append(coverings, partitionCoverings...)
		return err
	}); err != nil {
		return false, err
	}

	ranges := intervalccl.OverlapCoveringMerge(coverings)

	for _, r := range ranges {
		payloads := r.Payload.([]interface{})
		// Because the old partitions are before the new partitions in the input
		// to OverlapCoveringMerge, if the last thing in the payload is
		// repartitioningBefore, then we don't have the fast path.
		if p := payloads[len(payloads)-1].(repartitioningSide); p == repartitioningBefore {
			return false, nil
		}
	}
	return true, nil
}

func init() {
	sql.RepartitioningFastPathAvailable = RepartitioningFastPathAvailable
}
