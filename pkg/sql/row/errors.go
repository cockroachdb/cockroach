// Copyright 2018 The Cockroach Authors.
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

package row

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// singleKVFetcher is a kvBatchFetcher that returns a single kv.
type singleKVFetcher struct {
	kvs  [1]roachpb.KeyValue
	done bool
}

// nextBatch implements the kvBatchFetcher interface.
func (f *singleKVFetcher) nextBatch(
	_ context.Context,
) (
	ok bool,
	kvs []roachpb.KeyValue,
	batchResponse []byte,
	numKvs int64,
	span roachpb.Span,
	err error,
) {
	if f.done {
		return false, nil, nil, 0, roachpb.Span{}, nil
	}
	f.done = true
	return true, f.kvs[:], nil, 0, roachpb.Span{}, nil
}

// getRangesInfo implements the kvBatchFetcher interface.
func (f *singleKVFetcher) getRangesInfo() []roachpb.RangeInfo {
	panic("getRangesInfo() called on singleKVFetcher")
}

// ConvertBatchError returns a user friendly constraint violation error.
func ConvertBatchError(
	ctx context.Context, tableDesc *sqlbase.ImmutableTableDescriptor, b *client.Batch,
) error {
	origPErr := b.MustPErr()
	if origPErr.Index == nil {
		return origPErr.GoError()
	}
	j := origPErr.Index.Index
	if j >= int32(len(b.Results)) {
		panic(fmt.Sprintf("index %d outside of results: %+v", j, b.Results))
	}
	result := b.Results[j]
	if cErr, ok := origPErr.GetDetail().(*roachpb.ConditionFailedError); ok && len(result.Rows) > 0 {
		key := result.Rows[0].Key
		// TODO(dan): There's too much internal knowledge of the sql table
		// encoding here (and this callsite is the only reason
		// DecodeIndexKeyPrefix is exported). Refactor this bit out.
		indexID, _, err := sqlbase.DecodeIndexKeyPrefix(tableDesc.TableDesc(), key)
		if err != nil {
			return err
		}
		index, err := tableDesc.FindIndexByID(indexID)
		if err != nil {
			return err
		}
		var rf Fetcher

		var valNeededForCol util.FastIntSet
		valNeededForCol.AddRange(0, len(index.ColumnIDs)-1)

		colIdxMap := make(map[sqlbase.ColumnID]int, len(index.ColumnIDs))
		cols := make([]sqlbase.ColumnDescriptor, len(index.ColumnIDs))
		for i, colID := range index.ColumnIDs {
			colIdxMap[colID] = i
			col, err := tableDesc.FindColumnByID(colID)
			if err != nil {
				return err
			}
			cols[i] = *col
		}

		tableArgs := FetcherTableArgs{
			Desc:             tableDesc,
			Index:            index,
			ColIdxMap:        colIdxMap,
			IsSecondaryIndex: indexID != tableDesc.PrimaryIndex.ID,
			Cols:             cols,
			ValNeededForCol:  valNeededForCol,
		}
		if err := rf.Init(
			false /* reverse */, false /* returnRangeInfo */, false /* isCheck */, &sqlbase.DatumAlloc{}, tableArgs,
		); err != nil {
			return err
		}
		f := singleKVFetcher{kvs: [1]roachpb.KeyValue{{Key: key}}}
		if cErr.ActualValue != nil {
			f.kvs[0].Value = *cErr.ActualValue
		}
		// Use the Fetcher to decode the single kv pair above by passing in
		// this singleKVFetcher implementation, which doesn't actually hit KV.
		if err := rf.StartScanFrom(ctx, &f); err != nil {
			return err
		}
		datums, _, _, err := rf.NextRowDecoded(ctx)
		if err != nil {
			return err
		}
		return NewUniquenessConstraintViolationError(index, datums)
	}
	return origPErr.GoError()
}

// NewUniquenessConstraintViolationError creates an error that represents a
// violation of a UNIQUE constraint.
func NewUniquenessConstraintViolationError(
	index *sqlbase.IndexDescriptor, vals []tree.Datum,
) error {
	valStrs := make([]string, 0, len(vals))
	for _, val := range vals {
		valStrs = append(valStrs, val.String())
	}

	return pgerror.NewErrorf(pgerror.CodeUniqueViolationError,
		"duplicate key value (%s)=(%s) violates unique constraint %q",
		strings.Join(index.ColumnNames, ","),
		strings.Join(valStrs, ","),
		index.Name)
}
