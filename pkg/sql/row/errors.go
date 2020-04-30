// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// singleKVFetcher is a kvBatchFetcher that returns a single kv.
type singleKVFetcher struct {
	kvs  [1]roachpb.KeyValue
	done bool
}

// nextBatch implements the kvBatchFetcher interface.
func (f *singleKVFetcher) nextBatch(
	_ context.Context,
) (ok bool, kvs []roachpb.KeyValue, batchResponse []byte, span roachpb.Span, err error) {
	if f.done {
		return false, nil, nil, roachpb.Span{}, nil
	}
	f.done = true
	return true, f.kvs[:], nil, roachpb.Span{}, nil
}

// GetRangesInfo implements the kvBatchFetcher interface.
func (f *singleKVFetcher) GetRangesInfo() []roachpb.RangeInfo {
	panic(errors.AssertionFailedf("GetRangesInfo() called on singleKVFetcher"))
}

// ConvertBatchError returns a user friendly constraint violation error.
func ConvertBatchError(
	ctx context.Context, tableDesc *sqlbase.ImmutableTableDescriptor, b *kv.Batch,
) error {
	origPErr := b.MustPErr()
	if origPErr.Index == nil {
		return origPErr.GoError()
	}
	j := origPErr.Index.Index
	if j >= int32(len(b.Results)) {
		return errors.AssertionFailedf("index %d outside of results: %+v", j, b.Results)
	}
	result := b.Results[j]
	if cErr, ok := origPErr.GetDetail().(*roachpb.ConditionFailedError); ok && len(result.Rows) > 0 {
		key := result.Rows[0].Key
		return NewUniquenessConstraintViolationError(ctx, tableDesc, key, cErr.ActualValue)
	}
	return origPErr.GoError()
}

// NewUniquenessConstraintViolationError creates an error that represents a
// violation of a UNIQUE constraint.
func NewUniquenessConstraintViolationError(
	ctx context.Context,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	key roachpb.Key,
	value *roachpb.Value,
) error {
	// Strip the tenant prefix and pretend use the system tenant's SQL codec for
	// the rest of this function. This is safe because the key is just used to
	// decode the corresponding datums and never escapes this function.
	codec := keys.SystemSQLCodec
	key, _, err := keys.DecodeTenantPrefix(key)
	if err != nil {
		return err
	}
	indexID, _, err := sqlbase.DecodeIndexKeyPrefix(codec, tableDesc.TableDesc(), key)
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
		codec,
		false, /* reverse */
		sqlbase.ScanLockingStrength_FOR_NONE,
		false, /* returnRangeInfo */
		false, /* isCheck */
		&sqlbase.DatumAlloc{},
		tableArgs,
	); err != nil {
		return err
	}
	f := singleKVFetcher{kvs: [1]roachpb.KeyValue{{Key: key}}}
	if value != nil {
		f.kvs[0].Value = *value
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

	valStrs := make([]string, 0, len(datums))
	for _, val := range datums {
		valStrs = append(valStrs, val.String())
	}

	return pgerror.Newf(pgcode.UniqueViolation,
		"duplicate key value (%s)=(%s) violates unique constraint %q",
		strings.Join(index.ColumnNames, ","),
		strings.Join(valStrs, ","),
		index.Name)
}
