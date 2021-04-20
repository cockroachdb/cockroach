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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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

// ConvertBatchError returns a user friendly constraint violation error.
func ConvertBatchError(ctx context.Context, tableDesc catalog.TableDescriptor, b *kv.Batch) error {
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

// KeyToDescTranslator is capable of translating a key found in an error to a
// table descriptor for error reporting.
type KeyToDescTranslator interface {
	// KeyToDesc attempts to translate the key found in an error to a table
	// descriptor. An implementation can return (nil, false) if the translation
	// failed because the key is not part of a table it was scanning, but is
	// instead part of an interleaved relative (parent/sibling/child) table.
	KeyToDesc(roachpb.Key) (catalog.TableDescriptor, bool)
}

// ConvertFetchError attempts to a map key-value error generated during a
// key-value fetch to a user friendly SQL error.
func ConvertFetchError(ctx context.Context, descForKey KeyToDescTranslator, err error) error {
	var wiErr *roachpb.WriteIntentError
	if errors.As(err, &wiErr) {
		key := wiErr.Intents[0].Key
		desc, _ := descForKey.KeyToDesc(key)
		return NewLockNotAvailableError(ctx, desc, key)
	}
	return err
}

// NewUniquenessConstraintViolationError creates an error that represents a
// violation of a UNIQUE constraint.
func NewUniquenessConstraintViolationError(
	ctx context.Context, tableDesc catalog.TableDescriptor, key roachpb.Key, value *roachpb.Value,
) error {
	index, names, values, err := DecodeRowInfo(ctx, tableDesc, key, value, false)
	if err != nil {
		return pgerror.Newf(pgcode.UniqueViolation,
			"duplicate key value: decoding err=%s", err)
	}

	// Exclude implicit partitioning columns and hash sharded index columns from
	// the error message.
	skipCols := index.ExplicitColumnStartIdx()
	return errors.WithDetail(
		pgerror.WithConstraintName(pgerror.Newf(pgcode.UniqueViolation,
			"duplicate key value violates unique constraint %q", index.Name,
		), index.Name),
		fmt.Sprintf(
			"Key (%s)=(%s) already exists.",
			strings.Join(names[skipCols:], ","),
			strings.Join(values[skipCols:], ","),
		),
	)
}

// NewLockNotAvailableError creates an error that represents an inability to
// acquire a lock. A nil tableDesc can be provided, which indicates that the
// table descriptor corresponding to the key is unknown due to a table
// interleaving.
func NewLockNotAvailableError(
	ctx context.Context, tableDesc catalog.TableDescriptor, key roachpb.Key,
) error {
	if tableDesc == nil {
		return pgerror.Newf(pgcode.LockNotAvailable,
			"could not obtain lock on row in interleaved table")
	}

	index, colNames, values, err := DecodeRowInfo(ctx, tableDesc, key, nil, false)
	if err != nil {
		return pgerror.Newf(pgcode.LockNotAvailable,
			"could not obtain lock on row: decoding err=%s", err)
	}

	return pgerror.Newf(pgcode.LockNotAvailable,
		"could not obtain lock on row (%s)=(%s) in %s@%s",
		strings.Join(colNames, ","),
		strings.Join(values, ","),
		tableDesc.GetName(),
		index.Name)
}

// DecodeRowInfo takes a table descriptor, a key, and an optional value and
// returns information about the corresponding SQL row. If successful, the index
// and corresponding column names and values to the provided KV are returned.
func DecodeRowInfo(
	ctx context.Context,
	tableDesc catalog.TableDescriptor,
	key roachpb.Key,
	value *roachpb.Value,
	allColumns bool,
) (_ *descpb.IndexDescriptor, columnNames []string, columnValues []string, _ error) {
	// Strip the tenant prefix and pretend to use the system tenant's SQL codec
	// for the rest of this function. This is safe because the key is just used
	// to decode the corresponding datums and never escapes this function.
	codec := keys.SystemSQLCodec
	key, _, err := keys.DecodeTenantPrefix(key)
	if err != nil {
		return nil, nil, nil, err
	}
	indexID, _, err := rowenc.DecodeIndexKeyPrefix(codec, tableDesc, key)
	if err != nil {
		return nil, nil, nil, err
	}
	index, err := tableDesc.FindIndexWithID(indexID)
	if err != nil {
		return nil, nil, nil, err
	}
	var rf Fetcher

	var colIDs []descpb.ColumnID
	if !allColumns {
		colIDs = make([]descpb.ColumnID, index.NumColumns())
		for i := range colIDs {
			colIDs[i] = index.GetColumnID(i)
		}
	} else if index.Primary() {
		colIDs = make([]descpb.ColumnID, len(tableDesc.PublicColumns()))
		for i, col := range tableDesc.PublicColumns() {
			colIDs[i] = col.GetID()
		}
	} else {
		colIDs = make([]descpb.ColumnID, 0, index.NumColumns()+index.NumExtraColumns()+index.NumStoredColumns())
		_ = index.ForEachColumnID(func(id descpb.ColumnID) error {
			colIDs = append(colIDs, id)
			return nil
		})
	}

	var valNeededForCol util.FastIntSet
	valNeededForCol.AddRange(0, len(colIDs)-1)

	var colIdxMap catalog.TableColMap
	cols := make([]descpb.ColumnDescriptor, len(colIDs))
	for i, colID := range colIDs {
		colIdxMap.Set(colID, i)
		col, err := tableDesc.FindColumnWithID(colID)
		if err != nil {
			return nil, nil, nil, err
		}
		cols[i] = *col.ColumnDesc()
	}

	tableArgs := FetcherTableArgs{
		Desc:             tableDesc,
		Index:            index.IndexDesc(),
		ColIdxMap:        colIdxMap,
		IsSecondaryIndex: indexID != tableDesc.GetPrimaryIndexID(),
		Cols:             cols,
		ValNeededForCol:  valNeededForCol,
	}
	rf.IgnoreUnexpectedNulls = true
	if err := rf.Init(
		ctx,
		codec,
		false, /* reverse */
		descpb.ScanLockingStrength_FOR_NONE,
		descpb.ScanLockingWaitPolicy_BLOCK,
		false, /* isCheck */
		&rowenc.DatumAlloc{},
		nil, /* memMonitor */
		tableArgs,
	); err != nil {
		return nil, nil, nil, err
	}
	f := singleKVFetcher{kvs: [1]roachpb.KeyValue{{Key: key}}}
	if value != nil {
		f.kvs[0].Value = *value
	}
	// Use the Fetcher to decode the single kv pair above by passing in
	// this singleKVFetcher implementation, which doesn't actually hit KV.
	if err := rf.StartScanFrom(ctx, &f); err != nil {
		return nil, nil, nil, err
	}
	datums, _, _, err := rf.NextRowDecoded(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	names := make([]string, len(cols))
	values := make([]string, len(cols))
	for i := range cols {
		names[i] = cols[i].Name
		if datums[i] == tree.DNull {
			continue
		}
		values[i] = datums[i].String()
	}
	return index.IndexDesc(), names, values, nil
}

func (f *singleKVFetcher) close(context.Context) {}
