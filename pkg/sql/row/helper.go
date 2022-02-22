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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/rowencpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const (
	// maxRowSizeFloor is the lower bound for sql.guardrails.max_row_size_{log|err}.
	maxRowSizeFloor = 1 << 10
	// maxRowSizeCeil is the upper bound for sql.guardrails.max_row_size_{log|err}.
	maxRowSizeCeil = 1 << 30
)

var maxRowSizeLog = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	"sql.guardrails.max_row_size_log",
	"maximum size of row (or column family if multiple column families are in use) that SQL can "+
		"write to the database, above which an event is logged to SQL_PERF (or SQL_INTERNAL_PERF "+
		"if the mutating statement was internal); use 0 to disable",
	kvserver.MaxCommandSizeDefault,
	func(size int64) error {
		if size != 0 && size < maxRowSizeFloor {
			return errors.Newf(
				"cannot set sql.guardrails.max_row_size_log to %v, must be 0 or >= %v",
				size, maxRowSizeFloor,
			)
		} else if size > maxRowSizeCeil {
			return errors.Newf(
				"cannot set sql.guardrails.max_row_size_log to %v, must be <= %v",
				size, maxRowSizeCeil,
			)
		}
		return nil
	},
).WithPublic()

var maxRowSizeErr = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	"sql.guardrails.max_row_size_err",
	"maximum size of row (or column family if multiple column families are in use) that SQL can "+
		"write to the database, above which an error is returned; use 0 to disable",
	512<<20, /* 512 MiB */
	func(size int64) error {
		if size != 0 && size < maxRowSizeFloor {
			return errors.Newf(
				"cannot set sql.guardrails.max_row_size_err to %v, must be 0 or >= %v",
				size, maxRowSizeFloor,
			)
		} else if size > maxRowSizeCeil {
			return errors.Newf(
				"cannot set sql.guardrails.max_row_size_err to %v, must be <= %v",
				size, maxRowSizeCeil,
			)
		}
		return nil
	},
).WithPublic()

// rowHelper has the common methods for table row manipulations.
type rowHelper struct {
	Codec keys.SQLCodec

	TableDesc catalog.TableDescriptor
	// Secondary indexes.
	Indexes      []catalog.Index
	indexEntries map[catalog.Index][]rowenc.IndexEntry

	// Computed during initialization for pretty-printing.
	primIndexValDirs []encoding.Direction
	secIndexValDirs  [][]encoding.Direction

	// Computed and cached.
	primaryIndexKeyPrefix []byte
	primaryIndexKeyCols   catalog.TableColSet
	primaryIndexValueCols catalog.TableColSet
	sortedColumnFamilies  map[descpb.FamilyID][]descpb.ColumnID

	// Used to check row size.
	maxRowSizeLog, maxRowSizeErr uint32
	internal                     bool
	metrics                      *Metrics
}

func newRowHelper(
	codec keys.SQLCodec,
	desc catalog.TableDescriptor,
	indexes []catalog.Index,
	sv *settings.Values,
	internal bool,
	metrics *Metrics,
) rowHelper {
	rh := rowHelper{
		Codec:     codec,
		TableDesc: desc,
		Indexes:   indexes,
		internal:  internal,
		metrics:   metrics,
	}

	// Pre-compute the encoding directions of the index key values for
	// pretty-printing in traces.
	rh.primIndexValDirs = catalogkeys.IndexKeyValDirs(rh.TableDesc.GetPrimaryIndex())

	rh.secIndexValDirs = make([][]encoding.Direction, len(rh.Indexes))
	for i := range rh.Indexes {
		rh.secIndexValDirs[i] = catalogkeys.IndexKeyValDirs(rh.Indexes[i])
	}

	rh.maxRowSizeLog = uint32(maxRowSizeLog.Get(sv))
	rh.maxRowSizeErr = uint32(maxRowSizeErr.Get(sv))

	return rh
}

// encodeIndexes encodes the primary and secondary index keys. The
// secondaryIndexEntries are only valid until the next call to encodeIndexes or
// encodeSecondaryIndexes. includeEmpty details whether the results should
// include empty secondary index k/v pairs.
func (rh *rowHelper) encodeIndexes(
	colIDtoRowIndex catalog.TableColMap,
	values []tree.Datum,
	ignoreIndexes util.FastIntSet,
	includeEmpty bool,
) (
	primaryIndexKey []byte,
	secondaryIndexEntries map[catalog.Index][]rowenc.IndexEntry,
	err error,
) {
	primaryIndexKey, err = rh.encodePrimaryIndex(colIDtoRowIndex, values)
	if err != nil {
		return nil, nil, err
	}
	secondaryIndexEntries, err = rh.encodeSecondaryIndexes(colIDtoRowIndex, values, ignoreIndexes, includeEmpty)
	if err != nil {
		return nil, nil, err
	}
	return primaryIndexKey, secondaryIndexEntries, nil
}

// encodePrimaryIndex encodes the primary index key.
func (rh *rowHelper) encodePrimaryIndex(
	colIDtoRowIndex catalog.TableColMap, values []tree.Datum,
) (primaryIndexKey []byte, err error) {
	if rh.primaryIndexKeyPrefix == nil {
		rh.primaryIndexKeyPrefix = rowenc.MakeIndexKeyPrefix(
			rh.Codec, rh.TableDesc.GetID(), rh.TableDesc.GetPrimaryIndexID(),
		)
	}
	idx := rh.TableDesc.GetPrimaryIndex()
	primaryIndexKey, containsNull, err := rowenc.EncodeIndexKey(
		rh.TableDesc, idx, colIDtoRowIndex, values, rh.primaryIndexKeyPrefix,
	)
	if containsNull {
		return nil, rowenc.MakeNullPKError(rh.TableDesc, idx, colIDtoRowIndex, values)
	}
	return primaryIndexKey, err
}

// encodeSecondaryIndexes encodes the secondary index keys based on a row's
// values.
//
// The secondaryIndexEntries are only valid until the next call to encodeIndexes
// or encodeSecondaryIndexes, when they are overwritten.
//
// This function will not encode index entries for any index with an ID in
// ignoreIndexes.
//
// includeEmpty details whether the results should include empty secondary index
// k/v pairs.
func (rh *rowHelper) encodeSecondaryIndexes(
	colIDtoRowIndex catalog.TableColMap,
	values []tree.Datum,
	ignoreIndexes util.FastIntSet,
	includeEmpty bool,
) (secondaryIndexEntries map[catalog.Index][]rowenc.IndexEntry, err error) {

	if rh.indexEntries == nil {
		rh.indexEntries = make(map[catalog.Index][]rowenc.IndexEntry, len(rh.Indexes))
	}

	for i := range rh.indexEntries {
		rh.indexEntries[i] = rh.indexEntries[i][:0]
	}

	for i := range rh.Indexes {
		index := rh.Indexes[i]
		if !ignoreIndexes.Contains(int(index.GetID())) {
			entries, err := rowenc.EncodeSecondaryIndex(rh.Codec, rh.TableDesc, index, colIDtoRowIndex, values, includeEmpty)
			if err != nil {
				return nil, err
			}
			rh.indexEntries[index] = append(rh.indexEntries[index], entries...)
		}
	}

	return rh.indexEntries, nil
}

// skipColumnNotInPrimaryIndexValue returns true if the value at column colID
// does not need to be encoded, either because it is already part of the primary
// key, or because it is not part of the primary index altogether. Composite
// datums are considered too, so a composite datum in a PK will return false.
func (rh *rowHelper) skipColumnNotInPrimaryIndexValue(
	colID descpb.ColumnID, value tree.Datum,
) (bool, error) {
	if rh.primaryIndexKeyCols.Empty() {
		rh.primaryIndexKeyCols = rh.TableDesc.GetPrimaryIndex().CollectKeyColumnIDs()
		rh.primaryIndexValueCols = rh.TableDesc.GetPrimaryIndex().CollectPrimaryStoredColumnIDs()
	}
	if !rh.primaryIndexKeyCols.Contains(colID) {
		return !rh.primaryIndexValueCols.Contains(colID), nil
	}
	if cdatum, ok := value.(tree.CompositeDatum); ok {
		// Composite columns are encoded in both the key and the value.
		return !cdatum.IsComposite(), nil
	}
	// Skip primary key columns as their values are encoded in the key of
	// each family. Family 0 is guaranteed to exist and acts as a
	// sentinel.
	return true, nil
}

func (rh *rowHelper) sortedColumnFamily(famID descpb.FamilyID) ([]descpb.ColumnID, bool) {
	if rh.sortedColumnFamilies == nil {
		rh.sortedColumnFamilies = make(map[descpb.FamilyID][]descpb.ColumnID, rh.TableDesc.NumFamilies())

		_ = rh.TableDesc.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
			colIDs := append([]descpb.ColumnID{}, family.ColumnIDs...)
			sort.Sort(descpb.ColumnIDs(colIDs))
			rh.sortedColumnFamilies[family.ID] = colIDs
			return nil
		})
	}
	colIDs, ok := rh.sortedColumnFamilies[famID]
	return colIDs, ok
}

// checkRowSize compares the size of a primary key column family against the
// max_row_size limits.
func (rh *rowHelper) checkRowSize(
	ctx context.Context, key *roachpb.Key, value *roachpb.Value, family descpb.FamilyID,
) error {
	size := uint32(len(*key)) + uint32(len(value.RawBytes))
	shouldLog := rh.maxRowSizeLog != 0 && size > rh.maxRowSizeLog
	shouldErr := rh.maxRowSizeErr != 0 && size > rh.maxRowSizeErr
	if !shouldLog && !shouldErr {
		return nil
	}
	details := eventpb.CommonLargeRowDetails{
		RowSize:    size,
		TableID:    uint32(rh.TableDesc.GetID()),
		FamilyID:   uint32(family),
		PrimaryKey: keys.PrettyPrint(rh.primIndexValDirs, *key),
	}
	if rh.internal && shouldErr {
		// Internal work should never err and always log if violating either limit.
		shouldErr = false
		shouldLog = true
	}
	if shouldLog {
		if rh.metrics != nil {
			rh.metrics.MaxRowSizeLogCount.Inc(1)
		}
		var event eventpb.EventPayload
		if rh.internal {
			event = &eventpb.LargeRowInternal{CommonLargeRowDetails: details}
		} else {
			event = &eventpb.LargeRow{CommonLargeRowDetails: details}
		}
		log.StructuredEvent(ctx, event)
	}
	if shouldErr {
		if rh.metrics != nil {
			rh.metrics.MaxRowSizeErrCount.Inc(1)
		}
		return pgerror.WithCandidateCode(&details, pgcode.ProgramLimitExceeded)
	}
	return nil
}

var deleteEncoding protoutil.Message = &rowencpb.IndexValueWrapper{
	Value:   nil,
	Deleted: true,
}

func (rh *rowHelper) deleteIndexEntry(
	ctx context.Context,
	batch *kv.Batch,
	index catalog.Index,
	valDirs []encoding.Direction,
	entry *rowenc.IndexEntry,
	traceKV bool,
) error {
	if index.UseDeletePreservingEncoding() {
		if traceKV {
			log.VEventf(ctx, 2, "Put (delete) %s", entry.Key)
		}

		batch.Put(entry.Key, deleteEncoding)
	} else {
		if traceKV {
			if valDirs != nil {
				log.VEventf(ctx, 2, "Del %s", keys.PrettyPrint(valDirs, entry.Key))
			} else {
				log.VEventf(ctx, 2, "Del %s", entry.Key)
			}
		}

		batch.Del(entry.Key)
	}
	return nil
}
