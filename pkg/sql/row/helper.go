// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package row

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/rowencpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
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
	settings.ApplicationLevel,
	"sql.guardrails.max_row_size_log",
	"maximum size of row (or column family if multiple column families are in use) that SQL can "+
		"write to the database, above which an event is logged to SQL_PERF (or SQL_INTERNAL_PERF "+
		"if the mutating statement was internal); use 0 to disable",
	kvserverbase.MaxCommandSizeDefault,
	settings.IntInRangeOrZeroDisable(maxRowSizeFloor, maxRowSizeCeil),
	settings.WithPublic,
)

var maxRowSizeErr = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"sql.guardrails.max_row_size_err",
	"maximum size of row (or column family if multiple column families are in use) that SQL can "+
		"write to the database, above which an error is returned; use 0 to disable",
	512<<20, /* 512 MiB */
	settings.IntInRangeOrZeroDisable(maxRowSizeFloor, maxRowSizeCeil),
	settings.WithPublic,
)

// RowHelper has the common methods for table row manipulations.
type RowHelper struct {
	Codec keys.SQLCodec

	TableDesc catalog.TableDescriptor
	// Secondary indexes.
	Indexes      []catalog.Index
	indexEntries map[catalog.Index][]rowenc.IndexEntry

	// Computed during initialization for pretty-printing.
	primIndexValDirs []encoding.Direction
	secIndexValDirs  [][]encoding.Direction

	// Computed and cached.
	PrimaryIndexKeyPrefix []byte
	primaryIndexKeyCols   catalog.TableColSet
	primaryIndexValueCols catalog.TableColSet
	sortedColumnFamilies  map[descpb.FamilyID][]descpb.ColumnID

	// Used to check row size.
	maxRowSizeLog, maxRowSizeErr uint32
	internal                     bool
	metrics                      *rowinfra.Metrics
}

func NewRowHelper(
	codec keys.SQLCodec,
	desc catalog.TableDescriptor,
	indexes []catalog.Index,
	sv *settings.Values,
	internal bool,
	metrics *rowinfra.Metrics,
) RowHelper {
	rh := RowHelper{
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
func (rh *RowHelper) encodeIndexes(
	ctx context.Context,
	colIDtoRowIndex catalog.TableColMap,
	values []tree.Datum,
	ignoreIndexes intsets.Fast,
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
	secondaryIndexEntries, err = rh.encodeSecondaryIndexes(ctx, colIDtoRowIndex, values, ignoreIndexes, includeEmpty)
	if err != nil {
		return nil, nil, err
	}
	return primaryIndexKey, secondaryIndexEntries, nil
}

func (rh *RowHelper) Init() {
	rh.PrimaryIndexKeyPrefix = rowenc.MakeIndexKeyPrefix(
		rh.Codec, rh.TableDesc.GetID(), rh.TableDesc.GetPrimaryIndexID(),
	)
}

// encodePrimaryIndex encodes the primary index key.
func (rh *RowHelper) encodePrimaryIndex(
	colIDtoRowIndex catalog.TableColMap, values []tree.Datum,
) (primaryIndexKey []byte, err error) {
	if rh.PrimaryIndexKeyPrefix == nil {
		rh.Init()
	}
	idx := rh.TableDesc.GetPrimaryIndex()
	primaryIndexKey, containsNull, err := rowenc.EncodeIndexKey(
		rh.TableDesc, idx, colIDtoRowIndex, values, rh.PrimaryIndexKeyPrefix,
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
func (rh *RowHelper) encodeSecondaryIndexes(
	ctx context.Context,
	colIDtoRowIndex catalog.TableColMap,
	values []tree.Datum,
	ignoreIndexes intsets.Fast,
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
			entries, err := rowenc.EncodeSecondaryIndex(ctx, rh.Codec, rh.TableDesc, index, colIDtoRowIndex, values, includeEmpty)
			if err != nil {
				return nil, err
			}
			rh.indexEntries[index] = append(rh.indexEntries[index], entries...)
		}
	}

	return rh.indexEntries, nil
}

// encodePrimaryIndexValuesToBuf encodes the given values, writing
// into the given buffer.
func (rh *RowHelper) encodePrimaryIndexValuesToBuf(
	vals []tree.Datum,
	valColIDMapping catalog.TableColMap,
	sortedColumnIDs []descpb.ColumnID,
	fetchedCols []catalog.Column,
	buf []byte,
) ([]byte, error) {
	var lastColID descpb.ColumnID
	for _, colID := range sortedColumnIDs {
		idx, ok := valColIDMapping.Get(colID)
		if !ok || vals[idx] == tree.DNull {
			// Column not being updated or inserted.
			continue
		}

		if skip, _ := rh.SkipColumnNotInPrimaryIndexValue(colID, vals[idx]); skip {
			continue
		}

		col := fetchedCols[idx]
		if lastColID > col.GetID() {
			return nil, errors.AssertionFailedf("cannot write column id %d after %d", col.GetID(), lastColID)
		}
		colIDDelta := valueside.MakeColumnIDDelta(lastColID, col.GetID())
		lastColID = col.GetID()
		var err error
		buf, err = valueside.Encode(buf, colIDDelta, vals[idx], nil)
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// SkipColumnNotInPrimaryIndexValue returns true if the value at column colID
// does not need to be encoded, either because it is already part of the primary
// key, or because it is not part of the primary index altogether. Composite
// datums are considered too, so a composite datum in a PK will return false
// (but will return true for couldBeComposite).
func (rh *RowHelper) SkipColumnNotInPrimaryIndexValue(
	colID descpb.ColumnID, value tree.Datum,
) (skip, couldBeComposite bool) {
	if rh.primaryIndexKeyCols.Empty() {
		rh.primaryIndexKeyCols = rh.TableDesc.GetPrimaryIndex().CollectKeyColumnIDs()
		rh.primaryIndexValueCols = rh.TableDesc.GetPrimaryIndex().CollectPrimaryStoredColumnIDs()
	}
	return rowenc.SkipColumnNotInPrimaryIndexValue(colID, value, rh.primaryIndexKeyCols, rh.primaryIndexValueCols)
}

func (rh *RowHelper) SortedColumnFamily(famID descpb.FamilyID) ([]descpb.ColumnID, bool) {
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

// CheckRowSize compares the size of a primary key column family against the
// max_row_size limits.
func (rh *RowHelper) CheckRowSize(
	ctx context.Context, key *roachpb.Key, valueBytes []byte, family descpb.FamilyID,
) error {
	size := uint32(len(*key)) + uint32(len(valueBytes))
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
		var event logpb.EventPayload
		if rh.internal {
			event = &eventpb.LargeRowInternal{CommonLargeRowDetails: details}
		} else {
			event = &eventpb.LargeRow{CommonLargeRowDetails: details}
		}
		log.StructuredEvent(ctx, severity.INFO, event)
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

func (rh *RowHelper) deleteIndexEntry(
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

// OriginTimetampCPutHelper is used by callers of Inserter, Updater,
// and Deleter when the caller wants updates to the primary key to be
// constructed using ConditionalPutRequests with the OriginTimestamp
// option set.
type OriginTimestampCPutHelper struct {
	OriginTimestamp hlc.Timestamp
	ShouldWinTie    bool
	// PreviousWasDeleted is used to indicate that the expected
	// value is non-existent. This is helpful in Deleter to
	// distinguish between a delete of a value that had no columns
	// in the value vs a delete of a non-existent value.
	PreviousWasDeleted bool
}

func (oh *OriginTimestampCPutHelper) IsSet() bool {
	return oh != nil && oh.OriginTimestamp.IsSet()
}

func (oh *OriginTimestampCPutHelper) CPutFn(
	ctx context.Context,
	b Putter,
	key *roachpb.Key,
	value *roachpb.Value,
	expVal []byte,
	traceKV bool,
) {
	if traceKV {
		log.VEventfDepth(ctx, 1, 2, "CPutWithOriginTimestamp %s -> %s @ %s", *key, value.PrettyPrint(), oh.OriginTimestamp)
	}
	b.CPutWithOriginTimestamp(key, value, expVal, oh.OriginTimestamp, oh.ShouldWinTie)
}

func (oh *OriginTimestampCPutHelper) DelWithCPut(
	ctx context.Context, b Putter, key *roachpb.Key, expVal []byte, traceKV bool,
) {
	if traceKV {
		log.VEventfDepth(ctx, 1, 2, "CPutWithOriginTimestamp %s -> nil (delete) @ %s", key, oh.OriginTimestamp)
	}
	b.CPutWithOriginTimestamp(key, nil, expVal, oh.OriginTimestamp, oh.ShouldWinTie)
}

func FetchSpecRequiresRawMVCCValues(spec fetchpb.IndexFetchSpec) bool {
	for idx := range spec.FetchedColumns {
		colID := spec.FetchedColumns[idx].ColumnID
		if colinfo.IsColIDSystemColumn(colID) {
			switch colinfo.GetSystemColumnKindFromColumnID(colID) {
			case catpb.SystemColumnKind_ORIGINID, catpb.SystemColumnKind_ORIGINTIMESTAMP:
				return true
			}
		}
	}
	return false
}
