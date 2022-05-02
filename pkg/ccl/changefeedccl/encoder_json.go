// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bytes"
	"context"
	gojson "encoding/json"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// jsonEncoder encodes changefeed entries as JSON. Keys are the primary key
// columns in a JSON array. Values are a JSON object mapping every column name
// to its value. Updated timestamps in rows and resolved timestamp payloads are
// stored in a sub-object under the `__crdb__` key in the top-level JSON object.
type jsonEncoder struct {
	updatedField, mvccTimestampField, beforeField, wrapped, keyOnly, keyInValue, topicInValue bool

	targets                 []jobspb.ChangefeedTargetSpecification
	alloc                   tree.DatumAlloc
	buf                     bytes.Buffer
	virtualColumnVisibility string

	// columnMapCache caches the TableColMap for the latest version of the
	// table descriptor thus far seen. It avoids the need to recompute the
	// map per row, which, prior to the change introducing this cache, could
	// amount for 10% of row processing time.
	columnMapCache map[descpb.ID]*tableColumnMapCacheEntry
}

// tableColumnMapCacheEntry stores a TableColMap for a given descriptor version.
type tableColumnMapCacheEntry struct {
	version descpb.DescriptorVersion
	catalog.TableColMap
}

var _ Encoder = &jsonEncoder{}

func makeJSONEncoder(
	opts map[string]string, targets []jobspb.ChangefeedTargetSpecification,
) (*jsonEncoder, error) {
	e := &jsonEncoder{
		targets:                 targets,
		keyOnly:                 changefeedbase.EnvelopeType(opts[changefeedbase.OptEnvelope]) == changefeedbase.OptEnvelopeKeyOnly,
		wrapped:                 changefeedbase.EnvelopeType(opts[changefeedbase.OptEnvelope]) == changefeedbase.OptEnvelopeWrapped,
		virtualColumnVisibility: opts[changefeedbase.OptVirtualColumns],
		columnMapCache:          map[descpb.ID]*tableColumnMapCacheEntry{},
	}
	_, e.updatedField = opts[changefeedbase.OptUpdatedTimestamps]
	_, e.mvccTimestampField = opts[changefeedbase.OptMVCCTimestamps]
	_, e.beforeField = opts[changefeedbase.OptDiff]
	if e.beforeField && !e.wrapped {
		return nil, errors.Errorf(`%s is only usable with %s=%s`,
			changefeedbase.OptDiff, changefeedbase.OptEnvelope, changefeedbase.OptEnvelopeWrapped)
	}
	_, e.keyInValue = opts[changefeedbase.OptKeyInValue]
	if e.keyInValue && !e.wrapped {
		return nil, errors.Errorf(`%s is only usable with %s=%s`,
			changefeedbase.OptKeyInValue, changefeedbase.OptEnvelope, changefeedbase.OptEnvelopeWrapped)
	}
	_, e.topicInValue = opts[changefeedbase.OptTopicInValue]
	if e.topicInValue && !e.wrapped {
		return nil, errors.Errorf(`%s is only usable with %s=%s`,
			changefeedbase.OptTopicInValue, changefeedbase.OptEnvelope, changefeedbase.OptEnvelopeWrapped)
	}
	return e, nil
}

// EncodeKey implements the Encoder interface.
func (e *jsonEncoder) EncodeKey(_ context.Context, row encodeRow) ([]byte, error) {
	jsonEntries, err := e.encodeKeyRaw(row)
	if err != nil {
		return nil, err
	}
	j, err := json.MakeJSON(jsonEntries)
	if err != nil {
		return nil, err
	}
	e.buf.Reset()
	j.Format(&e.buf)
	return e.buf.Bytes(), nil
}

func (e *jsonEncoder) encodeKeyRaw(row encodeRow) ([]interface{}, error) {
	colIdxByID := e.getTableColMap(row.tableDesc)
	primaryIndex := row.tableDesc.GetPrimaryIndex()
	jsonEntries := make([]interface{}, primaryIndex.NumKeyColumns())
	for i := 0; i < primaryIndex.NumKeyColumns(); i++ {
		colID := primaryIndex.GetKeyColumnID(i)
		idx, ok := colIdxByID.Get(colID)
		if !ok {
			return nil, errors.Errorf(`unknown column id: %d`, colID)
		}
		datum, col := row.datums[idx], row.tableDesc.PublicColumns()[idx]
		if err := datum.EnsureDecoded(col.GetType(), &e.alloc); err != nil {
			return nil, err
		}
		var err error
		jsonEntries[i], err = tree.AsJSON(
			datum.Datum,
			sessiondatapb.DataConversionConfig{},
			time.UTC,
		)
		if err != nil {
			return nil, err
		}
	}
	return jsonEntries, nil
}

// EncodeValue implements the Encoder interface.
func (e *jsonEncoder) EncodeValue(_ context.Context, row encodeRow) ([]byte, error) {
	if e.keyOnly || (!e.wrapped && row.deleted) {
		return nil, nil
	}

	var after map[string]interface{}
	if !row.deleted {
		family, err := row.tableDesc.FindFamilyByID(row.familyID)
		if err != nil {
			return nil, err
		}
		include := make(map[descpb.ColumnID]struct{}, len(family.ColumnIDs))
		var yes struct{}
		for _, colID := range family.ColumnIDs {
			include[colID] = yes
		}
		after = make(map[string]interface{})
		for i, col := range row.tableDesc.PublicColumns() {
			_, inFamily := include[col.GetID()]
			virtual := col.IsVirtual() && e.virtualColumnVisibility == string(changefeedbase.OptVirtualColumnsNull)
			if inFamily || virtual {
				datum := row.datums[i]
				if err := datum.EnsureDecoded(col.GetType(), &e.alloc); err != nil {
					return nil, err
				}
				after[col.GetName()], err = tree.AsJSON(
					datum.Datum,
					sessiondatapb.DataConversionConfig{},
					time.UTC,
				)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	var before map[string]interface{}
	if row.prevDatums != nil && !row.prevDeleted {
		family, err := row.prevTableDesc.FindFamilyByID(row.prevFamilyID)
		if err != nil {
			return nil, err
		}
		include := make(map[descpb.ColumnID]struct{})
		var yes struct{}
		for _, colID := range family.ColumnIDs {
			include[colID] = yes
		}
		before = make(map[string]interface{})
		for i, col := range row.prevTableDesc.PublicColumns() {
			_, inFamily := include[col.GetID()]
			virtual := col.IsVirtual() && e.virtualColumnVisibility == string(changefeedbase.OptVirtualColumnsNull)
			if inFamily || virtual {
				datum := row.prevDatums[i]
				if err := datum.EnsureDecoded(col.GetType(), &e.alloc); err != nil {
					return nil, err
				}
				before[col.GetName()], err = tree.AsJSON(
					datum.Datum,
					sessiondatapb.DataConversionConfig{},
					time.UTC,
				)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	var jsonEntries map[string]interface{}
	if e.wrapped {
		if after != nil {
			jsonEntries = map[string]interface{}{`after`: after}
		} else {
			jsonEntries = map[string]interface{}{`after`: nil}
		}
		if e.beforeField {
			if before != nil {
				jsonEntries[`before`] = before
			} else {
				jsonEntries[`before`] = nil
			}
		}
		if e.keyInValue {
			keyEntries, err := e.encodeKeyRaw(row)
			if err != nil {
				return nil, err
			}
			jsonEntries[`key`] = keyEntries
		}
		if e.topicInValue {
			jsonEntries[`topic`] = row.topic
		}
	} else {
		jsonEntries = after
	}

	if e.updatedField || e.mvccTimestampField {
		var meta map[string]interface{}
		if e.wrapped {
			meta = jsonEntries
		} else {
			meta = make(map[string]interface{}, 1)
			jsonEntries[jsonMetaSentinel] = meta
		}
		if e.updatedField {
			meta[`updated`] = row.updated.AsOfSystemTime()
		}
		if e.mvccTimestampField {
			meta[`mvcc_timestamp`] = row.mvccTimestamp.AsOfSystemTime()
		}
	}

	j, err := json.MakeJSON(jsonEntries)
	if err != nil {
		return nil, err
	}
	e.buf.Reset()
	j.Format(&e.buf)
	return e.buf.Bytes(), nil
}

// EncodeResolvedTimestamp implements the Encoder interface.
func (e *jsonEncoder) EncodeResolvedTimestamp(
	_ context.Context, _ string, resolved hlc.Timestamp,
) ([]byte, error) {
	meta := map[string]interface{}{
		`resolved`: eval.TimestampToDecimalDatum(resolved).Decimal.String(),
	}
	var jsonEntries interface{}
	if e.wrapped {
		jsonEntries = meta
	} else {
		jsonEntries = map[string]interface{}{
			jsonMetaSentinel: meta,
		}
	}
	return gojson.Marshal(jsonEntries)
}

// getTableColMap gets the TableColMap for the provided table descriptor,
// optionally consulting its cache.
func (e *jsonEncoder) getTableColMap(desc catalog.TableDescriptor) catalog.TableColMap {
	ce, exists := e.columnMapCache[desc.GetID()]
	if exists {
		switch {
		case ce.version == desc.GetVersion():
			return ce.TableColMap
		case ce.version > desc.GetVersion():
			return catalog.ColumnIDToOrdinalMap(desc.PublicColumns())
		default:
			// Construct a new entry.
			delete(e.columnMapCache, desc.GetID())
		}
	}
	ce = &tableColumnMapCacheEntry{
		version:     desc.GetVersion(),
		TableColMap: catalog.ColumnIDToOrdinalMap(desc.PublicColumns()),
	}
	e.columnMapCache[desc.GetID()] = ce
	return ce.TableColMap
}
