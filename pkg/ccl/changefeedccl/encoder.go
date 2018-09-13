// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bytes"
	gojson "encoding/json"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/pkg/errors"
)

// Encoder turns a row into a serialized changefeed key, value, or resolved
// timestamp. It represents one of the `format=` changefeed options.
type Encoder interface {
	// EncodeKey encodes the primary key of the given row. The columns of the
	// row are expected to match 1:1 with the `Columns` field of the
	// `TableDescriptor`, but only the primary key fields will be used. The
	// returned bytes are only valid until the next call to Encode*.
	EncodeKey(*sqlbase.TableDescriptor, sqlbase.EncDatumRow) ([]byte, error)
	// EncodeKey encodes the primary key of the given row. The columns of the
	// row are expected to match 1:1 with the `Columns` field of the
	// `TableDescriptor`. The returned bytes are only valid until the next call
	// to Encode*.
	EncodeValue(*sqlbase.TableDescriptor, sqlbase.EncDatumRow, hlc.Timestamp) ([]byte, error)
	// EncodeKey encodes a resolved timestamp payload. The returned bytes are
	// only valid until the next call to Encode*.
	EncodeResolvedTimestamp(hlc.Timestamp) ([]byte, error)
}

func getEncoder(opts map[string]string) (Encoder, error) {
	switch formatType(opts[optFormat]) {
	case ``, optFormatJSON:
		return makeJSONEncoder(opts), nil
	default:
		return nil, errors.Errorf(`unknown %s: %s`, optFormat, opts[optFormat])
	}
}

// jsonEncoder encodes changefeed entries as JSON. Keys are the primary key
// columns in a JSON array. Values are a JSON object mapping every column name
// to its value. Updated timestamps in rows and resolved timestamp payloads are
// stored in a sub-object under the `__crdb__` key in the top-level JSON object.
type jsonEncoder struct {
	opts map[string]string

	alloc sqlbase.DatumAlloc
	buf   bytes.Buffer
}

var _ Encoder = &jsonEncoder{}

func makeJSONEncoder(opts map[string]string) *jsonEncoder {
	return &jsonEncoder{opts: opts}
}

// EncodeKey implements the Encoder interface.
func (e *jsonEncoder) EncodeKey(
	tableDesc *sqlbase.TableDescriptor, row sqlbase.EncDatumRow,
) ([]byte, error) {
	colIdxByID := tableDesc.ColumnIdxMap()
	jsonEntries := make([]interface{}, len(tableDesc.PrimaryIndex.ColumnIDs))
	for i, colID := range tableDesc.PrimaryIndex.ColumnIDs {
		idx, ok := colIdxByID[colID]
		if !ok {
			return nil, errors.Errorf(`unknown column id: %d`, colID)
		}
		datum, col := row[idx], tableDesc.Columns[idx]
		if err := datum.EnsureDecoded(&col.Type, &e.alloc); err != nil {
			return nil, err
		}
		var err error
		jsonEntries[i], err = tree.AsJSON(datum.Datum)
		if err != nil {
			return nil, err
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

// EncodeValue implements the Encoder interface.
func (e *jsonEncoder) EncodeValue(
	tableDesc *sqlbase.TableDescriptor, row sqlbase.EncDatumRow, updated hlc.Timestamp,
) ([]byte, error) {
	columns := tableDesc.Columns
	jsonEntries := make(map[string]interface{}, len(columns))
	if _, ok := e.opts[optUpdatedTimestamps]; ok {
		jsonEntries[jsonMetaSentinel] = map[string]interface{}{
			`updated`: tree.TimestampToDecimal(updated).Decimal.String(),
		}
	}
	for i, col := range columns {
		datum := row[i]
		if err := datum.EnsureDecoded(&col.Type, &e.alloc); err != nil {
			return nil, err
		}
		var err error
		jsonEntries[col.Name], err = tree.AsJSON(datum.Datum)
		if err != nil {
			return nil, err
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
func (e *jsonEncoder) EncodeResolvedTimestamp(resolved hlc.Timestamp) ([]byte, error) {
	resolvedMetaRaw := map[string]interface{}{
		jsonMetaSentinel: map[string]interface{}{
			`resolved`: tree.TimestampToDecimal(resolved).Decimal.String(),
		},
	}
	return gojson.Marshal(resolvedMetaRaw)
}
