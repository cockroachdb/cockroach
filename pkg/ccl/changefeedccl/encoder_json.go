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

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
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
	updatedField, mvccTimestampField, beforeField, keyInValue, topicInValue bool
	envelopeType                                                            changefeedbase.EnvelopeType

	targets changefeedbase.Targets
	buf     bytes.Buffer
}

var _ Encoder = &jsonEncoder{}

func canJSONEncodeMetadata(e changefeedbase.EnvelopeType) bool {
	// bare envelopes use the _crdb_ key to avoid collisions with column names.
	// wrapped envelopes can put metadata at the top level because the columns
	// are nested under the "after:" key.
	return e == changefeedbase.OptEnvelopeBare || e == changefeedbase.OptEnvelopeWrapped
}

func makeJSONEncoder(
	opts changefeedbase.EncodingOptions, targets changefeedbase.Targets,
) (*jsonEncoder, error) {
	e := &jsonEncoder{
		targets:      targets,
		envelopeType: opts.Envelope,
	}
	e.updatedField = opts.UpdatedTimestamps
	e.mvccTimestampField = opts.MVCCTimestamps
	// In the bare envelope we don't output diff directly, it's incorporated into the
	// projection as desired.
	e.beforeField = opts.Diff && opts.Envelope != changefeedbase.OptEnvelopeBare
	e.keyInValue = opts.KeyInValue
	e.topicInValue = opts.TopicInValue
	if !canJSONEncodeMetadata(e.envelopeType) {
		if e.keyInValue {
			return nil, errors.Errorf(`%s is only usable with %s=%s`,
				changefeedbase.OptKeyInValue, changefeedbase.OptEnvelope, changefeedbase.OptEnvelopeWrapped)
		}
		if e.topicInValue {
			return nil, errors.Errorf(`%s is only usable with %s=%s`,
				changefeedbase.OptTopicInValue, changefeedbase.OptEnvelope, changefeedbase.OptEnvelopeWrapped)
		}
	}
	return e, nil
}

// EncodeKey implements the Encoder interface.
func (e *jsonEncoder) EncodeKey(_ context.Context, row cdcevent.Row) ([]byte, error) {
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

func (e *jsonEncoder) encodeKeyRaw(row cdcevent.Row) ([]interface{}, error) {
	var jsonEntries []interface{}
	if err := row.ForEachKeyColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		j, err := tree.AsJSON(d, sessiondatapb.DataConversionConfig{}, time.UTC)
		if err != nil {
			return err
		}
		jsonEntries = append(jsonEntries, j)
		return nil
	}); err != nil {
		return nil, err
	}

	return jsonEntries, nil
}

func rowAsGoNative(row cdcevent.Row) (map[string]interface{}, error) {
	if !row.HasValues() || row.IsDeleted() {
		return nil, nil
	}

	result := make(map[string]interface{})
	if err := row.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) (err error) {
		result[col.Name], err = tree.AsJSON(d, sessiondatapb.DataConversionConfig{}, time.UTC)
		return err
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// EncodeValue implements the Encoder interface.
func (e *jsonEncoder) EncodeValue(
	ctx context.Context, evCtx eventContext, updatedRow cdcevent.Row, prevRow cdcevent.Row,
) ([]byte, error) {
	if e.envelopeType == changefeedbase.OptEnvelopeKeyOnly {
		return nil, nil
	}

	if updatedRow.IsDeleted() && !canJSONEncodeMetadata(e.envelopeType) {
		return nil, nil
	}

	after, err := rowAsGoNative(updatedRow)
	if err != nil {
		return nil, err
	}

	before, err := rowAsGoNative(prevRow)
	if err != nil {
		return nil, err
	}

	var jsonEntries map[string]interface{}
	var meta map[string]interface{}
	if canJSONEncodeMetadata(e.envelopeType) {
		if e.envelopeType == changefeedbase.OptEnvelopeWrapped {
			if after != nil {
				jsonEntries = map[string]interface{}{`after`: after}
			} else {
				jsonEntries = map[string]interface{}{`after`: nil}
			}
			meta = jsonEntries
		} else {
			meta = make(map[string]interface{}, 1)
			jsonEntries = after
			jsonEntries[jsonMetaSentinel] = meta
		}
		if e.beforeField {
			if before != nil {
				jsonEntries[`before`] = before
			} else {
				jsonEntries[`before`] = nil
			}
		}
		if e.keyInValue {
			keyEntries, err := e.encodeKeyRaw(updatedRow)
			if err != nil {
				return nil, err
			}
			jsonEntries[`key`] = keyEntries
		}
		if e.topicInValue {
			jsonEntries[`topic`] = evCtx.topic
		}
	} else {
		jsonEntries = after
	}

	// TODO (zinger): Existing behavior special-cases these fields for
	// no particular reason. Fold this into the above block.
	if e.updatedField || e.mvccTimestampField {
		if meta == nil {
			if e.envelopeType == changefeedbase.OptEnvelopeWrapped {
				meta = jsonEntries
			} else {
				meta = make(map[string]interface{}, 1)
				jsonEntries[jsonMetaSentinel] = meta
			}
		}
		if e.updatedField {
			meta[`updated`] = evCtx.updated.AsOfSystemTime()
		}
		if e.mvccTimestampField {
			meta[`mvcc_timestamp`] = evCtx.mvcc.AsOfSystemTime()
		}
	}

	if metaFields, ok := jsonEntries[jsonMetaSentinel]; ok {
		m, ok := metaFields.(map[string]interface{})
		if !ok || len(m) == 0 {
			delete(jsonEntries, jsonMetaSentinel)
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
	if e.envelopeType == changefeedbase.OptEnvelopeWrapped {
		jsonEntries = meta
	} else {
		jsonEntries = map[string]interface{}{
			jsonMetaSentinel: meta,
		}
	}
	return gojson.Marshal(jsonEntries)
}
