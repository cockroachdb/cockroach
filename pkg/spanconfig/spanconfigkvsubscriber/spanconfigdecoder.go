// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigkvsubscriber

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// SpanConfigDecoder decodes rows from system.span_configurations. It's not
// safe for concurrent use.
type SpanConfigDecoder struct {
	alloc   tree.DatumAlloc
	columns []catalog.Column
	decoder valueside.Decoder
}

// NewSpanConfigDecoder instantiates a SpanConfigDecoder.
func NewSpanConfigDecoder() *SpanConfigDecoder {
	columns := systemschema.SpanConfigurationsTable.PublicColumns()
	return &SpanConfigDecoder{
		columns: columns,
		decoder: valueside.MakeDecoder(columns),
	}
}

// decode a span config entry given a KV from the
// system.span_configurations table.
func (sd *SpanConfigDecoder) decode(kv roachpb.KeyValue) (spanconfig.Record, error) {
	// First we need to decode the start_key field from the index key.
	var rawSp roachpb.Span
	var conf roachpb.SpanConfig
	{
		types := []*types.T{sd.columns[0].GetType()}
		startKeyRow := make([]rowenc.EncDatum, 1)
		if _, err := rowenc.DecodeIndexKey(keys.SystemSQLCodec, startKeyRow, nil /* colDirs */, kv.Key); err != nil {
			return spanconfig.Record{}, errors.Wrapf(err, "failed to decode key: %v", kv.Key)
		}
		if err := startKeyRow[0].EnsureDecoded(types[0], &sd.alloc); err != nil {
			return spanconfig.Record{}, err
		}
		rawSp.Key = []byte(tree.MustBeDBytes(startKeyRow[0].Datum))
	}
	if !kv.Value.IsPresent() {
		return spanconfig.Record{},
			errors.AssertionFailedf("missing value for start key: %s", rawSp.Key)
	}

	// The remaining columns are stored as a family.
	bytes, err := kv.Value.GetTuple()
	if err != nil {
		return spanconfig.Record{}, err
	}

	datums, err := sd.decoder.Decode(&sd.alloc, bytes)
	if err != nil {
		return spanconfig.Record{}, err
	}
	if endKey := datums[1]; endKey != tree.DNull {
		rawSp.EndKey = []byte(tree.MustBeDBytes(endKey))
	}
	if config := datums[2]; config != tree.DNull {
		if err := protoutil.Unmarshal([]byte(tree.MustBeDBytes(config)), &conf); err != nil {
			return spanconfig.Record{}, err
		}
	}

	return spanconfig.MakeRecord(spanconfig.DecodeTarget(rawSp), conf)
}

func (sd *SpanConfigDecoder) TranslateEvent(
	ctx context.Context, ev *kvpb.RangeFeedValue,
) (*BufferEvent, bool) {
	deleted := !ev.Value.IsPresent()
	var value roachpb.Value
	if deleted {
		if !ev.PrevValue.IsPresent() {
			// It's possible to write a KV tombstone on top of another KV
			// tombstone -- both the new and old value will be empty. We simply
			// ignore these events.
			return nil, false
		}

		// Since the end key is not part of the primary key, we need to
		// decode the previous value in order to determine what it is.
		value = ev.PrevValue
	} else {
		value = ev.Value
	}
	record, err := sd.decode(roachpb.KeyValue{
		Key:   ev.Key,
		Value: value,
	})
	if err != nil {
		log.Fatalf(ctx, "failed to decode row: %v", err) // non-retryable error; just fatal
	}

	if log.ExpensiveLogEnabled(ctx, 1) {
		log.Infof(ctx, "received span configuration update for %s (deleted=%t)",
			record.GetTarget(), deleted)
	}

	var update spanconfig.Update
	if deleted {
		update, err = spanconfig.Deletion(record.GetTarget())
		if err != nil {
			log.Fatalf(ctx, "failed to construct Deletion: %+v", err)
		}
	} else {
		update = spanconfig.Update(record)
	}

	return &BufferEvent{update, ev.Value.Timestamp}, true
}

// TestingDecoderFn exports the decoding routine for testing purposes.
func TestingDecoderFn() func(roachpb.KeyValue) (spanconfig.Record, error) {
	return NewSpanConfigDecoder().decode
}
