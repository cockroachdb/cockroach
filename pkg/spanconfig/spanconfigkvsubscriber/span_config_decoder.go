// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigkvsubscriber

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
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

// spanConfigDecoder decodes rows from system.span_configurations. It's not
// safe for concurrent use.
type spanConfigDecoder struct {
	alloc   tree.DatumAlloc
	columns []catalog.Column
	decoder valueside.Decoder
}

// newSpanConfigDecoder instantiates a spanConfigDecoder.
func newSpanConfigDecoder() *spanConfigDecoder {
	columns := systemschema.SpanConfigurationsTable.PublicColumns()
	return &spanConfigDecoder{
		columns: columns,
		decoder: valueside.MakeDecoder(columns),
	}
}

// decode a span config entry given a KV from the
// system.span_configurations table.
func (sd *spanConfigDecoder) decode(kv roachpb.KeyValue) (spanconfig.Record, error) {
	// First we need to decode the start_key field from the index key.
	var rawSp roachpb.Span
	var conf roachpb.SpanConfig
	{
		types := []*types.T{sd.columns[0].GetType()}
		startKeyRow := make([]rowenc.EncDatum, 1)
		_, _, err := rowenc.DecodeIndexKey(keys.SystemSQLCodec, types, startKeyRow, nil /* colDirs */, kv.Key)
		if err != nil {
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

	return spanconfig.Record{
		Target: spanconfig.DecodeTarget(rawSp),
		Config: conf,
	}, nil
}

func (sd *spanConfigDecoder) translateEvent(
	ctx context.Context, ev *roachpb.RangeFeedValue,
) rangefeedbuffer.Event {
	deleted := !ev.Value.IsPresent()
	var value roachpb.Value
	if deleted {
		if !ev.PrevValue.IsPresent() {
			// It's possible to write a KV tombstone on top of another KV
			// tombstone -- both the new and old value will be empty. We simply
			// ignore these events.
			return nil
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
		log.Infof(ctx, "received span configuration update for %s (deleted=%t)", record.Target, deleted)
	}

	var update spanconfig.Update
	if deleted {
		update = spanconfig.Deletion(record.Target)
	} else {
		update = spanconfig.Update(record)
	}

	return &bufferEvent{update, ev.Value.Timestamp}
}

// TestingDecoderFn exports the decoding routine for testing purposes.
func TestingDecoderFn() func(roachpb.KeyValue) (spanconfig.Record, error) {
	return newSpanConfigDecoder().decode
}
