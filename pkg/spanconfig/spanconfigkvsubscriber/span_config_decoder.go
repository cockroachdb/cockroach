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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
func (sd *spanConfigDecoder) decode(kv roachpb.KeyValue) (entry roachpb.SpanConfigEntry, _ error) {
	// First we need to decode the start_key field from the index key.
	{
		types := []*types.T{sd.columns[0].GetType()}
		startKeyRow := make([]rowenc.EncDatum, 1)
		_, matches, _, err := rowenc.DecodeIndexKey(keys.SystemSQLCodec, types, startKeyRow, nil /* colDirs */, kv.Key)
		if err != nil {
			return roachpb.SpanConfigEntry{}, errors.Wrapf(err, "failed to decode key: %v", kv.Key)
		}
		if !matches {
			return roachpb.SpanConfigEntry{},
				errors.AssertionFailedf(
					"system.span_configurations descriptor does not match key: %v", kv.Key,
				)
		}
		if err := startKeyRow[0].EnsureDecoded(types[0], &sd.alloc); err != nil {
			return roachpb.SpanConfigEntry{}, err
		}
		entry.Span.Key = []byte(tree.MustBeDBytes(startKeyRow[0].Datum))
	}
	if !kv.Value.IsPresent() {
		return roachpb.SpanConfigEntry{},
			errors.AssertionFailedf("missing value for start key: %s", entry.Span.Key)
	}

	// The remaining columns are stored as a family.
	bytes, err := kv.Value.GetTuple()
	if err != nil {
		return roachpb.SpanConfigEntry{}, err
	}

	datums, err := sd.decoder.Decode(&sd.alloc, bytes)
	if err != nil {
		return roachpb.SpanConfigEntry{}, err
	}
	if endKey := datums[1]; endKey != tree.DNull {
		entry.Span.EndKey = []byte(tree.MustBeDBytes(endKey))
	}
	if config := datums[2]; config != tree.DNull {
		if err := protoutil.Unmarshal([]byte(tree.MustBeDBytes(config)), &entry.Config); err != nil {
			return roachpb.SpanConfigEntry{}, err
		}
	}

	return entry, nil
}

// TestingDecoderFn exports the decoding routine for testing purposes.
func TestingDecoderFn() func(roachpb.KeyValue) (roachpb.SpanConfigEntry, error) {
	return newSpanConfigDecoder().decode
}
