// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamproducer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

type streamEventBatcher struct {
	batch          streampb.StreamEvent_Batch
	size           int
	spanCfgDecoder cdcevent.Decoder
	appTenantID    roachpb.TenantID
}

func makeStreamEventBatcher(
	decoder cdcevent.Decoder, appTenantID roachpb.TenantID,
) *streamEventBatcher {
	return &streamEventBatcher{
		batch:          streampb.StreamEvent_Batch{},
		spanCfgDecoder: decoder,
		appTenantID:    appTenantID,
	}
}

func (seb *streamEventBatcher) reset() {
	seb.size = 0
	seb.batch.KeyValues = seb.batch.KeyValues[:0]
	seb.batch.Ssts = seb.batch.Ssts[:0]
	seb.batch.DelRanges = seb.batch.DelRanges[:0]
}

func (seb *streamEventBatcher) addSST(sst *kvpb.RangeFeedSSTable) {
	seb.batch.Ssts = append(seb.batch.Ssts, *sst)
	seb.size += sst.Size()
}

func (seb *streamEventBatcher) addKV(kv *roachpb.KeyValue) {
	seb.batch.KeyValues = append(seb.batch.KeyValues, *kv)
	seb.size += kv.Size()
}

func (seb *streamEventBatcher) addDelRange(d *kvpb.RangeFeedDeleteRange) {
	// DelRange's span is already trimmed to enclosed within
	// the subscribed span, just emit it.
	seb.batch.DelRanges = append(seb.batch.DelRanges, *d)
	seb.size += d.Size()
}

func (seb *streamEventBatcher) getSize() int {
	return seb.size
}

func (seb *streamEventBatcher) addKVAsSpanConfig(ctx context.Context, kv *roachpb.KeyValue) error {
	row, err := seb.spanCfgDecoder.DecodeKV(ctx, *kv, cdcevent.CurrentRow, kv.Value.Timestamp, false)
	if err != nil {
		return err
	}
	// We assume the system.Span_Configurations schema is
	// CREATE TABLE system.span_configurations (
	//	start_key    BYTES NOT NULL,
	//  end_key      BYTES NOT NULL,
	//	config        BYTES NOT NULL,
	//	CONSTRAINT "primary" PRIMARY KEY (start_key),
	//	CONSTRAINT check_bounds CHECK (start_key < end_key),
	//	FAMILY "primary" (start_key, end_key, config)
	// )
	//
	// In the unlikely (and sad) event we update the span config schema),
	// we will have to version gate this decoding step.

	colsAsBytes := make([][]byte, 0, 3)
	err = row.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		colsAsBytes = append(colsAsBytes, []byte(*d.(*tree.DBytes)))
		return nil
	})
	if err != nil {
		return err
	}
	if len(colsAsBytes) != 3 {
		return errors.AssertionFailedf("the row emitted from the span config table did not have 3" +
			" columns")
	}
	span := roachpb.Span{
		Key:    colsAsBytes[0],
		EndKey: colsAsBytes[1],
	}
	_, tenantID, err := keys.DecodeTenantPrefix(span.Key)
	if err != nil {
		return err
	}
	if !tenantID.Equal(seb.appTenantID) {
		// Don't send span config updates for other tenants.
		return nil
	}

	var conf roachpb.SpanConfig
	if err := protoutil.Unmarshal(colsAsBytes[2], &conf); err != nil {
		return err
	}

	rec, err := spanconfig.MakeRecord(spanconfig.DecodeTarget(span), conf)
	if err != nil {
		return err
	}
	spanCfg := roachpb.SpanConfigEntry{
		Target: rec.GetTarget().ToProto(),
		Config: rec.GetConfig(),
	}

	seb.batch.SpanConfigs = append(seb.batch.SpanConfigs, spanCfg)
	seb.size += spanCfg.Size()
	return nil
}
