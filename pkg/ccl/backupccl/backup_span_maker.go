// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/errors"
)

type requestSpanMaker struct {
	requestSpans     []spanAndTime
	specs            *execinfrapb.BackupDataSpec
	rangeSizedSpans  bool
	rangeIterFactory rangedesc.IteratorFactory
}

const maxChunkSize = 100

// Aim to make at least 4 chunks per worker, ensuring size is >=1 and <= max.
const softMinPerWorkerChunkCount = 4

func (rsm *requestSpanMaker) queueWork(numSenders int) chan []spanAndTime {
	todo := make(chan []spanAndTime, len(rsm.requestSpans))

	chunkSize := (len(rsm.requestSpans) / (numSenders * softMinPerWorkerChunkCount)) + 1
	if chunkSize > maxChunkSize {
		chunkSize = maxChunkSize
	}

	chunk := make([]spanAndTime, 0, chunkSize)
	for i := range rsm.requestSpans {
		if !rsm.rangeSizedSpans {
			todo <- []spanAndTime{rsm.requestSpans[i]}
			continue
		}

		chunk = append(chunk, rsm.requestSpans[i])
		if len(chunk) > chunkSize {
			todo <- chunk
			chunk = make([]spanAndTime, 0, chunkSize)
		}
	}
	if len(chunk) > 0 {
		todo <- chunk
	}
	return todo
}

func (rsm *requestSpanMaker) spanCountFromSpecs() int {
	return len(rsm.specs.Spans) + len(rsm.specs.IntroducedSpans)
}

func initRequestSpanMaker(
	rangeSizedSpans bool,
	specs *execinfrapb.BackupDataSpec,
	rangeIterfactory rangedesc.IteratorFactory,
) *requestSpanMaker {
	totalSpans := len(specs.Spans) + len(specs.IntroducedSpans)
	return &requestSpanMaker{
		requestSpans:     make([]spanAndTime, 0, totalSpans),
		rangeSizedSpans:  rangeSizedSpans,
		specs:            specs,
		rangeIterFactory: rangeIterfactory,
	}
}

func (rsm *requestSpanMaker) createRequestSpans(ctx context.Context) error {
	if err := rsm.addSpans(ctx, rsm.specs.IntroducedSpans, hlc.Timestamp{}, rsm.specs.BackupStartTime); err != nil {
		return err
	}
	if err := rsm.addSpans(ctx, rsm.specs.Spans, rsm.specs.BackupStartTime, rsm.specs.BackupEndTime); err != nil {
		return err
	}
	log.Infof(ctx, "backup processor is assigned %d spans", rsm.spanCountFromSpecs())
	return nil
}

func (rsm *requestSpanMaker) addSpans(
	ctx context.Context, spans []roachpb.Span, start, end hlc.Timestamp,
) error {
	for _, fullSpan := range spans {
		remainingSpan := fullSpan

		if rsm.rangeSizedSpans {
			rdi, err := rsm.rangeIterFactory.NewIterator(ctx, fullSpan)
			if err != nil {
				return err
			}
			for ; rdi.Valid(); rdi.Next() {
				rangeDesc := rdi.CurRangeDescriptor()
				rangeSpan := roachpb.Span{Key: rangeDesc.StartKey.AsRawKey(), EndKey: rangeDesc.EndKey.AsRawKey()}
				subspan := remainingSpan.Intersect(rangeSpan)
				if !subspan.Valid() {
					return errors.AssertionFailedf("%s not in %s of %s", rangeSpan, remainingSpan, fullSpan)
				}
				rsm.requestSpans = append(rsm.requestSpans, spanAndTime{span: subspan, start: start, end: end})
				remainingSpan.Key = subspan.EndKey
			}
		}

		if remainingSpan.Valid() {
			rsm.requestSpans = append(rsm.requestSpans, spanAndTime{span: remainingSpan, start: start, end: end})
		}
		rsm.requestSpans[len(rsm.requestSpans)-1].finishesSpec = true
	}
	return nil
}
