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

// makeRequestSpans prepares a list of request spans, each of which will be sent
// in an individual export request.
func makeRequestSpans(
	ctx context.Context,
	specs *execinfrapb.BackupDataSpec,
	rangeIterFactory rangedesc.IteratorFactory,
	rangeSizedSpans bool,
) ([]spanAndTime, error) {
	requestSpans := make([]spanAndTime, 0, len(specs.Spans)+len(specs.IntroducedSpans))
	addSpans := func(ctx context.Context, spans []roachpb.Span, start, end hlc.Timestamp) error {
		for _, fullSpan := range spans {
			remainingSpan := fullSpan

			if rangeSizedSpans {
				rdi, err := rangeIterFactory.NewIterator(ctx, fullSpan)
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
					requestSpans = append(requestSpans, spanAndTime{span: subspan, start: start, end: end})
					remainingSpan.Key = subspan.EndKey
				}
			}

			if remainingSpan.Valid() {
				requestSpans = append(requestSpans, spanAndTime{span: remainingSpan, start: start, end: end})
			}
			requestSpans[len(requestSpans)-1].finishesSpec = true
		}
		return nil
	}

	if err := addSpans(ctx, specs.IntroducedSpans, hlc.Timestamp{}, specs.BackupStartTime); err != nil {
		return nil, err
	}
	if err := addSpans(ctx, specs.Spans, specs.BackupStartTime, specs.BackupEndTime); err != nil {
		return nil, err
	}
	return requestSpans, nil
}

const maxChunkSize = 100

// Aim to make at least 4 chunks per worker, ensuring size is >=1 and <= max.
const softMinPerWorkerChunkCount = 4

// makeWorkQueue chunks and queues the requests spans onto a channel that
// workers will pull off of to process.
func makeWorkQueue(
	numSenders int, requestSpans []spanAndTime, rangeSizedSpans bool,
) chan []spanAndTime {
	todo := make(chan []spanAndTime, len(requestSpans))

	chunkSize := (len(requestSpans) / (numSenders * softMinPerWorkerChunkCount)) + 1
	if chunkSize > maxChunkSize {
		chunkSize = maxChunkSize
	}

	chunk := make([]spanAndTime, 0, chunkSize)
	for i := range requestSpans {
		if !rangeSizedSpans {
			todo <- []spanAndTime{requestSpans[i]}
			continue
		}

		chunk = append(chunk, requestSpans[i])
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

func queueRequestSpans(
	ctx context.Context,
	specs *execinfrapb.BackupDataSpec,
	rangeIterFactory rangedesc.IteratorFactory,
	rangeSizedSpans bool,
	numSenders int,
) (chan []spanAndTime, error) {
	requestSpans, err := makeRequestSpans(ctx, specs, rangeIterFactory, rangeSizedSpans)
	if err != nil {
		return nil, err
	}
	specSpanCount := len(specs.IntroducedSpans) + len(specs.Spans)
	log.Infof(ctx, "backup processor %d assigned spans were split into %d request spans", specSpanCount, len(requestSpans))
	return makeWorkQueue(numSenders, requestSpans, rangeSizedSpans), nil
}
