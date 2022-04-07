// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
)

type ingestionPerformanceStats struct {
	dataSize sz

	bufferFlushes     int // number of buffer flushes.
	flushesDueToSize  int // number of buffer flushes due to buffer size.
	batches           int // number of batches (addsstable calls) sent.
	batchesDueToRange int // number of batches due to range bounds.
	batchesDueToSize  int // number of batches due to batch size.
	splitRetries      int // extra sub-batches created due to unexpected splits.

	splits, scatters int // number of splits/scatters sent.
	scatterMoved     sz  // total size moved by scatter calls.

	fillWait    time.Duration // time spent between buffer flushes.
	sortWait    time.Duration // time spent sorting buffers.
	flushWait   time.Duration // time spent flushing buffers.
	batchWait   time.Duration // time spent flushing batches (inc split/scatter/send).
	sendWait    time.Duration // time spent sending batches (addsstable+retries)
	splitWait   time.Duration // time spent splitting.
	scatterWait time.Duration // time spent scattering.
	commitWait  time.Duration // time spent waiting for commit timestamps.

	sendWaitByStore map[roachpb.StoreID]time.Duration

	// span tracks the total span into which this batcher has flushed. It is
	// only maintained if log.V(1), so if vmodule is upped mid-ingest it may be
	// incomplete.
	span roachpb.Span
}

func (s ingestionPerformanceStats) LogTimings(ctx context.Context, name, action string) {
	log.Infof(ctx,
		"%s adder %s; ingested %s: %s filling; %v sorting; %v / %v flushing; %v sending; %v splitting; %d; %v scattering, %d, %v; %v commit-wait",
		name,
		redact.Safe(action),
		s.dataSize,
		timing(s.fillWait),
		timing(s.sortWait),
		timing(s.flushWait),
		timing(s.batchWait),
		timing(s.sendWait),
		timing(s.splitWait),
		s.splits,
		timing(s.scatterWait),
		s.scatters,
		s.scatterMoved,
		timing(s.commitWait),
	)
}

func (s ingestionPerformanceStats) LogFlushes(
	ctx context.Context, name, action string, bufSize sz,
) {
	log.Infof(ctx,
		"%s adder %s; flushed into %s %d times, %d due to buffer size (%s); flushing chunked into %d files (%d for ranges, %d for sst size) +%d split-retries",
		name,
		redact.Safe(action),
		s.span,
		s.bufferFlushes,
		s.flushesDueToSize,
		bufSize,
		s.batches,
		s.batchesDueToRange,
		s.batchesDueToSize,
		s.splitRetries,
	)
}

func (s ingestionPerformanceStats) LogPerStoreTimings(ctx context.Context, name string) {
	if len(s.sendWaitByStore) == 0 {
		return
	}
	ids := make(roachpb.StoreIDSlice, 0, len(s.sendWaitByStore))
	for i := range s.sendWaitByStore {
		ids = append(ids, i)
	}
	sort.Sort(ids)

	var sb strings.Builder
	for i, id := range ids {
		// Hack: fill the map with placeholder stores if we haven't seen the store
		// with ID below K for all but lowest K, so that next time we print a zero.
		if i > 0 && ids[i-1] != id-1 {
			s.sendWaitByStore[id-1] = 0
			fmt.Fprintf(&sb, "%d: %s;", id-1, timing(0))
		}
		fmt.Fprintf(&sb, "%d: %s;", id, timing(s.sendWaitByStore[id]))

	}
	log.Infof(ctx, "%s waited on sending to: %s", name, redact.Safe(sb.String()))
}

type sz int64

func (b sz) String() string { return string(humanizeutil.IBytes(int64(b))) }
func (b sz) SafeValue()     {}

type timing time.Duration

func (t timing) String() string { return time.Duration(t).Round(time.Second).String() }
func (t timing) SafeValue()     {}
