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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"go.opentelemetry.io/otel/attribute"
)

const tagAddSSTable = "addsstable"
const tagAddSSTableNumRequests = "addsst_num_requests"
const tagAddSSTableNumIngestAsWrite = "addsst_num_ingest_as_writes"
const tagAddSSTableDataSize = "addsst_data_size_mb"
const tagAddSSTableTotalDuration = "addsst_total_duration"
const tagAddSSTableStoreThroughput = "addsst_store_info"

type addSSTableTag struct {
	tracerName string
	mu         struct {
		syncutil.Mutex

		// sendWait is the total time spent waiting for responses for
		// AddSSTable requests.
		sendWait time.Duration

		// numRequests is the number of AddSSTable requests sent.
		numRequests int

		// numIngestAsWrites is the number of AddSSTable requests whose data will be
		// ingested in a regular WriteBatch, instead of directly adding the SST to
		// the storage engine.
		numIngestAsWrites int

		// dataSize is the total byte size of SSTs that have been ingested.
		dataSize int64

		// sendWaitByStore maps the store ID to the total time spent waiting for
		// responses for AddSSTableRequests that were served by that store.
		sendWaitByStore map[roachpb.StoreID]time.Duration

		// dataIngestedByStore maps the store ID to the total byte size of SSTs that
		// have been ingested by that store.
		dataIngestedByStore map[roachpb.StoreID]int64
	}
}

// Render implements the tracing.LazyTag interface.
func (a *addSSTableTag) Render() []attribute.KeyValue {
	a.mu.Lock()
	defer a.mu.Unlock()

	const mb = 1 << 20
	tags := make([]attribute.KeyValue, 0)

	tags = append(tags, attribute.KeyValue{
		Key:   attribute.Key(prefixTag(a.tracerName, tagAddSSTableNumRequests)),
		Value: attribute.IntValue(a.mu.numRequests),
	})

	if a.mu.numIngestAsWrites > 0 {
		tags = append(tags, attribute.KeyValue{
			Key:   attribute.Key(prefixTag(a.tracerName, tagAddSSTableNumIngestAsWrite)),
			Value: attribute.IntValue(a.mu.numIngestAsWrites),
		})
	}

	if a.mu.dataSize > 0 {
		tags = append(tags, attribute.KeyValue{
			Key:   attribute.Key(prefixTag(a.tracerName, tagAddSSTableDataSize)),
			Value: attribute.Int64Value(a.mu.dataSize / mb),
		})
	}

	if a.mu.sendWait > 0 {
		tags = append(tags, attribute.KeyValue{
			Key:   attribute.Key(prefixTag(a.tracerName, tagAddSSTableTotalDuration)),
			Value: attribute.StringValue(string(humanizeutil.Duration(a.mu.sendWait))),
		})
	}

	for storeID, sendWait := range a.mu.sendWaitByStore {
		dataIngestedBytes := a.mu.dataIngestedByStore[storeID]
		dataIngestedMB := float64(dataIngestedBytes) / mb
		sendWaitSeconds := sendWait.Seconds()

		var throughput float64
		if sendWaitSeconds == 0 {
			throughput = 0
		} else {
			throughput = dataIngestedMB / sendWait.Seconds()
		}

		if throughput > 0 {
			throughputStr := fmt.Sprintf("store-id:%d, throughput (mb/s):%.2f", storeID, throughput)
			tags = append(tags, attribute.KeyValue{
				Key:   attribute.Key(prefixTag(a.tracerName, tagAddSSTableStoreThroughput)),
				Value: attribute.StringValue(throughputStr),
			})
		}
	}

	return tags
}

// notify updates the addsstable field with information about AddSSTable
// requests in the trace recording rec.
func (a *addSSTableTag) notify(duration time.Duration, stats *roachpb.AddSSTableStats) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if stats.IngestAsWrite {
		a.mu.numIngestAsWrites++
	}

	a.mu.dataSize += stats.DataSizeBytes

	// If there is >1 Stores we count the time taken to serve the
	// request against all involved stores; if this value is small then
	// edge case is immaterial, and if it is large,  we don't know which
	// store incurred more/less of this time so just blame them all
	// (averaging it out could hide one big delay).
	for _, storeID := range stats.Stores {
		a.mu.dataIngestedByStore[storeID] += stats.DataSizeBytes
		a.mu.sendWaitByStore[storeID] += duration
	}

	a.mu.sendWait += duration
	a.mu.numRequests++
}

func newAddSSTableTag(name string) *addSSTableTag {
	t := addSSTableTag{tracerName: name}

	t.mu.Lock()
	t.mu.dataIngestedByStore = make(map[roachpb.StoreID]int64)
	t.mu.sendWaitByStore = make(map[roachpb.StoreID]time.Duration)
	t.mu.Unlock()

	return &t
}

var _ tracing.LazyTag = &addSSTableTag{}

const tagAdminSplit = "adminsplit"
const tagAdminSplitNumRequests = "split_num_requests"
const tagAdminSplitTotalDuration = "split_total_duration"

type splitTag struct {
	tracerName string
	mu         struct {
		syncutil.Mutex

		// sendWait is the total time spent waiting for responses for
		// AdminSplit requests.
		sendWait time.Duration

		// numRequests is the number of AdminSplit requests sent.
		numRequests int
	}
}

// Render implements the LazyTag interface.
func (s *splitTag) Render() []attribute.KeyValue {
	tags := make([]attribute.KeyValue, 0)
	tags = append(tags, attribute.KeyValue{
		Key:   attribute.Key(prefixTag(s.tracerName, tagAdminSplitNumRequests)),
		Value: attribute.IntValue(s.mu.numRequests),
	})

	if s.mu.sendWait > 0 {
		tags = append(tags, attribute.KeyValue{
			Key:   attribute.Key(prefixTag(s.tracerName, tagAdminSplitTotalDuration)),
			Value: attribute.StringValue(string(humanizeutil.Duration(s.mu.sendWait))),
		})
	}
	return tags
}

// notify updates the splitTag fields with information about AdminSplit requests
// in the trace recording rec.
func (s *splitTag) notify(duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.sendWait += duration
	s.mu.numRequests++
}

var _ tracing.LazyTag = &splitTag{}

const tagAdminScatter = "adminscatter"
const tagAdminScatterNumRequests = "scatter_num_requests"
const tagAdminScatterTotalDuration = "scatter_total_duration"
const tagAdminScatterDataMoved = "scatter_data_moved_mb"

type scatterTag struct {
	tracerName string
	mu         struct {
		syncutil.Mutex

		// sendWait is the total time spent waiting for responses for
		// AdminScatter requests.
		sendWait time.Duration

		// numRequests is the number of AdminScatter requests sent.
		numRequests int

		// dataInScatteredRanges is the amount of data in the ranges that were part
		// of the AdminScatter requests key span. Note, this does not necessarily
		// mean we scattered ranges with these many bytes, since the scatter request
		// for a range is best effort.
		dataInScatteredRanges sz
	}
}

// Render implements the LazyTag interface.
func (s *scatterTag) Render() []attribute.KeyValue {
	tags := make([]attribute.KeyValue, 0)
	tags = append(tags, attribute.KeyValue{
		Key:   attribute.Key(prefixTag(s.tracerName, tagAdminScatterNumRequests)),
		Value: attribute.IntValue(s.mu.numRequests),
	})

	if s.mu.sendWait > 0 {
		tags = append(tags, attribute.KeyValue{
			Key:   attribute.Key(prefixTag(s.tracerName, tagAdminScatterTotalDuration)),
			Value: attribute.StringValue(string(humanizeutil.Duration(s.mu.sendWait))),
		})
	}

	if s.mu.dataInScatteredRanges > 0 {
		const mb = 1 << 20
		dataMovedMB := fmt.Sprintf("%.2f", float64(s.mu.dataInScatteredRanges)/mb)
		tags = append(tags, attribute.KeyValue{
			Key:   attribute.Key(prefixTag(s.tracerName, tagAdminScatterDataMoved)),
			Value: attribute.StringValue(dataMovedMB),
		})
	}

	return tags
}

// notify updates the scatterTag fields with information about AdminScatter
// requests in the trace recording rec.
func (s *scatterTag) notify(duration time.Duration, stats *roachpb.AdminScatterStats) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.sendWait += duration
	s.mu.numRequests++
	s.mu.dataInScatteredRanges += sz(stats.DataInScatteredRangesBytes)
}

var _ tracing.LazyTag = &scatterTag{}

// ingestionTracer adds ingestion information to the trace, in the form of tags.
// The ingestionTracer is associated with a tracing span.
type ingestionTracer struct {
	name       string
	sp         *tracing.Span
	addSSTTag  *addSSTableTag
	splitTag   *splitTag
	scatterTag *scatterTag
}

func prefixTag(prefix string, tag string) string {
	return fmt.Sprintf("%s-%s", prefix, tag)
}

func (i *ingestionTracer) setLazyTag(sp *tracing.Span, tagName string) {
	oldTag, ok := sp.GetLazyTag(prefixTag(i.name, tagName))
	switch tagName {
	case tagAddSSTable:
		if ok {
			oldAddSSTableTag := oldTag.(*addSSTableTag)
			oldAddSSTableTag.mu.Lock()
			i.addSSTTag.mu.numRequests = oldAddSSTableTag.mu.numRequests
			i.addSSTTag.mu.numIngestAsWrites = oldAddSSTableTag.mu.numIngestAsWrites
			i.addSSTTag.mu.sendWait = oldAddSSTableTag.mu.sendWait
			i.addSSTTag.mu.dataSize = oldAddSSTableTag.mu.dataSize

			for k, v := range oldAddSSTableTag.mu.sendWaitByStore {
				i.addSSTTag.mu.sendWaitByStore[k] = v
			}
			for k, v := range oldAddSSTableTag.mu.dataIngestedByStore {
				i.addSSTTag.mu.dataIngestedByStore[k] = v
			}
			oldAddSSTableTag.mu.Unlock()
		}
		sp.SetLazyTag(prefixTag(i.name, tagName), i.addSSTTag)
	case tagAdminScatter:
		if ok {
			oldScatterTag := oldTag.(*scatterTag)
			oldScatterTag.mu.Lock()
			i.scatterTag.mu.sendWait = oldScatterTag.mu.sendWait
			i.scatterTag.mu.numRequests = oldScatterTag.mu.numRequests
			i.scatterTag.mu.dataInScatteredRanges = oldScatterTag.mu.dataInScatteredRanges
			oldScatterTag.mu.Unlock()
		}
		sp.SetLazyTag(prefixTag(i.name, tagName), i.scatterTag)
	case tagAdminSplit:
		if ok {
			oldSplitTag := oldTag.(*splitTag)
			oldSplitTag.mu.Lock()
			i.splitTag.mu.sendWait = oldSplitTag.mu.sendWait
			i.splitTag.mu.numRequests = oldSplitTag.mu.numRequests
			oldSplitTag.mu.Unlock()
		}
		sp.SetLazyTag(prefixTag(i.name, tagName), i.splitTag)
	default:
		panic(fmt.Sprintf("unknown tag name %s", tagName))
	}
}

// newIngestionTracer returnsa an instance of the ingestionTracer.
func newIngestionTracer(name string, sp *tracing.Span) *ingestionTracer {
	i := &ingestionTracer{
		name:       name,
		addSSTTag:  newAddSSTableTag(name),
		splitTag:   &splitTag{tracerName: name},
		scatterTag: &scatterTag{tracerName: name},
	}

	i.setLazyTag(sp, tagAddSSTable)
	i.setLazyTag(sp, tagAdminSplit)
	i.setLazyTag(sp, tagAdminScatter)

	i.sp = sp
	return i
}

// notify aggregates the duration and stats for the corresponding bulk request.
func (i *ingestionTracer) notify(duration time.Duration, stats *roachpb.BulkRequestStats) {
	if stats == nil {
		return
	}

	switch t := stats.Value.(type) {
	case *roachpb.BulkRequestStats_AdminScatter:
		i.scatterTag.notify(duration, t.AdminScatter)
	case *roachpb.BulkRequestStats_AdminSplit:
		i.splitTag.notify(duration)
	case *roachpb.BulkRequestStats_AddSSTable:
		i.addSSTTag.notify(duration, t.AddSSTable)
	default:
	}
}
