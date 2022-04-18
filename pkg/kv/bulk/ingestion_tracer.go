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
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
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

		sendWait time.Duration

		numRequests       int
		numIngestAsWrites int

		dataSize int64

		sendWaitByStore     map[roachpb.StoreID]time.Duration
		dataIngestedByStore map[roachpb.StoreID]int64
	}
}

// Render implements the tracing.LazyTag interface.
//
// TODO(adityamaru): TBD on what we want to render. This is usually rendered on
// the tracing span associated with the import/restore processor's context. Some
// of this information might be more useful on the job resumer's context
// instead, aggregated/bucketed over some time frame across all nodes.
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

func (a *addSSTableTag) notify(rec tracing.Recording) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	sp, found := rec.FindSpan(AddSSTableOpName)
	if !found {
		return errors.AssertionFailedf("expected recording to contain %s span", AddSSTableOpName)
	}

	var processedStats bool
	for _, sr := range sp.StructuredRecords {
		var stats roachpb.AddSSTableStats
		if !types.Is(sr.Payload, &stats) {
			continue
		}

		if processedStats {
			return errors.AssertionFailedf("span has more than one AddSSTableStats structured recordings")
		}

		processedStats = true
		if err := protoutil.Unmarshal(sr.Payload.Value, &stats); err != nil {
			return err
		}
		if stats.IngestAsWrite {
			a.mu.numIngestAsWrites++
		}

		a.mu.dataSize += stats.DataSize

		// Should only ever really be one iteration but if somehow it isn't,
		// e.g. if a request was redirected, go ahead and count it against all
		// involved stores; if it is small this edge case is immaterial, and
		// if it is large, it's probably one big one but we don't know which
		// so just blame them all (averaging it out could hide one big delay).
		for _, storeID := range stats.StoreIds {
			a.mu.dataIngestedByStore[storeID] += stats.DataSize
			a.mu.sendWaitByStore[storeID] += sp.Duration
		}
	}

	a.mu.sendWait += sp.Duration
	a.mu.numRequests++
	return nil
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

		sendWait    time.Duration
		numRequests int
	}
}

// Render implements the LazyTag interface.
//
// TODO(adityamaru): TBD on what to render.
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

func (s *splitTag) notify(rec tracing.Recording) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sp, found := rec.FindSpan(AdminSplitOpName)
	if !found {
		return errors.AssertionFailedf("expected recording to contain %s span", AdminSplitOpName)
	}

	s.mu.sendWait += sp.Duration
	s.mu.numRequests++
	return nil
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

		sendWait    time.Duration
		numRequests int
		dataMoved   sz
	}
}

// Render implements the LazyTag interface.
//
// TODO(adityamaru): TBD on what to render.
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

	if s.mu.dataMoved > 0 {
		const mb = 1 << 20
		dataMovedMB := fmt.Sprintf("%.2f", float64(s.mu.dataMoved)/mb)
		tags = append(tags, attribute.KeyValue{
			Key:   attribute.Key(prefixTag(s.tracerName, tagAdminScatterDataMoved)),
			Value: attribute.StringValue(dataMovedMB),
		})
	}

	return tags
}

func (s *scatterTag) notify(rec tracing.Recording) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sp, found := rec.FindSpan(AdminScatterOpName)
	if !found {
		return errors.AssertionFailedf("expected recording to contain %s span", AdminScatterOpName)
	}

	s.mu.sendWait += sp.Duration
	s.mu.numRequests++
	var processedStats bool
	for _, sr := range sp.StructuredRecords {
		var stats roachpb.AdminScatterStats
		if !types.Is(sr.Payload, &stats) {
			continue
		}

		if processedStats {
			return errors.AssertionFailedf("span has more than one AdminScatterStats structured recordings")
		}

		processedStats = true
		if err := protoutil.Unmarshal(sr.Payload.Value, &stats); err != nil {
			return err
		}
		s.mu.dataMoved += sz(stats.DataMoved)
	}
	return nil
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

func (i *ingestionTracer) setLazyTag(sp *tracing.Span, tagName string) error {
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
			i.scatterTag.mu.dataMoved = oldScatterTag.mu.dataMoved
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
		return errors.AssertionFailedf("unknown tag name %s", tagName)
	}
	return nil
}

// newIngestionTracer returnsa an instance of the ingestionTracer.
func newIngestionTracer(name string, sp *tracing.Span) (*ingestionTracer, error) {
	i := &ingestionTracer{
		name:       name,
		addSSTTag:  newAddSSTableTag(name),
		splitTag:   &splitTag{tracerName: name},
		scatterTag: &scatterTag{tracerName: name},
	}

	if err := i.setLazyTag(sp, tagAddSSTable); err != nil {
		return nil, err
	}
	if err := i.setLazyTag(sp, tagAdminSplit); err != nil {
		return nil, err
	}
	if err := i.setLazyTag(sp, tagAdminScatter); err != nil {
		return nil, err
	}

	i.sp = sp
	return i, nil
}

// notifyAddSSTable aggregates information from the recording of an
// AddSSTable request.
func (i *ingestionTracer) notifyAddSSTable(ctx context.Context, rec tracing.Recording) {
	if rec == nil {
		log.Warning(ctx, "received empty addSSTable trace recording")
		return
	}

	err := i.addSSTTag.notify(rec)
	if err != nil {
		log.Warningf(ctx, "failed to notify the ingestion trace of an AddSSTable: %+v", err)
	}
}

// notifyAdminSplit aggregates information from the recording of an
// AdminSplit request.
func (i *ingestionTracer) notifyAdminSplit(ctx context.Context, rec tracing.Recording) {
	if rec == nil {
		log.Warning(ctx, "received empty AdminSplit trace recording")
		return
	}

	err := i.splitTag.notify(rec)
	if err != nil {
		log.Warningf(ctx, "failed to notify the ingestion trace of an AdminSplit: %+v", err)
	}
}

// notifyAdminScatter aggregates information from the recording of an
// AdminScatter request.
func (i *ingestionTracer) notifyAdminScatter(ctx context.Context, rec tracing.Recording) {
	if rec == nil {
		log.Warning(ctx, "received empty AdminScatter trace recording")
		return
	}

	err := i.scatterTag.notify(rec)
	if err != nil {
		log.Warningf(ctx, "failed to notify the ingestion trace of an AdminScatter: %+v", err)
	}
}
