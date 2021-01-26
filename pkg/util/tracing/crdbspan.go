// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracing

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/logtags"
	"github.com/gogo/protobuf/types"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
)

// crdbSpan is a span for internal crdb usage. This is used to power SQL session
// tracing.
type crdbSpan struct {
	traceID      uint64 // probabilistically unique.
	spanID       uint64 // probabilistically unique.
	parentSpanID uint64

	operation string
	startTime time.Time

	// logTags are set to the log tags that were available when this Span was
	// created, so that there's no need to eagerly copy all of those log tags
	// into this Span's tags. If the Span's tags are actually requested, these
	// logTags will be copied out at that point.
	//
	// Note that these tags have not gone through the log tag -> Span tag
	// remapping procedure; tagName() needs to be called before exposing each
	// tag's key to a user.
	logTags *logtags.Buffer

	mu crdbSpanMu
}

type crdbSpanMu struct {
	syncutil.Mutex
	// duration is initialized to -1 and set on Finish().
	duration time.Duration

	// recording maintains state once StartRecording() is called.
	recording struct {
		// recordingType is the recording type of the ongoing recording, if any.
		// Its 'load' method may be called without holding the surrounding mutex,
		// but its 'swap' method requires the mutex.
		recordingType atomicRecordingType
		recordedLogs  []opentracing.LogRecord
		// children contains the list of child spans started after this Span
		// started recording.
		children []*crdbSpan
		// remoteSpan contains the list of remote child spans manually imported.
		remoteSpans []tracingpb.RecordedSpan
	}

	// tags are only set when recording. These are tags that have been added to
	// this Span, and will be appended to the tags in logTags when someone
	// needs to actually observe the total set of tags that is a part of this
	// Span.
	// TODO(radu): perhaps we want a recording to capture all the tags (even
	// those that were set before recording started)?
	tags opentracing.Tags

	stats      SpanStats
	structured []Structured

	// The Span's associated baggage.
	baggage map[string]string
}

func (s *crdbSpan) recordingType() RecordingType {
	if s == nil {
		return RecordingOff
	}
	return s.mu.recording.recordingType.load()
}

// enableRecording start recording on the Span. From now on, log events and
// child spans will be stored.
//
// If parent != nil, the Span will be registered as a child of the respective
// parent.
// If separate recording is specified, the child is not registered with the
// parent. Thus, the parent's recording will not include this child.
func (s *crdbSpan) enableRecording(parent *crdbSpan, recType RecordingType) {
	if parent != nil {
		parent.addChild(s)
	}
	if recType == RecordingOff {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.recording.recordingType.swap(recType)
	if recType == RecordingVerbose {
		s.setBaggageItemLocked(verboseTracingBaggageKey, "1")
	}
	// Clear any previously recorded info. This is needed by SQL SessionTracing,
	// who likes to start and stop recording repeatedly on the same Span, and
	// collect the (separate) recordings every time.
	s.mu.recording.recordedLogs = nil
	s.mu.recording.children = nil
	s.mu.recording.remoteSpans = nil
}

func (s *crdbSpan) disableRecording() {
	s.mu.Lock()
	defer s.mu.Unlock()
	oldRecType := s.mu.recording.recordingType.swap(RecordingOff)
	// We test the duration as a way to check if the Span has been finished. If it
	// has, we don't want to do the call below as it might crash (at least if
	// there's a netTr).
	if (s.mu.duration == -1) && (oldRecType == RecordingVerbose) {
		// Clear the verboseTracingBaggageKey baggage item, assuming that it was set by
		// enableRecording().
		s.setBaggageItemLocked(verboseTracingBaggageKey, "")
	}
}

func (s *crdbSpan) getRecording(m mode) Recording {
	if s == nil {
		return nil
	}
	if m == modeLegacy && s.recordingType() == RecordingOff {
		// In legacy tracing (pre always-on), we avoid allocations when the
		// Span is not actively recording.
		//
		// TODO(tbg): we could consider doing the same when background tracing
		// is on but the current span contains "nothing of interest".
		return nil
	}
	s.mu.Lock()
	// The capacity here is approximate since we don't know how many grandchildren
	// there are.
	result := make(Recording, 0, 1+len(s.mu.recording.children)+len(s.mu.recording.remoteSpans))
	// Shallow-copy the children so we can process them without the lock.
	children := s.mu.recording.children
	result = append(result, s.getRecordingLocked(m))
	result = append(result, s.mu.recording.remoteSpans...)
	s.mu.Unlock()

	for _, child := range children {
		result = append(result, child.getRecording(m)...)
	}

	// Sort the spans by StartTime, except the first Span (the root of this
	// recording) which stays in place.
	toSort := sortPool.Get().(*Recording) // avoids allocations in sort.Sort
	*toSort = result[1:]
	sort.Sort(toSort)
	*toSort = nil
	sortPool.Put(toSort)
	return result
}

func (s *crdbSpan) importRemoteSpans(remoteSpans []tracingpb.RecordedSpan) error {
	// Change the root of the remote recording to be a child of this Span. This is
	// usually already the case, except with DistSQL traces where remote
	// processors run in spans that FollowFrom an RPC Span that we don't collect.
	remoteSpans[0].ParentSpanID = s.spanID

	s.mu.Lock()
	s.mu.recording.remoteSpans = append(s.mu.recording.remoteSpans, remoteSpans...)
	s.mu.Unlock()
	return nil
}

func (s *crdbSpan) setTagLocked(key string, value interface{}) {
	if s.mu.tags == nil {
		s.mu.tags = make(opentracing.Tags)
	}
	s.mu.tags[key] = value
}

func (s *crdbSpan) record(msg string) {
	if s.recordingType() != RecordingVerbose {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.mu.recording.recordedLogs) < maxLogsPerSpan {
		s.mu.recording.recordedLogs = append(s.mu.recording.recordedLogs, opentracing.LogRecord{
			Timestamp: time.Now(),
			Fields: []otlog.Field{
				otlog.String(tracingpb.LogMessageField, msg),
			},
		})
	}
}

func (s *crdbSpan) logStructured(item Structured) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.structured = append(s.mu.structured, item)
}

func (s *crdbSpan) setBaggageItemAndTag(restrictedKey, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setBaggageItemLocked(restrictedKey, value)
	// Don't set the tag if this is the special cased baggage item indicating
	// span verbosity, as it is named nondescriptly and the recording knows
	// how to display its verbosity independently.
	if restrictedKey != verboseTracingBaggageKey {
		s.setTagLocked(restrictedKey, value)
	}
}

func (s *crdbSpan) setBaggageItemLocked(restrictedKey, value string) {
	if oldVal, ok := s.mu.baggage[restrictedKey]; ok && oldVal == value {
		// No-op.
		return
	}
	if s.mu.baggage == nil {
		s.mu.baggage = make(map[string]string)
	}
	s.mu.baggage[restrictedKey] = value
}

// getRecordingLocked returns the Span's recording. This does not include
// children.
func (s *crdbSpan) getRecordingLocked(m mode) tracingpb.RecordedSpan {
	rs := tracingpb.RecordedSpan{
		TraceID:      s.traceID,
		SpanID:       s.spanID,
		ParentSpanID: s.parentSpanID,
		Operation:    s.operation,
		StartTime:    s.startTime,
		Duration:     s.mu.duration,
	}

	addTag := func(k, v string) {
		if rs.Tags == nil {
			rs.Tags = make(map[string]string)
		}
		rs.Tags[k] = v
	}

	// When nobody is configured to see our spans, skip some allocations
	// related to Span UX improvements.
	onlyBackgroundTracing := m == modeBackground && s.recordingType() == RecordingOff
	if !onlyBackgroundTracing {
		if rs.Duration == -1 {
			// -1 indicates an unfinished Span. For a recording it's better to put some
			// duration in it, otherwise tools get confused. For example, we export
			// recordings to Jaeger, and spans with a zero duration don't look nice.
			rs.Duration = timeutil.Now().Sub(rs.StartTime)
			addTag("_unfinished", "1")
		}
		if s.mu.recording.recordingType.load() == RecordingVerbose {
			addTag("_verbose", "1")
		}
	}

	if s.mu.stats != nil {
		stats, err := types.MarshalAny(s.mu.stats)
		if err != nil {
			panic(err)
		}
		rs.DeprecatedStats = stats
	}

	if s.mu.structured != nil {
		rs.InternalStructured = make([]*types.Any, 0, len(s.mu.structured))
		for i := range s.mu.structured {
			item, err := types.MarshalAny(s.mu.structured[i])
			if err != nil {
				// An error here is an error from Marshal; these
				// are unlikely to happen.
				continue
			}
			rs.InternalStructured = append(rs.InternalStructured, item)
		}
	}

	if len(s.mu.baggage) > 0 {
		rs.Baggage = make(map[string]string)
		for k, v := range s.mu.baggage {
			rs.Baggage[k] = v
		}
	}
	if !onlyBackgroundTracing && s.logTags != nil {
		setLogTags(s.logTags.Get(), func(remappedKey string, tag *logtags.Tag) {
			addTag(remappedKey, tag.ValueStr())
		})
	}
	if len(s.mu.tags) > 0 {
		for k, v := range s.mu.tags {
			// We encode the tag values as strings.
			addTag(k, fmt.Sprint(v))
		}
	}
	rs.Logs = make([]tracingpb.LogRecord, len(s.mu.recording.recordedLogs))
	for i, r := range s.mu.recording.recordedLogs {
		rs.Logs[i].Time = r.Timestamp
		rs.Logs[i].Fields = make([]tracingpb.LogRecord_Field, len(r.Fields))
		for j, f := range r.Fields {
			rs.Logs[i].Fields[j] = tracingpb.LogRecord_Field{
				Key:   f.Key(),
				Value: fmt.Sprint(f.Value()),
			}
		}
	}

	return rs
}

func (s *crdbSpan) addChild(child *crdbSpan) {
	s.mu.Lock()
	// Only record the child if the parent still has room.
	if len(s.mu.recording.children) < maxChildrenPerSpan {
		s.mu.recording.children = append(s.mu.recording.children, child)
	}
	s.mu.Unlock()
}

var sortPool = sync.Pool{
	New: func() interface{} {
		return &Recording{}
	},
}

// Less implements sort.Interface.
func (r Recording) Less(i, j int) bool {
	return r[i].StartTime.Before(r[j].StartTime)
}

// Swap implements sort.Interface.
func (r Recording) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

// Len implements sort.Interface.
func (r Recording) Len() int {
	return len(r)
}

type atomicRecordingType RecordingType

// load returns the recording type.
func (art *atomicRecordingType) load() RecordingType {
	return RecordingType(atomic.LoadInt32((*int32)(art)))
}

// swap stores the new recording type and returns the old one.
func (art *atomicRecordingType) swap(recType RecordingType) RecordingType {
	return RecordingType(atomic.SwapInt32((*int32)(art), int32(recType)))
}
