// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tracing

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/logtags"
	opentracing "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/rcrowley/go-metrics"
)

const ComponentTagName string = "syscomponent"

// componentTraces wraps a ComponentTraces protobuf message with a
// mutex for concurrency, sample & target counts for reservoir
// sampling, and a map for pending spans.
type componentTraces struct {
	syncutil.Mutex
	ComponentTraces

	sampleCount  int64
	targetCount  int64
	pendingSpans map[uint64]*ComponentSpan
}

func (ct *componentTraces) addPendingSpan(c *ComponentSpan) {
	ct.Lock()
	ct.pendingSpans[c.Span.(*span).SpanID] = c
	ct.Unlock()
}

func (ct *componentTraces) removePendingSpanAndAddSample(
	c *ComponentSpan, sample ComponentSamples_Sample,
) {
	ct.Lock()
	delete(ct.pendingSpans, c.Span.(*span).SpanID)
	ct.Samples = append(ct.Samples, sample)
	ct.Unlock()
}

func (ct *componentTraces) removePendingSpan(c *ComponentSpan) {
	ct.Lock()
	delete(ct.pendingSpans, c.Span.(*span).SpanID)
	ct.Unlock()
}

// recording wraps a map of componentTraces objects with a mutex for
// concurrency.
type recording struct {
	syncutil.RWMutex

	targetCount int64
	traces      map[string]*componentTraces
	// eventLogs are stopped when this recording goes away
	eventLogs []*EventLog
}

// getTraces returns an existing componentTraces for the specified
// component name, or creates a new one if one doesn't yet exist.
func (r *recording) getTraces(name string) *componentTraces {
	r.RLock()
	ca, ok := r.traces[name]
	r.RUnlock()
	if !ok {
		ca = &componentTraces{
			targetCount:  r.targetCount,
			pendingSpans: map[uint64]*ComponentSpan{},
		}
		r.Lock()
		r.traces[name] = ca
		r.Unlock()
	}
	return ca
}

type recordingSlice struct {
	syncutil.RWMutex
	values []*recording
}

// activeRecordings holds a mutex-protected slice of active recordings.
var activeRecordings recordingSlice

// stuckRecording is a recording for any known-stuck component spans.
var stuckRecording = &recording{traces: map[string]*componentTraces{}}

// componentMetrics holds counters for spans, events, and errors.
type componentMetrics struct {
	spanCount  metrics.Counter
	eventCount metrics.Counter
	stuckCount metrics.Counter
	errorCount metrics.Counter
	mu         struct {
		syncutil.Mutex
		events map[string]int64
	}
}

func (cm *componentMetrics) incEvent(ev string) {
	cm.mu.Lock()
	if cm.mu.events == nil {
		cm.mu.events = make(map[string]int64)
	}
	cm.mu.events[ev]++
	cm.mu.Unlock()
}

func (cm *componentMetrics) getEvents() map[string]int64 {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	m := make(map[string]int64)
	for ev, cnt := range cm.mu.events {
		m[ev] = cnt
	}
	return m
}

// metricsMap holds a mutex-protected map from component name to
// componentMetrics.
type metricsMap struct {
	syncutil.RWMutex
	values map[string]*componentMetrics
}

// compMetrics is a static instance of metricsMap.
var compMetrics = &metricsMap{values: map[string]*componentMetrics{}}

// getComponentMetrics returns the named component metrics, or creates
// one if one does not yet exist.
func (mm *metricsMap) getComponentMetrics(component string) *componentMetrics {
	mm.RLock()
	cm, ok := mm.values[component]
	mm.RUnlock()
	if !ok {
		mm.Lock()
		cm = &componentMetrics{
			spanCount:  metrics.NewCounter(),
			eventCount: metrics.NewCounter(),
			stuckCount: metrics.NewCounter(),
			errorCount: metrics.NewCounter(),
		}
		mm.values[component] = cm
		mm.Unlock()
	}
	return cm
}

func incSpanCount(component string, inc int64) {
	compMetrics.getComponentMetrics(component).spanCount.Inc(inc)
}

func incEventCount(component string, event string, inc int64) {
	cm := compMetrics.getComponentMetrics(component)
	cm.eventCount.Inc(inc)
	if event != "" {
		cm.incEvent(event)
	}
}

func incStuckCount(component string, inc int64) {
	compMetrics.getComponentMetrics(component).stuckCount.Inc(inc)
}

func decStuckCount(component string, dec int64) {
	compMetrics.getComponentMetrics(component).stuckCount.Dec(dec)
}

func incErrorCount(component string, inc int64) {
	compMetrics.getComponentMetrics(component).errorCount.Inc(inc)
}

// IsComponentRecordingActive returns whether there are active recordings.
func IsComponentRecordingActive() bool {
	activeRecordings.RLock()
	defer activeRecordings.RUnlock()
	return len(activeRecordings.values) > 0
}

// Record starts a recording that lasts until the stop channel is
// closed, targeting at least targetCount samples of traces through
// each active system component. Returns a map from component name to
// traces.
func Record(stop <-chan time.Time, targetCount int64) map[string]ComponentTraces {
	fmt.Printf("!!! Record\n")
	activeRecordings.Lock()
	rec := &recording{
		targetCount: targetCount,
		traces:      map[string]*componentTraces{},
	}
	activeRecordings.values = append(activeRecordings.values, rec)
	activeRecordings.Unlock()

	// Sleep until the stop channel receives.
	fmt.Printf("!!! Record about sleep\n")
	<-stop
	fmt.Printf("!!! Record sleep... done\n")

	// Finish all the EventLogs
	// !!! check locking here
	for _, el := range rec.eventLogs {
		el.Finish()
	}

	// Remove the recording from the active recordings slice.
	activeRecordings.Lock()
	for i := range activeRecordings.values {
		if rec == activeRecordings.values[i] {
			activeRecordings.values = append(activeRecordings.values[:i], activeRecordings.values[i+1:]...)
			break
		}
	}
	activeRecordings.Unlock()

	addPendingFn := func(
		ct *ComponentTraces, stuck bool, pendingSpans map[uint64]*ComponentSpan,
	) {
		// Cycle through pending spans.
		for _, ps := range pendingSpans {
			sp := ps.Span.(*span)
			sample := ComponentSamples_Sample{
				Stuck:   stuck,
				Pending: true,
				Spans:   GetRecording(sp),
			}
			ct.Samples = append(ct.Samples, sample)
		}
	}

	// Build the map from component name to traces.
	result := map[string]ComponentTraces{}
	for k, v := range rec.traces {
		// Clone traces from the component and add in pending spans.
		v.Lock()
		// TODO(spencer): something failing here in "merger not found for type:int".
		//ct := proto.Clone(&v.ComponentTraces).(*ComponentTraces)
		ct := v.ComponentTraces
		ct.Timestamp = timeutil.Now()
		addPendingFn(&ct, false /* stuck */, v.pendingSpans)
		v.Unlock()
		result[k] = ct
	}

	// Add any pending spans that are in the stuck singleton recording.
	stuckRecording.Lock()
	for k, v := range stuckRecording.traces {
		ct := result[k]
		ct.Timestamp = timeutil.Now()
		addPendingFn(&ct, true /* stuck */, v.pendingSpans)
		result[k] = ct
	}
	stuckRecording.Unlock()

	fmt.Printf("!!! Record done\n")
	return result
}

// GetComponentActivity returns a map of component activity counters,
// one per active component.
func GetComponentActivity() map[string]ComponentActivity {
	// Build the map from component name to activity.
	result := map[string]ComponentActivity{}

	// Add metrics & collection timestamp.
	compMetrics.RLock()
	for k, v := range compMetrics.values {
		var ca ComponentActivity
		ca.SpanCount = v.spanCount.Count()
		ca.EventCount = v.eventCount.Count()
		ca.StuckCount = v.stuckCount.Count()
		ca.Errors = v.errorCount.Count()
		ca.Timestamp = timeutil.Now()
		ca.CustomEvents = v.getEvents()
		result[k] = ca
	}
	compMetrics.RUnlock()

	return result
}

// shouldRecordComponent returns whether or not to record a trace for
// the specified component. This requires that a recording is active
// and that the target number of traces has not been probabilistically
// exceeded.
func shouldRecordComponent(component string) (shouldRecord bool) {
	activeRecordings.RLock()
	defer activeRecordings.RUnlock()
	for _, r := range activeRecordings.values {
		ct := r.getTraces(component)
		ct.Lock()
		ct.sampleCount++
		// Probabilistically maintain a reservoir sample (but we
		// nevertheless always sample if the recording type of the
		// parent is ComponentRecording so we can stitch together
		// spans into a distributed trace.
		//
		// Note that this combines the probabilities if there are
		// multiple pending traces recordings, which will lead to
		// over-sampling.
		shouldRecord = shouldRecord || rand.Int63n(ct.sampleCount) < ct.targetCount
		ct.Unlock()
	}
	return shouldRecord
}

// startRecordingComponent adds the supplied component span as a
// pending span to all of the active recordings.
func startRecordingComponent(csp *ComponentSpan) {
	// Add span to the pendingSpans map for each pending traces recording.
	if csp.stuck {
		stuckRecording.getTraces(csp.component).addPendingSpan(csp)
	}
	activeRecordings.RLock()
	defer activeRecordings.RUnlock()
	for _, r := range activeRecordings.values {
		r.getTraces(csp.component).addPendingSpan(csp)
	}
}

// finishRecordingComponent removes the supplied component span from
// pending span maps on all active recordings and adds the sample to
// the component traces.
func finishRecordingComponent(csp *ComponentSpan, sample ComponentSamples_Sample) {
	activeRecordings.RLock()
	for _, r := range activeRecordings.values {
		r.getTraces(csp.component).removePendingSpanAndAddSample(csp, sample)
	}
	activeRecordings.RUnlock()

	if csp.stuck {
		stuckRecording.getTraces(csp.component).removePendingSpan(csp)
	}
}

// RecordComponentEvent records an event on behalf of a component. The component
// maintains a counter for each event.
func RecordComponentEvent(component string, event string) {
	recordComponentErrInner(component, event, "" /* err */)
}

// RecordComponentErr records an error event on behalf of a component. The
// component maintains a counter for each event.
// If err is "", this is equivalent to RecordCompoenentEvent().
func RecordComponentErr(component string, event string, err string) {
	recordComponentErrInner(component, event, err)
}

func recordComponentErrInner(component string, event string, err string) {
	if err != "" {
		incErrorCount(component, 1)
	}
	incEventCount(component, event, 1)

	activeRecordings.RLock()
	defer activeRecordings.RUnlock()
	for _, r := range activeRecordings.values {
		ct := r.getTraces(component)
		ct.Lock()
		if ct.Events == nil {
			ct.Events = make(map[string]*ComponentTraces_Event)
		}
		if ev, ok := ct.Events[event]; !ok {
			ct.Events[event] = &ComponentTraces_Event{Count: 1, Error: err}
		} else {
			ev.Count++
		}
		ct.Unlock()
	}
}

func SetComponentErr(osp opentracing.Span, err error) {
	panic("!!! unimplemented")
}

type ComponentSpan struct {
	opentracing.Span
	component string
	operation string

	errs []error

	sampled bool
	pSpan   opentracing.Span
	stuck   bool

	// !!! replaced with Span.Tracer()
	// !!! t opentracing.Tracer
}

// FinishWithError is like Finish() and it additionally records an error on the
// span if err is not nil.
func (c *ComponentSpan) FinishWithError(err error) {
	c.SetError(err)
	c.Finish()
}

func (c *ComponentSpan) Finish() {
	c.Span.Finish()
	if !c.sampled {
		return
	}

	// If the parent span was recording using a recording type other than
	// ComponentRecording, combine recorded spans collected as part of this
	// componentSpan with the parent span.
	if c.pSpan != nil && IsRecording(c.pSpan) && GetRecordingType(c.pSpan) != ComponentRecording {
		CombineSpans(c.pSpan, c.Span)
	}
	spans := GetRecording(c.Span)
	StopRecording(c.Span)

	var errStrs []string
	for _, err := range c.errs {
		errStrs = append(errStrs, err.Error())
	}
	errStr := strings.Join(errStrs, "\n")

	sample := ComponentSamples_Sample{
		Error: errStr,
		Stuck: c.stuck,
		Spans: spans,
	}
	finishRecordingComponent(c, sample)

	if c.stuck {
		decStuckCount(c.component, 1)
		// If stuck, then this ComponentSpan is a child of the original
		// ComponentSpan (which is now c.pSpan) and that original ComponentSpan was
		// orphaned when MarkAsStuck() was called - and so it's up to us to finish
		// it.
		// !!! fail here if there's no pSpan
		c.pSpan.Finish()
	}
}

func (c *ComponentSpan) SetError(err error) {
	if err != nil {
		c.errs = append(c.errs, err)
		recordComponentErrInner(c.component, fmt.Sprintf("%T", err), err.Error())
	}
}

func (c *ComponentSpan) MarkAsStuck(ctx context.Context) context.Context {
	if c.stuck {
		return ctx
	}
	incStuckCount(c.component, 1)
	if c.sampled {
		c.stuck = true
		stuckRecording.getTraces(c.component).addPendingSpan(c)
		return ctx
	}
	ctx, newC := startComponentSpanInner(
		ctx, c.Span.Tracer(), nil, /* parentOverride */
		c.component, c.operation+" [stuck]", true /* stuck */)
	*c = newC
	return ctx
}

// StartComponentSpan takes in a context and returns a derived one in
// the event that component tracing is enabled and this trace is
// chosen to be probabilistically sampled. If a trace is started, the
// returned context is linked to a child span with snowball recording
// enabled. Note that the snowball tracing used during a component
// trace is collected locally and not returned via RPC responses.
//
// The component identifies the component using a dot-separated
// hierarchical nomenclature. For example,
// "gateway.distsql.processor". The stuck parameter should be true
// if the component trace is being started in the event that a retry
// loop has been entered or a component has waited in a select block
// past a duration threshold. If true, the trace is always started,
// even if there are no pending component recordings, and registered in
// a singleton instance of a ComponentTraces map, to be returned with
// any call to inspect system component traces.
//
// Returns a derived context for further use, and a finish method
// which must be invoked by the caller when the caller exits. Note
// that the finish method should be invoked with the error value (if
// any) for the calling method.
//
// !!! comment about how a different span is being put in the ctx
func StartComponentSpan(
	ctx context.Context, tracer opentracing.Tracer, component string, operation string,
) (context.Context, ComponentSpan) {
	incSpanCount(component, 1)
	return startComponentSpanInner(ctx, tracer, nil, /* parentOverride */
		component, operation, false /* stuck */)
}

func StartComponentSpanChildOf(
	ctx context.Context,
	tracer opentracing.Tracer,
	parentOverride opentracing.StartSpanOption,
	component string,
	operation string,
) (context.Context, ComponentSpan) {
	incSpanCount(component, 1)
	return startComponentSpanInner(ctx, tracer, parentOverride, component, operation, false /* stuck */)
}

func startComponentSpanInner(
	ctx context.Context,
	tracer opentracing.Tracer,
	parentOverride opentracing.StartSpanOption,
	component string,
	operation string,
	stuck bool,
) (context.Context, ComponentSpan) {
	t := tracer.(*Tracer)
	var parentIsRecording bool
	var pSpan opentracing.Span
	if parentOverride == nil {
		// Get parent span if one exists and determine whether it's recording.
		pSpan = opentracing.SpanFromContext(ctx)
		if pSpan != nil {
			parentIsRecording = IsRecording(pSpan)
		}
	}

	// Determine whether this component should be recorded based on
	// pending component recordings.
	shouldRecord := stuck
	separateRecording := true
	if parentIsRecording && GetRecordingType(pSpan) == ComponentRecording {
		shouldRecord = true
		// Create a separate recording unless the parent's recording group
		// doesn't contain a component span. In this case, we want to keep
		// a single recording group so we record the non-component spans
		// along with this component span's sample. This happens across
		// RPC boundaries.
		separateRecording = parentRecordingHasComponentSpan(pSpan)
	}
	shouldRecord = shouldRecord || shouldRecordComponent(component)

	// If we're not recording this component's trace, return immediately.
	if !shouldRecord {
		var cSpan opentracing.Span
		if pSpan != nil {
			cSpan = StartChildSpan(operation, pSpan, nil /* logTags */, false /* separateRecording */)
		} else {
			cSpan = &t.noopSpan
		}
		compSpan := ComponentSpan{
			Span:      cSpan,
			component: component,
			operation: operation,
			// !!! t:         cSpan.Tracer(),
		}
		cSpan.SetTag(ComponentTagName, component)
		ctx = opentracing.ContextWithSpan(ctx, cSpan)
		return ctx, compSpan
	}

	// Create the component span. If there's no parent span or it's a
	// black hole span, then create a new span that's recordable.
	var cSpan opentracing.Span
	if pSpan == nil || IsBlackHoleSpan(pSpan) {
		if parentOverride == nil {
			cSpan = t.StartSpan(operation, Recordable, LogTagsFromCtx(ctx))
		} else {
			cSpan = t.StartSpan(operation, Recordable, LogTagsFromCtx(ctx), parentOverride)
		}
	} else {
		// Otherwise, start a child span which uses its own recording group.
		cSpan = StartChildSpan(operation, pSpan, logtags.FromContext(ctx), separateRecording)
	}
	cSpan.SetTag(ComponentTagName, component)

	// Start the recording (if not already started by the parent) and
	// set up the span collection.
	if !parentIsRecording {
		StartRecording(cSpan, ComponentRecording)
	}

	compSpan := ComponentSpan{
		Span:      cSpan,
		component: component,
		sampled:   true,
		pSpan:     pSpan,
		stuck:     stuck,
	}

	startRecordingComponent(&compSpan)

	return opentracing.ContextWithSpan(ctx, cSpan), compSpan
}

type EventLog struct {
	mu struct {
		syncutil.Mutex
		sp       ComponentSpan
		finished bool
	}
}

func (c *EventLog) Finish() {
	c.mu.Lock()
	if c.mu.finished {
		return
	}
	c.mu.finished = true
	c.mu.sp.Finish()
	c.mu.Unlock()
}

func (c *EventLog) Log(msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.finished {
		return
	}
	c.mu.sp.LogFields(otlog.String("event", msg))
}

func (c *EventLog) Finished() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.finished
}

func StartEventLog(
	ctx context.Context, tracer opentracing.Tracer, component string, operation string,
) *EventLog {
	isRecording := shouldRecordComponent(component)
	// !!! the locking here is broken
	activeRecordings.Lock()
	var rec *recording
	if len(activeRecordings.values) == 0 {
		isRecording = false
	} else {
		rec = activeRecordings.values[0]
	}
	activeRecordings.Unlock()

	if !isRecording {
		ev := &EventLog{}
		ev.mu.finished = true
		return ev
	}
	_, sp := startComponentSpanInner(ctx, tracer, nil, /* parentOverride */
		component, operation, false /* stuck */)
	ev := &EventLog{}
	ev.mu.sp = sp

	activeRecordings.Lock()
	rec.eventLogs = append(rec.eventLogs, ev)
	activeRecordings.Unlock()
	return ev
}

// !!! this method seems funky because it looks at a collection of spans which
// can change at any moment. That's probably not what we want. Rething.
func parentRecordingHasComponentSpan(osp opentracing.Span) bool {
	sp := osp.(*span)
	sp.mu.Lock()
	rg := sp.mu.recordingGroup
	sp.mu.Unlock()

	rg.Lock()
	spans := rg.spans
	rg.Unlock()
	for _, s := range spans {
		s.mu.Lock()
		_, ok := s.mu.tags[ComponentTagName]
		s.mu.Unlock()
		if ok {
			return true
		}
	}
	return false
}

func convertToStrings(values []interface{}) []string {
	strs := make([]string, 0, len(values))
	for _, v := range values {
		strs = append(strs, fmt.Sprint(v))
	}
	return strs
}
