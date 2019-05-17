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
	"github.com/rcrowley/go-metrics"
)

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

func (ca *componentTraces) addPendingSpan(c *ComponentSpan) {
	ca.Lock()
	ca.pendingSpans[c.Span.(*span).SpanID] = c
	ca.Unlock()
}

// recording wraps a map of componentTraces objects with a mutex for
// concurrency.
type recording struct {
	syncutil.RWMutex

	targetCount int64
	traces      map[string]*componentTraces
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

func incEventCount(component string, inc int64) {
	compMetrics.getComponentMetrics(component).eventCount.Inc(inc)
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
	activeRecordings.Lock()
	rec := &recording{
		targetCount: targetCount,
		traces:      map[string]*componentTraces{},
	}
	activeRecordings.values = append(activeRecordings.values, rec)
	activeRecordings.Unlock()

	// Sleep until the stop channel receives.
	<-stop

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
				Attributes: sp.getTags(),
				Stuck:      stuck,
				Pending:    true,
				Spans:      GetRecording(sp),
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
		ct := r.getTraces(csp.component)
		ct.Lock()
		ct.Samples = append(ct.Samples, sample)
		delete(ct.pendingSpans, csp.Span.(*span).SpanID)
		ct.Unlock()
	}
	activeRecordings.RUnlock()

	if csp.stuck {
		ct := stuckRecording.getTraces(csp.component)
		ct.Lock()
		delete(ct.pendingSpans, csp.Span.(*span).SpanID)
		ct.Unlock()
	}
}

// RecordComponentEvent records an event on behalf of a component. The component
// maintains a counter for each event.
func RecordComponentEvent(component string, event string) {
	RecordComponentErr(component, event, "" /* err */)
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
	incEventCount(component, 1)

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

func (c *ComponentSpan) FinishWithError(err error) {
	c.SetError(err)
	c.Finish()
}

func (c *ComponentSpan) Finish() {
	if !c.sampled {
		c.Span.Finish()
		return
	}

	c.Span.Finish()
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
		Attributes: c.Span.(*span).getTags(),
		Error:      errStr,
		Stuck:      c.stuck,
		Spans:      spans,
	}
	finishRecordingComponent(c, sample)

	if c.stuck {
		decStuckCount(c.component, 1)
		// If stuck, then this ComponentSpan is a child of the original
		// ComponentSpan (which is now c.pSpan) and that original ComponentSpan was
		// orphaned when MarkAsStuck() was called - and so it's up to us to finish
		// it. !!! explain better
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
	if c.sampled {
		c.stuck = true
		incStuckCount(c.component, 1)
		stuckRecording.getTraces(c.component).addPendingSpan(c)
		return ctx
	}
	ctx, newC := startComponentSpanInner(
		ctx, c.Span.Tracer(), c.component, c.operation+" [stuck]", true /* stuck */)
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
	return startComponentSpanInner(ctx, tracer, component, operation, false /* stuck */)
}

func startComponentSpanInner(
	ctx context.Context, tracer opentracing.Tracer, component string, operation string, stuck bool,
) (context.Context, ComponentSpan) {
	t := tracer.(*Tracer)
	// Get parent span if one exists and determine whether it's recording.
	var parentIsRecording bool
	pSpan := opentracing.SpanFromContext(ctx)
	if pSpan != nil {
		parentIsRecording = IsRecording(pSpan)
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
		ctx = opentracing.ContextWithSpan(ctx, cSpan)
		return ctx, compSpan
	}

	// Create the component span. If there's no parent span or it's a
	// black hole span, then create a new span that's recordable.
	var cSpan opentracing.Span
	if pSpan == nil || IsBlackHoleSpan(pSpan) {
		cSpan = t.StartSpan(operation, Recordable, LogTagsFromCtx(ctx))
	} else {
		// Otherwise, start a child span which uses its own recording group.
		if !separateRecording {
			fmt.Printf("using parent %+v recording for %s\n", pSpan, component)
		}
		cSpan = StartChildSpan(operation, pSpan, logtags.FromContext(ctx), separateRecording)
	}
	cSpan.SetTag("component", component)

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

func parentRecordingHasComponentSpan(osp opentracing.Span) bool {
	sp := osp.(*span)
	sp.mu.Lock()
	rg := sp.mu.recordingGroup
	sp.mu.Unlock()

	rg.Lock()
	defer rg.Unlock()
	for _, s := range rg.spans {
		s.mu.Lock()
		_, ok := s.mu.tags["component"]
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
