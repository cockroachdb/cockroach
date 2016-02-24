package opentracing

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
)

// Span represents an active, un-finished span in the OpenTracing system.
//
// Spans are created by the Tracer interface.
type Span interface {
	// Sets or changes the operation name.
	SetOperationName(operationName string) Span

	// Adds a tag to the span.
	//
	// Tag values can be of arbitrary types, however the treatment of complex
	// types is dependent on the underlying tracing system implementation.
	// It is expected that most tracing systems will handle primitive types
	// like strings and numbers. If a tracing system cannot understand how
	// to handle a particular value type, it may ignore the tag, but shall
	// not panic.
	//
	// If there is a pre-existing tag set for `key`, it is overwritten.
	SetTag(key string, value interface{}) Span

	// Sets the end timestamp and calls the `Recorder`s RecordSpan()
	// internally.
	//
	// Finish() should be the last call made to any span instance, and to do
	// otherwise leads to undefined behavior.
	Finish()
	// FinishWithOptions is like Finish() but with explicit control over
	// timestamps and log data.
	FinishWithOptions(opts FinishOptions)

	// LogEvent() is equivalent to
	//
	//   Log(LogData{Event: event})
	//
	LogEvent(event string)

	// LogEventWithPayload() is equivalent to
	//
	//   Log(LogData{Event: event, Payload: payload0})
	//
	LogEventWithPayload(event string, payload interface{})

	// Log() records `data` to this Span.
	//
	// See LogData for semantic details.
	Log(data LogData)

	// SetBaggageItem sets a key:value pair on this Span that also
	// propagates to future Span children.
	//
	// SetBaggageItem() enables powerful functionality given a full-stack
	// opentracing integration (e.g., arbitrary application data from a mobile
	// app can make it, transparently, all the way into the depths of a storage
	// system), and with it some powerful costs: use this feature with care.
	//
	// IMPORTANT NOTE #1: SetBaggageItem() will only propagate trace
	// baggage items to *future* children of the Span.
	//
	// IMPORTANT NOTE #2: Use this thoughtfully and with care. Every key and
	// value is copied into every local *and remote* child of this Span, and
	// that can add up to a lot of network and cpu overhead.
	//
	// IMPORTANT NOTE #3: Baggage item keys have a restricted format:
	// implementations may wish to use them as HTTP header keys (or key
	// suffixes), and of course HTTP headers are case insensitive.
	//
	// As such, `restrictedKey` MUST match the regular expression
	// `(?i:[a-z0-9][-a-z0-9]*)` and is case-insensitive. That is, it must
	// start with a letter or number, and the remaining characters must be
	// letters, numbers, or hyphens. See CanonicalizeBaggageKey(). If
	// `restrictedKey` does not meet these criteria, SetBaggageItem()
	// results in undefined behavior.
	//
	// Returns a reference to this Span for chaining, etc.
	SetBaggageItem(restrictedKey, value string) Span

	// Gets the value for a baggage item given its key. Returns the empty string
	// if the value isn't found in this Span.
	//
	// See the `SetBaggageItem` notes about `restrictedKey`.
	BaggageItem(restrictedKey string) string

	// Provides access to the Tracer that created this Span.
	Tracer() Tracer
}

// LogData is data associated to a Span. Every LogData instance should specify
// at least one of Event and/or Payload.
type LogData struct {
	// The timestamp of the log record; if set to the default value (the unix
	// epoch), implementations should use time.Now() implicitly.
	Timestamp time.Time

	// Event (if non-empty) should be the stable name of some notable moment in
	// the lifetime of a Span. For instance, a Span representing a browser page
	// load might add an Event for each of the Performance.timing moments
	// here: https://developer.mozilla.org/en-US/docs/Web/API/PerformanceTiming
	//
	// While it is not a formal requirement, Event strings will be most useful
	// if they are *not* unique; rather, tracing systems should be able to use
	// them to understand how two similar Spans relate from an internal timing
	// perspective.
	Event string

	// Payload is a free-form potentially structured object which Tracer
	// implementations may retain and record all, none, or part of.
	//
	// If included, `Payload` should be restricted to data derived from the
	// instrumented application; in particular, it should not be used to pass
	// semantic flags to a Log() implementation.
	//
	// For example, an RPC system could log the wire contents in both
	// directions, or a SQL library could log the query (with or without
	// parameter bindings); tracing implementations may truncate or otherwise
	// record only a snippet of these payloads (or may strip out PII, etc,
	// etc).
	Payload interface{}
}

// FinishOptions allows Span.FinishWithOptions callers to override the finish
// timestamp and provide log data via a bulk interface.
type FinishOptions struct {
	// FinishTime overrides the Span's finish time, or implicitly becomes
	// time.Now() if FinishTime.IsZero().
	//
	// FinishTime must resolve to a timestamp that's >= the Span's StartTime
	// (per StartSpanOptions).
	FinishTime time.Time

	// BulkLogData allows the caller to specify the contents of many Log()
	// calls with a single slice. May be nil.
	//
	// None of the LogData.Timestamp values may be .IsZero() (i.e., they must
	// be set explicitly). Also, they must be >= the Span's start timestamp and
	// <= the FinishTime (or time.Now() if FinishTime.IsZero()). Otherwise the
	// behavior of FinishWithOptions() is undefined.
	//
	// If specified, the caller hands off ownership of BulkLogData at
	// FinishWithOptions() invocation time.
	BulkLogData []LogData
}

// Tags are a generic map from an arbitrary string key to an opaque value type.
// The underlying tracing system is responsible for interpreting and
// serializing the values.
type Tags map[string]interface{}

// Merge incorporates the keys and values from `other` into this `Tags`
// instance, then returns same.
func (t Tags) Merge(other Tags) Tags {
	for k, v := range other {
		t[k] = v
	}
	return t
}

var regexBaggage = regexp.MustCompile("^(?i:[a-z0-9][-a-z0-9]*)$")

// CanonicalizeBaggageKey returns the canonicalized version of baggage item
// key `key`, and true if and only if the key was valid.
//
// It is more performant to use lowercase keys only.
func CanonicalizeBaggageKey(key string) (string, bool) {
	if !regexBaggage.MatchString(key) {
		return "", false
	}
	return strings.ToLower(key), true
}

// StartChildSpan is a simple helper to start a child span given only its parent (per StartSpanOptions.Parent) and an operation name per Span.SetOperationName.
func StartChildSpan(parent Span, operationName string) Span {
	return parent.Tracer().StartSpanWithOptions(StartSpanOptions{
		OperationName: operationName,
		Parent:        parent,
	})
}

// InjectSpan is a helper that injects a Span instance into a carrier.
//
// See Tracer.Injector() and Injector.InjectSpan().
func InjectSpan(sp Span, format interface{}, carrier interface{}) error {
	var inj Injector
	if inj = sp.Tracer().Injector(format); inj == nil {
		return fmt.Errorf(
			"Unsupported PropagationFormat: %v (type: %v)",
			format,
			reflect.TypeOf(format))
	}
	return inj.InjectSpan(sp, carrier)
}
