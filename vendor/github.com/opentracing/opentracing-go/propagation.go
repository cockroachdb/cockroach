package opentracing

import "errors"

///////////////////////////////////////////////////////////////////////////////
// CORE PROPAGATION INTERFACES:
///////////////////////////////////////////////////////////////////////////////

var (
	// ErrTraceNotFound occurs when the `carrier` passed to Extractor.JoinTrace()
	// is valid and uncorrupted but has insufficient information to join or
	// resume a trace.
	ErrTraceNotFound = errors.New("opentracing: Trace not found in Extraction carrier")

	// ErrInvalidCarrier errors occur when Injector.InjectSpan() or
	// Extractor.JoinTrace() implementations expect a different type of
	// `carrier` than they are given.
	ErrInvalidCarrier = errors.New("opentracing: Invalid Injection/Extraction carrier")

	// ErrTraceCorrupted occurs when the `carrier` passed to Extractor.JoinTrace()
	// is of the expected type but is corrupted.
	ErrTraceCorrupted = errors.New("opentracing: Trace data corrupted in Extraction carrier")
)

// Injector is responsible for injecting Span instances in a manner suitable
// for propagation via a format-specific "carrier" object. Typically the
// injection will take place across an RPC boundary, but message queues and
// other IPC mechanisms are also reasonable places to use an Injector.
//
// See Extractor and Tracer.Injector.
type Injector interface {
	// InjectSpan takes `span` and injects it into `carrier`. The actual type
	// of `carrier` depends on the `format` passed to `Tracer.Injector()`.
	//
	// Implementations may return opentracing.ErrInvalidCarrier or any other
	// implementation-specific error if injection fails.
	InjectSpan(span Span, carrier interface{}) error
}

// Extractor is responsible for extracting Span instances from an
// format-specific "carrier" object. Typically the extraction will take place
// on the server side of an RPC boundary, but message queues and other IPC
// mechanisms are also reasonable places to use an Extractor.
//
// See Injector and Tracer.Extractor.
type Extractor interface {
	// JoinTrace returns a Span instance with operation name `operationName`
	// given `carrier`, or (nil, opentracing.ErrTraceNotFound) if no trace could be found to
	// join with in the `carrier`.
	//
	// Implementations may return opentracing.ErrInvalidCarrier,
	// opentracing.ErrTraceCorrupted, or implementation-specific errors if there
	// are more fundamental problems with `carrier`.
	//
	// Upon success, the returned Span instance is already started.
	JoinTrace(operationName string, carrier interface{}) (Span, error)
}

///////////////////////////////////////////////////////////////////////////////
// BUILTIN PROPAGATION FORMATS:
///////////////////////////////////////////////////////////////////////////////

// BuiltinFormat is used to demarcate the values within package `opentracing`
// that are intended for use with the Tracer.Injector() and Tracer.Extractor()
// methods.
type BuiltinFormat byte

const (
	// SplitBinary encodes the Span in a SplitBinaryCarrier instance.
	//
	// The `carrier` for injection and extraction must be a
	// `*SplitBinaryCarrier` instance.
	SplitBinary BuiltinFormat = iota

	// SplitText encodes the Span in a SplitTextCarrier instance.
	//
	// The `carrier` for injection and extraction must be a `*SplitTextCarrier`
	// instance.
	SplitText

	// GoHTTPHeader encodes the Span into a Go http.Header instance (both the
	// tracer state and any baggage).
	//
	// The `carrier` for both injection and extraction must be an http.Header
	// instance.
	GoHTTPHeader
)

// SplitTextCarrier breaks a propagated Span into two pieces.
//
// The Span is separated in this way for a variety of reasons; the most
// important is to give OpenTracing users a portable way to opt out of
// Baggage propagation entirely if they deem it a stability risk.
//
// It is legal to provide one or both maps as `nil`; they will be created
// as needed. If non-nil maps are provided, they will be used without
// clearing them out on injection.
type SplitTextCarrier struct {
	// TracerState is Tracer-specific context that must cross process
	// boundaries. For example, in Dapper this would include a trace_id, a
	// span_id, and a bitmask representing the sampling status for the given
	// trace.
	TracerState map[string]string

	// Any Baggage for the encoded Span (per Span.SetBaggageItem).
	Baggage map[string]string
}

// NewSplitTextCarrier creates a new SplitTextCarrier.
func NewSplitTextCarrier() *SplitTextCarrier {
	return &SplitTextCarrier{}
}

// SplitBinaryCarrier breaks a propagated Span into two pieces.
//
// The Span is separated in this way for a variety of reasons; the most
// important is to give OpenTracing users a portable way to opt out of
// Baggage propagation entirely if they deem it a stability risk.
//
// Both byte slices may be nil; on injection, what is provided will be cleared
// and the resulting capacity used.
type SplitBinaryCarrier struct {
	// TracerState is Tracer-specific context that must cross process
	// boundaries. For example, in Dapper this would include a trace_id, a
	// span_id, and a bitmask representing the sampling status for the given
	// trace.
	TracerState []byte

	// Any Baggage for the encoded Span (per Span.SetBaggageItem).
	Baggage []byte
}

// NewSplitBinaryCarrier creates a new SplitTextCarrier.
func NewSplitBinaryCarrier() *SplitBinaryCarrier {
	return &SplitBinaryCarrier{}
}
