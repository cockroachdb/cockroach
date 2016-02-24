package opentracing

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
)

const (
	// ContextIDHTTPHeaderPrefix precedes the opentracing-related ContextID HTTP
	// headers.
	ContextIDHTTPHeaderPrefix = "Open-Tracing-Context-Id-"

	// TagsHTTPHeaderPrefix precedes the opentracing-related trace-tags HTTP
	// headers.
	TagsHTTPHeaderPrefix = "Open-Tracing-Trace-Tags-"
)

// InjectSpanInHeader encodes Span `sp` in `h` as a series of HTTP headers.
// Values are URL-escaped.
func InjectSpanInHeader(
	sp Span,
	h http.Header,
) error {
	// First, look for a GoHTTPHeader injector (our preference).
	injector := sp.Tracer().Injector(GoHTTPHeader)
	if injector != nil {
		return injector.InjectSpan(sp, h)
	}

	// Else, fall back on SplitText.
	if injector = sp.Tracer().Injector(SplitText); injector == nil {
		return errors.New("No suitable injector")
	}
	carrier := NewSplitTextCarrier()
	if err := injector.InjectSpan(sp, carrier); err != nil {
		return err
	}
	for headerSuffix, val := range carrier.TracerState {
		h.Add(ContextIDHTTPHeaderPrefix+headerSuffix, url.QueryEscape(val))
	}
	for headerSuffix, val := range carrier.Baggage {
		h.Add(TagsHTTPHeaderPrefix+headerSuffix, url.QueryEscape(val))
	}
	return nil
}

// JoinTraceFromHeader decodes a Span with operation name `operationName` from
// `h`, expecting that header values are URL-escpaed.
//
// If `operationName` is empty, the caller must later call
// `Span.SetOperationName` on the returned `Span`.
func JoinTraceFromHeader(
	operationName string,
	h http.Header,
	tracer Tracer,
) (Span, error) {
	// First, look for a GoHTTPHeader extractor (our
	// preference).
	extractor := tracer.Extractor(GoHTTPHeader)
	if extractor != nil {
		return extractor.JoinTrace(operationName, h)
	}

	// Else, fall back on SplitText.
	if extractor = tracer.Extractor(SplitText); extractor == nil {
		return nil, errors.New("No suitable extractor")
	}

	carrier := NewSplitTextCarrier()
	for key, val := range h {
		if strings.HasPrefix(key, ContextIDHTTPHeaderPrefix) {
			// We don't know what to do with anything beyond slice item v[0]:
			unescaped, err := url.QueryUnescape(val[0])
			if err != nil {
				return nil, err
			}
			carrier.TracerState[strings.TrimPrefix(key, ContextIDHTTPHeaderPrefix)] = unescaped
		} else if strings.HasPrefix(key, TagsHTTPHeaderPrefix) {
			// We don't know what to do with anything beyond slice item v[0]:
			unescaped, err := url.QueryUnescape(val[0])
			if err != nil {
				return nil, err
			}
			carrier.Baggage[strings.TrimPrefix(key, TagsHTTPHeaderPrefix)] = unescaped
		}
	}
	return extractor.JoinTrace(operationName, carrier)
}
