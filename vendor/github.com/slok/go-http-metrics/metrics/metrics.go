package metrics

import (
	"context"
	"time"
)

// HTTPReqProperties are the metric properties for the metrics based
// on client request.
type HTTPReqProperties struct {
	// Service is the service that has served the request.
	Service string
	// ID is the id of the request handler.
	ID string
	// Method is the method of the request.
	Method string
	// Code is the response of the request.
	Code string
}

// HTTPProperties are the metric properties for the global server metrics.
type HTTPProperties struct {
	// Service is the service that has served the request.
	Service string
	// ID is the id of the request handler.
	ID string
}

// Recorder knows how to record and measure the metrics. This
// Interface has the required methods to be used with the HTTP
// middlewares.
type Recorder interface {
	// ObserveHTTPRequestDuration measures the duration of an HTTP request.
	ObserveHTTPRequestDuration(ctx context.Context, props HTTPReqProperties, duration time.Duration)
	// ObserveHTTPResponseSize measures the size of an HTTP response in bytes.
	ObserveHTTPResponseSize(ctx context.Context, props HTTPReqProperties, sizeBytes int64)
	// AddInflightRequests increments and decrements the number of inflight request being
	// processed.
	AddInflightRequests(ctx context.Context, props HTTPProperties, quantity int)
}

// Dummy is a dummy recorder.
const Dummy = dummy(0)

type dummy int

func (dummy) ObserveHTTPRequestDuration(_ context.Context, _ HTTPReqProperties, _ time.Duration) {}
func (dummy) ObserveHTTPResponseSize(_ context.Context, _ HTTPReqProperties, _ int64)            {}
func (dummy) AddInflightRequests(_ context.Context, _ HTTPProperties, _ int)                     {}

var _ Recorder = Dummy
