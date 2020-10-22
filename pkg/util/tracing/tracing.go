// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package tracing provides distributed tracing functionality.
// Need:
// - Open/start a span, child span, serialize and deserialize a span
// - Log into a span, and annotate span with metadata (name, id)
// - Thread local storage for passing on trace context
// - Sampling of traces
// - Reason about inflight spans
// - About metadata being created
// - Structured metadata events
// - Gateway centric way of collecting spans
//
// Ref: https://storage.googleapis.com/pub-tools-public-publication-data/pdf/36356.pdf
// Ref: https://netflixtechblog.com/building-netflixs-distributed-tracing-infrastructure-bb856c319304
package tracing

type tracer interface {
	createRootSpan() span
}

// will probably hold onto the tracer
type span interface {
	createChildSpan(...string /*  tags */) span
	addTag()
	structuredEvent(event)
	eventf() // could be special cased into the above
	finish()

	enablerecording(bool)
	getrecording() recording
}

type recording []event

func example1() {
	r := tracer.createRootSpan()
	r.structuredEvent(boo)
	r.getrecording()  // does NOT contain boo

	r.enablerecording(true)
	r.structuredEvent(boo)
	r.getrecording() // does contain boo
}

func example2() {
	r := tracer.createRootSpan()

	s := r.createChildSpan()
	s.enablerecording(true)
	s.structuredEvent(boo)
	s.finish()
	s.getrecording() // does contain boo

	r.finish()
	r.getrecording() // what does this contain?
	// knz: why?
	// irfan: nvm

	s.enablerecording(false) // INVALID (finished)
	r.enablerecording(true) // INVALID (finished)
	s.structuredEvent(baa) // INVALID (finished)\
	r.getrecording() // what does this contain?

	// knz: proposal: recording should only happen after finish.
	// tbg: prefer the simplicity of a recording always giving back all that has been
	// recorded, and getting recording from a finished span guaranteeing that no more
	// events are going to be recorded.
	// irfan: don't like the distinction between record collecting local
	// children and ignoring remote ones. should do either all children or none
	// of them.
}

// - spans that collect all events
// - by default, eventf is a noop, but structured eventf
// - want the ability to turn it off completely


// De/ser, could go into grpc interceptors (would be nice to hide it away).
// any response proto could impl interfaces that let us shove spans into it. ref: node_batch.go

type registry {...}
var _ tracer = &registry{}

func (r *registry) CreateChildSpan(spanID spanID) spanID {
	...
}


type tracerKey struct{}
type spanKey struct{}

type spanID struct {...}


func GetSpanFromContext(ctx context.Context) span {
	tracer := GetTracerFromContext(ctx)
	if tracer == nil {
		return noopSpan{}
	}
	spanID := GetSpanIDFromContext(ctx)
	if spanID == default {
return noopSpan{}
}
return wrapSpan{tracer, spanID}
}

type wrapSpan{
tracer: tracer
spanID: spanID
}

func (w wrapSpan) createChildSpan() span {
	spanID := w.tracer.(*registry).CreateChildSpan(w.spanID)
	return wrapSpan{w.tracer, spanID}
}

