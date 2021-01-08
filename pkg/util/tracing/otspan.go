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

import "github.com/opentracing/opentracing-go"

// otSpan is a span for an external opentracing compatible tracer such as
// lightstep, zipkin, jaeger, etc.
type otSpan struct {
	// shadowTr is the shadowTracer this span was created from. We need
	// to hold on to it separately because shadowSpan.Tracer() returns
	// the wrapper tracer and we lose the ability to find out
	// what tracer it is. This is important when deriving children from
	// this span, as we want to avoid mixing different tracers, which
	// would otherwise be the result of cluster settings changed.
	shadowTr   *shadowTracer
	shadowSpan opentracing.Span
}
