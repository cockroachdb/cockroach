// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package tracing encapsulates all tracing facilities used in CockroachDB.
// Tracing exposes a lifetime-of-a-request view of operations, tracking it
// through various internal components and across RPC boundaries. The concepts
// and primitives used in this package are standard for most distributed
// tracing libraries[1][2], but we'll capture it all here anyway.
//
// 1. The Data Model
//
//          [Span A]  <--- (root span)
//              |
//       +------+------+
//       |             |
//   [Span B]      [Span C] <--- (C is a "child of" A)
//       |             |
//   [Span D]      +---+-------+
//                 |           |
//             [Span E]    [Span F] >>> [Span G] <--- (G "follows from" F)
//
//
// Traces are defined implicitly by their Spans. A Trace can be thought of a
// directed acyclic graph of Spans, where edges between Spans indicate that
// they're causally related. An alternate (and usually the more useful)
// rendering[3] of traces is a temporal one:
//
//
//  ––|–––––––|–––––––|–––––––|–––––––|–––––––|–––––––|–––––––|–> time
//
//   [Span A···················································]
//     [Span B··············································]
//        [Span D··········································]
//      [Span C········································]
//           [Span E·······]           [Span F··] [Span G··]
//
//
// The causal relation between spans can be one of two types:
// - Parent-child relation: Typically used when the parent span depends on the
//                          result of the child span (during an RPC call, the
//                          client-side span would be the parent of the
//                          server-side span). See [4].
// - Follows-from relation: Typically used when the first span does not in any
//                          way depend on the result of the second (think of a
//                          goroutine that spins off another that outlives it).
//                          Note that we still refer to the "first" and "second"
//                          span as being the "parent" and "child" respectively
//                          (they're still nodes in the DAG, just with a
//                          different kind of edge between them)[5].
//
// Each Span[6] is logically comprised of the following:
// - An operation name
// - Timing information (start timestamp, duration)
// - A set of zero or more tags (for annotation, visible when rendering spans)
// - A set of zero or more baggage items (data that crosses process boundaries)
// - References to other spans (mediated by the relations described above)
// - Recording data[7] (structured data/messages visible when rendering spans)
//
// Spans are created through a Tracer. Related, Tracers also understand how to
// serialize and deserialize[8] Spans across process boundaries (using only the
// Span metadata[9]). We've defined handy GRPC interceptors[10] that let us do
// this across RPC boundaries.
//
// The tracing package is tightly coupled with the context package. Since we
// want the tracing infrastructure to be plumbed through the various layers in
// the callstack, we tuck the Span object within a context.Context[11].
//
// Since this package is used pervasively, the implementation is very
// performance-sensitive. It tries to avoid allocations (even
// trying to avoid allocating Span objects[12] whenever possible), and avoids
// doing work unless strictly necessary. One example of this is us checking to
// see if a given Span is a "noop span"[13] (i.e. does not have any sinks
// configured). This then lets us short-circuit work that would be discarded
// anyway.
//
// The tracing package internally makes use of an opentracing[2]-compatible
// library. This gives us the ability to configure external collectors for
// tracing information[14], like lightstep or zipkin.
//
// -----------------------------------------------------------------------------
//
// [1]: https://research.google/pubs/pub36356/
// [2]: https://opentracing.io/specification/
// [3]: `Recording.String`
// [4]: `ChildSpan`
// [5]: `ForkSpan`. "forking" a Span is the same as creating a new one
//      with a "follows from" relation.
// [6]: `crdbSpan`
// [7]: `Span.SetVerbose`. To understand the specifics of what exactly is
//      captured in Span recording, when Spans have children that may be either
//      local or remote, look towards `WithParentAnd{Auto,Manual}Collection`
// [8]: `Tracer.{InjectMetaInto,ExtractMetaFrom}`
// [9]: `SpanMeta`
// [10]: `{Client,Server}Interceptor`
// [11]: `SpanFromContext`
// [12]: WithForceRealSpan
// [13]: `Span.isNoop`
// [14]: `shadowTracer`
package tracing
