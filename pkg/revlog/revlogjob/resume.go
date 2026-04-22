// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// ResumeStateForPartition derives the per-producer ResumeState for
// a partition's assigned spans from the rehydrated TickManager
// state.
//
// The persisted Frontier may have spans that don't line up with the
// new partition's spans (e.g. partitioning changed across the
// resume). For each new span, we look at every overlapping
// persisted entry and take the lowest persisted ts — the most
// conservative ("re-derive any events back to the laggiest
// persisted position") position the producer can safely resume
// from without missing events.
//
// StartingFlushOrders is the same map passed to every producer:
// any producer might contribute to any open tick and must use a
// flushorder strictly above the prior incarnation's max for that
// tick to preserve per-key ordering.
func ResumeStateForPartition(state State, spans []roachpb.Span) ResumeState {
	var resumes []SpanResume
	if state.Frontier != nil {
		for _, sp := range spans {
			var minTS hlc.Timestamp
			var found bool
			for esp, ts := range state.Frontier.Entries() {
				if !sp.Overlaps(esp) || !ts.IsSet() {
					continue
				}
				if !found || ts.Less(minTS) {
					minTS = ts
					found = true
				}
			}
			if found {
				resumes = append(resumes, SpanResume{Span: sp, Resolved: minTS})
			}
		}
	}

	var flushOrders map[hlc.Timestamp]int32
	if len(state.OpenTicks) > 0 {
		flushOrders = make(map[hlc.Timestamp]int32, len(state.OpenTicks))
		for tickEnd, files := range state.OpenTicks {
			var next int32
			for _, f := range files {
				if f.FlushOrder >= next {
					next = f.FlushOrder + 1
				}
			}
			flushOrders[tickEnd] = next
		}
	}

	return ResumeState{SpanResumes: resumes, StartingFlushOrders: flushOrders}
}

// resumeToSpec converts a per-producer ResumeState into the wire
// fields on RevlogSpec.
func resumeToSpec(spec *execinfrapb.RevlogSpec, resume ResumeState) {
	if len(resume.SpanResumes) > 0 {
		spec.ResumeStarts = make([]execinfrapb.RevlogResumeSpan, len(resume.SpanResumes))
		for i, sr := range resume.SpanResumes {
			spec.ResumeStarts[i] = execinfrapb.RevlogResumeSpan{
				Span:     sr.Span,
				Resolved: sr.Resolved,
			}
		}
	}
	if len(resume.StartingFlushOrders) > 0 {
		spec.TickStartingFlushOrders = make(
			[]execinfrapb.RevlogTickResume, 0, len(resume.StartingFlushOrders),
		)
		for tickEnd, flushOrder := range resume.StartingFlushOrders {
			spec.TickStartingFlushOrders = append(
				spec.TickStartingFlushOrders, execinfrapb.RevlogTickResume{
					TickEnd:            tickEnd,
					StartingFlushOrder: flushOrder,
				},
			)
		}
	}
}

// resumeFromSpec is the processor-side inverse of resumeToSpec.
func resumeFromSpec(spec execinfrapb.RevlogSpec) ResumeState {
	var r ResumeState
	if len(spec.ResumeStarts) > 0 {
		r.SpanResumes = make([]SpanResume, len(spec.ResumeStarts))
		for i, rs := range spec.ResumeStarts {
			r.SpanResumes[i] = SpanResume{Span: rs.Span, Resolved: rs.Resolved}
		}
	}
	if len(spec.TickStartingFlushOrders) > 0 {
		r.StartingFlushOrders = make(map[hlc.Timestamp]int32, len(spec.TickStartingFlushOrders))
		for _, t := range spec.TickStartingFlushOrders {
			r.StartingFlushOrders[t.TickEnd] = t.StartingFlushOrder
		}
	}
	return r
}

// rangefeedStart picks the timestamp the producer's rangefeed
// subscription should start at: the lowest per-span resume position
// across this producer's spans (the "laggiest" span, which we must
// catch up from to avoid missing events for that span), clamped to
// be no lower than spec.StartHLC. With no resume info, returns
// spec.StartHLC.
func rangefeedStart(spec execinfrapb.RevlogSpec, resume ResumeState) hlc.Timestamp {
	start := spec.StartHLC
	var picked bool
	for _, sr := range resume.SpanResumes {
		if sr.Resolved.Less(spec.StartHLC) {
			// Defensive: the persisted frontier should never be
			// below StartHLC; if it somehow is, ignore it.
			continue
		}
		if !picked || sr.Resolved.Less(start) {
			start = sr.Resolved
			picked = true
		}
	}
	return start
}
