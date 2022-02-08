// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfig

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// Target specifies the target of an associated span configuration.
//
// TODO(arul): In the future, we will expand this to include system targets.
type Target struct {
	span roachpb.Span
}

func MakeTarget(t roachpb.SpanConfigTarget) Target {
	switch t.Union.(type) {
	case *roachpb.SpanConfigTarget_Span:
		return MakeSpanTarget(*t.GetSpan())
	default:
		panic("cannot handle target")
	}
}

// MakeSpanTarget constructs and returns a span target.
func MakeSpanTarget(span roachpb.Span) Target {
	return Target{
		span: span,
	}
}

// GetSpan returns the underlying roachpb.Span if the target is a span target
// and nil otherwise.
func (t *Target) GetSpan() *roachpb.Span {
	if t.span.Equal(roachpb.Span{}) {
		return nil
	}
	return &t.span
}

// Encode returns an encoded span suitable for persistence in
// system.span_configurations.
func (t Target) Encode() roachpb.Span {
	if t.GetSpan() != nil {
		return t.span
	}
	panic("cannot handle any other type of target")
}

// Less returns true if the receiver is less than the supplied target.
func (t *Target) Less(o Target) bool {
	return t.Encode().Key.Compare(o.Encode().Key) < 0
}

// Equal returns true iff the receiver is equal to the supplied target.
func (t *Target) Equal(o Target) bool {
	return t.GetSpan().Equal(*o.GetSpan())
}

// String returns a formatted version of the traget suitable for printing.
func (t Target) String() string {
	return t.GetSpan().String()
}

// IsEmpty returns true if the receiver is an empty target.
func (t Target) IsEmpty() bool {
	return t.GetSpan().Equal(roachpb.Span{})

}

func (t Target) SpanConfigTargetProto() roachpb.SpanConfigTarget {
	if t.GetSpan() != nil {
		return roachpb.SpanConfigTarget{
			Union: &roachpb.SpanConfigTarget_Span{
				Span: t.GetSpan(),
			},
		}
	}

	panic("cannot handle any other type of target")
}

// DecodeTarget takes a raw span and decodes it into a Target given its
// encoding. It is the inverse of Encode.
func DecodeTarget(sp roachpb.Span) Target {
	return Target{
		span: sp,
	}
}

// Targets is  a slice of span config targets.
type Targets []Target

// Len implement sort.Interface.
func (t Targets) Len() int { return len(t) }

// Swap implements sort.Interface.
func (t Targets) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

// Less implements Sort.Interface.
func (t Targets) Less(i, j int) bool {
	return t[i].Less(t[j])
}

// RecordsToSpanConfigEntries converts a list of records to a list
// roachpb.SpanConfigEntry protos suitable for sending over the wire.
func RecordsToSpanConfigEntries(records []Record) []roachpb.SpanConfigEntry {
	entries := make([]roachpb.SpanConfigEntry, 0, len(records))
	for _, rec := range records {
		entries = append(entries, roachpb.SpanConfigEntry{
			Target: rec.Target.SpanConfigTargetProto(),
			Config: rec.Config,
		})
	}
	return entries
}

// EntriesToRecords converts a list of roachpb.SpanConfigEntries
// (received over the wire) to a list of Records.
func EntriesToRecords(entries []roachpb.SpanConfigEntry) []Record {
	records := make([]Record, 0, len(entries))
	for _, entry := range entries {
		records = append(records, Record{
			Target: MakeTarget(entry.Target),
			Config: entry.Config,
		})
	}
	return records
}

// TargetsToTargetProtos converts a list of targets to a list of
// roachpb.SpanConfigTarget protos suitable for sending over the wire.
func TargetsToTargetProtos(targets []Target) []roachpb.SpanConfigTarget {
	targetProtos := make([]roachpb.SpanConfigTarget, 0, len(targets))
	for _, target := range targets {
		targetProtos = append(targetProtos, target.SpanConfigTargetProto())
	}
	return targetProtos
}

// TargetProtosToTargets converts a list of roachpb.SpanConfigTargets
// (received over the wire) to a list of Targets.
func TargetProtosToTargets(protoTargtets []roachpb.SpanConfigTarget) []Target {
	targets := make([]Target, 0, len(protoTargtets))
	for _, t := range protoTargtets {
		targets = append(targets, MakeTarget(t))
	}
	return targets
}
