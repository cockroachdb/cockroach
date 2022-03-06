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

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// Target specifies the target of an associated span configuration.
type Target struct {
	span roachpb.Span

	systemTarget SystemTarget
}

// MakeTarget returns a new Target.
func MakeTarget(t roachpb.SpanConfigTarget) (Target, error) {
	switch t.Union.(type) {
	case *roachpb.SpanConfigTarget_Span:
		return MakeTargetFromSpan(*t.GetSpan()), nil
		// TODO(arul): Add a case here for SpanConfigTarget_SystemTarget once we've
		// taught and tested the KVAccessor to work with system targets.
	default:
		return Target{}, errors.AssertionFailedf("unknown type of system target %v", t)
	}
}

// MakeTargetFromSpan constructs and returns a span target.
func MakeTargetFromSpan(span roachpb.Span) Target {
	return Target{span: span}
}

// MakeTargetFromSystemTarget returns a Target which wraps a system target.
func MakeTargetFromSystemTarget(systemTarget SystemTarget) Target {
	return Target{systemTarget: systemTarget}
}

// IsSpanTarget returns true if the target is a span target.
func (t Target) IsSpanTarget() bool {
	return !t.span.Equal(roachpb.Span{})
}

// GetSpan returns the underlying roachpb.Span if the target is a span
// target; panics if that isn't he case.
func (t Target) GetSpan() roachpb.Span {
	if !t.IsSpanTarget() {
		panic("target is not a span target")
	}
	return t.span
}

// IsSystemTarget returns true if the underlying target is a system target.
func (t Target) IsSystemTarget() bool {
	return !t.systemTarget.isEmpty()
}

// GetSystemTarget returns the underlying SystemTarget; it panics if that is not
// the case.
func (t Target) GetSystemTarget() SystemTarget {
	if !t.IsSystemTarget() {
		panic("target is not a system target")
	}
	return t.systemTarget
}

// Encode returns an encoded span suitable for persistence in
// system.span_configurations.
func (t Target) Encode() roachpb.Span {
	switch {
	case t.IsSpanTarget():
		return t.span
	case t.IsSystemTarget():
		return t.systemTarget.encode()
	default:
		panic("cannot handle any other type of target")
	}
}

// Less returns true if the receiver is considered less than the supplied
// target.
func (t Target) Less(o Target) bool {
	// We consider system targets to be less than span targets.

	// If both targets are system targets delegate to the base type.
	if t.IsSystemTarget() && o.IsSystemTarget() {
		return t.GetSystemTarget().less(o.GetSystemTarget())
	}

	// Check if one of the targets is a system target and return accordingly.
	if t.IsSystemTarget() {
		return true
	} else if o.IsSystemTarget() {
		return false
	}

	// We're dealing with 2 span targets; compare their start keys.
	if !t.GetSpan().Key.Equal(o.GetSpan().Key) {
		return t.GetSpan().Key.Compare(o.GetSpan().Key) < 0
	}
	// If the start keys are equal, compare their end keys.
	return t.GetSpan().EndKey.Compare(o.GetSpan().EndKey) < 0
}

// Equal returns true iff the receiver is equal to the supplied target.
func (t Target) Equal(o Target) bool {
	if t.IsSpanTarget() && o.IsSpanTarget() {
		return t.GetSpan().Equal(o.GetSpan())
	}

	if t.IsSystemTarget() && o.IsSystemTarget() {
		return t.GetSystemTarget().equal(o.GetSystemTarget())
	}

	// We're dealing with one span target and one system target, so they're not
	// equal.
	return false
}

// String returns a formatted version of the traget suitable for printing.
func (t Target) String() string {
	if t.IsSpanTarget() {
		return t.GetSpan().String()
	}
	return t.GetSystemTarget().String()
}

// isEmpty returns true if the receiver is an empty target.
func (t Target) isEmpty() bool {
	return t.systemTarget.isEmpty() && t.span.Equal(roachpb.Span{})
}

// ToProto returns a roachpb.SpanConfigTarget equivalent to the receiver.
func (t Target) ToProto() roachpb.SpanConfigTarget {
	switch {
	case t.IsSpanTarget():
		sp := t.GetSpan()
		return roachpb.SpanConfigTarget{
			Union: &roachpb.SpanConfigTarget_Span{
				Span: &sp,
			},
		}
	case t.IsSystemTarget():
		return roachpb.SpanConfigTarget{
			Union: &roachpb.SpanConfigTarget_SystemSpanConfigTarget{
				SystemSpanConfigTarget: &roachpb.SystemSpanConfigTarget{
					SourceTenantID: t.GetSystemTarget().SourceTenantID,
					TargetTenantID: t.GetSystemTarget().TargetTenantID,
				},
			},
		}
	default:
		panic("cannot handle any other type of target")
	}
}

// DecodeTarget takes a raw span and decodes it into a Target given its
// encoding. It is the inverse of Encode.
func DecodeTarget(span roachpb.Span) Target {
	if spanStartKeyConformsToSystemTargetEncoding(span) {
		systemTarget, err := decodeSystemTarget(span)
		if err != nil {
			panic(err)
		}
		return Target{systemTarget: systemTarget}
	}
	return Target{span: span}
}

// Targets is  a slice of span config targets.
type Targets []Target

// Len implement sort.Interface.
func (t Targets) Len() int { return len(t) }

// Swap implements sort.Interface.
func (t Targets) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// Less implements Sort.Interface.
func (t Targets) Less(i, j int) bool {
	return t[i].Less(t[j])
}

// RecordsToEntries converts a list of records to a list roachpb.SpanConfigEntry
// protos suitable for sending over the wire.
func RecordsToEntries(records []Record) []roachpb.SpanConfigEntry {
	entries := make([]roachpb.SpanConfigEntry, 0, len(records))
	for _, rec := range records {
		entries = append(entries, roachpb.SpanConfigEntry{
			Target: rec.Target.ToProto(),
			Config: rec.Config,
		})
	}
	return entries
}

// EntriesToRecords converts a list of roachpb.SpanConfigEntries
// (received over the wire) to a list of Records.
func EntriesToRecords(entries []roachpb.SpanConfigEntry) ([]Record, error) {
	records := make([]Record, 0, len(entries))
	for _, entry := range entries {
		target, err := MakeTarget(entry.Target)
		if err != nil {
			return nil, err
		}
		records = append(records, Record{
			Target: target,
			Config: entry.Config,
		})
	}
	return records, nil
}

// TargetsToProtos converts a list of targets to a list of
// roachpb.SpanConfigTarget protos suitable for sending over the wire.
func TargetsToProtos(targets []Target) []roachpb.SpanConfigTarget {
	targetProtos := make([]roachpb.SpanConfigTarget, 0, len(targets))
	for _, target := range targets {
		targetProtos = append(targetProtos, target.ToProto())
	}
	return targetProtos
}

// TargetsFromProtos converts a list of roachpb.SpanConfigTargets
// (received over the wire) to a list of Targets.
func TargetsFromProtos(protoTargets []roachpb.SpanConfigTarget) ([]Target, error) {
	targets := make([]Target, 0, len(protoTargets))
	for _, t := range protoTargets {
		target, err := MakeTarget(t)
		if err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	return targets, nil
}
