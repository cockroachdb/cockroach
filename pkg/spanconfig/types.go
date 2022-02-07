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

// Record	ties a target to its corresponding config.
type Record struct {
	Target Target

	Config roachpb.SpanConfig
}

type Target interface {
	Encode() roachpb.Span

	IsSpanTarget() bool
	IsSystemTarget() bool

	Less(o Target) bool
	Equal(o Target) bool

	TargetProto() *roachpb.SpanConfigTarget
}

// Targets is  a slice of span config targets.
type Targets []Target

// Len implement sort.Interface.
func (t Targets) Len() int { return len(t) }

// Swap implements sort.Interface.
func (t Targets) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

// Less implements Sort.Interface.
// Sort order:
// - Spans are sorted first ordered by start key.
// - SystemSpanConfig targets ordered by
//    - host tenant applied targets sort first; the targeted tenant's ID is used
//		to break ties.
//    - secondary tenants applied targets come next, sorted by tenantID.
func (t Targets) Less(i, j int) bool {
	return t[i].Less(t[j])
}

// Update captures a span and the corresponding config change. It's the unit of
// what can be applied to a StoreWriter. The embedded span captures what's being
// updated; the config captures what it's being updated to. An empty config
// indicates a deletion.
type Update Record

// Deletion constructs an update that represents a deletion over the given span.
func Deletion(target Target) Update {
	return Update{
		Target: target,
		Config: roachpb.SpanConfig{}, // delete
	}
}

// Addition constructs an update that represents adding the given config over
// the given span.
func Addition(target Target, conf roachpb.SpanConfig) Update {
	return Update{
		Target: target,
		Config: conf,
	}
}

// Deletion returns true if the update corresponds to a span config being
// deleted.
func (u Update) Deletion() bool {
	return u.Config.IsEmpty()
}

// Addition returns true if the update corresponds to a span config being added.
func (u Update) Addition() bool {
	return !u.Deletion()
}

func (u Update) IsSystemSpanConfigUpdate() bool {
	return u.Target.IsSystemTarget()
}

func (u Update) IsSpanConfigUpdate() bool {
	return !u.IsSystemSpanConfigUpdate()
}
