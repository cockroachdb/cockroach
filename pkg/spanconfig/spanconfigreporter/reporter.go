// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package spanconfigreporter reports on whether ranges over the queried spans
// conform to the span configs that apply to them.
package spanconfigreporter

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/constraint"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesciter"
)

// rangeDescPageSize controls the page size when iterating through range
// descriptors. It's settable only by the system tenant.
var rangeDescPageSize = settings.RegisterIntSetting(
	settings.SystemOnly,
	"spanconfig.reporter.range_desc_page_size",
	"pa",
	100,
	func(i int64) error {
		if i < 5 || i > 25000 {
			return fmt.Errorf("expected range_desc_page_size to be in range [5, 25000], got %d", i)
		}
		return nil
	},
)

// Liveness is the subset of the interface satisfied by CRDB's node liveness
// component that the reporter relies on.
type Liveness interface {
	IsLive(roachpb.NodeID) (bool, error)
}

// Reporter is used to figure out whether ranges backing specific spans conform
// to the span configs that apply over them. It's a concrete implementation of
// the spanconfig.Reporter interface.
type Reporter struct {
	dep struct {
		Liveness
		constraint.StoreResolver
		rangedesciter.Iterator
		spanconfig.StoreReader
	}

	settings *cluster.Settings
	knobs    *spanconfig.TestingKnobs
}

var _ spanconfig.Reporter = &Reporter{}

// New constructs and returns a Reporter.
func New(
	liveness Liveness,
	resolver constraint.StoreResolver,
	reader spanconfig.StoreReader,
	iterator rangedesciter.Iterator,
	settings *cluster.Settings,
	knobs *spanconfig.TestingKnobs,
) *Reporter {
	r := &Reporter{
		settings: settings,
		knobs:    knobs,
	}
	r.dep.Liveness = liveness
	r.dep.StoreResolver = resolver
	r.dep.Iterator = iterator
	r.dep.StoreReader = reader
	return r
}

// TODO(irfansharif): Support the equivalent of "critical localities", perhaps
// through a different API than the one below since it's not quite
// span-oriented.

// SpanConfigConformance implements the spanconfig.Reporter interface.
func (r *Reporter) SpanConfigConformance(
	ctx context.Context, spans []roachpb.Span,
) (roachpb.SpanConfigConformanceReport, error) {
	// XXX: Actually use the spans parameter. Update the rangedesc.Iterator
	// interfaces to take in a keyspan and bound meta{1,2} search just to
	// segments that would possibly overlap with that keyspan. Until this
	// keyspan scoping is done, we can't let this be used in tenants.
	_ = spans

	// XXX: Write an end-to-end test using actual SQL and zone configs. Set configs
	// on a table, disable replication, see conformance. Enable repl, change
	// configs, etc. Use tenants as well for this mode. Do this for tenants as well.
	// Do this after some form of this API is exposed through SQL/an endpoint.

	// XXX: Can we improve the SpanConfigConformanceReport proto type? Perhaps
	// include some {meta,}data about the span config being violated as well? Or
	// include the span config directly and provide helper libraries to compute
	// human-readable "why is this in violation" text.
	// - Only include range ID + replica descriptors + keys?
	// - Type to represent exactly which constraint exactly is being violated?
	// - Segment over/under replicated by what replica type (voter/non-voter)
	//   exactly is over/under replicated?

	report := roachpb.SpanConfigConformanceReport{}
	if err := r.dep.Iterate(ctx, int(rangeDescPageSize.Get(&r.settings.SV)), func() {
		report = roachpb.SpanConfigConformanceReport{} // init
	}, func(descriptors ...roachpb.RangeDescriptor) error {
		for _, desc := range descriptors {
			conf, err := r.dep.StoreReader.GetSpanConfigForKey(ctx, desc.StartKey)
			if err != nil {
				return err
			}

			status := desc.Replicas().ReplicationStatus(
				func(rDesc roachpb.ReplicaDescriptor) bool {
					isLive, err := r.dep.Liveness.IsLive(rDesc.NodeID)
					if err != nil {
						// As of 2022-10, this error only appears if we're
						// asking for the liveness of a node ID that doesn't
						// exist, which should never happen. Shout loudly
						// and declare things as non-live.
						log.Errorf(ctx, "programming error: unexpected err: %v", err)
						return false
					}
					return isLive
				}, int(conf.GetNumVoters()), int(conf.GetNumNonVoters()))
			if !status.Available {
				report.Unavailable = append(report.Unavailable, desc)
			}
			if status.UnderReplicated || status.UnderReplicatedNonVoters {
				report.UnderReplicated = append(report.UnderReplicated, desc)
			}
			if status.OverReplicated || status.OverReplicatedNonVoters {
				report.OverReplicated = append(report.OverReplicated, desc)
			}

			// Compute constraint violations for the overall (affecting voters
			// and non-voters alike) and voter constraints.
			overall := constraint.AnalyzeConstraints(
				r.dep.StoreResolver,
				desc.Replicas().Descriptors(),
				conf.NumReplicas, conf.Constraints)
			for i, c := range overall.Constraints {
				if c.NumReplicas == 0 {
					c.NumReplicas = conf.NumReplicas
				}
				if len(overall.SatisfiedBy[i]) < int(c.NumReplicas) {
					report.ViolatingConstraints = append(report.ViolatingConstraints, desc)
					break
				}
			}
			voters := constraint.AnalyzeConstraints(
				r.dep.StoreResolver,
				desc.Replicas().Voters().Descriptors(),
				conf.GetNumVoters(), conf.VoterConstraints)
			for i, c := range voters.Constraints {
				if c.NumReplicas == 0 {
					c.NumReplicas = conf.GetNumVoters()
				}
				if len(voters.SatisfiedBy[i]) < int(c.NumReplicas) {
					report.ViolatingConstraints = append(report.ViolatingConstraints, desc)
					break
				}
			}
		}
		return nil
	}); err != nil {
		return roachpb.SpanConfigConformanceReport{}, err
	}
	return report, nil
}
