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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
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
	GetIsLiveMap() livenesspb.IsLiveMap
}

// Reporter is used to figure out whether ranges backing specific spans conform
// to the span configs that apply over them. It's a concrete implementation of
// the spanconfig.Reporter interface.
type Reporter struct {
	dep struct {
		// NB: The data dependencies in the implementation are:
		//
		// i.   point-in-time view over gossip-backed node liveness;
		// ii.  point-in-time view of range descriptors (done transactionally);
		// iii. the store resolver resolving store IDs to store descriptors;
		// iv.  view over what span configs apply to what keyspans;
		//
		// TODO(irfansharif): For (iii) and (iv) we might not have a
		// point-in-time snapshot of the data.
		// - For (iii) it's possible that as we iterate through the set of range
		//   descriptors, a few of which refer to some store S, we're racing
		//   against that newly-added store's info not yet being available
		//   through gossip. This is exceedingly unlikely, but if we see it
		//   happen, we can expose some snapshot of the StoreResolver state like
		//   we have for liveness.
		// - For (iv) too we're not grabbing a read lock over the backing
		//   spanconfig.KVSubscriber while reading off each span config, so it's
		//   possible we generate the report for two range descriptors with span
		//   configs from different points in time. If this too becomes a
		//   problem, we can explicitly generate a snapshot like we do for
		//   liveness.
		Liveness
		rangedesc.Scanner
		constraint.StoreResolver
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
	scanner rangedesc.Scanner,
	settings *cluster.Settings,
	knobs *spanconfig.TestingKnobs,
) *Reporter {
	r := &Reporter{
		settings: settings,
		knobs:    knobs,
	}
	r.dep.Liveness = liveness
	r.dep.StoreResolver = resolver
	r.dep.Scanner = scanner
	r.dep.StoreReader = reader
	return r
}

// TODO(irfansharif): Support the equivalent of "critical localities", perhaps
// through a different API than the one below since it's not quite
// span-oriented.
//
// TODO(irfansharif): Once wired up the SQL code or exposed through an endpoint,
// write an end-to-end test using actual SQL and zone configs. Set configs on a
// table, disable replication, see conformance report. Enable repl, change
// configs, repeat. Do this for tenants as well.

// SpanConfigConformance implements the spanconfig.Reporter interface.
func (r *Reporter) SpanConfigConformance(
	ctx context.Context, spans []roachpb.Span,
) (roachpb.SpanConfigConformanceReport, error) {
	report := roachpb.SpanConfigConformanceReport{}
	unavailableNodes := make(map[roachpb.NodeID]struct{})

	isLiveMap := r.dep.Liveness.GetIsLiveMap()
	for _, span := range spans {
		if err := r.dep.Scan(ctx, int(rangeDescPageSize.Get(&r.settings.SV)),
			func() { report = roachpb.SpanConfigConformanceReport{} /* init */ },
			span,
			func(descriptors ...roachpb.RangeDescriptor) error {
				for _, desc := range descriptors {
					conf, err := r.dep.StoreReader.GetSpanConfigForKey(ctx, desc.StartKey)
					if err != nil {
						return err
					}

					status := desc.Replicas().ReplicationStatus(
						func(rDesc roachpb.ReplicaDescriptor) bool {
							isLive := isLiveMap[rDesc.NodeID].IsLive
							if !isLive {
								unavailableNodes[rDesc.NodeID] = struct{}{}
							}
							return isLive
						}, int(conf.GetNumVoters()), int(conf.GetNumNonVoters()))
					if !status.Available {
						report.Unavailable = append(report.Unavailable,
							roachpb.ConformanceReportedRange{
								RangeDescriptor: desc,
								Config:          conf,
							})
					}
					if status.UnderReplicated || status.UnderReplicatedNonVoters {
						report.UnderReplicated = append(report.UnderReplicated,
							roachpb.ConformanceReportedRange{
								RangeDescriptor: desc,
								Config:          conf,
							})
					}
					if status.OverReplicated || status.OverReplicatedNonVoters {
						report.OverReplicated = append(report.OverReplicated,
							roachpb.ConformanceReportedRange{
								RangeDescriptor: desc,
								Config:          conf,
							})
					}

					// Compute constraint violations for the overall (affecting voters
					// and non-voters alike) and voter constraints.
					overall := constraint.AnalyzeConstraints(
						r.dep.StoreResolver,
						desc.Replicas().Descriptors(),
						conf.NumReplicas, conf.Constraints)
					for i, c := range overall.Constraints {
						if c.NumReplicas == 0 {
							// NB: This is a weird artifact of
							// constraint.NumReplicas, which if set to zero is
							// used to imply that the constraint will applies to
							// all replicas. Setting it explicitly makes the
							// code below less fragile.
							c.NumReplicas = conf.NumReplicas
						}
						if len(overall.SatisfiedBy[i]) < int(c.NumReplicas) {
							report.ViolatingConstraints = append(report.ViolatingConstraints,
								roachpb.ConformanceReportedRange{
									RangeDescriptor: desc,
									Config:          conf,
								})
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
							report.ViolatingConstraints = append(report.ViolatingConstraints,
								roachpb.ConformanceReportedRange{
									RangeDescriptor: desc,
									Config:          conf,
								})
							break
						}
					}
				}
				return nil
			}); err != nil {
			return roachpb.SpanConfigConformanceReport{}, err
		}
	}

	for nid := range unavailableNodes {
		report.UnavailableNodeIDs = append(report.UnavailableNodeIDs, int32(nid))
	}
	return report, nil
}
