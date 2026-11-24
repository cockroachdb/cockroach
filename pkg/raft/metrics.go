// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raft

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// Metrics contains all the metrics reported in Raft.
type Metrics struct {
	AcceptedFortificationResponses         *metric.Counter
	RejectedFortificationResponses         *metric.Counter
	SkippedFortificationDueToLackOfSupport *metric.Counter

	FlowsEnteredStateProbe     *metric.Counter
	FlowsEnteredStateReplicate *metric.Counter
	FlowsEnteredStateSnapshot  *metric.Counter
}

var (
	acceptedFortificationResponsesMeta = metric.Metadata{
		Name:        "raft.fortification_resp.accepted",
		Help:        "The number of accepted fortification responses. Calculated on the raft leader",
		Measurement: "Accepted Fortification Responses",
		Unit:        metric.Unit_COUNT,
	}
	rejectedFortificationResponsesMeta = metric.Metadata{
		Name:        "raft.fortification_resp.rejected",
		Help:        "The number of rejected fortification responses. Calculated on the raft leader",
		Measurement: "Rejected Fortification Responses",
		Unit:        metric.Unit_COUNT,
	}
	skippedFortificationDueToLackOfSupportMeta = metric.Metadata{
		Name: "raft.fortification.skipped_no_support",
		Help: "The number of fortification requests that were skipped (not sent) due to lack of store" +
			" liveness support",
		Measurement: "Skipped Fortifications",
		Unit:        metric.Unit_COUNT,
	}

	metaRaftFlowsEnteredProbe = metric.Metadata{
		Name:        "raft.flows.entered.state_probe",
		Help:        "The number of leader->peer flows transitioned to StateProbe",
		Measurement: "Flows",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftFlowsEnteredReplicate = metric.Metadata{
		Name:        "raft.flows.entered.state_replicate",
		Help:        "The number of leader->peer flows transitioned to StateReplicate",
		Measurement: "Flows",
		Unit:        metric.Unit_COUNT,
	}
	metaRaftFlowsEnteredSnapshot = metric.Metadata{
		Name:        "raft.flows.entered.state_snapshot",
		Help:        "The number of of leader->peer flows transitioned to StateSnapshot",
		Measurement: "Flows",
		Unit:        metric.Unit_COUNT,
	}
)

// NewMetrics creates a new Metrics instance with all related metric fields.
func NewMetrics() *Metrics {
	return &Metrics{
		AcceptedFortificationResponses: metric.NewCounter(acceptedFortificationResponsesMeta),
		RejectedFortificationResponses: metric.NewCounter(rejectedFortificationResponsesMeta),
		SkippedFortificationDueToLackOfSupport: metric.NewCounter(
			skippedFortificationDueToLackOfSupportMeta),

		FlowsEnteredStateProbe:     metric.NewCounter(metaRaftFlowsEnteredProbe),
		FlowsEnteredStateReplicate: metric.NewCounter(metaRaftFlowsEnteredReplicate),
		FlowsEnteredStateSnapshot:  metric.NewCounter(metaRaftFlowsEnteredSnapshot),
	}
}
