// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvflowdispatch

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

var (
	pendingDispatches = metric.Metadata{
		Name:        "kvadmission.flow_token_dispatch.pending_%s",
		Help:        "Number of pending %s flow token dispatches",
		Measurement: "Dispatches",
		Unit:        metric.Unit_COUNT,
	}

	pendingNodes = metric.Metadata{
		Name:        "kvadmission.flow_token_dispatch.pending_nodes",
		Help:        "Number of nodes pending flow token dispatches",
		Measurement: "Nodes",
		Unit:        metric.Unit_COUNT,
	}

	localDispatches = metric.Metadata{
		Name:        "kvadmission.flow_token_dispatch.local_%s",
		Help:        "Number of local %s flow token dispatches",
		Measurement: "Dispatches",
		Unit:        metric.Unit_COUNT,
	}

	remoteDispatches = metric.Metadata{
		Name:        "kvadmission.flow_token_dispatch.remote_%s",
		Help:        "Number of remote %s flow token dispatches",
		Measurement: "Dispatches",
		Unit:        metric.Unit_COUNT,
	}

	coalescedDispatches = metric.Metadata{
		Name:        "kvadmission.flow_token_dispatch.coalesced_%s",
		Help:        "Number of coalesced %s flow token dispatches (where we're informing the sender of a higher log entry being admitted)",
		Measurement: "Dispatches",
		Unit:        metric.Unit_COUNT,
	}
)

type metrics struct {
	PendingDispatches   [admissionpb.NumWorkClasses]*metric.Gauge
	CoalescedDispatches [admissionpb.NumWorkClasses]*metric.Counter
	LocalDispatch       [admissionpb.NumWorkClasses]*metric.Counter
	RemoteDispatch      [admissionpb.NumWorkClasses]*metric.Counter
	PendingNodes        *metric.Gauge
}

var _ metric.Struct = &metrics{}

func newMetrics() *metrics {
	var m metrics
	for _, wc := range []admissionpb.WorkClass{
		admissionpb.RegularWorkClass,
		admissionpb.ElasticWorkClass,
	} {
		m.PendingDispatches[wc] = metric.NewGauge(
			annotateMetricTemplateWithWorkClass(wc, pendingDispatches),
		)
		m.LocalDispatch[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClass(wc, localDispatches),
		)
		m.RemoteDispatch[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClass(wc, remoteDispatches),
		)
		m.CoalescedDispatches[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClass(wc, coalescedDispatches),
		)
	}
	m.PendingNodes = metric.NewGauge(pendingNodes)
	return &m
}

func (m *metrics) MetricStruct() {}

// annotateMetricTemplateWithWorkClass uses the given metric template to build
// one suitable for the specific work class.
func annotateMetricTemplateWithWorkClass(
	wc admissionpb.WorkClass, tmpl metric.Metadata,
) metric.Metadata {
	rv := tmpl
	rv.Name = fmt.Sprintf(tmpl.Name, wc)
	rv.Help = fmt.Sprintf(tmpl.Help, wc)
	return rv
}
