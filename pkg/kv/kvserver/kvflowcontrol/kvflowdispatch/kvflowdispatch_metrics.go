// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowdispatch

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

var (
	pendingDispatches = metric.Metadata{
		Name:        "kvadmission.dispatch.pending_%s",
		Help:        "Number of pending %s dispatches",
		Measurement: "Dispatches",
		Unit:        metric.Unit_COUNT,
	}

	pendingNodes = metric.Metadata{
		Name:        "kvadmission.dispatch.pending_nodes",
		Help:        "Number of nodes pending dispatches",
		Measurement: "Nodes",
		Unit:        metric.Unit_COUNT,
	}

	coalescedDispatches = metric.Metadata{
		Name:        "kvadmission.dispatch.coalesced_%s",
		Help:        "Number of coalesced %s dispatches (where we're informing the sender of a higher log entry being admitted)",
		Measurement: "Dispatches",
		Unit:        metric.Unit_COUNT,
	}
)

type metrics struct {
	PendingDispatches   [admissionpb.NumWorkClasses]*metric.Gauge
	CoalescedDispatches [admissionpb.NumWorkClasses]*metric.Counter
	PendingNodes        *metric.Gauge
}

var _ metric.Struct = &metrics{}

func newMetrics() *metrics {
	var m metrics
	for _, wc := range []admissionpb.WorkClass{
		admissionpb.RegularWorkClass,
		admissionpb.ElasticWorkClass,
	} {
		wc := wc // copy loop variable
		m.PendingDispatches[wc] = metric.NewGauge(
			annotateMetricTemplateWithWorkClass(wc, pendingDispatches),
		)
		m.CoalescedDispatches[wc] = metric.NewCounter(
			annotateMetricTemplateWithWorkClass(wc, pendingDispatches),
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
