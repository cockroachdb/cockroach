// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamproducer

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

var (
	metaOutOfBoundSSTs = metric.Metadata{
		Name:        "streaming.sst_out_of_bound",
		Help:        "SSTs exceeding the subscribed span",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}

	metaOutOfBoundSSTSize = metric.Metadata{
		Name:        "streaming.sst_out_of_bound_size",
		Help:        "Size of SST exceeding the subscribed span",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}

	metaRangefeedInitialScanError = metric.Metadata{
		Name:        "streaming.rangefeed_initial_scan_errors",
		Help:        "Errors encountered in rangefeed initial scan",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}

	metaStreamTerminationError = metric.Metadata{
		Name:        "streaming.stream_termination_errors",
		Help:        "Errors that terminated the event stream",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}

	metaProducerJobError = metric.Metadata{
		Name:        "streaming.producer_job_errors",
		Help:        "Errors encountered in executing the producer job",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}

	metaProducerJobTimeouts = metric.Metadata{
		Name:        "streaming.producer_job_timeouts",
		Help:        "Producer job timeouts",
		Measurement: "Timeouts",
		Unit:        metric.Unit_COUNT,
	}
)

type Metrics struct {
	OutOfBoundSSTs            *metric.Counter
	OutOfBoundSSTSize         *metric.Gauge
	RangefeedInitialScanError *metric.Counter
	StreamTerminationError    *metric.Counter
	ProducerJobError          *metric.Counter
	ProducerJobTimeouts       *metric.Counter
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

func MakeMetrics(histogramWindow time.Duration) metric.Struct {
	return &Metrics{
		OutOfBoundSSTs:            metric.NewCounter(metaOutOfBoundSSTs),
		OutOfBoundSSTSize:         metric.NewGauge(metaOutOfBoundSSTSize),
		RangefeedInitialScanError: metric.NewCounter(metaRangefeedInitialScanError),
		StreamTerminationError:    metric.NewCounter(metaStreamTerminationError),
		ProducerJobError:          metric.NewCounter(metaProducerJobError),
		ProducerJobTimeouts:       metric.NewCounter(metaProducerJobTimeouts),
	}
}

func init() {
	jobs.MakeStreamProduceMetricsHook = MakeMetrics
}
