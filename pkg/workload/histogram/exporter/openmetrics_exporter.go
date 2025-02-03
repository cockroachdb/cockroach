// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package exporter

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/util"
	"github.com/codahale/hdrhistogram"
	"github.com/gogo/protobuf/proto"
	prom "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// OpenMetricsExporter exports metrics in openmetrics format
type OpenMetricsExporter struct {
	writer *bufio.Writer
	labels []*prom.LabelPair

	// histHighestTrackableValues stores histogram.HighestTrackableValue which is defined as the last bucket value of
	// the histogram. This is used for emitting the limit of the histogram that is being emitted.
	histHighestTrackableValues map[string]*histHighestTrackableValue
}

type histHighestTrackableValue struct {
	value int64
	now   time.Time
}

func (o *OpenMetricsExporter) Validate(filePath string) error {
	if strings.HasSuffix(filePath, ".json") {
		return fmt.Errorf("file path must not end with .json")
	}
	return nil
}

func (o *OpenMetricsExporter) Init(w *io.Writer) {
	o.writer = bufio.NewWriter(*w)
	o.histHighestTrackableValues = make(map[string]*histHighestTrackableValue)
}

func (o *OpenMetricsExporter) SnapshotAndWrite(
	hist *hdrhistogram.Histogram, now time.Time, elapsed time.Duration, name *string,
) error {

	// expfmt.MetricFamilyToOpenMetrics expects prometheus.MetricFamily
	// so converting HdrHistogram to MetricFamily here
	if err := o.emitMetricFamily(ConvertHdrHistogramToPrometheusMetricFamily(hist, name, now, o.labels)); err != nil {
		return err
	}

	// emit elapsed metric for this run
	if err := o.emitGaugeMetric(*name+"_elapsed", float64(elapsed.Milliseconds()), now); err != nil {
		return err
	}

	// emit max metric for this run
	if err := o.emitGaugeMetric(*name+"_max", float64(hist.Max()), now); err != nil {
		return err
	}

	// emit mean metric for this run
	if err := o.emitGaugeMetric(*name+"_mean", hist.Mean(), now); err != nil {
		return err
	}

	if o.histHighestTrackableValues[*name] == nil {
		o.histHighestTrackableValues[*name] = &histHighestTrackableValue{
			value: hist.HighestTrackableValue(),
			now:   now,
		}
	}

	return nil
}

func (o *OpenMetricsExporter) Close(f func() error) error {

	// emit the highest trackable value of each histogram
	if err := o.emitHistHighestTrackableValue(); err != nil {
		return err
	}

	// Adds the `#EOF` in the openmetrics file
	if _, err := expfmt.FinalizeOpenMetrics(o.writer); err != nil {
		return err
	}

	if err := o.writer.Flush(); err != nil {
		return err
	}

	if f != nil {
		return f()
	}
	return nil
}

func (o *OpenMetricsExporter) emitGaugeMetric(
	name string, value float64, timestamp time.Time,
) error {
	name = util.SanitizeMetricName(name)
	gaugeMetric := prom.MetricFamily{
		Name: &name,
		Help: nil,
		Type: prom.MetricType_GAUGE.Enum(),
		Metric: []*prom.Metric{
			{
				Gauge: &prom.Gauge{
					Value: &value,
				},
				TimestampMs: proto.Int64(timestamp.UTC().UnixMilli()),
				Label:       o.labels,
			},
		},
	}

	return o.emitMetricFamily(&gaugeMetric)
}

func (o *OpenMetricsExporter) emitHistHighestTrackableValue() error {
	for name, highestTrackable := range o.histHighestTrackableValues {
		if err := o.emitGaugeMetric(name+"_highest_trackable_value", float64(highestTrackable.value), highestTrackable.now); err != nil {
			return err
		}
	}
	return nil
}

func (o *OpenMetricsExporter) emitMetricFamily(metricFamily *prom.MetricFamily) error {
	if _, err := expfmt.MetricFamilyToOpenMetrics(
		o.writer,
		metricFamily,
	); err != nil {
		return err
	}
	return nil
}

func (o *OpenMetricsExporter) Labels() []*prom.LabelPair {
	return o.labels
}

func (o *OpenMetricsExporter) SetLabels(labels *map[string]string) {
	var labelValues []*prom.LabelPair

	for label, value := range *labels {
		labelName := util.SanitizeKey(label)

		// In case the label value already has surrounding quotes, we should trim them
		labelValue := util.SanitizeValue(strings.Trim(value, `"`))
		labelPair := &prom.LabelPair{
			Name:  &labelName,
			Value: &labelValue,
		}
		labelValues = append(labelValues, labelPair)
	}

	o.labels = labelValues
}
