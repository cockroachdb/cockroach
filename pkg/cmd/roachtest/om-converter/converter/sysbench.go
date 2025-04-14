// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package converter

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/util"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/model"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/sink"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
)

type SysbenchConverter struct {
	sink sink.Sink
}

type sysbenchMetrics struct {
	Time         int64  `json:"time"`
	Threads      string `json:"threads"`
	Transactions string `json:"transactions"`
	Qps          string `json:"qps"`
	ReadQps      string `json:"readQps"`
	WriteQps     string `json:"writeQps"`
	OtherQps     string `json:"otherQps"`
	P95Latency   string `json:"p95Latency"`
	Errors       string `json:"errors"`
	Reconnects   string `json:"reconnects"`
}

func (s *SysbenchConverter) Convert(labels []model.Label, src model.FileInfo) (err error) {
	// Get a buffer from the pool for the scanner
	scannerBuf := getBuffer()
	defer putBuffer(scannerBuf)

	// Create scanner with pooled buffer
	scanner := bufio.NewScanner(bytes.NewReader(src.Content))
	scanner.Buffer(scannerBuf.Bytes()[:scannerBufferSize], statsMaxBuffer)

	// Get buffers from pool for metrics
	rawMetricsBuf := getBuffer()
	defer putBuffer(rawMetricsBuf)

	aggregatedMetricsBuf := getBuffer()
	defer putBuffer(aggregatedMetricsBuf)

	openmetricsMap := make(map[string][]openmetricsValues)
	labelString := util.LabelMapToString(getLabelMap(src, labels, true, "", nil))

	// Process the file
	if err = s.processFile(scanner, openmetricsMap, rawMetricsBuf, labelString); err != nil {
		return err
	}

	// Write aggregated metrics
	if err = s.writeAggregatedMetrics(openmetricsMap, aggregatedMetricsBuf, labelString); err != nil {
		return err
	}

	// Sink the buffers
	if err = s.sink.Sink(rawMetricsBuf, src.Path, RawStatsFile); err != nil {
		return err
	}
	return s.sink.Sink(aggregatedMetricsBuf, src.Path, AggregatedStatsFile)
}

func (s *SysbenchConverter) processFile(
	scanner *bufio.Scanner,
	openmetricsMap map[string][]openmetricsValues,
	metricsBuf *bytes.Buffer,
	labelString string,
) error {
	for scanner.Scan() {
		line := scanner.Text()

		var metrics sysbenchMetrics
		if err := json.Unmarshal([]byte(line), &metrics); err != nil {
			return err
		}

		addCurrentSnapshotToOpenmetrics(metrics, openmetricsMap)
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	getOpenmetricsBytes(openmetricsMap, labelString, metricsBuf)
	return nil
}

func (s *SysbenchConverter) writeAggregatedMetrics(
	openmetricsMap map[string][]openmetricsValues,
	buf *bytes.Buffer,
	labelString string,
) error {
	writeQpsSum := 0.0
	readQpsSum := 0.0
	otherQpsSum := 0.0
	totalQpsSum := 0.0
	count := 0.0
	var timestamp int64

	for _, values := range openmetricsMap["read_qps"] {
		val, _ := strconv.ParseFloat(values.Value, 32)
		readQpsSum += val
		count++
		timestamp = values.Time
	}

	for _, values := range openmetricsMap["write_qps"] {
		val, _ := strconv.ParseFloat(values.Value, 32)
		writeQpsSum += val
	}

	for _, values := range openmetricsMap["other_qps"] {
		val, _ := strconv.ParseFloat(values.Value, 32)
		otherQpsSum += val
	}

	for _, values := range openmetricsMap["qps"] {
		val, _ := strconv.ParseFloat(values.Value, 32)
		totalQpsSum += val
	}

	if count == 0 {
		return fmt.Errorf("no metrics found to aggregate")
	}

	labelString = labelString + `,unit="ops/s",is_higher_better="true"`

	// Write aggregated metrics
	if _, err := buf.WriteString(roachtestutil.GetOpenmetricsGaugeType("sysbench_read_qps")); err != nil {
		return err
	}
	if _, err := buf.WriteString(fmt.Sprintf("sysbench_read_qps{%s} %f %d\n", labelString, readQpsSum/count, timestamp)); err != nil {
		return err
	}
	if _, err := buf.WriteString(roachtestutil.GetOpenmetricsGaugeType("sysbench_write_qps")); err != nil {
		return err
	}
	if _, err := buf.WriteString(fmt.Sprintf("sysbench_write_qps{%s} %f %d\n", labelString, writeQpsSum/count, timestamp)); err != nil {
		return err
	}
	if _, err := buf.WriteString(roachtestutil.GetOpenmetricsGaugeType("sysbench_other_qps")); err != nil {
		return err
	}
	if _, err := buf.WriteString(fmt.Sprintf("sysbench_other_qps{%s} %f %d\n", labelString, otherQpsSum/count, timestamp)); err != nil {
		return err
	}
	if _, err := buf.WriteString(roachtestutil.GetOpenmetricsGaugeType("sysbench_total_qps")); err != nil {
		return err
	}
	if _, err := buf.WriteString(fmt.Sprintf("sysbench_total_qps{%s} %f %d\n", labelString, totalQpsSum/count, timestamp)); err != nil {
		return err
	}
	if _, err := buf.WriteString("# EOF"); err != nil {
		return err
	}

	return nil
}

type openmetricsValues struct {
	Value string
	Time  int64
}

func addCurrentSnapshotToOpenmetrics(
	metrics sysbenchMetrics, openmetricsMap map[string][]openmetricsValues,
) {
	time := metrics.Time
	openmetricsMap["threads"] = append(openmetricsMap["threads"], openmetricsValues{Value: metrics.Threads, Time: time})
	openmetricsMap["transactions"] = append(openmetricsMap["transactions"], openmetricsValues{Value: metrics.Transactions, Time: time})
	openmetricsMap["qps"] = append(openmetricsMap["qps"], openmetricsValues{Value: metrics.Qps, Time: time})
	openmetricsMap["read_qps"] = append(openmetricsMap["read_qps"], openmetricsValues{Value: metrics.ReadQps, Time: time})
	openmetricsMap["write_qps"] = append(openmetricsMap["write_qps"], openmetricsValues{Value: metrics.WriteQps, Time: time})
	openmetricsMap["other_qps"] = append(openmetricsMap["other_qps"], openmetricsValues{Value: metrics.OtherQps, Time: time})
	openmetricsMap["p95_latency"] = append(openmetricsMap["p95_latency"], openmetricsValues{Value: metrics.P95Latency, Time: time})
	openmetricsMap["errors"] = append(openmetricsMap["errors"], openmetricsValues{Value: metrics.Errors, Time: time})
	openmetricsMap["reconnects"] = append(openmetricsMap["reconnects"], openmetricsValues{Value: metrics.Reconnects, Time: time})
}

// Convert openmetricsMap to bytes for writing to file
func getOpenmetricsBytes(
	openmetricsMap map[string][]openmetricsValues, labelString string, metricsBuf *bytes.Buffer,
) {
	for key, values := range openmetricsMap {
		metricName := util.SanitizeMetricName(key)
		metricsBuf.WriteString(roachtestutil.GetOpenmetricsGaugeType(metricName))
		for _, value := range values {
			metricsBuf.WriteString(fmt.Sprintf("%s{%s} %s %d\n", metricName, labelString, value.Value, value.Time))
		}
	}

	// Add # EOF at the end for openmetrics
	metricsBuf.WriteString("# EOF\n")
}

func NewSysbenchConverter(sink sink.Sink) Converter {
	return &SysbenchConverter{
		sink: sink,
	}
}
