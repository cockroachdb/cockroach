// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package converter

import (
	"bufio"
	"bytes"
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/util"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/model"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/sink"
)

const BufferSize = 10 * 1024 * 1024 // buffer size to 10MB

type StatsExporterConverter struct {
	sink                   sink.Sink
	HigherBetterMetricsSet map[string]bool
	MetricLabels           map[string][]model.Label
}

func (s StatsExporterConverter) Convert(labels []model.Label, src model.FileInfo) (err error) {
	scanner := bufio.NewScanner(bytes.NewReader(src.Content))
	scanner.Buffer(make([]byte, 1024*1024), BufferSize)
	buf := bytes.NewBuffer(nil)
	metricsBuffer := bufio.NewWriter(buf)

	totalMetricsBuffer := bytes.NewBuffer(nil)
	totalMetricsWriter := bufio.NewWriter(totalMetricsBuffer)

	defer func() {
		err = metricsBuffer.Flush()
		if err != nil {
			return
		}
		err = s.sink.Sink(buf, src.Path, RawStatsFile)
	}()

	defer func() {
		err = totalMetricsWriter.Flush()
		if err != nil {
			return
		}
		err = s.sink.Sink(totalMetricsBuffer, src.Path, AggregatedStatsFile)
	}()

	for scanner.Scan() {
		line := scanner.Text()

		var clusterStatsRun clusterstats.ClusterStatRun
		if err = json.Unmarshal([]byte(line), &clusterStatsRun); err != nil {
			return err
		}

		var totalClusterRun clusterstats.ClusterStatRun

		totalClusterRun.Total = clusterStatsRun.Total
		clusterStatsRun.Total = nil
		labelString := util.LabelMapToString(getLabelMap(src, labels, false, "", nil))
		openMetricsBuffer, err := clusterstats.SerializeOpenmetricsReport(clusterStatsRun, &labelString)
		if err != nil {
			return err
		}

		if _, err = metricsBuffer.Write(openMetricsBuffer.Bytes()); err != nil {
			return err
		}

		for key, val := range totalClusterRun.Total {
			labelString = util.LabelMapToString(getLabelMap(src, labels, true, key, s.MetricLabels[key]))
			currentStatsMap := make(map[string]float64)
			currentStatsMap[key] = val
			currentTotalStatsRun := clusterstats.ClusterStatRun{Total: currentStatsMap}
			tempBuffer, err := clusterstats.SerializeOpenmetricsReport(currentTotalStatsRun, &labelString)
			lines := bytes.Split(tempBuffer.Bytes(), []byte("\n"))

			// Remove the last line if it's "# EOF"
			if len(lines) > 0 && bytes.Equal(lines[len(lines)-1], []byte("# EOF")) {
				lines = lines[:len(lines)-1]
			} else if len(lines) > 0 && bytes.Equal(lines[len(lines)-2], []byte("# EOF")) {
				lines = lines[:len(lines)-2]
			}

			// Rebuild the buffer
			newBuf := bytes.NewBuffer(nil)
			for _, line := range lines {
				newBuf.Write(line)
				//// Don't add newline after the last line (optional)
				//if i < len(lines)-1 {
				newBuf.WriteByte('\n')
				//}
			}
			openMetricsBuffer = newBuf
			if _, err = totalMetricsBuffer.Write(openMetricsBuffer.Bytes()); err != nil {
				return err
			}
		}
	}
	totalMetricsBuffer.WriteString("# EOF")
	return err
}

func NewStatsExporterConverter(sink sink.Sink, metricSpec []model.Metric) Converter {
	HigherBetterMetricsSet := map[string]bool{}
	metricLabels := make(map[string][]model.Label)
	for _, spec := range metricSpec {
		for _, val := range spec.HigherBetterMetrics {
			HigherBetterMetricsSet[val] = true
		}

		for _, val := range spec.MetricLabels {
			metricLabels[val.Name] = append(metricLabels[val.Name], val.Labels...)
		}
	}

	return &StatsExporterConverter{
		sink:                   sink,
		HigherBetterMetricsSet: HigherBetterMetricsSet,
		MetricLabels:           metricLabels,
	}
}
