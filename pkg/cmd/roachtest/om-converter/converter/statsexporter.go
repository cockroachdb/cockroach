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

type StatsExporterConverter struct {
	sink sink.Sink
}

func (s StatsExporterConverter) Convert(labels []model.Label, src model.FileInfo) (err error) {
	scanner := bufio.NewScanner(bytes.NewReader(src.Content))
	buf := bytes.NewBuffer(nil)
	metricsBuffer := bufio.NewWriter(buf)

	defer func() {
		err = metricsBuffer.Flush()
		if err != nil {
			return
		}
		err = s.sink.Sink(buf, src.Path, "stats.om")
	}()

	for scanner.Scan() {
		line := scanner.Text()

		var clusterStatsRun clusterstats.ClusterStatRun
		if err = json.Unmarshal([]byte(line), &clusterStatsRun); err != nil {
			return err
		}

		labelString := util.LabelMapToString(getLabelMap(src, labels))
		openMetricsBuffer, err := clusterstats.SerializeOpenmetricsReport(clusterStatsRun, &labelString)
		if err != nil {
			return err
		}

		if _, err = metricsBuffer.Write(openMetricsBuffer.Bytes()); err != nil {
			return err
		}
	}

	return err
}

func NewStatsExporterConverter(sink sink.Sink) Converter {
	return &StatsExporterConverter{
		sink: sink,
	}
}
