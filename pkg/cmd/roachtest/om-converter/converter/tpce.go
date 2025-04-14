// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package converter

import (
	"bufio"
	"bytes"
	"encoding/json"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/util"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/model"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/om-converter/sink"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/tests"
)

type TpceConverter struct {
	sink sink.Sink
}

func (t *TpceConverter) Convert(labels []model.Label, src model.FileInfo) (err error) {
	// Get a buffer from the pool for the scanner
	scannerBuf := getBuffer()
	defer putBuffer(scannerBuf)

	// Create scanner with pooled buffer
	scanner := bufio.NewScanner(bytes.NewReader(src.Content))
	scanner.Buffer(scannerBuf.Bytes()[:scannerBufferSize], statsMaxBuffer)

	labelString := util.LabelMapToString(getLabelMap(src, labels, true, "", nil))
	runDate, _ := getTestDateAndName(src)
	timestamp, err := time.Parse("20060102", strings.Split(runDate, "-")[0])
	if err != nil {
		return err
	}

	// Get a buffer from the pool for metrics
	metricsBuf := getBuffer()
	defer putBuffer(metricsBuf)

	for scanner.Scan() {
		line := scanner.Text()
		var metrics tests.TpceMetrics
		if err := json.Unmarshal([]byte(line), &metrics); err != nil {
			return err
		}
		openmetricBytes := tests.GetTpceOpenmetricsBytes(metrics, "0", labelString, timestamp.Unix())
		if _, err = metricsBuf.Write(openmetricBytes); err != nil {
			return err
		}
	}

	if err = scanner.Err(); err != nil {
		return err
	}

	return t.sink.Sink(metricsBuf, src.Path, AggregatedStatsFile)
}

func NewTpceConverter(sink sink.Sink) Converter {
	return &TpceConverter{
		sink: sink,
	}
}
