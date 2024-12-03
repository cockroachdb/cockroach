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
	scanner := bufio.NewScanner(bytes.NewReader(src.Content))
	scanner.Buffer(make([]byte, 1024*1024), BufferSize)
	labelString := util.LabelMapToString(getLabelMap(src, labels, true, "", nil))
	runDate, _ := getTestDateAndName(src)
	timestamp, err := time.Parse("20060102", strings.Split(runDate, "-")[0])
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer([]byte{})
	for scanner.Scan() {
		line := scanner.Text()
		var metrics tests.TpceMetrics
		if err := json.Unmarshal([]byte(line), &metrics); err != nil {
			return err
		}
		openmetricBytes := tests.GetTpceOpenmetricsBytes(metrics, "0", labelString, timestamp.Unix())
		buf.Write(openmetricBytes)
	}
	return t.sink.Sink(buf, src.Path, AggregatedStatsFile)
}

func NewTpceConverter(sink sink.Sink) Converter {
	return &TpceConverter{
		sink: sink,
	}
}
