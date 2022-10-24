// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package insights

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/obs"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	common "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/common/v1"
	logs "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/logs/v1"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type sink interface {
	AddInsight(*Insight)
}

type compositeSink struct {
	sinks []sink
}

func (c *compositeSink) AddInsight(insight *Insight) {
	for _, s := range c.sinks {
		s.AddInsight(insight)
	}
}

var _ sink = &compositeSink{}

type obsSink struct {
	exporter obs.EventsExporter
}

func (s *obsSink) AddInsight(insight *Insight) {
	// FIXME(todd): Handle the error.
	bytes, _ := insight.Marshal()

	log.Shoutf(context.Background(), logpb.Severity_INFO, "Sending event!")
	// Next steps:
	// - Write an integration test around seeing events over there.
	s.exporter.SendEvent(context.Background(), obspb.ExecutionInsightEvent, logs.LogRecord{
		TimeUnixNano: uint64(timeutil.Now().UnixNano()),
		Body:         &common.AnyValue{Value: &common.AnyValue_BytesValue{BytesValue: bytes}},
	})
}

var _ sink = &obsSink{}
