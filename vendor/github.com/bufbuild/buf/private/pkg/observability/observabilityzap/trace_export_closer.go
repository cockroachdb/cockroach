// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package observabilityzap

import (
	"go.opencensus.io/trace"
	"go.uber.org/zap"
)

type traceExportCloser struct {
	logger *zap.Logger
}

func newTraceExportCloser(logger *zap.Logger) *traceExportCloser {
	return &traceExportCloser{
		logger: logger,
	}
}

// ExportSpan implements the opencensus trace.Exporter interface.
func (t *traceExportCloser) ExportSpan(spanData *trace.SpanData) {
	if spanData == nil || !spanData.IsSampled() {
		return
	}
	if checkedEntry := t.logger.Check(zap.DebugLevel, spanData.Name); checkedEntry != nil {
		fields := []zap.Field{
			zap.Duration("duration", spanData.EndTime.Sub(spanData.StartTime)),
		}
		for key, value := range spanData.Attributes {
			fields = append(fields, zap.Any(key, value))
		}
		checkedEntry.Write(fields...)
	}
}

func (t *traceExportCloser) Close() error {
	return nil
}
