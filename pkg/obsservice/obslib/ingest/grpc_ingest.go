// Copyright 2022 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package ingest

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/transform"
	logspb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/collector/logs/v1"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// EventIngester implements the OTLP Logs gRPC service, accepting connections
// and ingesting events.
type EventIngester struct {
	consumer obslib.EventConsumer
}

var _ logspb.LogsServiceServer = &EventIngester{}

func MakeEventIngester(_ context.Context, consumer obslib.EventConsumer) *EventIngester {
	return &EventIngester{
		consumer: consumer,
	}
}

// Export implements the LogsServiceServer gRPC service.
//
// NB: "Export" is a bit of a misnomer here. On the client side,
// it makes sense, but on this end of the wire, we are *Ingesting*
// events, not exporting them. This is simply the receiving end
// of that process. This is done to maintain compatibility between
// the OpenTelemetry Collector and our EventsExporter.
func (e *EventIngester) Export(
	ctx context.Context, request *logspb.ExportLogsServiceRequest,
) (*logspb.ExportLogsServiceResponse, error) {
	if err := e.unpackAndConsumeEvents(ctx, request); err != nil {
		log.Errorf(ctx, "error consuming events: %v", err)
		return nil, err
	}
	return &logspb.ExportLogsServiceResponse{}, nil
}

// TODO(abarganier): Add context cancellation here to cap transformation/unpack time.
// TODO(abarganier): Add metric to track context cancellations (counter tracking failed transformations)
func (e *EventIngester) unpackAndConsumeEvents(
	ctx context.Context, request *logspb.ExportLogsServiceRequest,
) error {
	ingestTime := time.Now()
	var retErr error = nil
	for _, resource := range request.ResourceLogs {
		for _, scopeLogs := range resource.ScopeLogs {
			for _, logRecord := range scopeLogs.LogRecords {
				transformed := transform.LogRecordToEvent(ingestTime, resource.Resource, scopeLogs.Scope, logRecord)
				if err := e.consumer.Consume(ctx, transformed); err != nil {
					retErr = errors.CombineErrors(retErr, err)
				}
			}
		}
	}
	return retErr
}
