// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	collpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	cpb "go.opentelemetry.io/proto/otlp/common/v1"
	lpb "go.opentelemetry.io/proto/otlp/logs/v1"
	rpb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
)

const (
	// attribute attached to the outgoing logs
	// helpful for identifying where the logs are coming from
	logAttributeServiceKey   = "service.name"
	logAttributeServiceValue = "cockroachdb"
	logAttributeSinkKey      = "sink.name"
)

// OpenTelemetry log sink
type otlpSink struct {
	conn     *grpc.ClientConn
	lsc      collpb.LogsServiceClient
	name     string
	resource *rpb.Resource
}

func newOTLPSink(config logconfig.OTLPSinkConfig) (*otlpSink, error) {
	dialOpts := []grpc.DialOption{
		grpc.WithUserAgent("CRDB OTLP over gRPC logs exporter"),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	if *config.Compression == logconfig.GzipCompression {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
	}

	conn, err := grpc.Dial(config.Address, dialOpts...)
	if err != nil {
		return nil, err
	}

	lsc := collpb.NewLogsServiceClient(conn)
	name := config.SinkName
	return &otlpSink{
		conn: conn,
		lsc:  lsc,
		name: name,
		resource: &rpb.Resource{
			Attributes: []*cpb.KeyValue{
				{
					Key:   logAttributeServiceKey,
					Value: &cpb.AnyValue{Value: &cpb.AnyValue_StringValue{StringValue: logAttributeServiceValue}},
				},
				{
					Key:   logAttributeSinkKey,
					Value: &cpb.AnyValue{Value: &cpb.AnyValue_StringValue{StringValue: name}},
				},
			},
		},
	}, nil
}

func (sink *otlpSink) isNotShutdown() bool {
	return sink.conn.GetState() != connectivity.Shutdown
}

func (sink *otlpSink) active() bool {
	return true
}

func (sink *otlpSink) attachHints(stacks []byte) []byte {
	return stacks
}

func (sink *otlpSink) exitCode() exit.Code {
	return exit.LoggingNetCollectorUnavailable()
}

// converts the raw bytes into OTEL log records
func extractRecordsToOTLP(b []byte) []*lpb.LogRecord {
	bodies := bytes.Split(b, []byte("\n"))
	records := make([]*lpb.LogRecord, 0, len(bodies))

	for _, body := range bodies {
		body = bytes.TrimSpace(body)
		if len(body) > 0 {
			records = append(records, &lpb.LogRecord{
				Body: &cpb.AnyValue{
					Value: &cpb.AnyValue_StringValue{
						StringValue: string(body),
					},
				},
			})
		}
	}

	return records
}

func (sink *otlpSink) output(b []byte, opts sinkOutputOptions) error {
	records := extractRecordsToOTLP(b)
	ctx := context.Background()

	_, err := sink.lsc.Export(ctx, &collpb.ExportLogsServiceRequest{
		ResourceLogs: []*lpb.ResourceLogs{
			{
				Resource: sink.resource,
				InstrumentationLibraryLogs: []*lpb.InstrumentationLibraryLogs{
					{
						Logs: records,
					},
				},
			},
		},
	})

	if status.Code(err) == codes.OK {
		return nil
	}

	return err
}
