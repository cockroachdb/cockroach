// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	collpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	cpb "go.opentelemetry.io/proto/otlp/common/v1"
	lpb "go.opentelemetry.io/proto/otlp/logs/v1"
	rpb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
)

// OpenTelemetry log sink
type otlpSink struct {
	conn *grpc.ClientConn
	lsc  collpb.LogsServiceClient
	name string
}

func newOtlpSink(config logconfig.OtlpSinkConfig) (*otlpSink, error) {
	dialOpts := []grpc.DialOption{grpc.WithUserAgent("CRDB OTLP over gRPC logs exporter")}
	if *config.Insecure {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		// Default to using the host's root CA.
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(nil)))
	}
	if *config.Compression == logconfig.GzipCompression {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
	}

	conn, err := grpc.Dial(config.Address, dialOpts...)
	if err != nil {
		return nil, err
	}

	lsc := collpb.NewLogsServiceClient(conn)
	return &otlpSink{
		conn: conn,
		lsc:  lsc,
		name: config.SinkName,
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
				Resource: &rpb.Resource{
					Attributes: []*cpb.KeyValue{
						{
							Key:   "service.name",
							Value: &cpb.AnyValue{Value: &cpb.AnyValue_StringValue{StringValue: "cockroachdb"}},
						},
						{
							Key:   "sink.name",
							Value: &cpb.AnyValue{Value: &cpb.AnyValue_StringValue{StringValue: sink.name}},
						},
					},
				},
				InstrumentationLibraryLogs: []*lpb.InstrumentationLibraryLogs{
					{
						InstrumentationLibrary: &cpb.InstrumentationLibrary{
							Name:    "cockroachdb.logger",
							Version: build.BinaryVersion(),
						},
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
