// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	collpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	cpb "go.opentelemetry.io/proto/otlp/common/v1"
	lpb "go.opentelemetry.io/proto/otlp/logs/v1"
	rpb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	grpc_gzip "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	// attribute attached to the outgoing logs
	// helpful for identifying where the logs are coming from
	logAttributeServiceKey   = "service.name"
	logAttributeServiceValue = "cockroachdb"
	logAttributeSinkKey      = "sink.name"
)

// pool for OTEL spec log record objects that we can reuse between requests
var otlpLogRecordPool = sync.Pool{
	New: func() any {
		return &lpb.LogRecord{
			Body: &cpb.AnyValue{
				Value: &cpb.AnyValue_StringValue{StringValue: ""},
			},
		}
	},
}

type otlpSinkClient interface {
	Export(ctx context.Context, in *collpb.ExportLogsServiceRequest) (*collpb.ExportLogsServiceResponse, error)
	Close() error
}

// OpenTelemetry log sink
type otlpSink struct {
	client otlpSinkClient
	// requestObject should not be modified concurrently as it is reused
	// between requests
	requestObject *collpb.ExportLogsServiceRequest
}

func newOTLPSink(config logconfig.OTLPSinkConfig) (*otlpSink, error) {
	sink := &otlpSink{
		requestObject: &collpb.ExportLogsServiceRequest{
			ResourceLogs: []*lpb.ResourceLogs{
				{
					Resource: &rpb.Resource{
						Attributes: []*cpb.KeyValue{
							{
								Key:   logAttributeServiceKey,
								Value: &cpb.AnyValue{Value: &cpb.AnyValue_StringValue{StringValue: logAttributeServiceValue}},
							},
							{
								Key:   logAttributeSinkKey,
								Value: &cpb.AnyValue{Value: &cpb.AnyValue_StringValue{StringValue: config.SinkName}},
							},
						},
					},
					InstrumentationLibraryLogs: []*lpb.InstrumentationLibraryLogs{
						{
							Logs: nil,
						},
					},
				},
			},
		},
	}

	setClient := sink.setHTTPClient
	if *config.Mode == logconfig.OTLPModeGRPC {
		setClient = sink.setGRPCClient
	}

	if err := setClient(&config); err != nil {
		return nil, err
	}

	return sink, nil
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

// converts the raw bytes into OTEL log records using the otlpLogRecordPool
func otlpExtractRecords(b []byte) []*lpb.LogRecord {
	body := string(b)
	records := make([]*lpb.LogRecord, 0, strings.Count(body, "\n")+1)

	start := 0
	for i, ch := range body {
		if ch == '\n' {
			if i > start {
				record := otlpLogRecordPool.Get().(*lpb.LogRecord)
				record.Body.Value.(*cpb.AnyValue_StringValue).StringValue = body[start:i]
				records = append(records, record)
			}
			start = i + 1
		}
	}

	// check at the very end to ensure entire buffer is processed
	if start < len(body) {
		record := otlpLogRecordPool.Get().(*lpb.LogRecord)
		record.Body.Value.(*cpb.AnyValue_StringValue).StringValue = body[start:]
		records = append(records, record)
	}

	return records
}

func (sink *otlpSink) output(b []byte, opts sinkOutputOptions) error {
	logging.metrics.IncrementCounter(OTLPSinkWriteAttempt, 1)
	ctx := context.Background()

	records := otlpExtractRecords(b)
	sink.requestObject.ResourceLogs[0].InstrumentationLibraryLogs[0].Logs = records

	// transmit the log over the network
	_, err := sink.client.Export(ctx, sink.requestObject)

	// put the records back into the pool
	for _, record := range records {
		record.Body.Value.(*cpb.AnyValue_StringValue).StringValue = ""
		otlpLogRecordPool.Put(record)
	}

	if status.Code(err) == codes.OK {
		return nil
	}

	if err != nil {
		logging.metrics.IncrementCounter(OTLPSinkWriteError, 1)
		return err
	}

	return nil
}

// otlpStatsHandler implements the stats.Handler interface to and is passed as
// a dial option to the grpc client in the otlp log sink to get grpc metrics.
type otlpStatsHandler struct{}

// TagRPC exists to satisfy the stats.Handler interface.
func (h *otlpStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return ctx
}

// TagConn exists to satisfy the stats.Handler interface.
func (h *otlpStatsHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return ctx
}

// HandleConn exists to satisfy the stats.Handler interface.
func (h *otlpStatsHandler) HandleConn(ctx context.Context, connInfo stats.ConnStats) {}

func (h *otlpStatsHandler) HandleRPC(ctx context.Context, rpcInfo stats.RPCStats) {
	switch st := rpcInfo.(type) {
	case *stats.Begin:
		if st.IsTransparentRetryAttempt {
			logging.metrics.IncrementCounter(OTLPSinkGRPCTransparentRetries, 1)
		}
	}
}

var _ stats.Handler = (*otlpStatsHandler)(nil)

// client used when sink is using gRPC for exporting logs
type otlpGRPCClient struct {
	conn *grpc.ClientConn
	lsc  collpb.LogsServiceClient
}

func (c *otlpGRPCClient) Close() error {
	if c.conn.GetState() == connectivity.Shutdown {
		return nil
	}
	// The reason for nolint:grpcconnclose is that we are not using *rpc.Context
	// as it is primarily used for communication between crdb nodes, and doesn't
	// fit this usecase.
	return c.conn.Close() // nolint:grpcconnclose
}

func (c *otlpGRPCClient) Export(
	ctx context.Context, in *collpb.ExportLogsServiceRequest,
) (*collpb.ExportLogsServiceResponse, error) {
	return c.lsc.Export(ctx, in)
}

var _ otlpSinkClient = (*otlpGRPCClient)(nil)

var statsHandlerOption = &otlpStatsHandler{}

func (sink *otlpSink) setGRPCClient(config *logconfig.OTLPSinkConfig) error {
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(statsHandlerOption),
	}

	if *config.Compression == logconfig.GzipCompression {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor(grpc_gzip.Name)))
	}

	conn, err := grpc.Dial(config.Address, dialOpts...)
	if err != nil {
		return err
	}
	lsc := collpb.NewLogsServiceClient(conn)

	sink.client = &otlpGRPCClient{
		conn: conn,
		lsc:  lsc,
	}

	return nil
}

// client used when sink is using HTTP for exporting logs
type otlpHTTPClient struct {
	client      *http.Client
	request     *http.Request
	compression string
	gzipWriter  *gzip.Writer
}

func (c *otlpHTTPClient) Close() error {
	return nil
}

func (c *otlpHTTPClient) Export(
	ctx context.Context, in *collpb.ExportLogsServiceRequest,
) (*collpb.ExportLogsServiceResponse, error) {
	body, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}

	request := c.request.Clone(context.Background())
	switch c.compression {
	case logconfig.NoneCompression:
		request.Body = io.NopCloser(bytes.NewReader(body))
	case logconfig.GzipCompression:
		// Content-Encoding header is set when the sink is initialized
		// so no need to set it here
		var buf bytes.Buffer
		c.gzipWriter.Reset(&buf)
		if _, err := c.gzipWriter.Write(body); err != nil {
			return nil, err
		}
		if err := c.gzipWriter.Close(); err != nil {
			return nil, err
		}
		request.Body = io.NopCloser(bytes.NewReader(buf.Bytes()))
	}

	resp, err := c.client.Do(request)
	resp.Body.Close()
	return nil, err
}

var _ otlpSinkClient = (*otlpHTTPClient)(nil)

func (sink *otlpSink) setHTTPClient(config *logconfig.OTLPSinkConfig) error {
	hc := &http.Client{
		Transport: &http.Transport{
			ForceAttemptHTTP2: true,
		},
	}
	request, err := http.NewRequest(http.MethodPost, config.Address, http.NoBody)
	if err != nil {
		return err
	}
	request.Header.Set(httputil.ContentTypeHeader, httputil.ProtoContentType)

	compression := *config.Compression
	if compression == logconfig.GzipCompression {
		request.Header.Set(httputil.ContentEncodingHeader, httputil.GzipEncoding)
	}

	sink.client = &otlpHTTPClient{
		client:      hc,
		request:     request,
		compression: compression,
		gzipWriter:  gzip.NewWriter(io.Discard),
	}

	return nil
}
