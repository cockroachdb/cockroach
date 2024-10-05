// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemachange

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/trace"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// EndSpan is a helper for ending a span and annotating it with error
// information, if relevant. Usage:
//
//	func mytracedfunc(ctx context.Context) (retErr err) {
//		ctx, span := tracer.Start(ctx, "mytracedfunc")
//		defer func() { EndSpan(span, retErr) }()
//	}
func EndSpan(span trace.Span, err error) {
	if err == nil {
		span.SetStatus(codes.Ok, "")
	} else if errors.Is(err, context.Canceled) {
		span.SetStatus(codes.Unset, "")
		span.SetAttributes(
			attribute.Bool("context.canceled", true),
		)
	} else {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

type PGXTracer struct {
	tracer trace.Tracer
}

func (l *PGXTracer) TraceQueryStart(
	ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData,
) context.Context {
	stringifiedArgs := util.Map(data.Args, func(arg any) string {
		return fmt.Sprintf("%#v", arg)
	})

	ctx, span := l.tracer.Start(ctx, "Query")
	span.SetAttributes(
		attribute.String("sql", data.SQL),
		attribute.StringSlice("args", stringifiedArgs),
	)

	return ctx
}

func (*PGXTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(
		attribute.String("command_tag", data.CommandTag.String()),
	)
	EndSpan(span, data.Err)
}

// OTLPFileClient is a [otlptrace.Client] that "uploads" spans to a .ndjson.gz file
// instead of sending them to a remote server.
//
// The output .ndjson.gz [1] conforms to the OTLP File Spec [2]. Unfortunately,
// most tooling will fail to read this format as OTeL's proto files made an
// backwards incompatible change [3]. This results in CockroachDB's OTeL SDK
// JSON encoding messaging with differently named keys. At the time of writing,
// no tooling exists to ingest this format nor any other format but plenty of
// tooling (JQ, DuckDB, Python) exists to work with JSON.
//
// If necessary, the NDJSON can be post-processed to be loaded into protobufs
// and then re-encoded as binary to be sent to an OTPL ingestor, like Jaeger.
//
// [1]: https://github.com/ndjson/ndjson-spec
// [2]: https://opentelemetry.io/docs/specs/otel/protocol/file-exporter/
// [3]: https://github.com/open-telemetry/opentelemetry-proto/blob/v0.15.0/opentelemetry/proto/trace/v1/trace.proto#L55-L82
type OTLPFileClient struct {
	// Path is the path to the tar.gz file that spans will be written to.
	Path string

	mu   syncutil.Mutex
	file *os.File
	gzip *gzip.Writer
	json *json.Encoder
}

var _ otlptrace.Client = &OTLPFileClient{}

// Start implements otlptrace.Client.
func (c *OTLPFileClient) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.file != nil {
		return nil
	}

	var err error
	c.file, err = os.Create(c.Path)
	if err != nil {
		return errors.WithStack(err)
	}

	c.gzip = gzip.NewWriter(c.file)
	c.json = json.NewEncoder(c.gzip)

	return nil
}

// Stop implements otlptrace.Client.
func (c *OTLPFileClient) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.gzip.Close(); err != nil {
		return errors.WithStack(err)
	}

	if err := c.file.Close(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// UploadTraces implements otlptrace.Client.
func (c *OTLPFileClient) UploadTraces(
	ctx context.Context, protoSpans []*tracepb.ResourceSpans,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.file == nil {
		return errors.New(".Start not called")
	}

	// NB: .Encode automatically terminates JSON with a newline.
	return c.json.Encode(&coltracepb.ExportTraceServiceRequest{
		ResourceSpans: protoSpans,
	})
}
