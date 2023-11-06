// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemachange

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/trace"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
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
	} else {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		// If the error is a context cancellation, add an attribute so we can
		// ignore the "errors".
		// TODO(chrisseto): Maybe we should just use codes.Unset and record a
		// "cancellation" event?
		if errors.Is(err, context.Canceled) {
			span.SetAttributes(
				attribute.Bool("context.canceled", true),
			)
		}
	}
	span.End()
}

type PGXTracer struct {
	tracer trace.Tracer
}

func (l *PGXTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
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

// OTLPFileClient is a [otlptrace.Client] that "uploads" spans to .tar.gz file
// instead of sending them to a remote server.
//
// The .tar.gz format is unfortunately bespoke as no equivalent format exists.
// The closest is the OTLP JSONNL format [1]. At the time of writing, the OTeL
// SDK used by cockroachdb produces incorrectly keyed jsonpb output due to a
// backwards incompatible change in the OTLP proto files [2]. As the binary
// representation can be correctly interpreted, this .tar.gz format was settled
// on. Future authors should feel free to modify this format or adopt other
// standards, should they be available.
//
// [1]: https://opentelemetry.io/docs/specs/otel/protocol/file-exporter/
// [2]: https://github.com/open-telemetry/opentelemetry-proto/blob/v0.15.0/opentelemetry/proto/trace/v1/trace.proto#L55-L82
type OTLPFileClient struct {
	// Path is the path to the tar.gz file that spans will be written to.
	Path string

	mu   sync.Mutex
	file *os.File
	gzip *gzip.Writer
	tar  *tar.Writer
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
	c.file, err = os.OpenFile(c.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0660)
	if err != nil {
		return errors.WithStack(err)
	}

	c.gzip = gzip.NewWriter(c.file)
	c.tar = tar.NewWriter(c.gzip)

	return nil
}

// Stop implements otlptrace.Client.
func (c *OTLPFileClient) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.tar.Close(); err != nil {
		return errors.WithStack(err)
	}

	if err := c.gzip.Close(); err != nil {
		return errors.WithStack(err)
	}

	if err := c.file.Close(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// UploadTraces implements otlptrace.Client.
func (c *OTLPFileClient) UploadTraces(ctx context.Context, protoSpans []*tracepb.ResourceSpans) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.file == nil {
		return errors.New(".Start not called")
	}

	// NB: google's proto.Marshal is intentionally used here. For some reason,
	// gogo proto panics when touching these messages. OTLP distributes
	// precompiled protobufs, which may need to be worked around to get gogo
	// proto working.
	out, err := proto.Marshal(&coltracepb.ExportTraceServiceRequest{
		ResourceSpans: protoSpans,
	})
	if err != nil {
		return err
	}

	if err := c.tar.WriteHeader(&tar.Header{
		Name: "ExportTraceServiceRequest",
		Mode: 0600,
		Size: int64(len(out)),
	}); err != nil {
		return err
	}

	if _, err := c.tar.Write(out); err != nil {
		return err
	}

	return nil
}
