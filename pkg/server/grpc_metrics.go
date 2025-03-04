// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	prometheusgo "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	GrpcMethodLabel     = "methodName"
	GrpcStatusCodeLabel = "statusCode"
	serverPrefix        = "/cockroach.server"
	tsdbPrefix          = "/cockroach.ts"
)

var serverGrpcMetricsEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"server.grpc.metrics.enabled",
	"enables to collection of grpc metrics",
	false,
)

type GrpcServerMetrics struct {
	RequestMetrics *metric.HistogramVec
	registry       *metric.Registry
	settings       *cluster.Settings
}

func NewGrpcServerMetrics(reg *metric.Registry, settings *cluster.Settings) *GrpcServerMetrics {
	metadata := metric.Metadata{
		Name:        "server.grpc.request.duration.nanos",
		Help:        "Duration of an grpc request in nanoseconds.",
		Measurement: "Duration",
		Unit:        metric.Unit_NANOSECONDS,
		MetricType:  prometheusgo.MetricType_HISTOGRAM,
	}

	histogramVec := metric.NewExportedHistogramVec(
		metadata,
		metric.ResponseTime30sBuckets,
		[]string{GrpcMethodLabel, GrpcStatusCodeLabel})
	reg.AddMetric(histogramVec)
	return &GrpcServerMetrics{
		RequestMetrics: histogramVec,
		registry:       reg,
		settings:       settings,
	}
}

func excludeMethodFromRecording(method string) bool {
	return !strings.HasPrefix(method, serverPrefix) && !strings.HasPrefix(method, tsdbPrefix)
}

// NewGrpcServerInterceptor creates a new gRPC server interceptor that records
// the duration of each RPC. The metric is labeled by the method name and the
// status code of the RPC.
func (m *GrpcServerMetrics) NewGrpcServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		if !serverGrpcMetricsEnabled.Get(&m.settings.SV) || excludeMethodFromRecording(info.FullMethod) {
			return handler(ctx, req)
		}

		startTime := timeutil.Now()
		resp, err := handler(ctx, req)
		duration := timeutil.Since(startTime)
		if serverGrpcMetricsEnabled.Get(&m.settings.SV) {
			var code codes.Code
			if err != nil {
				code = status.Code(err)
			} else {
				code = codes.OK
			}

			m.RequestMetrics.Observe(map[string]string{
				GrpcMethodLabel:     info.FullMethod,
				GrpcStatusCodeLabel: code.String(),
			}, float64(duration.Nanoseconds()))
		}
		return resp, err
	}
}
