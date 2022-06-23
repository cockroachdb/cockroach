// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otlptracegrpc // import "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"

import (
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/internal/otlpconfig"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/internal/retry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Option applies an option to the gRPC driver.
type Option interface {
	applyGRPCOption(*otlpconfig.Config)
}

// RetryConfig defines configuration for retrying batches in case of export
// failure using an exponential backoff.
type RetryConfig retry.Config

type wrappedOption struct {
	otlpconfig.GRPCOption
}

func (w wrappedOption) applyGRPCOption(cfg *otlpconfig.Config) {
	w.ApplyGRPCOption(cfg)
}

// WithInsecure disables client transport security for the exporter's gRPC connection
// just like grpc.WithInsecure() https://pkg.go.dev/google.golang.org/grpc#WithInsecure
// does. Note, by default, client security is required unless WithInsecure is used.
func WithInsecure() Option {
	return wrappedOption{otlpconfig.WithInsecure()}
}

// WithEndpoint allows one to set the endpoint that the exporter will
// connect to the collector on. If unset, it will instead try to use
// connect to DefaultCollectorHost:DefaultCollectorPort.
func WithEndpoint(endpoint string) Option {
	return wrappedOption{otlpconfig.WithEndpoint(endpoint)}
}

// WithReconnectionPeriod allows one to set the delay between next connection attempt
// after failing to connect with the collector.
func WithReconnectionPeriod(rp time.Duration) Option {
	return wrappedOption{otlpconfig.NewGRPCOption(func(cfg *otlpconfig.Config) {
		cfg.ReconnectionPeriod = rp
	})}
}

func compressorToCompression(compressor string) otlpconfig.Compression {
	switch compressor {
	case "gzip":
		return otlpconfig.GzipCompression
	}

	otel.Handle(fmt.Errorf("invalid compression type: '%s', using no compression as default", compressor))
	return otlpconfig.NoCompression
}

// WithCompressor will set the compressor for the gRPC client to use when sending requests.
// It is the responsibility of the caller to ensure that the compressor set has been registered
// with google.golang.org/grpc/encoding. This can be done by encoding.RegisterCompressor. Some
// compressors auto-register on import, such as gzip, which can be registered by calling
// `import _ "google.golang.org/grpc/encoding/gzip"`.
func WithCompressor(compressor string) Option {
	return wrappedOption{otlpconfig.WithCompression(compressorToCompression(compressor))}
}

// WithHeaders will send the provided headers with gRPC requests.
func WithHeaders(headers map[string]string) Option {
	return wrappedOption{otlpconfig.WithHeaders(headers)}
}

// WithTLSCredentials allows the connection to use TLS credentials
// when talking to the server. It takes in grpc.TransportCredentials instead
// of say a Certificate file or a tls.Certificate, because the retrieving of
// these credentials can be done in many ways e.g. plain file, in code tls.Config
// or by certificate rotation, so it is up to the caller to decide what to use.
func WithTLSCredentials(creds credentials.TransportCredentials) Option {
	return wrappedOption{otlpconfig.NewGRPCOption(func(cfg *otlpconfig.Config) {
		cfg.Traces.GRPCCredentials = creds
	})}
}

// WithServiceConfig defines the default gRPC service config used.
func WithServiceConfig(serviceConfig string) Option {
	return wrappedOption{otlpconfig.NewGRPCOption(func(cfg *otlpconfig.Config) {
		cfg.ServiceConfig = serviceConfig
	})}
}

// WithDialOption opens support to any grpc.DialOption to be used. If it conflicts
// with some other configuration the GRPC specified via the collector the ones here will
// take preference since they are set last.
func WithDialOption(opts ...grpc.DialOption) Option {
	return wrappedOption{otlpconfig.NewGRPCOption(func(cfg *otlpconfig.Config) {
		cfg.DialOptions = opts
	})}
}

// WithTimeout tells the driver the max waiting time for the backend to process
// each spans batch. If unset, the default will be 10 seconds.
func WithTimeout(duration time.Duration) Option {
	return wrappedOption{otlpconfig.WithTimeout(duration)}
}

// WithRetry configures the retry policy for transient errors that may occurs
// when exporting traces. An exponential back-off algorithm is used to ensure
// endpoints are not overwhelmed with retries. If unset, the default retry
// policy will retry after 5 seconds and increase exponentially after each
// error for a total of 1 minute.
func WithRetry(settings RetryConfig) Option {
	return wrappedOption{otlpconfig.WithRetry(retry.Config(settings))}
}
