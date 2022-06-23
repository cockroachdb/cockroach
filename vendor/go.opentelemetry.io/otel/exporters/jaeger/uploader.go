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

package jaeger // import "go.opentelemetry.io/otel/exporters/jaeger"

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/exporters/jaeger/internal/third_party/thrift/lib/go/thrift"

	gen "go.opentelemetry.io/otel/exporters/jaeger/internal/gen-go/jaeger"
)

// batchUploader send a batch of spans to Jaeger
type batchUploader interface {
	upload(context.Context, *gen.Batch) error
	shutdown(context.Context) error
}

type EndpointOption interface {
	newBatchUploader() (batchUploader, error)
}

type endpointOptionFunc func() (batchUploader, error)

func (fn endpointOptionFunc) newBatchUploader() (batchUploader, error) {
	return fn()
}

// WithAgentEndpoint configures the Jaeger exporter to send spans to a Jaeger agent
// over compact thrift protocol. This will use the following environment variables for
// configuration if no explicit option is provided:
//
// - OTEL_EXPORTER_JAEGER_AGENT_HOST is used for the agent address host
// - OTEL_EXPORTER_JAEGER_AGENT_PORT is used for the agent address port
//
// The passed options will take precedence over any environment variables and default values
// will be used if neither are provided.
func WithAgentEndpoint(options ...AgentEndpointOption) EndpointOption {
	return endpointOptionFunc(func() (batchUploader, error) {
		cfg := &agentEndpointConfig{
			agentClientUDPParams{
				AttemptReconnecting: true,
				Host:                envOr(envAgentHost, "localhost"),
				Port:                envOr(envAgentPort, "6831"),
			},
		}
		for _, opt := range options {
			opt.apply(cfg)
		}

		client, err := newAgentClientUDP(cfg.agentClientUDPParams)
		if err != nil {
			return nil, err
		}

		return &agentUploader{client: client}, nil
	})
}

type AgentEndpointOption interface {
	apply(*agentEndpointConfig)
}

type agentEndpointConfig struct {
	agentClientUDPParams
}

type agentEndpointOptionFunc func(*agentEndpointConfig)

func (fn agentEndpointOptionFunc) apply(cfg *agentEndpointConfig) {
	fn(cfg)
}

// WithAgentHost sets a host to be used in the agent client endpoint.
// This option overrides any value set for the
// OTEL_EXPORTER_JAEGER_AGENT_HOST environment variable.
// If this option is not passed and the env var is not set, "localhost" will be used by default.
func WithAgentHost(host string) AgentEndpointOption {
	return agentEndpointOptionFunc(func(o *agentEndpointConfig) {
		o.Host = host
	})
}

// WithAgentPort sets a port to be used in the agent client endpoint.
// This option overrides any value set for the
// OTEL_EXPORTER_JAEGER_AGENT_PORT environment variable.
// If this option is not passed and the env var is not set, "6831" will be used by default.
func WithAgentPort(port string) AgentEndpointOption {
	return agentEndpointOptionFunc(func(o *agentEndpointConfig) {
		o.Port = port
	})
}

// WithLogger sets a logger to be used by agent client.
func WithLogger(logger *log.Logger) AgentEndpointOption {
	return agentEndpointOptionFunc(func(o *agentEndpointConfig) {
		o.Logger = logger
	})
}

// WithDisableAttemptReconnecting sets option to disable reconnecting udp client.
func WithDisableAttemptReconnecting() AgentEndpointOption {
	return agentEndpointOptionFunc(func(o *agentEndpointConfig) {
		o.AttemptReconnecting = false
	})
}

// WithAttemptReconnectingInterval sets the interval between attempts to re resolve agent endpoint.
func WithAttemptReconnectingInterval(interval time.Duration) AgentEndpointOption {
	return agentEndpointOptionFunc(func(o *agentEndpointConfig) {
		o.AttemptReconnectInterval = interval
	})
}

// WithMaxPacketSize sets the maximum UDP packet size for transport to the Jaeger agent.
func WithMaxPacketSize(size int) AgentEndpointOption {
	return agentEndpointOptionFunc(func(o *agentEndpointConfig) {
		o.MaxPacketSize = size
	})
}

// WithCollectorEndpoint defines the full URL to the Jaeger HTTP Thrift collector. This will
// use the following environment variables for configuration if no explicit option is provided:
//
// - OTEL_EXPORTER_JAEGER_ENDPOINT is the HTTP endpoint for sending spans directly to a collector.
// - OTEL_EXPORTER_JAEGER_USER is the username to be sent as authentication to the collector endpoint.
// - OTEL_EXPORTER_JAEGER_PASSWORD is the password to be sent as authentication to the collector endpoint.
//
// The passed options will take precedence over any environment variables.
// If neither values are provided for the endpoint, the default value of "http://localhost:14268/api/traces" will be used.
// If neither values are provided for the username or the password, they will not be set since there is no default.
func WithCollectorEndpoint(options ...CollectorEndpointOption) EndpointOption {
	return endpointOptionFunc(func() (batchUploader, error) {
		cfg := &collectorEndpointConfig{
			endpoint:   envOr(envEndpoint, "http://localhost:14268/api/traces"),
			username:   envOr(envUser, ""),
			password:   envOr(envPassword, ""),
			httpClient: http.DefaultClient,
		}

		for _, opt := range options {
			opt.apply(cfg)
		}

		return &collectorUploader{
			endpoint:   cfg.endpoint,
			username:   cfg.username,
			password:   cfg.password,
			httpClient: cfg.httpClient,
		}, nil
	})
}

type CollectorEndpointOption interface {
	apply(*collectorEndpointConfig)
}

type collectorEndpointConfig struct {
	// endpoint for sending spans directly to a collector.
	endpoint string

	// username to be used for authentication with the collector endpoint.
	username string

	// password to be used for authentication with the collector endpoint.
	password string

	// httpClient to be used to make requests to the collector endpoint.
	httpClient *http.Client
}

type collectorEndpointOptionFunc func(*collectorEndpointConfig)

func (fn collectorEndpointOptionFunc) apply(cfg *collectorEndpointConfig) {
	fn(cfg)
}

// WithEndpoint is the URL for the Jaeger collector that spans are sent to.
// This option overrides any value set for the
// OTEL_EXPORTER_JAEGER_ENDPOINT environment variable.
// If this option is not passed and the environment variable is not set,
// "http://localhost:14268/api/traces" will be used by default.
func WithEndpoint(endpoint string) CollectorEndpointOption {
	return collectorEndpointOptionFunc(func(o *collectorEndpointConfig) {
		o.endpoint = endpoint
	})
}

// WithUsername sets the username to be used in the authorization header sent for all requests to the collector.
// This option overrides any value set for the
// OTEL_EXPORTER_JAEGER_USER environment variable.
// If this option is not passed and the environment variable is not set, no username will be set.
func WithUsername(username string) CollectorEndpointOption {
	return collectorEndpointOptionFunc(func(o *collectorEndpointConfig) {
		o.username = username
	})
}

// WithPassword sets the password to be used in the authorization header sent for all requests to the collector.
// This option overrides any value set for the
// OTEL_EXPORTER_JAEGER_PASSWORD environment variable.
// If this option is not passed and the environment variable is not set, no password will be set.
func WithPassword(password string) CollectorEndpointOption {
	return collectorEndpointOptionFunc(func(o *collectorEndpointConfig) {
		o.password = password
	})
}

// WithHTTPClient sets the http client to be used to make request to the collector endpoint.
func WithHTTPClient(client *http.Client) CollectorEndpointOption {
	return collectorEndpointOptionFunc(func(o *collectorEndpointConfig) {
		o.httpClient = client
	})
}

// agentUploader implements batchUploader interface sending batches to
// Jaeger through the UDP agent.
type agentUploader struct {
	client *agentClientUDP
}

var _ batchUploader = (*agentUploader)(nil)

func (a *agentUploader) shutdown(ctx context.Context) error {
	done := make(chan error, 1)
	go func() {
		done <- a.client.Close()
	}()

	select {
	case <-ctx.Done():
		// Prioritize not blocking the calling thread and just leak the
		// spawned goroutine to close the client.
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func (a *agentUploader) upload(ctx context.Context, batch *gen.Batch) error {
	return a.client.EmitBatch(ctx, batch)
}

// collectorUploader implements batchUploader interface sending batches to
// Jaeger through the collector http endpoint.
type collectorUploader struct {
	endpoint   string
	username   string
	password   string
	httpClient *http.Client
}

var _ batchUploader = (*collectorUploader)(nil)

func (c *collectorUploader) shutdown(ctx context.Context) error {
	// The Exporter will cancel any active exports and will prevent all
	// subsequent exports, so nothing to do here.
	return nil
}

func (c *collectorUploader) upload(ctx context.Context, batch *gen.Batch) error {
	body, err := serialize(batch)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", c.endpoint, body)
	if err != nil {
		return err
	}
	if c.username != "" && c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	req.Header.Set("Content-Type", "application/x-thrift")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}

	_, _ = io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("failed to upload traces; HTTP status code: %d", resp.StatusCode)
	}
	return nil
}

func serialize(obj thrift.TStruct) (*bytes.Buffer, error) {
	buf := thrift.NewTMemoryBuffer()
	if err := obj.Write(context.Background(), thrift.NewTBinaryProtocolConf(buf, &thrift.TConfiguration{})); err != nil {
		return nil, err
	}
	return buf.Buffer, nil
}
