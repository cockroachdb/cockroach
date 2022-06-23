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

package otlpconfig

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
)

var httpSchemeRegexp = regexp.MustCompile(`(?i)^(http://|https://)`)

func ApplyGRPCEnvConfigs(cfg *Config) {
	e := EnvOptionsReader{
		GetEnv:   os.Getenv,
		ReadFile: ioutil.ReadFile,
	}

	e.ApplyGRPCEnvConfigs(cfg)
}

func ApplyHTTPEnvConfigs(cfg *Config) {
	e := EnvOptionsReader{
		GetEnv:   os.Getenv,
		ReadFile: ioutil.ReadFile,
	}

	e.ApplyHTTPEnvConfigs(cfg)
}

type EnvOptionsReader struct {
	GetEnv   func(string) string
	ReadFile func(filename string) ([]byte, error)
}

func (e *EnvOptionsReader) ApplyHTTPEnvConfigs(cfg *Config) {
	opts := e.GetOptionsFromEnv()
	for _, opt := range opts {
		opt.ApplyHTTPOption(cfg)
	}
}

func (e *EnvOptionsReader) ApplyGRPCEnvConfigs(cfg *Config) {
	opts := e.GetOptionsFromEnv()
	for _, opt := range opts {
		opt.ApplyGRPCOption(cfg)
	}
}

func (e *EnvOptionsReader) GetOptionsFromEnv() []GenericOption {
	var opts []GenericOption

	// Endpoint
	if v, ok := e.getEnvValue("ENDPOINT"); ok {
		if isInsecureEndpoint(v) {
			opts = append(opts, WithInsecure())
		} else {
			opts = append(opts, WithSecure())
		}

		opts = append(opts, WithEndpoint(trimSchema(v)))
	}
	if v, ok := e.getEnvValue("TRACES_ENDPOINT"); ok {
		if isInsecureEndpoint(v) {
			opts = append(opts, WithInsecure())
		} else {
			opts = append(opts, WithSecure())
		}

		opts = append(opts, WithEndpoint(trimSchema(v)))
	}

	// Certificate File
	if path, ok := e.getEnvValue("CERTIFICATE"); ok {
		if tls, err := e.readTLSConfig(path); err == nil {
			opts = append(opts, WithTLSClientConfig(tls))
		} else {
			otel.Handle(fmt.Errorf("failed to configure otlp exporter certificate '%s': %w", path, err))
		}
	}
	if path, ok := e.getEnvValue("TRACES_CERTIFICATE"); ok {
		if tls, err := e.readTLSConfig(path); err == nil {
			opts = append(opts, WithTLSClientConfig(tls))
		} else {
			otel.Handle(fmt.Errorf("failed to configure otlp traces exporter certificate '%s': %w", path, err))
		}
	}

	// Headers
	if h, ok := e.getEnvValue("HEADERS"); ok {
		opts = append(opts, WithHeaders(stringToHeader(h)))
	}
	if h, ok := e.getEnvValue("TRACES_HEADERS"); ok {
		opts = append(opts, WithHeaders(stringToHeader(h)))
	}

	// Compression
	if c, ok := e.getEnvValue("COMPRESSION"); ok {
		opts = append(opts, WithCompression(stringToCompression(c)))
	}
	if c, ok := e.getEnvValue("TRACES_COMPRESSION"); ok {
		opts = append(opts, WithCompression(stringToCompression(c)))
	}
	// Timeout
	if t, ok := e.getEnvValue("TIMEOUT"); ok {
		if d, err := strconv.Atoi(t); err == nil {
			opts = append(opts, WithTimeout(time.Duration(d)*time.Millisecond))
		}
	}
	if t, ok := e.getEnvValue("TRACES_TIMEOUT"); ok {
		if d, err := strconv.Atoi(t); err == nil {
			opts = append(opts, WithTimeout(time.Duration(d)*time.Millisecond))
		}
	}

	return opts
}

func isInsecureEndpoint(endpoint string) bool {
	return strings.HasPrefix(strings.ToLower(endpoint), "http://") || strings.HasPrefix(strings.ToLower(endpoint), "unix://")
}

func trimSchema(endpoint string) string {
	return httpSchemeRegexp.ReplaceAllString(endpoint, "")
}

// getEnvValue gets an OTLP environment variable value of the specified key using the GetEnv function.
// This function already prepends the OTLP prefix to all key lookup.
func (e *EnvOptionsReader) getEnvValue(key string) (string, bool) {
	v := strings.TrimSpace(e.GetEnv(fmt.Sprintf("OTEL_EXPORTER_OTLP_%s", key)))
	return v, v != ""
}

func (e *EnvOptionsReader) readTLSConfig(path string) (*tls.Config, error) {
	b, err := e.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return CreateTLSConfig(b)
}

func stringToCompression(value string) Compression {
	switch value {
	case "gzip":
		return GzipCompression
	}

	return NoCompression
}

func stringToHeader(value string) map[string]string {
	headersPairs := strings.Split(value, ",")
	headers := make(map[string]string)

	for _, header := range headersPairs {
		nameValue := strings.SplitN(header, "=", 2)
		if len(nameValue) < 2 {
			continue
		}
		name, err := url.QueryUnescape(nameValue[0])
		if err != nil {
			continue
		}
		trimmedName := strings.TrimSpace(name)
		value, err := url.QueryUnescape(nameValue[1])
		if err != nil {
			continue
		}
		trimmedValue := strings.TrimSpace(value)

		headers[trimmedName] = trimmedValue
	}

	return headers
}
