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

package resource // import "go.opentelemetry.io/otel/sdk/resource"

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
)

// config contains configuration for Resource creation.
type config struct {
	// detectors that will be evaluated.
	detectors []Detector
	// SchemaURL to associate with the Resource.
	schemaURL string
}

// Option is the interface that applies a configuration option.
type Option interface {
	// apply sets the Option value of a config.
	apply(*config)
}

// WithAttributes adds attributes to the configured Resource.
func WithAttributes(attributes ...attribute.KeyValue) Option {
	return WithDetectors(detectAttributes{attributes})
}

type detectAttributes struct {
	attributes []attribute.KeyValue
}

func (d detectAttributes) Detect(context.Context) (*Resource, error) {
	return NewSchemaless(d.attributes...), nil
}

// WithDetectors adds detectors to be evaluated for the configured resource.
func WithDetectors(detectors ...Detector) Option {
	return detectorsOption{detectors: detectors}
}

type detectorsOption struct {
	detectors []Detector
}

func (o detectorsOption) apply(cfg *config) {
	cfg.detectors = append(cfg.detectors, o.detectors...)
}

// WithFromEnv adds attributes from environment variables to the configured resource.
func WithFromEnv() Option {
	return WithDetectors(fromEnv{})
}

// WithHost adds attributes from the host to the configured resource.
func WithHost() Option {
	return WithDetectors(host{})
}

// WithTelemetrySDK adds TelemetrySDK version info to the configured resource.
func WithTelemetrySDK() Option {
	return WithDetectors(telemetrySDK{})
}

// WithSchemaURL sets the schema URL for the configured resource.
func WithSchemaURL(schemaURL string) Option {
	return schemaURLOption(schemaURL)
}

type schemaURLOption string

func (o schemaURLOption) apply(cfg *config) {
	cfg.schemaURL = string(o)
}

// WithOS adds all the OS attributes to the configured Resource.
// See individual WithOS* functions to configure specific attributes.
func WithOS() Option {
	return WithDetectors(
		osTypeDetector{},
		osDescriptionDetector{},
	)
}

// WithOSType adds an attribute with the operating system type to the configured Resource.
func WithOSType() Option {
	return WithDetectors(osTypeDetector{})
}

// WithOSDescription adds an attribute with the operating system description to the
// configured Resource. The formatted string is equivalent to the output of the
// `uname -snrvm` command.
func WithOSDescription() Option {
	return WithDetectors(osDescriptionDetector{})
}

// WithProcess adds all the Process attributes to the configured Resource.
// See individual WithProcess* functions to configure specific attributes.
func WithProcess() Option {
	return WithDetectors(
		processPIDDetector{},
		processExecutableNameDetector{},
		processExecutablePathDetector{},
		processCommandArgsDetector{},
		processOwnerDetector{},
		processRuntimeNameDetector{},
		processRuntimeVersionDetector{},
		processRuntimeDescriptionDetector{},
	)
}

// WithProcessPID adds an attribute with the process identifier (PID) to the
// configured Resource.
func WithProcessPID() Option {
	return WithDetectors(processPIDDetector{})
}

// WithProcessExecutableName adds an attribute with the name of the process
// executable to the configured Resource.
func WithProcessExecutableName() Option {
	return WithDetectors(processExecutableNameDetector{})
}

// WithProcessExecutablePath adds an attribute with the full path to the process
// executable to the configured Resource.
func WithProcessExecutablePath() Option {
	return WithDetectors(processExecutablePathDetector{})
}

// WithProcessCommandArgs adds an attribute with all the command arguments (including
// the command/executable itself) as received by the process the configured Resource.
func WithProcessCommandArgs() Option {
	return WithDetectors(processCommandArgsDetector{})
}

// WithProcessOwner adds an attribute with the username of the user that owns the process
// to the configured Resource.
func WithProcessOwner() Option {
	return WithDetectors(processOwnerDetector{})
}

// WithProcessRuntimeName adds an attribute with the name of the runtime of this
// process to the configured Resource.
func WithProcessRuntimeName() Option {
	return WithDetectors(processRuntimeNameDetector{})
}

// WithProcessRuntimeVersion adds an attribute with the version of the runtime of
// this process to the configured Resource.
func WithProcessRuntimeVersion() Option {
	return WithDetectors(processRuntimeVersionDetector{})
}

// WithProcessRuntimeDescription adds an attribute with an additional description
// about the runtime of the process to the configured Resource.
func WithProcessRuntimeDescription() Option {
	return WithDetectors(processRuntimeDescriptionDetector{})
}
