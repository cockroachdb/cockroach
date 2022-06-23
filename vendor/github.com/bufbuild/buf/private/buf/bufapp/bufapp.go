// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bufapp

import (
	"crypto/tls"
	"fmt"

	"github.com/bufbuild/buf/private/pkg/app/appname"
	"github.com/bufbuild/buf/private/pkg/cert/certclient"
)

const currentVersion = "v1"

// ExternalConfig is an external config.
type ExternalConfig struct {
	// If editing ExternalConfig, make sure to update ExternalConfig.IsEmpty!

	Version string                             `json:"version,omitempty" yaml:"version,omitempty"`
	TLS     certclient.ExternalClientTLSConfig `json:"tls,omitempty" yaml:"tls,omitempty"`
}

// IsEmpty returns true if the externalConfig is empty.
func (e ExternalConfig) IsEmpty() bool {
	return e.Version == "" && e.TLS.IsEmpty()
}

// Config is a config.
type Config struct {
	TLS *tls.Config
}

// NewConfig returns a new Config for the ExternalConfig.
func NewConfig(
	container appname.Container,
	externalConfig ExternalConfig,
) (*Config, error) {
	if externalConfig.Version != currentVersion && !externalConfig.IsEmpty() {
		return nil, fmt.Errorf("buf configuration at %q must declare 'version: %s'", container.ConfigDirPath(), currentVersion)
	}
	tlsConfig, err := certclient.NewClientTLSConfig(container, externalConfig.TLS)
	if err != nil {
		return nil, err
	}
	return &Config{
		TLS: tlsConfig,
	}, nil
}
