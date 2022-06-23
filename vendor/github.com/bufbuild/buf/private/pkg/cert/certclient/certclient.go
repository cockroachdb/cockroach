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

package certclient

import (
	"crypto/tls"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/bufbuild/buf/private/pkg/app/appname"
)

// ExternalClientTLSConfig allows users to configure TLS on the client side.
type ExternalClientTLSConfig struct {
	Use               string   `json:"use,omitempty" yaml:"use,omitempty"`
	RootCertFilePaths []string `json:"root_cert_file_paths,omitempty" yaml:"root_cert_file_paths,omitempty"`
}

// IsEmpty returns true if the ExternalClientTLSConfig is empty.
func (e ExternalClientTLSConfig) IsEmpty() bool {
	return e.Use == "" && len(e.RootCertFilePaths) == 0
}

// NewClientTLSConfig creates a new *tls.Config from the ExternalTLSConfig
//
// The default is to use the system TLS config.
func NewClientTLSConfig(
	container appname.Container,
	externalClientTLSConfig ExternalClientTLSConfig,
) (*tls.Config, error) {
	switch t := strings.ToLower(strings.TrimSpace(externalClientTLSConfig.Use)); t {
	case "local":
		rootCertFilePaths := externalClientTLSConfig.RootCertFilePaths
		if len(rootCertFilePaths) == 0 {
			rootCertFilePaths = []string{
				filepath.Join(
					container.ConfigDirPath(),
					"tls",
					"root.pem",
				),
			}
		}
		return newClientTLSConfigFromRootCertFiles(rootCertFilePaths...)
	case "", "system":
		return newClientSystemTLSConfig(), nil
	case "false":
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown tls.use: %q", t)
	}
}
