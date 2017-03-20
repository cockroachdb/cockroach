// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package security

import (
	"crypto/tls"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"

	"github.com/pkg/errors"
)

// CertificateManager lives for the duration of the process and manages certificates and keys.
// It reloads all certificates when triggered and construct tls.Config objects for
// servers or clients.
//
// Important note: Load() performs some sanity checks (file pairs match, CA certs don't disappear),
// but these are by no means complete. Completeness is not required as nodes restarting have
// no fallback if invalid certs/keys are present.
type CertificateManager struct {
	// Immutable fields after object construction.
	certsDir string

	// mu protects all remaining fields.
	mu syncutil.RWMutex

	// If false, this is the first load. Needed to ensure we do not drop certain certs.
	initialized bool

	// Set of certs. These are swapped in during Load(), and never mutated afterwards.
	caCert      *CertInfo
	nodeCert    *CertInfo
	clientCerts map[string]*CertInfo

	// TLS configs. Initialized lazily. Wiped on every successful Load().
	// Server-side config.
	serverConfig *tls.Config
	// Client-side config for the cockroach node.
	// All other client tls.Config objects are built as requested and not cached.
	clientConfig *tls.Config
}

// NewCertificateManager creates a new certificate manager.
func NewCertificateManager(certsDir string) (*CertificateManager, error) {
	cm := &CertificateManager{certsDir: certsDir}
	return cm, cm.LoadCertificates()
}

// CACert returns the CA cert. May be nil.
func (cm *CertificateManager) CACert() *CertInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.caCert
}

// NodeCert returns the Node cert. May be nil.
func (cm *CertificateManager) NodeCert() *CertInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.nodeCert
}

// ClientCerts returns the Client certs.
func (cm *CertificateManager) ClientCerts() map[string]*CertInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.clientCerts
}

// LoadCertificates creates a CertificateLoader to load all certs and keys.
// Upon success, it swaps the existing certificates for the new ones.
func (cm *CertificateManager) LoadCertificates() error {
	cl := NewCertificateLoader(cm.certsDir)
	if err := cl.Load(); err != nil {
		return errors.Errorf("problem loading certs directory %s", cm.certsDir)
	}

	var caCert, nodeCert *CertInfo
	clientCerts := make(map[string]*CertInfo)
	for _, ci := range cl.Certificates() {
		switch ci.FileUsage {
		case CAPem:
			caCert = ci
		case NodePem:
			nodeCert = ci
		case ClientPem:
			clientCerts[ci.Name] = ci
		}
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.initialized {
		// If we ran before, make sure we don't reload with missing certificates.
		if cm.caCert != nil && caCert == nil {
			return errors.New("CA certificate has disappeared")
		}
		if cm.nodeCert != nil && nodeCert == nil {
			return errors.New("node certificate has disappeared")
		}
	}

	// Swap everything.
	cm.caCert = caCert
	cm.nodeCert = nodeCert
	cm.clientCerts = clientCerts
	cm.initialized = true

	cm.serverConfig = nil
	cm.clientConfig = nil

	return nil
}

// GetServerTLSConfig returns the most up-to-date server tls.Config.
func (cm *CertificateManager) GetServerTLSConfig() (*tls.Config, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.serverConfig != nil {
		return cm.serverConfig, nil
	}

	if cm.caCert == nil {
		return nil, errors.New("no CA certificate found")
	}
	if cm.nodeCert == nil {
		return nil, errors.New("no node certificate found")
	}

	cfg, err := newServerTLSConfig(
		cm.nodeCert.FileContents,
		cm.nodeCert.KeyFileContents,
		cm.caCert.FileContents)
	if err != nil {
		return nil, err
	}

	cm.serverConfig = cfg
	return cfg, nil
}

// GetClientTLSConfig returns the most up-to-date server tls.Config.
func (cm *CertificateManager) GetClientTLSConfig(user string) (*tls.Config, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	var ci *CertInfo
	if user == NodeUser {
		// Node user requested: use the combined server/client certificate.
		if cm.clientConfig != nil {
			return cm.clientConfig, nil
		}
		if cm.nodeCert == nil {
			return nil, errors.New("no node certificate found")
		}
		ci = cm.nodeCert
	} else {
		// Other clients.
		clientCi, ok := cm.clientCerts[user]
		if !ok {
			return nil, errors.Errorf("no client certificate found for user %s", user)
		}
		ci = clientCi
	}

	if cm.caCert == nil {
		return nil, errors.New("no CA certificate found")
	}

	cfg, err := newClientTLSConfig(
		ci.FileContents,
		ci.KeyFileContents,
		cm.caCert.FileContents)
	if err != nil {
		return nil, err
	}

	if user == NodeUser {
		cm.clientConfig = cfg
	}
	return cfg, nil
}
