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
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"golang.org/x/net/context"

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
	cm := &CertificateManager{certsDir: os.ExpandEnv(certsDir)}
	return cm, cm.LoadCertificates()
}

// NewCertificateManagerFirstRun creates a new certificate manager.
// The certsDir is created if it does not exist.
// This should only be called when generating certificates, the server has
// no business creating the certs directory.
func NewCertificateManagerFirstRun(certsDir string) (*CertificateManager, error) {
	cm := &CertificateManager{certsDir: os.ExpandEnv(certsDir)}
	if err := NewCertificateLoader(cm.certsDir).MaybeCreateCertsDir(); err != nil {
		return nil, err
	}

	return cm, cm.LoadCertificates()
}

// RegisterSignalHandler registers a signal handler for SIGHUP, triggering a
// refresh of the certificates directory on notification.
func (cm *CertificateManager) RegisterSignalHandler(stopper *stop.Stopper) {
	// Setup signal handler.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-stopper.ShouldStop():
				signal.Stop(sigs)
				return
			case sig := <-sigs:
				log.Infof(context.Background(), "received signal %q, triggering certificate reload", sig)
				if err := cm.LoadCertificates(); err != nil {
					log.Warningf(context.Background(), "could not reload certificates: %v", err)
				} else {
					log.Info(context.Background(), "successfully reloaded certificates")
				}
			}
		}
	}()
}

// CACertPath returns the expected file path for the CA certificate.
func (cm *CertificateManager) CACertPath() string {
	return filepath.Join(cm.certsDir, "ca"+certExtension)
}

// NodeCertPath returns the expected file path for the node certificate.
func (cm *CertificateManager) NodeCertPath() string {
	return filepath.Join(cm.certsDir, "node"+certExtension)
}

// NodeKeyPath returns the expected file path for the node key.
func (cm *CertificateManager) NodeKeyPath() string {
	return filepath.Join(cm.certsDir, "node"+keyExtension)
}

// ClientCertPath returns the expected file path for the user's certificate.
func (cm *CertificateManager) ClientCertPath(user string) string {
	return filepath.Join(cm.certsDir, "client."+user+certExtension)
}

// ClientKeyPath returns the expected file path for the user's key.
func (cm *CertificateManager) ClientKeyPath(user string) string {
	return filepath.Join(cm.certsDir, "client."+user+keyExtension)
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

// GetServerTLSConfig returns a server TLS config with a callback to fetch the
// latest TLS config. We still attempt to get the config to make sure
// the initial call has a valid config loaded.
func (cm *CertificateManager) GetServerTLSConfig() (*tls.Config, error) {
	if _, err := cm.GetEmbeddedServerTLSConfig(nil); err != nil {
		return nil, err
	}
	return &tls.Config{
		GetConfigForClient: cm.GetEmbeddedServerTLSConfig,
	}, nil
}

// GetEmbeddedServerTLSConfig returns the most up-to-date server tls.Config.
// This is the callback set in tls.Config.GetConfigForClient. We currently
// ignore the ClientHelloInfo object.
func (cm *CertificateManager) GetEmbeddedServerTLSConfig(
	_ *tls.ClientHelloInfo,
) (*tls.Config, error) {
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

// getClientCertsLocked returns the client cert/key for the specified user,
// or an error if not found.
// cm.mu must be held.
func (cm *CertificateManager) getClientCertsLocked(user string) (*CertInfo, error) {
	ci, ok := cm.clientCerts[user]
	if !ok {
		return nil, errors.Errorf("no client certificate found for user %s", user)
	}

	return ci, nil
}

// getNodeClientCertsLocked returns the client cert/key for the node user.
// cm.mu must be held.
func (cm *CertificateManager) getNodeClientCertsLocked() (*CertInfo, error) {
	if cm.nodeCert == nil {
		return nil, errors.New("no node certificate found")
	}

	return cm.nodeCert, nil
}

// GetClientTLSConfig returns the most up-to-date server tls.Config.
// Returns the dual-purpose node certs if user == NodeUser.
func (cm *CertificateManager) GetClientTLSConfig(user string) (*tls.Config, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// We always need the CA cert.
	if cm.caCert == nil {
		return nil, errors.New("no CA certificate found")
	}

	if user != NodeUser {
		clientCert, err := cm.getClientCertsLocked(user)
		if err != nil {
			return nil, err
		}

		cfg, err := newClientTLSConfig(
			clientCert.FileContents,
			clientCert.KeyFileContents,
			cm.caCert.FileContents)
		if err != nil {
			return nil, err
		}

		return cfg, nil
	}

	// We're the node user:
	// Return the cache config if we have one.
	if cm.clientConfig != nil {
		return cm.clientConfig, nil
	}

	clientCert, err := cm.getNodeClientCertsLocked()
	if err != nil {
		return nil, err
	}

	cfg, err := newClientTLSConfig(
		clientCert.FileContents,
		clientCert.KeyFileContents,
		cm.caCert.FileContents)
	if err != nil {
		return nil, err
	}

	// Cache the config.
	cm.clientConfig = cfg
	return cfg, nil
}

// GetClientCertPaths returns the paths to the client cert and key.
// Returns the node cert and key if user == NodeUser.
func (cm *CertificateManager) GetClientCertPaths(user string) (string, string, error) {
	var clientCert *CertInfo
	var err error

	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if user == NodeUser {
		clientCert, err = cm.getNodeClientCertsLocked()
	} else {
		clientCert, err = cm.getClientCertsLocked(user)
	}
	if err != nil {
		return "", "", err
	}

	return filepath.Join(cm.certsDir, clientCert.Filename),
		filepath.Join(cm.certsDir, clientCert.KeyFilename),
		nil
}

// GetCACertPath returns the path to the CA certificate.
func (cm *CertificateManager) GetCACertPath() (string, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if cm.caCert == nil {
		return "", errors.New("no CA certificate found")
	}

	return filepath.Join(cm.certsDir, cm.caCert.Filename), nil
}
