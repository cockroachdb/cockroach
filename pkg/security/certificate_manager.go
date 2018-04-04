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

package security

import (
	"context"
	"crypto/tls"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"

	"github.com/pkg/errors"
)

var (
	metaCAExpiration = metric.Metadata{
		Name: "security.certificate.expiration.ca",
		Help: "Expiration timestamp in seconds since Unix epoch for the CA certificate. 0 means no certificate or error."}
	metaNodeExpiration = metric.Metadata{
		Name: "security.certificate.expiration.node",
		Help: "Expiration timestamp in seconds since Unix epoch for the node certificate. 0 means no certificate or error."}
)

// CertificateManager lives for the duration of the process and manages certificates and keys.
// It reloads all certificates when triggered and construct tls.Config objects for
// servers or clients.
//
// Important note: Load() performs some sanity checks (file pairs match, CA certs don't disappear),
// but these are by no means complete. Completeness is not required as nodes restarting have
// no fallback if invalid certs/keys are present.
type CertificateManager struct {
	// Certificate directory is not modified after initialization.
	certsDir string
	// The metrics struct is initialized at init time and metrics do their
	// own locking.
	certMetrics CertificateMetrics

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

// CertificateMetrics holds metrics about the various certificates.
// These are initialized when the certificate manager is created and updated
// on reload.
type CertificateMetrics struct {
	CAExpiration   *metric.Gauge
	NodeExpiration *metric.Gauge
}

func makeCertificateManager(certsDir string) *CertificateManager {
	cm := &CertificateManager{certsDir: os.ExpandEnv(certsDir)}
	// Initialize metrics:
	cm.certMetrics = CertificateMetrics{
		CAExpiration:   metric.NewGauge(metaCAExpiration),
		NodeExpiration: metric.NewGauge(metaNodeExpiration),
	}
	return cm
}

// NewCertificateManager creates a new certificate manager.
func NewCertificateManager(certsDir string) (*CertificateManager, error) {
	cm := makeCertificateManager(certsDir)
	return cm, cm.LoadCertificates()
}

// NewCertificateManagerFirstRun creates a new certificate manager.
// The certsDir is created if it does not exist.
// This should only be called when generating certificates, the server has
// no business creating the certs directory.
func NewCertificateManagerFirstRun(certsDir string) (*CertificateManager, error) {
	cm := makeCertificateManager(certsDir)
	if err := NewCertificateLoader(cm.certsDir).MaybeCreateCertsDir(); err != nil {
		return nil, err
	}

	return cm, cm.LoadCertificates()
}

// Metrics returns the metrics struct.
func (cm *CertificateManager) Metrics() CertificateMetrics {
	return cm.certMetrics
}

// RegisterSignalHandler registers a signal handler for SIGHUP, triggering a
// refresh of the certificates directory on notification.
func (cm *CertificateManager) RegisterSignalHandler(stopper *stop.Stopper) {
	go func() {
		ch := sysutil.RefreshSignaledChan()
		for {
			select {
			case <-stopper.ShouldStop():
				return
			case sig := <-ch:
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
// Callers should check for an internal Error field.
func (cm *CertificateManager) CACert() *CertInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.caCert
}

// checkCertIsValid returns an error if the passed cert is missing or has an error.
func checkCertIsValid(cert *CertInfo) error {
	if cert == nil {
		return errors.New("not found")
	}
	return cert.Error
}

// NodeCert returns the Node cert. May be nil.
// Callers should check for an internal Error field.
func (cm *CertificateManager) NodeCert() *CertInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.nodeCert
}

// ClientCerts returns the Client certs.
// Callers should check for internal Error fields.
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
		return errors.Wrapf(err, "problem loading certs directory %s", cm.certsDir)
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
		// If we ran before, make sure we don't reload with missing/bad certificates.
		if err := checkCertIsValid(caCert); checkCertIsValid(cm.caCert) == nil && err != nil {
			return errors.Wrap(err, "reload would lose valid CA cert")
		}
		if err := checkCertIsValid(nodeCert); checkCertIsValid(cm.nodeCert) == nil && err != nil {
			return errors.Wrap(err, "reload would lose valid node cert")
		}
	}

	// Swap everything.
	cm.caCert = caCert
	cm.nodeCert = nodeCert
	cm.clientCerts = clientCerts
	cm.initialized = true

	cm.serverConfig = nil
	cm.clientConfig = nil

	cm.updateMetricsLocked()
	return nil
}

// updateMetricsLocked updates the values on the certificate metrics.
// The metrics may not exist (eg: in tests that build their own CertificateManager).
// If the corresponding certificate is missing or invalid (Error != nil), we reset the
// metric to zero.
// cm.mu must be held to protect the certificates. Metrics do their own atomicity.
func (cm *CertificateManager) updateMetricsLocked() {
	// CA certificate expiration.
	if cm.certMetrics.CAExpiration != nil {
		if cm.caCert != nil && cm.caCert.Error == nil {
			cm.certMetrics.CAExpiration.Update(cm.caCert.ExpirationTime.Unix())
		} else {
			cm.certMetrics.CAExpiration.Update(0)
		}
	}

	// Node certificate expiration.
	// TODO(marc): we need to examine the entire certificate chain here, if the CA cert
	// used to sign the node cert expires sooner, then that is the expiration time to report.
	if cm.certMetrics.NodeExpiration != nil {
		if cm.nodeCert != nil && cm.nodeCert.Error == nil {
			cm.certMetrics.NodeExpiration.Update(cm.nodeCert.ExpirationTime.Unix())
		} else {
			cm.certMetrics.NodeExpiration.Update(0)
		}
	}
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

	if err := checkCertIsValid(cm.caCert); err != nil {
		return nil, errors.Wrap(err, "problem with CA certificate")
	}
	if err := checkCertIsValid(cm.nodeCert); err != nil {
		return nil, errors.Wrap(err, "problem with node certificate")
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
	ci := cm.clientCerts[user]
	if err := checkCertIsValid(ci); err != nil {
		return nil, errors.Wrapf(err, "problem with client cert for user %s", user)
	}

	return ci, nil
}

// getNodeClientCertsLocked returns the client cert/key for the node user.
// cm.mu must be held.
func (cm *CertificateManager) getNodeClientCertsLocked() (*CertInfo, error) {
	if err := checkCertIsValid(cm.nodeCert); err != nil {
		return nil, errors.Wrap(err, "problem with node certificate")
	}

	return cm.nodeCert, nil
}

// GetClientTLSConfig returns the most up-to-date server tls.Config.
// Returns the dual-purpose node certs if user == NodeUser.
func (cm *CertificateManager) GetClientTLSConfig(user string) (*tls.Config, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// We always need the CA cert.
	if err := checkCertIsValid(cm.caCert); err != nil {
		return nil, errors.Wrap(err, "problem with CA certificate")
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

	if err := checkCertIsValid(cm.caCert); err != nil {
		return "", errors.Wrap(err, "problem with CA certificate")
	}

	return filepath.Join(cm.certsDir, cm.caCert.Filename), nil
}

// ListCertificates returns all loaded certificates, or an error if not yet initialized.
func (cm *CertificateManager) ListCertificates() ([]*CertInfo, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if !cm.initialized {
		return nil, errors.New("certificate manager has not been initialized")
	}

	ret := make([]*CertInfo, 0, 2+len(cm.clientCerts))
	if cm.caCert != nil {
		ret = append(ret, cm.caCert)
	}
	if cm.nodeCert != nil {
		ret = append(ret, cm.nodeCert)
	}
	if cm.clientCerts != nil {
		for _, cert := range cm.clientCerts {
			ret = append(ret, cert)
		}
	}

	return ret, nil
}
