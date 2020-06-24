// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
)

var (
	metaCAExpiration = metric.Metadata{
		Name:        "security.certificate.expiration.ca",
		Help:        "Expiration for the CA certificate. 0 means no certificate or error.",
		Measurement: "Certificate Expiration",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaClientCAExpiration = metric.Metadata{
		Name:        "security.certificate.expiration.client-ca",
		Help:        "Expiration for the client CA certificate. 0 means no certificate or error.",
		Measurement: "Certificate Expiration",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaUICAExpiration = metric.Metadata{
		Name:        "security.certificate.expiration.ui-ca",
		Help:        "Expiration for the UI CA certificate. 0 means no certificate or error.",
		Measurement: "Certificate Expiration",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaNodeExpiration = metric.Metadata{
		Name:        "security.certificate.expiration.node",
		Help:        "Expiration for the node certificate. 0 means no certificate or error.",
		Measurement: "Certificate Expiration",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaNodeClientExpiration = metric.Metadata{
		Name:        "security.certificate.expiration.node-client",
		Help:        "Expiration for the node's client certificate. 0 means no certificate or error.",
		Measurement: "Certificate Expiration",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaUIExpiration = metric.Metadata{
		Name:        "security.certificate.expiration.ui",
		Help:        "Expiration for the UI certificate. 0 means no certificate or error.",
		Measurement: "Certificate Expiration",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}

	metaTenantServerCAExpiration = metric.Metadata{
		Name:        "security.certificate.expiration.ca-server-tenant",
		Help:        "Expiration for the Tenant Server CA certificate. 0 means no certificate or error.",
		Measurement: "Certificate Expiration",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaTenantServerExpiration = metric.Metadata{
		Name:        "security.certificate.expiration.server-tenant",
		Help:        "Expiration for the Tenant Server certificate. 0 means no certificate or error.",
		Measurement: "Certificate Expiration",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaTenantClientCAExpiration = metric.Metadata{
		Name:        "security.certificate.expiration.ca-client-tenant",
		Help:        "Expiration for the Tenant Client CA certificate. 0 means no certificate or error.",
		Measurement: "Certificate Expiration",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaTenantClientExpiration = metric.Metadata{
		Name:        "security.certificate.expiration.client-tenant",
		Help:        "Expiration for the Tenant Client certificate. 0 means no certificate or error.",
		Measurement: "Certificate Expiration",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
)

// CertificateManager lives for the duration of the process and manages certificates and keys.
// It reloads all certificates when triggered and construct tls.Config objects for
// servers or clients.
//
// Important note: Load() performs some sanity checks (file pairs match, CA certs don't disappear),
// but these are by no means complete. Completeness is not required as nodes restarting have
// no fallback if invalid certs/keys are present.
//
// The nomenclature for certificates is as follows, all within the certs-dir.
// - ca.crt             main CA certificate.
//                      Used to verify everything unless overridden by more specifica CAs.
// - ca-client.crt      CA certificate to verify client certificates. If it does not exist,
//                      fall back on 'ca.crt'.
// - node.crt           node certificate.
//                      Server-side certificate (always) and client-side certificate unless
//                      client.node.crt is found.
//                      Verified using 'ca.crt'.
// - client.<user>.crt  client certificate for 'user'. Verified using 'ca.crt', or 'ca-client.crt'.
// - client.node.crt    client certificate for the 'node' user. If it does not exist,
//                      fall back on 'node.crt'.
type CertificateManager struct {
	tenantIdentifier string
	CertsLocator

	// The metrics struct is initialized at init time and metrics do their
	// own locking.
	certMetrics CertificateMetrics

	// mu protects all remaining fields.
	mu syncutil.RWMutex

	// If false, this is the first load. Needed to ensure we do not drop certain certs.
	initialized bool

	// Set of certs. These are swapped in during Load(), and never mutated afterwards.
	caCert         *CertInfo // default CA certificate
	clientCACert   *CertInfo // optional: certificate to verify client certificates
	uiCACert       *CertInfo // optional: certificate to verify UI certficates
	nodeCert       *CertInfo // certificate for nodes (always server cert, sometimes client cert)
	nodeClientCert *CertInfo // optional: client certificate for 'node' user. Also included in 'clientCerts'
	uiCert         *CertInfo // optional: server certificate for the admin UI.
	clientCerts    map[string]*CertInfo

	// Certs only used with multi-tenancy.
	tenantServerCACert, tenantServerCert, tenantClientCACert, tenantClientCert *CertInfo

	// TLS configs. Initialized lazily. Wiped on every successful Load().
	// Server-side config.
	serverConfig *tls.Config
	// Ditto tenant server.
	tenantServerConfig *tls.Config
	// Server-side config for the Admin UI.
	uiServerConfig *tls.Config
	// Client-side config for the cockroach node.
	// All other client tls.Config objects are built as requested and not cached.
	clientConfig *tls.Config
	// Client config for the tenant (if running in a SQL tenant server).
	tenantClientConfig *tls.Config
}

// CertificateMetrics holds metrics about the various certificates.
// These are initialized when the certificate manager is created and updated
// on reload.
type CertificateMetrics struct {
	CAExpiration             *metric.Gauge
	ClientCAExpiration       *metric.Gauge
	UICAExpiration           *metric.Gauge
	NodeExpiration           *metric.Gauge
	NodeClientExpiration     *metric.Gauge
	UIExpiration             *metric.Gauge
	TenantServerCAExpiration *metric.Gauge
	TenantServerExpiration   *metric.Gauge
	TenantClientCAExpiration *metric.Gauge
	TenantClientExpiration   *metric.Gauge
}

func makeCertificateManager(certsDir string, opts ...func(*cmOptions)) *CertificateManager {
	var o cmOptions
	for _, fn := range opts {
		fn(&o)
	}

	return &CertificateManager{
		CertsLocator:     MakeCertsLocator(certsDir),
		tenantIdentifier: o.tenantIdentifier,
		certMetrics: CertificateMetrics{
			CAExpiration:             metric.NewGauge(metaCAExpiration),
			ClientCAExpiration:       metric.NewGauge(metaClientCAExpiration),
			UICAExpiration:           metric.NewGauge(metaUICAExpiration),
			NodeExpiration:           metric.NewGauge(metaNodeExpiration),
			NodeClientExpiration:     metric.NewGauge(metaNodeClientExpiration),
			UIExpiration:             metric.NewGauge(metaUIExpiration),
			TenantServerCAExpiration: metric.NewGauge(metaTenantServerCAExpiration),
			TenantServerExpiration:   metric.NewGauge(metaTenantServerExpiration),
			TenantClientCAExpiration: metric.NewGauge(metaTenantClientCAExpiration),
			TenantClientExpiration:   metric.NewGauge(metaTenantClientExpiration),
		},
	}
}

type cmOptions struct {
	// tenantIdentifier, if set, specifies the tenant to use for loading tenant
	// client certs.
	tenantIdentifier string
}

// ForTenant is an option to NewCertificateManager which ties the manager to
// the provided tenant. Without this option, tenant client certs are not
// available.
func ForTenant(tenantIdentifier string) func(*cmOptions) {
	return func(opts *cmOptions) {
		opts.tenantIdentifier = tenantIdentifier
	}
}

// NewCertificateManager creates a new certificate manager.
func NewCertificateManager(certsDir string, opts ...func(*cmOptions)) (*CertificateManager, error) {
	cm := makeCertificateManager(certsDir, opts...)
	return cm, cm.LoadCertificates()
}

// NewCertificateManagerFirstRun creates a new certificate manager.
// The certsDir is created if it does not exist.
// This should only be called when generating certificates, the server has
// no business creating the certs directory.
func NewCertificateManagerFirstRun(
	certsDir string, opts ...func(*cmOptions),
) (*CertificateManager, error) {
	cm := makeCertificateManager(certsDir, opts...)
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

// A CertsLocator provides locations to certificates.
type CertsLocator struct {
	certsDir string // os.ExpandEnv'ed
}

// MakeCertsLocator initializes a CertsLocator.
func MakeCertsLocator(certsDir string) CertsLocator {
	return CertsLocator{certsDir: os.ExpandEnv(certsDir)}
}

// CACertPath returns the expected file path for the CA certificate.
func (cl CertsLocator) CACertPath() string {
	return filepath.Join(cl.certsDir, CACertFilename())
}

// CACertFilename returns the expected file name for the CA certificate.
func CACertFilename() string { return "ca" + certExtension }

// TenantServerCACertPath returns the expected file path for the Tenant server
// CA certificate.
func (cl CertsLocator) TenantServerCACertPath() string {
	return filepath.Join(cl.certsDir, TenantServerCACertFilename())
}

// TenantServerCACertFilename returns the expected file name for the Tenant server CA
// certificate.
func TenantServerCACertFilename() string {
	return "ca-server-tenant" + certExtension
}

// TenantClientCACertPath returns the expected file path for the Tenant client CA
// certificate.
func (cl CertsLocator) TenantClientCACertPath() string {
	return filepath.Join(cl.certsDir, TenantClientCACertFilename())
}

// TenantClientCACertFilename returns the expected file name for the Tenant CA
// certificate.
func TenantClientCACertFilename() string {
	return "ca-client-tenant" + certExtension
}

// ClientCACertPath returns the expected file path for the CA certificate
// used to verify client certificates.
func (cl CertsLocator) ClientCACertPath() string {
	return filepath.Join(cl.certsDir, "ca-client"+certExtension)
}

// UICACertPath returns the expected file path for the CA certificate
// used to verify Admin UI certificates.
func (cl CertsLocator) UICACertPath() string {
	return filepath.Join(cl.certsDir, "ca-ui"+certExtension)
}

// NodeCertPath returns the expected file path for the node certificate.
func (cl CertsLocator) NodeCertPath() string {
	return filepath.Join(cl.certsDir, NodeCertFilename())
}

// NodeCertFilename returns the expected file name for the node certificate.
func NodeCertFilename() string {
	return "node" + certExtension
}

// NodeKeyPath returns the expected file path for the node key.
func (cl CertsLocator) NodeKeyPath() string {
	return filepath.Join(cl.certsDir, NodeKeyFilename())
}

// NodeKeyFilename returns the expected file name for the node key.
func NodeKeyFilename() string {
	return "node" + keyExtension
}

// TenantServerCertPath returns the expected file path for the tenant server
// certificate.
func (cl CertsLocator) TenantServerCertPath() string {
	return filepath.Join(cl.certsDir, TenantServerCertFilename())
}

// TenantServerCertFilename returns the expected file name for the tenant server
// certificate.
func TenantServerCertFilename() string {
	return "server-tenant" + certExtension
}

// TenantServerKeyPath returns the expected file path for the tenant server key.
func (cl CertsLocator) TenantServerKeyPath() string {
	return filepath.Join(cl.certsDir, TenantServerKeyFilename())
}

// TenantServerKeyFilename returns the expected file name for the tenant server
// key.
func TenantServerKeyFilename() string {
	return "server-tenant" + keyExtension
}

// UICertPath returns the expected file path for the UI certificate.
func (cl CertsLocator) UICertPath() string {
	return filepath.Join(cl.certsDir, "ui"+certExtension)
}

// UIKeyPath returns the expected file path for the UI key.
func (cl CertsLocator) UIKeyPath() string {
	return filepath.Join(cl.certsDir, "ui"+keyExtension)
}

// TenantClientCertPath returns the expected file path for the user's certificate.
func (cl CertsLocator) TenantClientCertPath(tenantIdentifier string) string {
	return filepath.Join(cl.certsDir, TenantClientCertFilename(tenantIdentifier))
}

// TenantClientCertFilename returns the expected file name for the user's certificate.
func TenantClientCertFilename(tenantIdentifier string) string {
	return "client-tenant." + tenantIdentifier + certExtension
}

// TenantClientKeyPath returns the expected file path for the tenant's key.
func (cl CertsLocator) TenantClientKeyPath(tenantIdentifier string) string {
	return filepath.Join(cl.certsDir, TenantClientKeyFilename(tenantIdentifier))
}

// TenantClientKeyFilename returns the expected file name for the user's key.
func TenantClientKeyFilename(tenantIdentifier string) string {
	return "client-tenant." + tenantIdentifier + keyExtension
}

// ClientCertPath returns the expected file path for the user's certificate.
func (cl CertsLocator) ClientCertPath(user string) string {
	return filepath.Join(cl.certsDir, ClientCertFilename(user))
}

// ClientCertFilename returns the expected file name for the user's certificate.
func ClientCertFilename(user string) string { return "client." + user + certExtension }

// ClientKeyPath returns the expected file path for the user's key.
func (cl CertsLocator) ClientKeyPath(user string) string {
	return filepath.Join(cl.certsDir, ClientKeyFilename(user))
}

// ClientKeyFilename returns the expected file name for the user's key.
func ClientKeyFilename(user string) string { return "client." + user + keyExtension }

// CACert returns the CA cert. May be nil.
// Callers should check for an internal Error field.
func (cm *CertificateManager) CACert() *CertInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.caCert
}

// ClientCACert returns the CA cert used to verify client certificates. May be nil.
// Callers should check for an internal Error field.
func (cm *CertificateManager) ClientCACert() *CertInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.clientCACert
}

// UICACert returns the CA cert used to verify the Admin UI certificate. May be nil.
// Callers should check for an internal Error field.
func (cm *CertificateManager) UICACert() *CertInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.uiCACert
}

// UICert returns the certificate used by the Admin UI. May be nil.
// Callers should check for an internal Error field.
func (cm *CertificateManager) UICert() *CertInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.uiCert
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

// Error is the error type for this package.
// TODO(knz): make this an error wrapper.
type Error struct {
	Message string
	Err     error
}

// Error implements the error interface.
func (e *Error) Error() string {
	return fmt.Sprintf("%s: %v", e.Message, e.Err)
}

// makeErrorf constructs an Error and returns it.
func makeErrorf(err error, format string, args ...interface{}) *Error {
	return &Error{
		Message: fmt.Sprintf(format, args...),
		Err:     err,
	}
}

// makeError constructs an Error with just a string.
func makeError(err error, s string) *Error { return makeErrorf(err, "%s", s) }

// LoadCertificates creates a CertificateLoader to load all certs and keys.
// Upon success, it swaps the existing certificates for the new ones.
func (cm *CertificateManager) LoadCertificates() error {
	cl := NewCertificateLoader(cm.certsDir)
	if err := cl.Load(); err != nil {
		return makeErrorf(err, "problem loading certs directory %s", cm.certsDir)
	}

	var caCert, clientCACert, uiCACert, nodeCert, uiCert, nodeClientCert *CertInfo
	var tenantServerCACert, tenantServerCert, tenantClientCACert, tenantClientCert *CertInfo
	clientCerts := make(map[string]*CertInfo)
	for _, ci := range cl.Certificates() {
		switch ci.FileUsage {
		case CAPem:
			caCert = ci
		case ClientCAPem:
			clientCACert = ci
		case UICAPem:
			uiCACert = ci
		case NodePem:
			nodeCert = ci
		case TenantServerCAPem:
			tenantServerCACert = ci
		case TenantServerPem:
			tenantServerCert = ci
		case TenantClientPem:
			// When there are multiple tenant client certs, pick the one we need only.
			// In practice, this is expected only during testing, when we share a certs
			// dir between multiple tenants.
			if ci.Name == cm.tenantIdentifier {
				tenantClientCert = ci
			}
		case TenantClientCAPem:
			tenantClientCACert = ci
		case UIPem:
			uiCert = ci
		case ClientPem:
			clientCerts[ci.Name] = ci
			if ci.Name == NodeUser {
				nodeClientCert = ci
			}
		default:
			return errors.Errorf("unsupported certificate %v", ci.Filename)
		}
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.initialized {
		// If we ran before, make sure we don't reload with missing/bad certificates.
		if err := checkCertIsValid(caCert); checkCertIsValid(cm.caCert) == nil && err != nil {
			return makeError(err, "reload would lose valid CA cert")
		}
		if err := checkCertIsValid(nodeCert); checkCertIsValid(cm.nodeCert) == nil && err != nil {
			return makeError(err, "reload would lose valid node cert")
		}
		if err := checkCertIsValid(nodeClientCert); checkCertIsValid(cm.nodeClientCert) == nil && err != nil {
			return makeErrorf(err, "reload would lose valid client cert for '%s'", NodeUser)
		}
		if err := checkCertIsValid(clientCACert); checkCertIsValid(cm.clientCACert) == nil && err != nil {
			return makeError(err, "reload would lose valid CA certificate for client verification")
		}
		if err := checkCertIsValid(uiCACert); checkCertIsValid(cm.uiCACert) == nil && err != nil {
			return makeError(err, "reload would lose valid CA certificate for UI")
		}
		if err := checkCertIsValid(uiCert); checkCertIsValid(cm.uiCert) == nil && err != nil {
			return makeError(err, "reload would lose valid UI certificate")
		}

		if err := checkCertIsValid(tenantServerCACert); checkCertIsValid(cm.tenantServerCACert) == nil && err != nil {
			return makeError(err, "reload would lose valid tenant server CA certificate")
		}
		if err := checkCertIsValid(tenantServerCert); checkCertIsValid(cm.tenantServerCert) == nil && err != nil {
			return makeError(err, "reload would lose valid tenant server certificate")
		}
		if err := checkCertIsValid(tenantClientCACert); checkCertIsValid(cm.tenantClientCACert) == nil && err != nil {
			return makeError(err, "reload would lose valid tenant client CA certificate")
		}
		if err := checkCertIsValid(tenantClientCert); checkCertIsValid(cm.tenantClientCert) == nil && err != nil {
			return makeError(err, "reload would lose valid tenant client certificate")
		}
	}

	if tenantClientCert == nil && cm.tenantIdentifier != "" {
		return makeErrorf(errors.New("tenant client cert not found"), "for %s", cm.tenantIdentifier)
	}

	if nodeClientCert == nil && nodeCert != nil {
		// No client certificate for node, but we have a node certificate. Check that
		// it contains the required client fields.
		if err := validateDualPurposeNodeCert(nodeCert); err != nil {
			return err
		}
	}

	// Swap everything.
	cm.caCert = caCert
	cm.clientCACert = clientCACert
	cm.uiCACert = uiCACert

	cm.nodeCert = nodeCert
	cm.nodeClientCert = nodeClientCert
	cm.uiCert = uiCert
	cm.clientCerts = clientCerts

	cm.initialized = true

	cm.serverConfig = nil
	cm.uiServerConfig = nil
	cm.clientConfig = nil

	cm.tenantClientConfig = nil
	cm.tenantServerConfig = nil

	cm.tenantServerCACert = tenantServerCACert
	cm.tenantServerCert = tenantServerCert
	cm.tenantClientCACert = tenantClientCACert
	cm.tenantClientCert = tenantClientCert

	cm.updateMetricsLocked()
	return nil
}

// updateMetricsLocked updates the values on the certificate metrics.
// The metrics may not exist (eg: in tests that build their own CertificateManager).
// If the corresponding certificate is missing or invalid (Error != nil), we reset the
// metric to zero.
// cm.mu must be held to protect the certificates. Metrics do their own atomicity.
func (cm *CertificateManager) updateMetricsLocked() {
	maybeSetMetric := func(m *metric.Gauge, ci *CertInfo) {
		if m == nil {
			return
		}
		if ci != nil && ci.Error == nil {
			m.Update(ci.ExpirationTime.Unix())
		} else {
			m.Update(0)
		}
	}

	// CA certificate expiration.
	maybeSetMetric(cm.certMetrics.CAExpiration, cm.caCert)

	// Client CA certificate expiration.
	maybeSetMetric(cm.certMetrics.ClientCAExpiration, cm.clientCACert)

	// UI CA certificate expiration.
	maybeSetMetric(cm.certMetrics.UICAExpiration, cm.uiCACert)

	// Node certificate expiration.
	// TODO(marc): we need to examine the entire certificate chain here, if the CA cert
	// used to sign the node cert expires sooner, then that is the expiration time to report.
	maybeSetMetric(cm.certMetrics.NodeExpiration, cm.nodeCert)

	// Node client certificate expiration.
	maybeSetMetric(cm.certMetrics.NodeClientExpiration, cm.nodeClientCert)

	// UI certificate expiration.
	maybeSetMetric(cm.certMetrics.UIExpiration, cm.uiCert)
}

// GetServerTLSConfig returns a server TLS config with a callback to fetch the
// latest TLS config. We still attempt to get the config to make sure
// the initial call has a valid config loaded.
func (cm *CertificateManager) GetServerTLSConfig() (*tls.Config, error) {
	if _, err := cm.getEmbeddedServerTLSConfig(nil); err != nil {
		return nil, err
	}
	return &tls.Config{
		GetConfigForClient: cm.getEmbeddedServerTLSConfig,
	}, nil
}

// getEmbeddedServerTLSConfig returns the most up-to-date server tls.Config.
// This is the callback set in tls.Config.GetConfigForClient. We currently
// ignore the ClientHelloInfo object.
func (cm *CertificateManager) getEmbeddedServerTLSConfig(
	_ *tls.ClientHelloInfo,
) (*tls.Config, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.serverConfig != nil {
		return cm.serverConfig, nil
	}

	ca, err := cm.getCACertLocked()
	if err != nil {
		return nil, err
	}

	nodeCert, err := cm.getNodeCertLocked()
	if err != nil {
		return nil, err
	}

	clientCA, err := cm.getClientCACertLocked()
	if err != nil {
		return nil, err
	}

	cfg, err := newServerTLSConfig(
		nodeCert.FileContents,
		nodeCert.KeyFileContents,
		ca.FileContents,
		clientCA.FileContents)
	if err != nil {
		return nil, err
	}

	cm.serverConfig = cfg
	return cfg, nil
}

// GetTenantServerTLSConfig returns a server TLS config with a callback to fetch
// the latest tenant server TLS config. We still attempt to get the config to
// make sure the initial call has a valid config loaded.
func (cm *CertificateManager) GetTenantServerTLSConfig() (*tls.Config, error) {
	f := cm.getEmbeddedTenantServerTLSConfig
	if _, err := f(nil); err != nil {
		return nil, err
	}
	return &tls.Config{
		GetConfigForClient: f,
		// NB: this is needed to use (*http.Server).ServeTLS, which tries to load
		// a certificate eagerly from the supplied strings (which are empty in
		// our case) unless:
		//
		// 	(len(config.Certificates) > 0 || config.GetCertificate != nil) == true
		//
		// TODO(tbg): should we generally do this for all server certs? The docs
		// are not clear whether this is a bug or feature.
		GetCertificate: func(hi *tls.ClientHelloInfo) (*tls.Certificate, error) {
			return nil, nil
		},
	}, nil
}

// getEmbeddedTenantServerTLSConfig is like getEmbeddedServerTLSConfig, but
// for serving tenants.
func (cm *CertificateManager) getEmbeddedTenantServerTLSConfig(
	_ *tls.ClientHelloInfo,
) (*tls.Config, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.tenantServerConfig != nil {
		return cm.tenantServerConfig, nil
	}

	serverCA, err := cm.getTenantServerCACertLocked()
	if err != nil {
		return nil, err
	}

	serverCert, err := cm.getTenantServerCertLocked()
	if err != nil {
		return nil, err
	}

	tenantCA, err := cm.getTenantClientCACertLocked()
	if err != nil {
		return nil, err
	}

	cfg, err := newServerTLSConfig(
		serverCert.FileContents,
		serverCert.KeyFileContents,
		serverCA.FileContents,
		tenantCA.FileContents)
	if err != nil {
		return nil, err
	}

	cm.tenantServerConfig = cfg
	return cfg, nil
}

// GetUIServerTLSConfig returns a server TLS config for the Admin UI with a
// callback to fetch the latest TLS config. We still attempt to get the config to make sure
// the initial call has a valid config loaded.
func (cm *CertificateManager) GetUIServerTLSConfig() (*tls.Config, error) {
	if _, err := cm.getEmbeddedUIServerTLSConfig(nil); err != nil {
		return nil, err
	}
	return &tls.Config{
		GetConfigForClient: cm.getEmbeddedUIServerTLSConfig,
	}, nil
}

// getEmbeddedUIServerTLSConfig returns the most up-to-date server tls.Config for the Admin UI.
// This is the callback set in tls.Config.GetConfigForClient. We currently
// ignore the ClientHelloInfo object.
func (cm *CertificateManager) getEmbeddedUIServerTLSConfig(
	_ *tls.ClientHelloInfo,
) (*tls.Config, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.uiServerConfig != nil {
		return cm.uiServerConfig, nil
	}

	uiCert, err := cm.getUICertLocked()
	if err != nil {
		return nil, err
	}

	cfg, err := newUIServerTLSConfig(
		uiCert.FileContents,
		uiCert.KeyFileContents)
	if err != nil {
		return nil, err
	}

	cm.uiServerConfig = cfg
	return cfg, nil
}

// getCACertLocked returns the general CA cert.
// cm.mu must be held.
func (cm *CertificateManager) getCACertLocked() (*CertInfo, error) {
	if err := checkCertIsValid(cm.caCert); err != nil {
		return nil, makeError(err, "problem with CA certificate")
	}
	return cm.caCert, nil
}

// getClientCACertLocked returns the CA cert used to verify client certificates.
// Use the client CA if it exists, otherwise fall back on the general CA.
// cm.mu must be held.
func (cm *CertificateManager) getClientCACertLocked() (*CertInfo, error) {
	if cm.clientCACert == nil {
		// No client CA: use general CA.
		return cm.getCACertLocked()
	}

	if err := checkCertIsValid(cm.clientCACert); err != nil {
		return nil, makeError(err, "problem with client CA certificate")
	}
	return cm.clientCACert, nil
}

// getUICACertLocked returns the CA cert for the Admin UI.
// Use the UI CA if it exists, otherwise fall back on the general CA.
// cm.mu must be held.
func (cm *CertificateManager) getUICACertLocked() (*CertInfo, error) {
	if cm.uiCACert == nil {
		// No UI CA: use general CA.
		return cm.getCACertLocked()
	}

	if err := checkCertIsValid(cm.uiCACert); err != nil {
		return nil, makeError(err, "problem with UI CA certificate")
	}
	return cm.uiCACert, nil
}

// getNodeCertLocked returns the node certificate.
// cm.mu must be held.
func (cm *CertificateManager) getNodeCertLocked() (*CertInfo, error) {
	if err := checkCertIsValid(cm.nodeCert); err != nil {
		return nil, makeError(err, "problem with node certificate")
	}
	return cm.nodeCert, nil
}

// getUICertLocked returns the UI certificate if present, otherwise returns
// the node certificate.
// cm.mu must be held.
func (cm *CertificateManager) getUICertLocked() (*CertInfo, error) {
	if cm.uiCert == nil {
		// No UI certificate: use node certificate.
		return cm.getNodeCertLocked()
	}
	if err := checkCertIsValid(cm.uiCert); err != nil {
		return nil, makeError(err, "problem with UI certificate")
	}
	return cm.uiCert, nil
}

// getClientCertLocked returns the client cert/key for the specified user,
// or an error if not found.
// cm.mu must be held.
func (cm *CertificateManager) getClientCertLocked(user string) (*CertInfo, error) {
	ci := cm.clientCerts[user]
	if err := checkCertIsValid(ci); err != nil {
		return nil, makeErrorf(err, "problem with client cert for user %s", user)
	}

	return ci, nil
}

// getNodeClientCertLocked returns the client cert/key for the node user.
// Use the client certificate for 'node' if it exists, otherwise use
// the node certificate which should be a combined client/server certificate.
// cm.mu must be held.
func (cm *CertificateManager) getNodeClientCertLocked() (*CertInfo, error) {
	if cm.nodeClientCert == nil {
		// No specific client cert for 'node': use multi-purpose node cert.
		return cm.getNodeCertLocked()
	}

	if err := checkCertIsValid(cm.nodeClientCert); err != nil {
		return nil, makeError(err, "problem with node client certificate")
	}
	return cm.nodeClientCert, nil
}

// getTenantCACertLocked returns the node's CA cert.
// cm.mu must be held.
func (cm *CertificateManager) getTenantServerCACertLocked() (*CertInfo, error) {
	c := cm.tenantServerCACert
	if err := checkCertIsValid(c); err != nil {
		return nil, makeError(err, "problem with tenant CA certificate")
	}
	return c, nil
}

// getTenantNodeCertLocked returns the tenant node cert.
// cm.mu must be held.
func (cm *CertificateManager) getTenantServerCertLocked() (*CertInfo, error) {
	c := cm.tenantServerCert
	if err := checkCertIsValid(c); err != nil {
		return nil, makeError(err, "problem with tenant server certificate")
	}
	return c, nil
}

// getTenantClientCACertLocked returns the CA cert used to verify tenant client
// certificates. Use the client CA if it exists, otherwise fall back on the
// general CA. cm.mu must be held.
func (cm *CertificateManager) getTenantClientCACertLocked() (*CertInfo, error) {
	c := cm.tenantClientCACert
	if err := checkCertIsValid(c); err != nil {
		return nil, makeError(err, "problem with tenant client CA certificate")
	}
	return c, nil
}

// getTenantClientCertLocked returns the tenant node cert.
// cm.mu must be held.
func (cm *CertificateManager) getTenantClientCertLocked() (*CertInfo, error) {
	c := cm.tenantClientCert
	if err := checkCertIsValid(c); err != nil {
		return nil, makeError(err, "problem with tenant client certificate")
	}
	return c, nil
}

// GetTenantClientTLSConfig returns the most up-to-date tenant client
// tls.Config.
func (cm *CertificateManager) GetTenantClientTLSConfig() (*tls.Config, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.tenantClientConfig != nil {
		return cm.tenantClientConfig, nil
	}

	ca, err := cm.getTenantServerCACertLocked()
	if err != nil {
		return nil, err
	}

	tenantClientCert, err := cm.getTenantClientCertLocked()
	if err != nil {
		return nil, err
	}

	cfg, err := newClientTLSConfig(
		tenantClientCert.FileContents,
		tenantClientCert.KeyFileContents,
		ca.FileContents)
	if err != nil {
		return nil, err
	}

	cm.tenantClientConfig = cfg
	return cfg, nil
}

// GetClientTLSConfig returns the most up-to-date client tls.Config.
// Returns the dual-purpose node certs if user == NodeUser and there is no
// separate client cert for 'node'.
func (cm *CertificateManager) GetClientTLSConfig(user string) (*tls.Config, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// We always need the CA cert.
	ca, err := cm.getCACertLocked()
	if err != nil {
		return nil, err
	}

	if user != NodeUser {
		clientCert, err := cm.getClientCertLocked(user)
		if err != nil {
			return nil, err
		}

		cfg, err := newClientTLSConfig(
			clientCert.FileContents,
			clientCert.KeyFileContents,
			ca.FileContents)
		if err != nil {
			return nil, err
		}

		return cfg, nil
	}

	// We're the node user:
	// Return the cached config if we have one.
	if cm.clientConfig != nil {
		return cm.clientConfig, nil
	}

	clientCert, err := cm.getNodeClientCertLocked()
	if err != nil {
		return nil, err
	}

	cfg, err := newClientTLSConfig(
		clientCert.FileContents,
		clientCert.KeyFileContents,
		ca.FileContents)
	if err != nil {
		return nil, err
	}

	// Cache the config.
	cm.clientConfig = cfg
	return cfg, nil
}

// GetUIClientTLSConfig returns the most up-to-date client tls.Config for Admin UI clients.
// It does not include a client certificate and uses the UI CA certificate if present.
func (cm *CertificateManager) GetUIClientTLSConfig() (*tls.Config, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// We always need the CA cert.
	uiCA, err := cm.getUICACertLocked()
	if err != nil {
		return nil, err
	}

	cfg, err := newUIClientTLSConfig(uiCA.FileContents)
	if err != nil {
		return nil, err
	}

	return cfg, nil
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
	if cm.clientCACert != nil {
		ret = append(ret, cm.clientCACert)
	}
	if cm.uiCACert != nil {
		ret = append(ret, cm.uiCACert)
	}
	if cm.nodeCert != nil {
		ret = append(ret, cm.nodeCert)
	}
	if cm.uiCert != nil {
		ret = append(ret, cm.uiCert)
	}
	if cm.clientCerts != nil {
		for _, cert := range cm.clientCerts {
			ret = append(ret, cert)
		}
	}

	return ret, nil
}
