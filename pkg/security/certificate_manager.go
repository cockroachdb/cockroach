// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security

import (
	"context"
	"crypto/tls"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
//   - ca.crt             main CA certificate.
//     Used to verify everything unless overridden by more specific CAs.
//   - ca-client.crt      CA certificate to verify client certificates. If it does not exist,
//     fall back on 'ca.crt'.
//   - node.crt           node certificate.
//     Server-side certificate (always) and client-side certificate unless
//     client.node.crt is found.
//     Verified using 'ca.crt'.
//   - client.<user>.crt  client certificate for 'user'. Verified using 'ca.crt', or 'ca-client.crt'.
//   - client.node.crt    client certificate for the 'node' user. If it does not exist,
//     fall back on 'node.crt'.
type CertificateManager struct {
	tenantIdentifier uint64
	timeSource       timeutil.TimeSource
	certnames.Locator

	tlsSettings TLSSettings

	// The metrics struct is initialized at init time and metrics do their
	// own locking.
	certMetrics *Metrics

	// Client cert expiration cache.
	clientCertExpirationCache *ClientCertExpirationCache

	// mu protects all remaining fields.
	mu syncutil.RWMutex

	// If false, this is the first load. Needed to ensure we do not drop certain certs.
	initialized bool

	// Set of certs. These are swapped in during Load(), and never mutated afterwards.
	caCert         *CertInfo // default CA certificate
	clientCACert   *CertInfo // optional: certificate to verify client certificates
	uiCACert       *CertInfo // optional: certificate to verify UI certificates
	nodeCert       *CertInfo // certificate for nodes (always server cert, sometimes client cert)
	nodeClientCert *CertInfo // optional: client certificate for 'node' user. Also included in 'clientCerts'
	uiCert         *CertInfo // optional: server certificate for the admin UI.
	clientCerts    map[username.SQLUsername]*CertInfo

	// Certs only used with multi-tenancy.
	tenantCACert, tenantCert, tenantSigningCert *CertInfo

	// TLS configs. Initialized lazily. Wiped on every successful Load().
	// Server-side config.
	serverConfig *tls.Config
	// Server-side config for the Admin UI.
	uiServerConfig *tls.Config
	// Client-side config for the cockroach node.
	// All other client tls.Config objects are built as requested and not cached.
	clientConfig *tls.Config
	// Client config for the tenant (if running in a SQL tenant server).
	tenantConfig *tls.Config
}

func makeCertificateManager(
	certsDir string, tlsSettings TLSSettings, opts ...Option,
) *CertificateManager {
	var o cmOptions
	for _, fn := range opts {
		fn(&o)
	}

	cm := &CertificateManager{
		Locator:          certnames.MakeLocator(certsDir),
		tenantIdentifier: o.tenantIdentifier,
		timeSource:       o.timeSource,
		tlsSettings:      tlsSettings,
	}
	cm.certMetrics = createMetricsLocked(cm)
	return cm
}

type cmOptions struct {
	// tenantIdentifier, if set, specifies the tenant to use for loading tenant
	// client certs.
	tenantIdentifier uint64

	// timeSource, if set, specifies the time source with which the metrics are set.
	timeSource timeutil.TimeSource
}

// Option is an option to NewCertificateManager.
type Option func(*cmOptions)

// ForTenant is an option to NewCertificateManager which ties the manager to
// the provided tenant. Without this option, tenant client certs are not
// available.
func ForTenant(tenantIdentifier uint64) Option {
	return func(opts *cmOptions) {
		opts.tenantIdentifier = tenantIdentifier
	}
}

// WithTimeSource allows the caller to pass a time source to be used
// by the Metrics struct (mostly for testing).
func WithTimeSource(ts timeutil.TimeSource) Option {
	return func(opts *cmOptions) {
		opts.timeSource = ts
	}
}

// NewCertificateManager creates a new certificate manager.
func NewCertificateManager(
	certsDir string, tlsSettings TLSSettings, opts ...Option,
) (*CertificateManager, error) {
	cm := makeCertificateManager(certsDir, tlsSettings, opts...)
	return cm, cm.LoadCertificates()
}

// NewCertificateManagerFirstRun creates a new certificate manager.
// The certsDir is created if it does not exist.
// This should only be called when generating certificates, the server has
// no business creating the certs directory.
func NewCertificateManagerFirstRun(
	certsDir string, tlsSettings TLSSettings, opts ...Option,
) (*CertificateManager, error) {
	cm := makeCertificateManager(certsDir, tlsSettings, opts...)
	if err := NewCertificateLoader(cm.CertsDir()).MaybeCreateCertsDir(); err != nil {
		return nil, err
	}

	return cm, cm.LoadCertificates()
}

// IsForTenant returns true iff this certificate manager is handling certs
// for a SQL-only server process.
func (cm *CertificateManager) IsForTenant() bool {
	return cm.tenantIdentifier != 0
}

// Metrics returns the metrics struct.
func (cm *CertificateManager) Metrics() *Metrics {
	return cm.certMetrics
}

// RegisterSignalHandler registers a signal handler for SIGHUP, triggering a
// refresh of the certificates directory on notification.
func (cm *CertificateManager) RegisterSignalHandler(
	ctx context.Context, stopper *stop.Stopper,
) error {
	return stopper.RunAsyncTask(ctx, "refresh-certs", func(ctx context.Context) {
		ch := sysutil.RefreshSignaledChan()
		for {
			select {
			case <-stopper.ShouldQuiesce():
				return
			case sig := <-ch:
				log.Ops.Infof(ctx, "received signal %q, triggering certificate reload", sig)
				if cache := cm.clientCertExpirationCache; cache != nil {
					cache.Clear()
				}
				if err := cm.LoadCertificates(); err != nil {
					log.Ops.Warningf(ctx, "could not reload certificates: %v", err)
					log.StructuredEvent(ctx, severity.INFO, &eventpb.CertsReload{Success: false, ErrorMessage: err.Error()})
				} else {
					log.StructuredEvent(ctx, severity.INFO, &eventpb.CertsReload{Success: true})
				}
			}
		}
	})
}

// RegisterExpirationCache registers a cache for client certificate expiration.
// It is called during server startup.
func (cm *CertificateManager) RegisterExpirationCache(cache *ClientCertExpirationCache) {
	cm.clientCertExpirationCache = cache
}

// MaybeUpsertClientExpiration updates or inserts the expiration time for the
// given client certificate. An update is contingent on whether the old
// expiration is after the new expiration.
func (cm *CertificateManager) MaybeUpsertClientExpiration(
	ctx context.Context, identity string, expiration int64,
) {
	if cache := cm.clientCertExpirationCache; cache != nil {
		cache.MaybeUpsert(ctx,
			identity,
			expiration,
			cm.certMetrics.ClientExpiration,
			cm.certMetrics.ClientTTL,
		)
	}
}

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
func (cm *CertificateManager) ClientCerts() map[username.SQLUsername]*CertInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.clientCerts
}

// ErrCertManagement is a marker error for errors produced in this package that
// can be identified with errors.Is().
var ErrCertManagement error = errCertManagement{}

type errCertManagement struct{}

func (errCertManagement) Error() string { return "error " }

// makeErrorf constructs an Error and returns it.
func makeErrorf(err error, format string, args ...interface{}) error {
	return errors.Mark(errors.Wrapf(err, format, args...), ErrCertManagement)
}

// LoadCertificates creates a CertificateLoader to load all certs and keys.
// Upon success, it swaps the existing certificates for the new ones.
func (cm *CertificateManager) LoadCertificates() error {
	cl := NewCertificateLoader(cm.CertsDir())
	if err := cl.Load(); err != nil {
		return makeErrorf(err, "problem loading certs directory %s", cm.CertsDir())
	}

	var caCert, clientCACert, uiCACert, nodeCert, uiCert, nodeClientCert *CertInfo
	var tenantCACert, tenantCert, tenantSigningCert *CertInfo
	clientCerts := make(map[username.SQLUsername]*CertInfo)
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
		case TenantPem:
			// When there are multiple tenant client certs, pick the one we need only.
			// In practice, this is expected only during testing, when we share a certs
			// dir between multiple tenants.
			tenantID, err := strconv.ParseUint(ci.Name, 10, 64)
			if err != nil {
				return errors.Errorf("invalid tenant id %s", ci.Name)
			}
			if tenantID == cm.tenantIdentifier {
				tenantCert = ci
			}
		case TenantSigningPem:
			// When there are multiple tenant signing certs, pick the one we need only.
			// In practice, this is expected only during testing, when we share a certs
			// dir between multiple tenants.
			tenantID, err := strconv.ParseUint(ci.Name, 10, 64)
			if err != nil {
				return errors.Errorf("invalid tenant id %s", ci.Name)
			}
			if tenantID == cm.tenantIdentifier {
				tenantSigningCert = ci
			}
		case TenantCAPem:
			tenantCACert = ci
		case UIPem:
			uiCert = ci
		case ClientPem:
			username := username.MakeSQLUsernameFromPreNormalizedString(ci.Name)
			clientCerts[username] = ci
			if username.IsNodeUser() {
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
			return makeErrorf(err, "reload would lose valid CA cert")
		}
		if err := checkCertIsValid(nodeCert); checkCertIsValid(cm.nodeCert) == nil && err != nil {
			return makeErrorf(err, "reload would lose valid node cert")
		}
		if err := checkCertIsValid(nodeClientCert); checkCertIsValid(cm.nodeClientCert) == nil && err != nil {
			return makeErrorf(err, "reload would lose valid client cert for '%s'", username.NodeUser)
		}
		if err := checkCertIsValid(clientCACert); checkCertIsValid(cm.clientCACert) == nil && err != nil {
			return makeErrorf(err, "reload would lose valid CA certificate for client verification")
		}
		if err := checkCertIsValid(uiCACert); checkCertIsValid(cm.uiCACert) == nil && err != nil {
			return makeErrorf(err, "reload would lose valid CA certificate for UI")
		}
		if err := checkCertIsValid(uiCert); checkCertIsValid(cm.uiCert) == nil && err != nil {
			return makeErrorf(err, "reload would lose valid UI certificate")
		}

		if err := checkCertIsValid(tenantCACert); checkCertIsValid(cm.tenantCACert) == nil && err != nil {
			return makeErrorf(err, "reload would lose valid tenant client CA certificate")
		}
		if err := checkCertIsValid(tenantCert); checkCertIsValid(cm.tenantCert) == nil && err != nil {
			return makeErrorf(err, "reload would lose valid tenant client certificate")
		}
	}

	if tenantCert == nil && cm.tenantIdentifier != 0 {
		return makeErrorf(errors.New("tenant client cert not found"), "for %d in %s", cm.tenantIdentifier, cm.CertsDir())
	}

	if nodeClientCert == nil && nodeCert != nil {
		// No client certificate for node, but we have a node certificate. Check that
		// if it is a valid certificate and can be used as a client node cert.
		if nodeCert.Error != nil {
			return makeErrorf(nodeCert.Error, "validating node cert")
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

	cm.tenantConfig = nil
	cm.tenantCACert = tenantCACert
	cm.tenantCert = tenantCert
	cm.tenantSigningCert = tenantSigningCert

	return nil
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

	var nodeCert *CertInfo
	if !cm.IsForTenant() {
		// Host cluster.
		nodeCert, err = cm.getNodeCertLocked()
		if err != nil {
			return nil, err
		}
	} else {
		// Tenant server.
		nodeCert, err = cm.getTenantCertLocked()
		if err != nil {
			return nil, err
		}
	}

	clientCA, err := cm.getClientCACertLocked()
	if err != nil {
		return nil, err
	}

	tenantCA, err := cm.getTenantCACertLocked()
	if err != nil {
		return nil, err
	}

	cfg, err := newServerTLSConfig(
		cm.tlsSettings,
		nodeCert.FileContents,
		nodeCert.KeyFileContents,
		ca.FileContents,
		clientCA.FileContents,
		tenantCA.FileContents,
	)
	if err != nil {
		return nil, err
	}

	cm.serverConfig = cfg
	return cfg, nil
}

// GetNodeClientTLSConfig returns a client TLS config suitable for
// dialing other KV nodes.
func (cm *CertificateManager) GetNodeClientTLSConfig() (*tls.Config, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Return the cached config if we have one.
	if cm.clientConfig != nil {
		return cm.clientConfig, nil
	}

	ca, err := cm.getCACertLocked()
	if err != nil {
		return nil, err
	}

	clientCert, err := cm.getNodeClientCertLocked()
	if err != nil {
		return nil, err
	}

	cfg, err := newClientTLSConfig(
		cm.tlsSettings,
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
		cm.tlsSettings,
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
		return nil, makeErrorf(err, "problem with CA certificate")
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
		return nil, makeErrorf(err, "problem with client CA certificate")
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
		return nil, makeErrorf(err, "problem with UI CA certificate")
	}
	return cm.uiCACert, nil
}

// getNodeCertLocked returns the node certificate.
// cm.mu must be held.
func (cm *CertificateManager) getNodeCertLocked() (*CertInfo, error) {
	if err := checkCertIsValid(cm.nodeCert); err != nil {
		return nil, makeErrorf(err, "problem with node certificate")
	}
	return cm.nodeCert, nil
}

// getUICertLocked returns the UI certificate if present, otherwise returns
// the node certificate.
// cm.mu must be held.
func (cm *CertificateManager) getUICertLocked() (*CertInfo, error) {
	if cm.uiCert == nil {
		// No UI certificate: use node certificate.
		if !cm.IsForTenant() {
			// Host cluster.
			return cm.getNodeCertLocked()
		}
		// Tenant server.
		return cm.getTenantCertLocked()
	}
	if err := checkCertIsValid(cm.uiCert); err != nil {
		return nil, makeErrorf(err, "problem with UI certificate")
	}
	return cm.uiCert, nil
}

// getClientCertLocked returns the client cert/key for the specified user,
// or an error if not found.
// This is used both to connect to KV nodes and SQL servers.
// cm.mu must be held.
//
// TODO(catj): split the logic here into two functions.
func (cm *CertificateManager) getClientCertLocked(user username.SQLUsername) (*CertInfo, error) {
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
		return cm.getNodeCertLocked()
	}

	if err := checkCertIsValid(cm.nodeClientCert); err != nil {
		return nil, makeErrorf(err, "problem with node client certificate")
	}
	return cm.nodeClientCert, nil
}

// getTenantCACertLocked returns the CA cert used to verify tenant client
// certificates. Use the tenant client CA if it exists, otherwise fall back to
// client CA. cm.mu must be held.
func (cm *CertificateManager) getTenantCACertLocked() (*CertInfo, error) {
	if cm.tenantCACert == nil {
		return cm.getClientCACertLocked()
	}
	c := cm.tenantCACert
	if err := checkCertIsValid(c); err != nil {
		return nil, makeErrorf(err, "problem with tenant client CA certificate")
	}
	return c, nil
}

// getTenantCertLocked returns the tenant node cert.
// cm.mu must be held.
func (cm *CertificateManager) getTenantCertLocked() (*CertInfo, error) {
	c := cm.tenantCert
	if err := checkCertIsValid(c); err != nil {
		return nil, makeErrorf(err, "problem with tenant client certificate")
	}
	return c, nil
}

// GetTenantTLSConfig returns the most up-to-date tenant client
// tls.Config.
func (cm *CertificateManager) GetTenantTLSConfig() (*tls.Config, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.tenantConfig != nil {
		return cm.tenantConfig, nil
	}

	// CA used to validate certs provided by KV nodes.
	ca, err := cm.getCACertLocked()
	if err != nil {
		return nil, err
	}

	caBlob := ca.FileContents

	if cm.tenantCACert != nil {
		// If it's available, we also include the tenant CA.
		tenantCA, err := cm.getTenantCACertLocked()
		if err == nil {
			caBlob = AppendCertificatesToBlob(caBlob, tenantCA.FileContents)
		}
	}

	// Client cert presented to KV nodes.
	tenantCert, err := cm.getTenantCertLocked()
	if err != nil {
		return nil, err
	}

	cfg, err := newClientTLSConfig(
		cm.tlsSettings,
		tenantCert.FileContents,
		tenantCert.KeyFileContents,
		caBlob)
	if err != nil {
		return nil, err
	}

	cm.tenantConfig = cfg
	return cfg, nil
}

// GetTenantSigningCert returns the most up-to-date tenant signing certificate.
func (cm *CertificateManager) GetTenantSigningCert() (*CertInfo, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	c := cm.tenantSigningCert
	if err := checkCertIsValid(c); err != nil {
		return nil, makeErrorf(err, "problem with tenant signing certificate")
	}
	return c, nil
}

// GetClientTLSConfig returns the most up-to-date client tls.Config.
// Returns the dual-purpose node certs if user == NodeUser and there is no
// separate client cert for 'node'.
func (cm *CertificateManager) GetClientTLSConfig(user username.SQLUsername) (*tls.Config, error) {
	if user.IsNodeUser() {
		return cm.GetNodeClientTLSConfig()
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	// The client could be connecting to a KV node or a tenant server.
	// We need at least one of the two CAs. If none is available, we
	// won't be able to verify the server's identity.
	if cm.caCert == nil && cm.tenantCACert == nil {
		return nil, makeErrorf(errors.New("no CA certificate found, cannot authenticate remote server"),
			"problem loading CA certificate")
	}
	var caBlob []byte
	if cm.caCert != nil {
		ca, err := cm.getCACertLocked()
		if err != nil {
			return nil, err
		}
		caBlob = AppendCertificatesToBlob(caBlob, ca.FileContents)
	}
	if cm.tenantCACert != nil {
		ca, err := cm.getTenantCACertLocked()
		if err != nil {
			return nil, err
		}
		caBlob = AppendCertificatesToBlob(caBlob, ca.FileContents)
	}

	clientCert, err := cm.getClientCertLocked(user)
	if err != nil {
		return nil, err
	}

	cfg, err := newClientTLSConfig(
		cm.tlsSettings,
		clientCert.FileContents,
		clientCert.KeyFileContents,
		caBlob)
	if err != nil {
		return nil, err
	}

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

	caBlob := uiCA.FileContents

	if cm.tenantCACert != nil {
		// If it's available, we also include the tenant CA.
		tenantCA, err := cm.getTenantCACertLocked()
		if err == nil {
			caBlob = AppendCertificatesToBlob(caBlob, tenantCA.FileContents)
		}
	}

	cfg, err := newUIClientTLSConfig(cm.tlsSettings, caBlob)
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
		return nil, errors.AssertionFailedf("certificate manager has not been initialized")
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
