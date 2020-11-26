// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/errors"
)

type lazyHTTPClient struct {
	sync.Once
	httpClient http.Client
	err        error
}

type lazyCertificateManager struct {
	sync.Once
	cm  *security.CertificateManager
	err error
}

func wrapError(err error) error {
	if !errors.HasType(err, (*security.Error)(nil)) {
		return &security.Error{
			Message: "problem using security settings",
			Err:     err,
		}
	}
	return err
}

// SecurityContext is a wrapper providing transport security helpers such as
// the certificate manager.
type SecurityContext struct {
	security.CertsLocator
	security.TLSSettings
	config *base.Config
	tenID  roachpb.TenantID
	lazy   struct {
		// The certificate manager. Must be accessed through GetCertificateManager.
		certificateManager lazyCertificateManager
		// httpClient uses the client TLS config. It is initialized lazily.
		httpClient lazyHTTPClient
	}
}

// MakeSecurityContext makes a SecurityContext.
//
// TODO(tbg): don't take a whole Config. This can be trimmed down significantly.
func MakeSecurityContext(
	cfg *base.Config, tlsSettings security.TLSSettings, tenID roachpb.TenantID,
) SecurityContext {
	return SecurityContext{
		CertsLocator: security.MakeCertsLocator(cfg.SSLCertsDir),
		TLSSettings:  tlsSettings,
		config:       cfg,
		tenID:        tenID,
	}
}

// GetCertificateManager returns the certificate manager, initializing it
// on the first call. If certificates should be used but none are found,
// fails eagerly.
func (ctx *SecurityContext) GetCertificateManager() (*security.CertificateManager, error) {
	ctx.lazy.certificateManager.Do(func() {
		var opts []security.Option
		if ctx.tenID != roachpb.SystemTenantID {
			opts = append(opts, security.ForTenant(ctx.tenID.ToUint64()))
		}
		ctx.lazy.certificateManager.cm, ctx.lazy.certificateManager.err =
			security.NewCertificateManager(ctx.config.SSLCertsDir, ctx, opts...)

		if ctx.lazy.certificateManager.err == nil && !ctx.config.Insecure {
			infos, err := ctx.lazy.certificateManager.cm.ListCertificates()
			if err != nil {
				ctx.lazy.certificateManager.err = err
			} else if len(infos) == 0 {
				// If we know there should be certificates (we're in secure mode)
				// but there aren't any, this likely indicates that the certs dir
				// was misconfigured.
				ctx.lazy.certificateManager.err = errors.New("no certificates found; does certs dir exist?")
			}
		}
	})
	return ctx.lazy.certificateManager.cm, ctx.lazy.certificateManager.err
}

// GetServerTLSConfig returns the server TLS config, initializing it if needed.
// If Insecure is true, return a nil config, otherwise ask the certificate
// manager for a server TLS config.
func (ctx *SecurityContext) GetServerTLSConfig() (*tls.Config, error) {
	// Early out.
	if ctx.config.Insecure {
		return nil, nil
	}

	cm, err := ctx.GetCertificateManager()
	if err != nil {
		return nil, wrapError(err)
	}

	tlsCfg, err := cm.GetServerTLSConfig()
	if err != nil {
		return nil, wrapError(err)
	}
	return tlsCfg, nil
}

// GetClientTLSConfig returns the client TLS config, initializing it if needed.
// If Insecure is true, return a nil config, otherwise ask the certificate
// manager for a TLS config using certs for the config.User.
// This TLSConfig might **NOT** be suitable to talk to the Admin UI, use GetUIClientTLSConfig instead.
func (ctx *SecurityContext) GetClientTLSConfig() (*tls.Config, error) {
	// Early out.
	if ctx.config.Insecure {
		return nil, nil
	}

	cm, err := ctx.GetCertificateManager()
	if err != nil {
		return nil, wrapError(err)
	}

	tlsCfg, err := cm.GetClientTLSConfig(ctx.config.User)
	if err != nil {
		return nil, wrapError(err)
	}
	return tlsCfg, nil
}

// GetTenantClientTLSConfig returns the client TLS config for the tenant, provided
// the SecurityContext operates on behalf of a secondary tenant (i.e. not the
// system tenant).
//
// If Insecure is true, return a nil config, otherwise retrieves the client
// certificate for the configured tenant from the cert manager.
func (ctx *SecurityContext) GetTenantClientTLSConfig() (*tls.Config, error) {
	// Early out.
	if ctx.config.Insecure {
		return nil, nil
	}

	cm, err := ctx.GetCertificateManager()
	if err != nil {
		return nil, wrapError(err)
	}

	tlsCfg, err := cm.GetTenantClientTLSConfig()
	if err != nil {
		return nil, wrapError(err)
	}
	return tlsCfg, nil
}

// getUIClientTLSConfig returns the client TLS config for Admin UI clients, initializing it if needed.
// If Insecure is true, return a nil config, otherwise ask the certificate
// manager for a TLS config configured to talk to the Admin UI.
// This TLSConfig is **NOT** suitable to talk to the GRPC or SQL servers, use GetClientTLSConfig instead.
func (ctx *SecurityContext) getUIClientTLSConfig() (*tls.Config, error) {
	// Early out.
	if ctx.config.Insecure {
		return nil, nil
	}

	cm, err := ctx.GetCertificateManager()
	if err != nil {
		return nil, wrapError(err)
	}

	tlsCfg, err := cm.GetUIClientTLSConfig()
	if err != nil {
		return nil, wrapError(err)
	}
	return tlsCfg, nil
}

// GetUIServerTLSConfig returns the server TLS config for the Admin UI, initializing it if needed.
// If Insecure is true, return a nil config, otherwise ask the certificate
// manager for a server UI TLS config.
//
// TODO(peter): This method is only used by `server.NewServer` and
// `Server.Start`. Move it.
func (ctx *SecurityContext) GetUIServerTLSConfig() (*tls.Config, error) {
	// Early out.
	if ctx.config.Insecure || ctx.config.DisableTLSForHTTP {
		return nil, nil
	}

	cm, err := ctx.GetCertificateManager()
	if err != nil {
		return nil, wrapError(err)
	}

	tlsCfg, err := cm.GetUIServerTLSConfig()
	if err != nil {
		return nil, wrapError(err)
	}
	return tlsCfg, nil
}

// GetHTTPClient returns the http client, initializing it
// if needed. It uses the client TLS config.
func (ctx *SecurityContext) GetHTTPClient() (http.Client, error) {
	ctx.lazy.httpClient.Do(func() {
		ctx.lazy.httpClient.httpClient.Timeout = 10 * time.Second
		var transport http.Transport
		ctx.lazy.httpClient.httpClient.Transport = &transport
		transport.TLSClientConfig, ctx.lazy.httpClient.err = ctx.getUIClientTLSConfig()
	})

	return ctx.lazy.httpClient.httpClient, ctx.lazy.httpClient.err
}

// getClientCertPaths returns the paths to the client cert and key. This uses
// the node certs for the NodeUser, and the actual client certs for all others.
func (ctx *SecurityContext) getClientCertPaths(user security.SQLUsername) (string, string) {
	if user.IsNodeUser() {
		return ctx.NodeCertPath(), ctx.NodeKeyPath()
	}
	return ctx.ClientCertPath(user), ctx.ClientKeyPath(user)
}

// CheckCertificateAddrs validates the addresses inside the configured
// certificates to be compatible with the configured listen and
// advertise addresses. This is an advisory function (to inform/educate
// the user) and not a requirement for security.
// This must also be called after ValidateAddrs() and after
// the certificate manager was initialized.
func (ctx *SecurityContext) CheckCertificateAddrs(cctx context.Context) {
	if ctx.config.Insecure {
		return
	}

	// By now the certificate manager must be initialized.
	cm, _ := ctx.GetCertificateManager()

	// Verify that the listen and advertise addresses are compatible
	// with the provided certificate.
	certInfo := cm.NodeCert()
	if certInfo.Error != nil {
		log.Ops.Shoutf(cctx, severity.ERROR,
			"invalid node certificate: %v", certInfo.Error)
	} else {
		cert := certInfo.ParsedCertificates[0]
		addrInfo := certAddrs(cert)

		// Log the certificate details in any case. This will aid during troubleshooting.
		log.Ops.Infof(cctx, "server certificate addresses: %s", addrInfo)

		var msg bytes.Buffer
		// Verify the compatibility. This requires that ValidateAddrs() has
		// been called already.
		host, _, err := net.SplitHostPort(ctx.config.AdvertiseAddr)
		if err != nil {
			panic(errors.AssertionFailedf("programming error: call ValidateAddrs() first"))
		}
		if err := cert.VerifyHostname(host); err != nil {
			fmt.Fprintf(&msg, "advertise address %q not in node certificate (%s)\n", host, addrInfo)
		}
		host, _, err = net.SplitHostPort(ctx.config.SQLAdvertiseAddr)
		if err != nil {
			panic(errors.AssertionFailedf("programming error: call ValidateAddrs() first"))
		}
		if err := cert.VerifyHostname(host); err != nil {
			fmt.Fprintf(&msg, "advertise SQL address %q not in node certificate (%s)\n", host, addrInfo)
		}
		if msg.Len() > 0 {
			log.Ops.Shoutf(cctx, severity.WARNING,
				"%s"+
					"Secure client connections are likely to fail.\n"+
					"Consider extending the node certificate or tweak --listen-addr/--advertise-addr/--sql-addr/--advertise-sql-addr.",
				msg.String())
		}
	}

	// TODO(tbg): Verify that the tenant listen and advertise addresses are
	// compatible with the provided certificate.

	// Verify that the http listen and advertise addresses are
	// compatible with the provided certificate.
	certInfo = cm.UICert()
	if certInfo == nil {
		// A nil UI cert means use the node cert instead;
		// see details in (*CertificateManager) getEmbeddedUIServerTLSConfig()
		// and (*CertificateManager) getUICertLocked().
		certInfo = cm.NodeCert()
	}
	if certInfo.Error != nil {
		log.Ops.Shoutf(cctx, severity.ERROR,
			"invalid UI certificate: %v", certInfo.Error)
	} else {
		cert := certInfo.ParsedCertificates[0]
		addrInfo := certAddrs(cert)

		// Log the certificate details in any case. This will aid during
		// troubleshooting.
		log.Ops.Infof(cctx, "web UI certificate addresses: %s", addrInfo)
	}
}

// HTTPRequestScheme returns "http" or "https" based on the value of
// Insecure and DisableTLSForHTTP.
func (ctx *SecurityContext) HTTPRequestScheme() string {
	return ctx.config.HTTPRequestScheme()
}

// certAddrs formats the list of addresses included in a certificate for
// printing in an error message.
func certAddrs(cert *x509.Certificate) string {
	// If an IP address was specified as listen/adv address, the
	// hostname validation will only use the IPAddresses field. So this
	// needs to be printed in all cases.
	addrs := make([]string, len(cert.IPAddresses))
	for i, ip := range cert.IPAddresses {
		addrs[i] = ip.String()
	}
	// For names, the hostname validation will use DNSNames if
	// the Subject Alt Name is present in the cert, otherwise
	// it will use the common name. We can't parse the
	// extensions here so we print both.
	return fmt.Sprintf("IP=%s; DNS=%s; CN=%s",
		strings.Join(addrs, ","),
		strings.Join(cert.DNSNames, ","),
		cert.Subject.CommonName)
}
