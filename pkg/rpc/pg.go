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
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
)

// LoadSecurityOptions extends a url.Values with SSL settings suitable for
// the given server config.
func (ctx *SecurityContext) LoadSecurityOptions(u *pgurl.URL, username security.SQLUsername) error {
	u.WithUsername(username.Normalized())
	if ctx.config.Insecure {
		u.WithInsecure()
	} else if net, _, _ := u.GetNetworking(); net == pgurl.ProtoTCP {
		tlsUsed, tlsMode, caCertPath := u.GetTLSOptions()
		if !tlsUsed {
			// TLS explicitly disabled by client. Nothing to do here.
			return nil
		}
		// Default is to verify the server's identity.
		if tlsMode == pgurl.TLSUnspecified {
			tlsMode = pgurl.TLSVerifyFull
		}

		// Only verify-full and verify-ca should be doing certificate verification.
		if tlsMode == pgurl.TLSVerifyFull || tlsMode == pgurl.TLSVerifyCA {
			if caCertPath == "" {
				// Fetch CA cert. This is required to exist, so try to load it. Because
				// the certs dir could not be loaded from options we check from the context.
				// GetCertificateManager checks that the SecurityContext has a certificate
				// manager with certs in the certsDir.
				if _, err := ctx.GetCertificateManager(); err == nil {
					// If GetCertificateManager successfully retrieves certs, then we can
					// just set the caCertPath option to the SecurityContext cert path.
					// Otherwise, we simply continue with no error and no caCertPath set.
					// Go will find the the server certificates within the OS
					// trust store, using CertificateManager's lazy initialization which uses
					// tls.Config from the crypto/tls package to load Server CA's from the OS
					// if they are not specified.
					// Documentation of tls.Config: https://pkg.go.dev/crypto/tls#Config
					caCertPath = ctx.CACertPath()
				}
			}
		}

		// (Re)populate the transport information.
		u.WithTransport(pgurl.TransportTLS(tlsMode, caCertPath))

		var missing bool // certs found on file system?
		loader := security.GetAssetLoader()

		// Fetch client certs, but don't fail if they're absent, we may be
		// using a password.
		certPath := ctx.ClientCertPath(username)
		keyPath := ctx.ClientKeyPath(username)
		_, err1 := loader.Stat(certPath)
		_, err2 := loader.Stat(keyPath)
		if err1 != nil || err2 != nil {
			missing = true
		}
		// If the command specifies user node, and we did not find
		// client.node.crt, try with just node.crt.
		if missing && username.IsNodeUser() {
			missing = false
			certPath = ctx.NodeCertPath()
			keyPath = ctx.NodeKeyPath()
			_, err1 = loader.Stat(certPath)
			_, err2 = loader.Stat(keyPath)
			if err1 != nil || err2 != nil {
				missing = true
			}
		}

		// If we found some certs, add them to the URL authentication
		// method.
		if !missing {
			pwEnabled, hasPw, pwd := u.GetAuthnPassword()
			if !pwEnabled {
				u.WithAuthn(pgurl.AuthnClientCert(certPath, keyPath))
			} else {
				u.WithAuthn(pgurl.AuthnPasswordAndCert(certPath, keyPath, hasPw, pwd))
			}
		}
	}
	return nil
}

// PGURL constructs a URL for the postgres endpoint, given a server
// config. There is no default database set.
func (ctx *SecurityContext) PGURL(user *url.Userinfo) (*pgurl.URL, error) {
	host, port, _ := addr.SplitHostPort(ctx.config.SQLAdvertiseAddr, base.DefaultPort)
	u := pgurl.New().
		WithNet(pgurl.NetTCP(host, port)).
		WithDatabase(catalogkeys.DefaultDatabaseName)

	username, _ := security.MakeSQLUsernameFromUserInput(user.Username(), security.UsernameValidation)
	if err := ctx.LoadSecurityOptions(u, username); err != nil {
		return nil, err
	}
	return u, nil
}
