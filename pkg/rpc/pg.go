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
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/errors"
)

// LoadSecurityOptions extends a url.Values with SSL settings suitable for
// the given server config.
func (ctx *SecurityContext) LoadSecurityOptions(u *pgurl.URL, username username.SQLUsername) error {
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
				// We need a CA certificate.
				// Try to use the cert manager to find one, and if that fails,
				// assume that the Go TLS code will fall back to a OS-level
				// common trust store.

				// First, initialize the cert manager.
				cm, err := ctx.GetCertificateManager()
				if err != nil {
					// The SecurityContext was unable to get a cert manager. We
					// can further distinguish between:
					// - cert manager initialized OK, but contains no certs.
					// - cert manager did not initialize (bad certs dir, file access error etc).
					// The former case is legitimate and we will fall back below.
					// The latter case is a real problem and needs to pop up to the user.
					if !errors.Is(err, errNoCertificatesFound) {
						// The certificate manager could not load properly. Let
						// the user know.
						return err
					}
					// Fall back: cert manager initialized OK, but no certs found.
				}
				if ourCACert := cm.CACert(); ourCACert != nil {
					// The CM has a CA cert. Use that.
					caCertPath = cm.FullPath(ourCACert)
				}
			}
			// Fallback: if caCertPath was not assigned above, either
			// we did not have a certs dir, or it did not contain
			// a CA cert. In that case, we rely on the OS trust store.
			// Documentation of tls.Config:
			//     https://pkg.go.dev/crypto/tls#Config
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

	username, _ := username.MakeSQLUsernameFromUserInput(user.Username(), username.UsernameValidation)
	if err := ctx.LoadSecurityOptions(u, username); err != nil {
		return nil, err
	}
	return u, nil
}
