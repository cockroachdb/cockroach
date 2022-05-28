// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clientconnurl

import (
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
)

// Options defines the configurable connection parameters
// used as input to compute connection URLs.
type Options struct {
	// Insecure corresponds to the --insecure flag.
	Insecure bool

	// CertsDir corresponds to the --certs-dir flag.
	CertsDir string

	// ServerAddr is the configured server address to use if the URL does not contain one.
	ServerAddr string

	// DefaultPort is the port number to use if ServerAddr does not contain one.
	DefaultPort string

	// DefaultDatabase is the default database name to use if the connection URL
	// does not contain one.
	DefaultDatabase string
}

// LoadSecurityOptions extends a url.Values with SSL settings suitable for
// the given server config.
func LoadSecurityOptions(opts *Options, u *pgurl.URL, user username.SQLUsername) error {
	u.WithUsername(user.Normalized())
	if opts.Insecure {
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

		loader := securityassets.GetAssetLoader()
		cl := certnames.MakeCertsLocator(opts.CertsDir)

		// Only verify-full and verify-ca should be doing certificate verification.
		if tlsMode == pgurl.TLSVerifyFull || tlsMode == pgurl.TLSVerifyCA {
			if caCertPath == "" {
				// We need a CA certificate.
				// Try to use the cert locator to find one, and if that fails,
				// assume that the Go TLS code will fall back to a OS-level
				// common trust store.

				exists, err := loader.FileExists(cl.CACertPath())
				if err != nil {
					return err
				}
				if exists {
					// The CL has found a CA cert. Use that.
					caCertPath = cl.CACertPath()
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

		// Fetch client certs, but don't fail if they're absent, we may be
		// using a password.
		certPath := cl.ClientCertPath(user)
		keyPath := cl.ClientKeyPath(user)
		_, err1 := loader.Stat(certPath)
		_, err2 := loader.Stat(keyPath)
		if err1 != nil || err2 != nil {
			missing = true
		}
		// If the command specifies user node, and we did not find
		// client.node.crt, try with just node.crt.
		if missing && user.IsNodeUser() {
			missing = false
			certPath = cl.NodeCertPath()
			keyPath = cl.NodeKeyPath()
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
func PGURL(opts *Options, user *url.Userinfo) (*pgurl.URL, error) {
	host, port, _ := addr.SplitHostPort(opts.ServerAddr, opts.DefaultPort)
	u := pgurl.New().
		WithNet(pgurl.NetTCP(host, port)).
		WithDatabase(opts.DefaultDatabase)

	username, _ := username.MakeSQLUsernameFromUserInput(user.Username(), username.PurposeValidation)
	if err := LoadSecurityOptions(opts, u, username); err != nil {
		return nil, err
	}
	return u, nil
}
