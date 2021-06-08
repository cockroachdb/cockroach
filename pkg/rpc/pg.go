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
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
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

		if caCertPath == "" {
			// Fetch CA cert. This is required to exist, so try to load it. We use
			// the fact that GetCertificateManager checks that "some certs" exist
			// and want to return "its error" here since we test it in
			// test_url_db_override.tcl.
			if _, err := ctx.GetCertificateManager(); err != nil {
				return wrapError(err)
			}
			caCertPath = ctx.CACertPath()
		}

		// (Re)populate the transport information.
		u.WithTransport(pgurl.TransportTLS(tlsMode, caCertPath))

		// Fetch client certs, but don't fail if they're absent, we may be
		// using a password.
		certPath, keyPath := ctx.getClientCertPaths(username)
		var missing bool // certs found on file system?
		loader := security.GetAssetLoader()
		for _, f := range []string{certPath, keyPath} {
			if _, err := loader.Stat(f); err != nil {
				missing = true
			}
		}
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
	host, port, _ := netutil.SplitHostPort(ctx.config.SQLAdvertiseAddr, base.DefaultPort)
	u := pgurl.New().
		WithNet(pgurl.NetTCP(host, port)).
		WithDatabase(catalogkeys.DefaultDatabaseName)

	username, _ := security.MakeSQLUsernameFromUserInput(user.Username(), security.UsernameValidation)
	if err := ctx.LoadSecurityOptions(u, username); err != nil {
		return nil, err
	}
	return u, nil
}
