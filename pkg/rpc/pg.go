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

	"github.com/cockroachdb/cockroach/pkg/security"
)

// LoadSecurityOptions extends a url.Values with SSL settings suitable for
// the given server config.
func (ctx *SecurityContext) LoadSecurityOptions(options url.Values, username string) error {
	if ctx.config.Insecure {
		options.Set("sslmode", "disable")
		options.Del("sslrootcert")
		options.Del("sslcert")
		options.Del("sslkey")
	} else {
		sslMode := options.Get("sslmode")
		if sslMode == "" || sslMode == "disable" {
			options.Set("sslmode", "verify-full")
		}

		if sslMode != "require" {
			// verify-ca and verify-full need a CA certificate.
			if options.Get("sslrootcert") == "" {
				// Fetch CA cert. This is required to exist, so try to load it. We use
				// the fact that GetCertificateManager checks that "some certs" exist
				// and want to return "its error" here since we test it in
				// test_url_db_override.tcl.
				if _, err := ctx.GetCertificateManager(); err != nil {
					return wrapError(err)
				}
				options.Set("sslrootcert", ctx.CACertPath())
			}
		} else {
			// require does not check the CA.
			options.Del("sslrootcert")
		}

		// Fetch certs, but don't fail if they're absent, we may be using a
		// password.
		certPath, keyPath := ctx.getClientCertPaths(username)
		var missing bool // certs found on file system?
		loader := security.GetAssetLoader()
		for _, f := range []string{certPath, keyPath} {
			if _, err := loader.Stat(f); err != nil {
				missing = true
			}
		}
		if !missing {
			if options.Get("sslcert") == "" {
				options.Set("sslcert", certPath)
			}
			if options.Get("sslkey") == "" {
				options.Set("sslkey", keyPath)
			}
		}
	}
	return nil
}

// PGURL constructs a URL for the postgres endpoint, given a server
// config. There is no default database set.
func (ctx *SecurityContext) PGURL(user *url.Userinfo) (*url.URL, error) {
	options := url.Values{}
	if err := ctx.LoadSecurityOptions(options, user.Username()); err != nil {
		return nil, err
	}
	return &url.URL{
		Scheme:   "postgresql",
		User:     user,
		Host:     ctx.config.SQLAdvertiseAddr,
		RawQuery: options.Encode(),
	}, nil
}
