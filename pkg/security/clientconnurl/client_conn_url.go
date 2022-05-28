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
	"fmt"
	"net/url"
	"path/filepath"

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

// UpdateURL is a helper that processes a connection URL passed on the
// command line, integrates any defaults set by earlier command-line arguments,
// then calls callbacks depending on which details were found in the new URL.
//
// sslStrict, when set to true, requires that the SSL file paths in
// a URL clearly map to a single certificate directory and restricts
// the set of supported SSL modes to just "disable" and "require".
//
// This is set for all non-SQL client commands, which only support
// the insecure boolean and certs-dir with maximum SSL validation.
func UpdateURL(
	newURL string,
	sslStrict bool,
	makeStrictErr func() error,
	usernameFlag func() (bool, string),
	foundUsername func(string),
	foundHostname func(string),
	foundPort func(string),
	foundDatabase func(string),
	insecureFlag func() (bool, bool),
	insecureOverride func(bool),
	certsDirFlag func() (bool, string),
	foundCertsDir func(string),
) (*pgurl.URL, error) {
	parsedURL, err := pgurl.Parse(newURL)
	if err != nil {
		return nil, err
	}

	if user := parsedURL.GetUsername(); user != "" {
		foundUsername(user)
	}

	// If some host/port information is available, forward it to
	// --host / --port.
	net, host, port := parsedURL.GetNetworking()
	if host != "" {
		foundHostname(host)
	}
	if port != "" {
		foundPort(port)
	}

	// If a database is specified, and the command supports databases,
	// forward it to --database.
	if db := parsedURL.GetDatabase(); db != "" {
		foundDatabase(db)
	}

	tlsUsed, tlsMode, caCertPath := parsedURL.GetTLSOptions()

	if tlsUsed && tlsMode == pgurl.TLSUnspecified && net == pgurl.ProtoTCP {
		// The sslmode argument was not specified and we are using TCP.
		// We may want to inject a default value in that case.
		// (We don't inject a transport if using unix sockets.)
		//
		// Is there a value to go by from a previous --insecure flag? If
		// so, use that.
		if insecureSpecified, insecureValue := insecureFlag(); insecureSpecified {
			var tp pgurl.TransportOption
			if insecureValue {
				tp = pgurl.TransportNone()
				tlsUsed = false
			} else {
				tlsMode = pgurl.TLSVerifyFull
				tp = pgurl.TransportTLS(tlsMode, caCertPath)
			}
			parsedURL.WithTransport(tp)
		} else {
			// No --insecure specified. We default to maximum security.
			tlsMode = pgurl.TLSVerifyFull
			parsedURL.WithTransport(pgurl.TransportTLS(tlsMode, caCertPath))
		}
	}

	if !tlsUsed {
		if sslStrict {
			// For "strict" mode (RPC client commands) we don't support non-TLS
			// yet. See https://github.com/cockroachdb/cockroach/issues/54007
			// Instead, we see a request for no TLS to imply insecure mode.
			insecureOverride(true)
		}
	} else {
		if sslStrict {
			switch tlsMode {
			case pgurl.TLSVerifyFull:
				// This is valid.
			default:
				return nil, makeStrictErr()
			}
		}
		insecureOverride(false)

		if sslStrict {
			// The "sslStrict" flag means the client command is using our
			// certificate manager instead of the certificate handler in
			// lib/pq.
			//
			// Our certificate manager is peculiar in that it requires
			// every file in the same directory (the "certs dir") and also
			// the files to be named after a fixed naming convention.
			//
			// Meanwhile, the URL format for security flags consists
			// of 3 options (sslrootcert, sslcert, sslkey) that *may*
			// refer to arbitrary files in arbitrary directories.
			// Regular SQL drivers are fine with that (including lib/pq)
			// but our cert manager definitely not (or, at least, not yet).
			//
			// So here we have to reverse-engineer the parameters needed
			// for the certificate manager from the URL and verify that
			// they conform to the restrictions of our cert manager. There
			// are three things that need to happen:
			//
			// - if the flag --certs-dir is not specified in the command
			//   line, we need to derive a path for the certificate
			//   directory from the URL options; our cert manager needs
			//   this as input.
			//
			// - we must verify that all 3 url options that determine
			//   files refer to the same directory; our cert manager does
			//   not know how to work otherwise.
			//
			// - we must also verify that the 3 options specify a file
			//   name that is compatible with our cert manager (namely,
			//   "ca.crt", "client.USERNAME.crt" and
			//   "client.USERNAME.key").
			//

			candidateCertsDir := ""
			hasCertsDir := false
			if certsDirSpecified, certsDir := certsDirFlag(); certsDirSpecified {
				// If a --certs-dir flag was preceding --url, we want to
				// check that the paths inside the URL match the value of
				// that explicit --certs-dir.
				//
				// If --certs-dir was not specified, we'll pick up
				// the first directory encountered below.
				candidateCertsDir = certsDir
				candidateCertsDir, err = filepath.Abs(candidateCertsDir)
				if err != nil {
					return nil, err
				}
			}

			// tryCertsDir digs into the SSL URL options to extract a valid
			// certificate directory. It also checks that the file names are those
			// expected by the certificate manager.
			tryCertsDir := func(optName, opt, expectedFilename string) error {
				if opt == "" {
					// Option not set: nothing to do.
					return nil
				}

				// Check the expected base file name.
				base := filepath.Base(opt)
				if base != expectedFilename {
					return fmt.Errorf("invalid file name for %q: expected %q, got %q", optName, expectedFilename, base)
				}

				// Extract the directory part.
				dir := filepath.Dir(opt)
				dir, err = filepath.Abs(dir)
				if err != nil {
					return err
				}
				if candidateCertsDir != "" {
					// A certificate directory has already been found in a previous option;
					// check that the new option uses the same.
					if candidateCertsDir != dir {
						return fmt.Errorf("non-homogeneous certificate directory: %s=%q, expected %q", optName, opt, candidateCertsDir)
					}
				} else {
					// First time seeing a directory, remember it.
					candidateCertsDir = dir
					hasCertsDir = true
				}

				return nil
			}

			userName := username.RootUserName()
			if hasConnUser, connUser := usernameFlag(); hasConnUser {
				userName, _ = username.MakeSQLUsernameFromUserInput(connUser, username.PurposeValidation)
			}
			if err := tryCertsDir("sslrootcert", caCertPath, certnames.CACertFilename()); err != nil {
				return nil, err
			}
			if clientCertEnabled, clientCertPath, clientKeyPath := parsedURL.GetAuthnCert(); clientCertEnabled {
				if err := tryCertsDir("sslcert", clientCertPath, certnames.ClientCertFilename(userName)); err != nil {
					return nil, err
				}
				if err := tryCertsDir("sslkey", clientKeyPath, certnames.ClientKeyFilename(userName)); err != nil {
					return nil, err
				}
			}

			if hasCertsDir {
				foundCertsDir(candidateCertsDir)
			}
		}
	}

	// Check that the URL so far is valid.
	err = parsedURL.Validate()
	return parsedURL, err
}
