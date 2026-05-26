// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clientsecopts

import (
	"fmt"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
)

// CLIFlagInterfaceForClientURL is an interface to the state of the
// variables attached to discrete flags (e.g. --host, --insecure)
// that can be passed before or after --url.
//
// It is used by AnalyzeClientURL below to peek into the state
// of flags set before --url and prepare state for subsequent
// flags passed after --url.
//
// NB: We use an interface here instead of directly depending on
// packages pflag and cliflags, so as to simplify the build
// dependencies and increase readability.
type CLIFlagInterfaceForClientURL interface {
	// StrictTLS returns true when the URL should enforce that TLS
	// client certs and CA cert should be placed into a single common
	// directory, and the server identity be fully verified.
	//
	// This is set for all non-SQL client commands, which only support
	// the insecure boolean and certs-dir with maximum TLS validation.
	StrictTLS() bool

	// NewStrictTLSConfigurationError will be called when a URL contains
	// parameters that conflict with the requirements of StrictTLS().
	NewStrictTLSConfigurationError() error

	// UserFlag should return whether the --user flag was specified, and its value.
	// Note that the value returned by UserFlag() should also reflect any prior
	// calls to SetUser().
	UserFlag() (bool, string)

	// SetUser is called when the URL contains a username.
	SetUser(string)

	// SetHost is called when the URL contains a server name / address.
	SetHost(string)

	// SetPort is called when the URL contains a server port name / number.
	SetPort(string)

	// SetDatabase is called when the URL contains a database name.
	SetDatabase(string)

	// InsecureFlag should return whether --insecure was specified and
	// its value.
	InsecureFlag() (bool, bool)

	// SetInsecure is called when the URL is found to imply --insecure.
	// TODO(knz): This behavior should be removed.
	// See https://github.com/cockroachdb/cockroach/issues/54007.
	SetInsecure(bool)

	// CertsDirFlag should return whether --certs-dir was specified and
	// its value.
	CertsDirFlag() (bool, string)

	// SetCertsDir is called when the URL is found to imply a
	// certificate directory location.
	SetCertsDir(string)
}

// AnalyzeClientURL is a helper that processes a connection URL passed on the
// command line, integrates any defaults set by earlier command-line arguments,
// then calls callbacks depending on which details were found in the new URL.
// Its focus is on TLS authentication options.
//
// sslStrict, when set to true, requires that the SSL file paths in
// a URL clearly map to a single certificate directory and restricts
// the set of supported SSL modes to just "disable" and "require".
//
// This is set for all non-SQL client commands, which only support
// the insecure boolean and certs-dir with maximum SSL validation.
func AnalyzeClientURL(newURL string, flags CLIFlagInterfaceForClientURL) (*pgurl.URL, error) {
	parsedURL, err := pgurl.Parse(newURL)
	if err != nil {
		return nil, err
	}

	if user := parsedURL.GetUsername(); user != "" {
		flags.SetUser(user)
	}

	// If some host/port information is available, forward it to
	// --host / --port.
	net, host, port := parsedURL.GetNetworking()
	if host != "" {
		flags.SetHost(host)
	}
	if port != "" {
		flags.SetPort(port)
	}

	// If a database is specified, and the command supports databases,
	// forward it to --database.
	if db := parsedURL.GetDatabase(); db != "" {
		flags.SetDatabase(db)
	}

	tlsUsed, tlsMode, caCertPath := parsedURL.GetTLSOptions()

	if tlsUsed && tlsMode == pgurl.TLSUnspecified && net == pgurl.ProtoTCP {
		// The sslmode argument was not specified and we are using TCP.
		// We may want to inject a default value in that case.
		// (We don't inject a transport if using unix sockets.)
		//
		// Is there a value to go by from a previous --insecure flag? If
		// so, use that.
		if insecureSpecified, insecureValue := flags.InsecureFlag(); insecureSpecified {
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
		if flags.StrictTLS() {
			// For "strict" mode (RPC client commands) we don't support non-TLS
			// yet. See https://github.com/cockroachdb/cockroach/issues/54007
			// Instead, we see a request for no TLS to imply insecure mode.
			flags.SetInsecure(true)
		}
	} else {
		if flags.StrictTLS() {
			switch tlsMode {
			case pgurl.TLSVerifyFull:
				// This is valid.
			default:
				return nil, flags.NewStrictTLSConfigurationError()
			}
		}
		flags.SetInsecure(false)

		if flags.StrictTLS() {
			// The "flags.StrictTLS()" flag means the client command is using our
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
			// For more information, see
			// https://github.com/cockroachdb/cockroach/issues/82075

			candidateCertsDir := ""
			hasCertsDir := false
			if certsDirSpecified, certsDir := flags.CertsDirFlag(); certsDirSpecified {
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
			if hasConnUser, connUser := flags.UserFlag(); hasConnUser {
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
				flags.SetCertsDir(candidateCertsDir)
			}
		}
	}

	// Check that the URL so far is valid.
	err = parsedURL.Validate()
	return parsedURL, err
}
