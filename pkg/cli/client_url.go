// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// This file implements the parsing of the client --url flag.
//
// This aims to offer consistent UX between uses the "combined" --url
// flag and the "discrete" separate flags --host / --port / etc.
//
// The flow of data between flags, configuration variables and usage
// by client commands goes as follows:
//
//            flags parser
//                /    \
//         .-----'      `-------.
//         |                    |
//       --url               --host, --port, etc
//         |                    |
//         |                    |
//   urlParser.Set()            |
//     (this file)              |
//         |                    |
//         `-------.    .-------'
//                  \  /
//          sqlCtx/cliCtx/baseCtx
//                   |
//                  / \
//         .-------'   `--------.
//         |                    |
//         |                    |
//      non-SQL           makeClientConnURL()
//     commands             (this file)
//    (quit, init, etc)         |
//                          SQL commands
//                        (user, zone, etc)
//

type urlParser struct {
	cmd    *cobra.Command
	cliCtx *cliContext

	// sslStrict, when set to true, requires that the SSL file paths in
	// a URL clearly map to a certificate directory and restricts the
	// set of supported SSL modes to just "disable" and "require".
	//
	// This is set for all non-SQL client commands, which only support
	// the insecure boolean and certs-dir with maximum SSL validation.
	sslStrict bool
}

func (u urlParser) String() string { return "" }

func (u urlParser) Type() string {
	return "<postgres://...>"
}

func (u urlParser) Set(v string) error {
	return u.setInternal(v, true /* warn */)
}

func (u urlParser) setInternal(v string, warn bool) error {
	parsedURL, err := pgurl.Parse(v)
	if err != nil {
		return err
	}

	cliCtx := u.cliCtx

	fl := flagSetForCmd(u.cmd)
	if user := parsedURL.GetUsername(); user != "" {
		// If the URL specifies a username, check whether a username was expected.
		if f := fl.Lookup(cliflags.User.Name); f == nil {
			// A client which does not support --user will also not use
			// makeClientConnURL(), so we can ignore/forget about the
			// information. We do not produce an error however, so that a
			// user can readily copy-paste the URL produced by `cockroach
			// start` even if the client command does not accept a username.
			if warn {
				fmt.Fprintf(stderr,
					"warning: --url specifies user/password, but command %q does not accept user/password details - details ignored\n",
					u.cmd.Name())
			}
		} else {
			// If username information is available, forward it to --user.
			cliCtx.sqlConnUser = user
			// Remember the --user flag was changed in case later code checks
			// the .Changed field.
			f.Changed = true
		}
	}

	// If some host/port information is available, forward it to
	// --host / --port.
	net, host, port := parsedURL.GetNetworking()
	if host != "" {
		cliCtx.clientConnHost = host
		fl.Lookup(cliflags.ClientHost.Name).Changed = true
	}
	if port != "" {
		cliCtx.clientConnPort = port
		fl.Lookup(cliflags.ClientPort.Name).Changed = true
	}

	// If a database is specified, and the command supports databases,
	// forward it to --database.
	if db := parsedURL.GetDatabase(); db != "" {
		if f := fl.Lookup(cliflags.Database.Name); f == nil {
			// A client which does not support --database does not need this
			// bit of information, so we can ignore/forget about it. We do
			// not produce an error however, so that a user can readily
			// copy-paste an URL they picked up from another tool (a GUI
			// tool for example).
			if warn {
				fmt.Fprintf(stderr,
					"warning: --url specifies database %q, but command %q does not accept a database name - database name ignored\n",
					db, u.cmd.Name())
			}
		} else {
			cliCtx.sqlConnDBName = db
			f.Changed = true
		}
	}

	flInsecure := fl.Lookup(cliflags.ClientInsecure.Name)
	tlsUsed, tlsMode, caCertPath := parsedURL.GetTLSOptions()

	if tlsUsed && tlsMode == pgurl.TLSUnspecified && net == pgurl.ProtoTCP {
		// The sslmode argument was not specified and we are using TCP.
		// We may want to inject a default value in that case.
		// (We don't inject a transport if using unix sockets.)
		//
		// Is there a value to go by from a previous --insecure flag? If
		// so, use that.
		if flInsecure.Changed {
			var tp pgurl.TransportOption
			if cliCtx.Insecure {
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
		if u.sslStrict {
			// For "strict" mode (RPC client commands) we don't support non-TLS
			// yet. See https://github.com/cockroachdb/cockroach/issues/54007
			// Instead, we see a request for no TLS to imply insecure mode.
			if err := flInsecure.Value.Set("true"); err != nil {
				return errors.Wrapf(err, "setting secure connection based on --url")
			}
		}
	} else {
		if u.sslStrict {
			switch tlsMode {
			case pgurl.TLSVerifyFull:
				// This is valid.
			default:
				return fmt.Errorf("command %q only supports sslmode=disable or sslmode=verify-full", u.cmd.Name())
			}
		}
		if err := flInsecure.Value.Set("false"); err != nil {
			return errors.Wrapf(err, "setting secure connection based on --url")
		}

		if u.sslStrict {
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
			foundCertsDir := false
			if fl.Lookup(cliflags.CertsDir.Name).Changed {
				// If a --certs-dir flag was preceding --url, we want to
				// check that the paths inside the URL match the value of
				// that explicit --certs-dir.
				//
				// If --certs-dir was not specified, we'll pick up
				// the first directory encountered below.
				candidateCertsDir = cliCtx.SSLCertsDir
				candidateCertsDir = os.ExpandEnv(candidateCertsDir)
				candidateCertsDir, err = filepath.Abs(candidateCertsDir)
				if err != nil {
					return err
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
					foundCertsDir = true
				}

				return nil
			}

			userName := security.RootUserName()
			if cliCtx.sqlConnUser != "" {
				userName, _ = security.MakeSQLUsernameFromUserInput(cliCtx.sqlConnUser, security.UsernameValidation)
			}
			if err := tryCertsDir("sslrootcert", caCertPath, security.CACertFilename()); err != nil {
				return err
			}
			if clientCertEnabled, clientCertPath, clientKeyPath := parsedURL.GetAuthnCert(); clientCertEnabled {
				if err := tryCertsDir("sslcert", clientCertPath, security.ClientCertFilename(userName)); err != nil {
					return err
				}
				if err := tryCertsDir("sslkey", clientKeyPath, security.ClientKeyFilename(userName)); err != nil {
					return err
				}
			}

			if foundCertsDir {
				if err := fl.Set(cliflags.CertsDir.Name, candidateCertsDir); err != nil {
					return errors.Wrapf(err, "extracting certificate directory")
				}
			}
		}
	}

	// Check that the URL so far is valid.
	if err := parsedURL.Validate(); err != nil {
		// This function is called by pflag.(*FlagSet).Set() and that code
		// does not know how to use errors.Wrap properly. Instead, it
		// reformats the error as a string, which loses any validation
		// details.
		// We make-do here by injecting the details as part of
		// the error message.
		// TODO(knz): Fix the upstream pflag and get rid of this
		// horrendous logic.
		msg := err.Error()
		if details := errors.FlattenDetails(err); details != "" {
			msg += "\n" + details
		}
		return fmt.Errorf("%s", msg)
	}

	// Store the parsed URL for later.
	cliCtx.sqlConnURL = parsedURL

	return nil
}

// makeClientConnURL constructs a connection URL from the parsed options.
// Do not call this function before command-line argument parsing has completed:
// this initializes the certificate manager with the configured --certs-dir.
func (cliCtx *cliContext) makeClientConnURL() (*pgurl.URL, error) {
	var purl *pgurl.URL
	if cliCtx.sqlConnURL != nil {
		// Reuse the result of parsing a previous --url argument.
		purl = cliCtx.sqlConnURL
	} else {
		// New URL. Start from scratch.
		purl = pgurl.New() // defaults filled in below.
	}

	// Fill in any defaults from any command-line arguments if there was
	// no --url flag, or if they were specified *after* the --url flag.
	//
	// Note: the username is filled in by LoadSecurityOptions() below.
	// If there was any password while parsing a --url flag,
	// it will be pre-populated via cliCtx.sqlConnURL above.
	purl.WithDatabase(cliCtx.sqlConnDBName)
	if _, host, port := purl.GetNetworking(); host != cliCtx.clientConnHost || port != cliCtx.clientConnPort {
		purl.WithNet(pgurl.NetTCP(cliCtx.clientConnHost, cliCtx.clientConnPort))
	}

	// Check the structure of the username.
	userName, err := security.MakeSQLUsernameFromUserInput(cliCtx.sqlConnUser, security.UsernameValidation)
	if err != nil {
		return nil, err
	}
	if userName.Undefined() {
		userName = security.RootUserName()
	}

	sCtx := rpc.MakeSecurityContext(cliCtx.Config, security.CommandTLSSettings{}, roachpb.SystemTenantID)
	if err := sCtx.LoadSecurityOptions(purl, userName); err != nil {
		return nil, err
	}

	// The construct above should have produced a valid URL already;
	// however a post-assertion doesn't hurt.
	if err := purl.Validate(); err != nil {
		return nil, err
	}

	return purl, nil
}
