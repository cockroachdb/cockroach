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
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
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
	return "postgresql://[user[:passwd]@]host[:port]/[db][?parameters...]"
}

func (u urlParser) Set(v string) error {
	return u.setInternal(v, true /* warn */)
}

func (u urlParser) setInternal(v string, warn bool) error {
	parsedURL, err := url.Parse(v)
	if err != nil {
		return err
	}

	// General URL format compatibility check.
	//
	// The canonical PostgreSQL URL scheme is "postgresql", however our
	// own client commands also accept "postgres" which is the scheme
	// registered/supported by lib/pq. Internally, lib/pq supports
	// both.
	if parsedURL.Scheme != "postgresql" && parsedURL.Scheme != "postgres" {
		return fmt.Errorf(`URL scheme must be "postgresql", not "%s"`, parsedURL.Scheme)
	}

	if parsedURL.Opaque != "" {
		return fmt.Errorf("unknown URL format: %s", v)
	}

	cliCtx := u.cliCtx
	fl := flagSetForCmd(u.cmd)

	// If user name / password information is available, forward it to
	// --user. We store the password for later re-collection by
	// makeClientConnURL().
	if parsedURL.User != nil {
		f := fl.Lookup(cliflags.User.Name)
		if f == nil {
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
			if err := f.Value.Set(parsedURL.User.Username()); err != nil {
				return errors.Wrapf(err, "extracting user")
			}
			if pw, pwSet := parsedURL.User.Password(); pwSet {
				cliCtx.sqlConnPasswd = pw
			}
		}
	}

	// If some host/port information is available, forward it to
	// --host / --port.
	if parsedURL.Host != "" {
		prevHost, prevPort := cliCtx.clientConnHost, cliCtx.clientConnPort
		if err := u.cmd.Flags().Set(cliflags.ClientHost.Name, parsedURL.Host); err != nil {
			return errors.Wrapf(err, "extracting host/port")
		}
		// Fill in previously set values for each component that wasn't specified.
		if cliCtx.clientConnHost == "" {
			cliCtx.clientConnHost = prevHost
		}
		if cliCtx.clientConnPort == "" {
			cliCtx.clientConnPort = prevPort
		}
	}

	// If a database path is available, forward it to --database.
	if parsedURL.Path != "" {
		dbPath := strings.TrimLeft(parsedURL.Path, "/")
		f := fl.Lookup(cliflags.Database.Name)
		if f == nil {
			// A client which does not support --database does not need this
			// bit of information, so we can ignore/forget about it. We do
			// not produce an error however, so that a user can readily
			// copy-paste an URL they picked up from another tool (a GUI
			// tool for example).
			if warn {
				fmt.Fprintf(stderr,
					"warning: --url specifies database %q, but command %q does not accept a database name - database name ignored\n",
					dbPath, u.cmd.Name())
			}
		} else {
			if err := f.Value.Set(dbPath); err != nil {
				return errors.Wrapf(err, "extracting database name")
			}
		}
	}

	// If some query options are available, try to decompose/capture as
	// much as possible. Anything not decomposed will be accumulated in
	// cliCtx.extraConnURLOptions.
	if parsedURL.RawQuery != "" {
		options, err := url.ParseQuery(parsedURL.RawQuery)
		if err != nil {
			return err
		}

		// If the URL specifies host/port as query args, we're having to do
		// with a unix socket. In that case, we don't want to populate
		// the host field in the URL.
		if options.Get("host") != "" {
			cliCtx.clientConnHost = ""
			cliCtx.clientConnPort = ""
		}

		cliCtx.extraConnURLOptions = options

		switch sslMode := options.Get("sslmode"); sslMode {
		case "", "disable":
			if err := fl.Set(cliflags.ClientInsecure.Name, "true"); err != nil {
				return errors.Wrapf(err, "setting insecure connection based on --url")
			}
		case "require", "verify-ca", "verify-full":
			if sslMode != "verify-full" && u.sslStrict {
				return fmt.Errorf("command %q only supports sslmode=disable or sslmode=verify-full", u.cmd.Name())
			}
			if err := fl.Set(cliflags.ClientInsecure.Name, "false"); err != nil {
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
				tryCertsDir := func(optName, expectedFilename string) error {
					opt := options.Get(optName)
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

				userName := security.RootUser
				if cliCtx.sqlConnUser != "" {
					userName = cliCtx.sqlConnUser
				}
				if err := tryCertsDir("sslrootcert", security.CACertFilename()); err != nil {
					return err
				}
				if err := tryCertsDir("sslcert", security.ClientCertFilename(userName)); err != nil {
					return err
				}
				if err := tryCertsDir("sslkey", security.ClientKeyFilename(userName)); err != nil {
					return err
				}

				if foundCertsDir {
					if err := fl.Set(cliflags.CertsDir.Name, candidateCertsDir); err != nil {
						return errors.Wrapf(err, "extracting certificate directory")
					}
				}
			}
		default:
			return fmt.Errorf(
				"unsupported sslmode=%s (supported: disable, require, verify-ca, verify-full)", sslMode)
		}
	}

	return nil
}

// makeClientConnURL constructs a connection URL from the parsed options.
// Do not call this function before command-line argument parsing has completed:
// this initializes the certificate manager with the configured --certs-dir.
func (cliCtx *cliContext) makeClientConnURL() (url.URL, error) {
	netHost := ""
	if cliCtx.clientConnHost != "" || cliCtx.clientConnPort != "" {
		netHost = net.JoinHostPort(cliCtx.clientConnHost, cliCtx.clientConnPort)
	}
	pgurl := url.URL{
		Scheme: "postgresql",
		Host:   netHost,
		Path:   cliCtx.sqlConnDBName,
	}

	if cliCtx.sqlConnUser != "" {
		if cliCtx.sqlConnPasswd != "" {
			pgurl.User = url.UserPassword(cliCtx.sqlConnUser, cliCtx.sqlConnPasswd)
		} else {
			pgurl.User = url.User(cliCtx.sqlConnUser)
		}
	}

	opts := url.Values{}
	for k, v := range cliCtx.extraConnURLOptions {
		opts[k] = v
	}

	if netHost != "" {
		// Only add TLS parameters when using a network connection.
		userName := cliCtx.sqlConnUser
		if userName == "" {
			userName = security.RootUser
		}
		sCtx := rpc.MakeSecurityContext(cliCtx.Config)
		if err := sCtx.LoadSecurityOptions(
			opts, userName,
		); err != nil {
			return url.URL{}, err
		}
	}

	pgurl.RawQuery = opts.Encode()
	return pgurl, nil
}
