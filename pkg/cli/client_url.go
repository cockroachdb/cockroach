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

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/security/clientsecopts"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
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

	// if is the flag set for cmd. This is initialized late in Set()
	// since some flags are only defined after the URL flag.
	fl *pflag.FlagSet

	// warn determines whether a warning is printed if an URL contains
	// user/database details and the command does not support that.
	warn bool

	// strictTLS, when set to true, requires that the SSL file paths in
	// a URL clearly map to a certificate directory and restricts the
	// set of supported SSL modes to just "disable" and "require".
	//
	// This is set for all non-SQL client commands, which only support
	// the insecure boolean and certs-dir with maximum SSL validation.
	strictTLS bool
}

func newURLParser(cmd *cobra.Command, cliCtx *cliContext, strictTLS bool, warn bool) pflag.Value {
	return &urlParser{
		cmd:       cmd,
		cliCtx:    cliCtx,
		warn:      warn,
		strictTLS: strictTLS,
	}
}

func (u *urlParser) String() string { return "" }

func (u *urlParser) Type() string {
	return "<postgres://...>"
}

func (u *urlParser) Set(v string) error {
	if u.fl == nil {
		// Late initialization of the flagset. We can't call this early as
		// we need all flags to be defined already, and some flags
		// are only defined (in cli/flags.go) after the URL flag has been defined.
		u.fl = flagSetForCmd(u.cmd)
	}

	purl, err := clientsecopts.AnalyzeClientURL(v, u)

	if err != nil {
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
	u.cliCtx.sqlConnURL = purl
	return err
}

var _ clientsecopts.CLIFlagInterfaceForClientURL = (*urlParser)(nil)

// StrictTLS implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) StrictTLS() bool { return u.strictTLS }

// UserFlag implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) UserFlag() (hasConnUser bool, connUser string) {
	return u.cliCtx.sqlConnUser != "", u.cliCtx.sqlConnUser
}

// SetUser implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) SetUser(user string) {
	// If the URL specifies a username, check whether a username was expected.
	if f := u.fl.Lookup(cliflags.User.Name); f == nil {
		// A client which does not support --user will also not use
		// makeClientConnURL(), so we can ignore/forget about the
		// information. We do not produce an error however, so that a
		// user can readily copy-paste the URL produced by `cockroach
		// start` even if the client command does not accept a username.
		if u.warn {
			fmt.Fprintf(stderr,
				"warning: --url specifies user/password, but command %q does not accept user/password details - details ignored\n",
				u.cmd.Name())
		}
	} else {
		// If username information is available, forward it to --user.
		u.cliCtx.sqlConnUser = user
		// Remember the --user flag was changed in case later code checks
		// the .Changed field.
		f.Changed = true
	}
}

// SetHost implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) SetHost(host string) {
	u.cliCtx.clientConnHost = host
	u.fl.Lookup(cliflags.ClientHost.Name).Changed = true
}

// SetPort implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) SetPort(port string) {
	u.cliCtx.clientConnPort = port
	u.fl.Lookup(cliflags.ClientPort.Name).Changed = true
}

// SetDatabase implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) SetDatabase(db string) {
	if f := u.fl.Lookup(cliflags.Database.Name); f == nil {
		// A client which does not support --database does not need this
		// bit of information, so we can ignore/forget about it. We do
		// not produce an error however, so that a user can readily
		// copy-paste an URL they picked up from another tool (a GUI
		// tool for example).
		if u.warn {
			fmt.Fprintf(stderr,
				"warning: --url specifies database %q, but command %q does not accept a database name - database name ignored\n",
				db, u.cmd.Name())
		}
	} else {
		u.cliCtx.sqlConnDBName = db
		f.Changed = true
	}
}

// CertsDirFlag implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) CertsDirFlag() (bool, string) {
	flCertsDir := u.fl.Lookup(cliflags.CertsDir.Name)
	return flCertsDir.Changed, u.cliCtx.Config.SSLCertsDir
}

// SetCertsDir implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) SetCertsDir(certsDir string) {
	u.cliCtx.Config.SSLCertsDir = certsDir
	u.fl.Lookup(cliflags.CertsDir.Name).Changed = true
}

// InsecureFlag implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) InsecureFlag() (bool, bool) {
	flInsecure := u.fl.Lookup(cliflags.ClientInsecure.Name)
	return flInsecure.Changed, u.cliCtx.Insecure
}

// SetInsecure implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) SetInsecure(insecure bool) {
	// Note: we purposefully do not update the .Changed field of the
	// --insecure flag.
	u.cliCtx.Config.Insecure = insecure
}

// NewStrictTLSConfigurationError implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) NewStrictTLSConfigurationError() error {
	return fmt.Errorf("command %q only supports sslmode=disable or sslmode=verify-full", u.cmd.Name())
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
	userName, err := username.MakeSQLUsernameFromUserInput(cliCtx.sqlConnUser, username.PurposeValidation)
	if err != nil {
		return nil, err
	}
	if userName.Undefined() {
		userName = username.RootUserName()
	}

	ccopts := clientsecopts.ClientSecurityOptions{
		Insecure: cliCtx.Config.Insecure,
		CertsDir: cliCtx.Config.SSLCertsDir,
	}
	if err := clientsecopts.LoadSecurityOptions(ccopts, purl, userName); err != nil {
		return nil, err
	}

	// The construct above should have produced a valid URL already;
	// however a post-assertion doesn't hurt.
	if err := purl.Validate(); err != nil {
		return nil, err
	}

	return purl, nil
}
