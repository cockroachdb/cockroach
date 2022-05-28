// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clienturl

import (
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlshell"
	"github.com/cockroachdb/cockroach/pkg/security/clientconnurl"
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
//   clientconnurl.ClientOptions, secOptions
//                   |
//                  / \
//         .-------'   `--------.
//         |                    |
//         |                    |
//      non-SQL           makeClientConnURL()
//     commands                 |
//    (quit, init, etc)         |
//                          SQL commands
//                        (user, zone, etc)
//

// NewURLParser creates a URL argument parser able to interleave
// itself with discrete flags that can modify the individual fields of
// clientconnurl.ClientOptions. It produces its output in
// copts.ExplicitURL.
//
// sslStrict, when set to true, requires that the SSL file paths in
// a URL clearly map to a certificate directory and restricts the
// set of supported SSL modes to just "disable" and "require".
//
// This is set for all non-SQL client commands, which only support
// the insecure boolean and certs-dir with maximum SSL validation.
//
// warnFn, if provided, is used to warn the user when the value
// specified by --url contains a database name or a user/password pair.
// This is used by non-SQL "cluster level" admin commands.
func NewURLParser(
	cmd *cobra.Command,
	copts *clientconnurl.ClientOptions,
	sslStrict bool,
	warnFn func(format string, args ...interface{}),
) pflag.Value {
	return urlParser{
		cmd:        cmd,
		clientOpts: copts,
		sslStrict:  sslStrict,
		warnFn:     warnFn,
	}
}

type urlParser struct {
	cmd        *cobra.Command
	clientOpts *clientconnurl.ClientOptions

	sslStrict bool
	warnFn    func(string, ...interface{})
}

func (u urlParser) String() string { return "" }

func (u urlParser) Type() string {
	return "<postgres://...>"
}

func (u urlParser) Set(v string) error {
	fl := FlagSetForCmd(u.cmd)

	usernameFlag := func() (hasConnUser bool, connUser string) {
		return u.clientOpts.User != "", u.clientOpts.User
	}

	foundUsername := func(user string) {
		// If the URL specifies a username, check whether a username was expected.
		if f := fl.Lookup(cliflags.User.Name); f == nil {
			// A client which does not support --user will also not use
			// makeClientConnURL(), so we can ignore/forget about the
			// information. We do not produce an error however, so that a
			// user can readily copy-paste the URL produced by `cockroach
			// start` even if the client command does not accept a username.
			if u.warnFn != nil {
				u.warnFn(
					"warning: --url specifies user/password, but command %q does not accept user/password details - details ignored\n",
					u.cmd.Name())
			}
		} else {
			// If username information is available, forward it to --user.
			u.clientOpts.User = user
			// Remember the --user flag was changed in case later code checks
			// the .Changed field.
			f.Changed = true
		}
	}

	foundHostname := func(host string) {
		u.clientOpts.ServerHost = host
		fl.Lookup(cliflags.ClientHost.Name).Changed = true
	}

	foundPort := func(port string) {
		u.clientOpts.ServerPort = port
		fl.Lookup(cliflags.ClientPort.Name).Changed = true
	}

	foundDatabase := func(db string) {
		if f := fl.Lookup(cliflags.Database.Name); f == nil {
			// A client which does not support --database does not need this
			// bit of information, so we can ignore/forget about it. We do
			// not produce an error however, so that a user can readily
			// copy-paste an URL they picked up from another tool (a GUI
			// tool for example).
			if u.warnFn != nil {
				u.warnFn(
					"warning: --url specifies database %q, but command %q does not accept a database name - database name ignored\n",
					db, u.cmd.Name())
			}
		} else {
			u.clientOpts.Database = db
			f.Changed = true
		}
	}

	flCertsDir := fl.Lookup(cliflags.CertsDir.Name)
	certsDirFlag := func() (certsDirSpecified bool, certsDir string) {
		return flCertsDir.Changed, flCertsDir.Value.String()
	}

	foundCertsDir := func(certsDir string) {
		_ = flCertsDir.Value.Set(certsDir)
		flCertsDir.Changed = true
	}

	flInsecure := fl.Lookup(cliflags.ClientInsecure.Name)
	insecureFlag := func() (flagSpecified bool, isInsecure bool) {
		vb, _ := strconv.ParseBool(flInsecure.Value.String())
		return flInsecure.Changed, vb
	}
	insecureOverride := func(insecure bool) {
		_ = flInsecure.Value.Set(strconv.FormatBool(insecure))
	}

	makeStrictErr := func() error {
		return fmt.Errorf("command %q only supports sslmode=disable or sslmode=verify-full", u.cmd.Name())
	}

	purl, err := clientconnurl.UpdateURL(v,
		u.sslStrict,
		makeStrictErr,
		usernameFlag,
		foundUsername,
		foundHostname,
		foundPort,
		foundDatabase,
		insecureFlag,
		insecureOverride,
		certsDirFlag,
		foundCertsDir,
	)

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
	u.clientOpts.ExplicitURL = purl
	return err
}

// MakeURLParserFn constructs a clisqlshell.URLParser
// suitable for use in clisqlcfg's ParseURL field.
func MakeURLParserFn(
	cmd *cobra.Command, baseClientOpts clientconnurl.ClientOptions,
) clisqlshell.URLParser {
	// clientOpts will be allocated on the heap and shared
	// by all subsequent invocations of the clisqlshell.URLParser.
	// This ensures that subsequent \connect invocations can
	// reuse each other's values.
	clientOpts := &baseClientOpts

	return func(url string) (*pgurl.URL, error) {
		// Parse it as if --url was specified.
		up := urlParser{cmd: cmd, clientOpts: clientOpts}
		if err := up.Set(url); err != nil {
			return nil, err
		}
		return clientOpts.ExplicitURL, nil
	}
}
