// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clienturl

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflagcfg"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlshell"
	"github.com/cockroachdb/cockroach/pkg/security/clientsecopts"
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
	clientOpts *clientsecopts.ClientOptions,
	strictTLS bool,
	warnFn func(format string, args ...interface{}),
) pflag.Value {
	return &urlParser{
		cmd:        cmd,
		clientOpts: clientOpts,
		warnFn:     warnFn,
		strictTLS:  strictTLS,
	}
}

type urlParser struct {
	cmd *cobra.Command
	fl  *pflag.FlagSet

	clientOpts *clientsecopts.ClientOptions

	// warnFn determines whether a warning is printed if an URL contains
	// user/database details and the command does not support that.
	warnFn func(string, ...interface{})

	// strictTLS, when set to true, requires that the SSL file paths in
	// a URL clearly map to a certificate directory and restricts the
	// set of supported SSL modes to just "disable" and "require".
	//
	// This is set for all non-SQL client commands, which only support
	// the insecure boolean and certs-dir with maximum SSL validation.
	strictTLS bool
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
		u.fl = cliflagcfg.FlagSetForCmd(u.cmd)
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

	u.checkMistakes(purl)

	// Store the parsed URL for later.
	u.clientOpts.ExplicitURL = purl
	return err
}

var _ clientsecopts.CLIFlagInterfaceForClientURL = (*urlParser)(nil)

// StrictTLS implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) StrictTLS() bool { return u.strictTLS }

// UserFlag implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) UserFlag() (hasConnUser bool, connUser string) {
	return u.clientOpts.User != "", u.clientOpts.User
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

// SetHost implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) SetHost(host string) {
	u.clientOpts.ServerHost = host
	// Also update the flag, so that subsequent CLI parameters
	// pick up the change. NB: 'cockroach demo' does not have a --host flag.
	if f := u.fl.Lookup(cliflags.ClientHost.Name); f != nil {
		f.Changed = true
	}
}

// SetPort implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) SetPort(port string) {
	u.clientOpts.ServerPort = port
	// Also update the flag, so that subsequent CLI parameters
	// pick up the change. NB: 'cockroach demo' does not have a --host flag.
	if f := u.fl.Lookup(cliflags.ClientPort.Name); f != nil {
		f.Changed = true
	}
}

// SetDatabase implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) SetDatabase(db string) {
	if f := u.fl.Lookup(cliflags.Database.Name); f == nil {
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

// CertsDirFlag implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) CertsDirFlag() (bool, string) {
	flCertsDir := u.fl.Lookup(cliflags.CertsDir.Name)
	if flCertsDir == nil {
		return false, ""
	}
	return flCertsDir.Changed, flCertsDir.Value.String()
}

// SetCertsDir implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) SetCertsDir(certsDir string) {
	flCertsDir := u.fl.Lookup(cliflags.CertsDir.Name)
	if flCertsDir != nil {
		_ = flCertsDir.Value.Set(certsDir)
		flCertsDir.Changed = true
	}
}

// InsecureFlag implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) InsecureFlag() (bool, bool) {
	flInsecure := u.fl.Lookup(cliflags.ClientInsecure.Name)
	if flInsecure == nil {
		return false, false
	}
	vb, _ := strconv.ParseBool(flInsecure.Value.String())
	return flInsecure.Changed, vb
}

// SetInsecure implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) SetInsecure(insecure bool) {
	flInsecure := u.fl.Lookup(cliflags.ClientInsecure.Name)
	if flInsecure != nil {
		_ = flInsecure.Value.Set(strconv.FormatBool(insecure))
	}
}

// NewStrictTLSConfigurationError implements the clientsecopts.CLIFlagInterfaceForClientURL interface.
func (u *urlParser) NewStrictTLSConfigurationError() error {
	return fmt.Errorf("command %q only supports sslmode=disable or sslmode=verify-full", u.cmd.Name())
}

// MakeURLParserFn constructs a clisqlshell.URLParser
// suitable for use in clisqlcfg's ParseURL field.
func MakeURLParserFn(
	cmd *cobra.Command, baseClientOpts clientsecopts.ClientOptions,
) clisqlshell.URLParser {
	// clientOpts will be allocated on the heap and shared
	// by all subsequent invocations of the clisqlshell.URLParser.
	// This ensures that subsequent \connect invocations can
	// reuse each other's values.
	clientOpts := &baseClientOpts

	return func(url string) (*pgurl.URL, error) {
		// Parse it as if --url was specified.
		up := NewURLParser(cmd, clientOpts, false /* strictTLS */, nil /* warnFn */)
		if err := up.Set(url); err != nil {
			return nil, err
		}
		return clientOpts.ExplicitURL, nil
	}
}

// checkMistakes reports likely mistakes to the user via warning messages.
func (u *urlParser) checkMistakes(purl *pgurl.URL) {
	if u.warnFn == nil {
		return
	}

	// Check mistaken use of -c as direct option, instead of using
	// options=-c...
	opts := purl.GetExtraOptions()
	for optName := range opts {
		if strings.HasPrefix(optName, "-c") {
			u.warnFn("\nwarning: found raw URL parameter \"%[1]s\"; "+
				"are you sure you did not mean to use \"options=%[1]s\" instead?\n\n", optName)
		}
		if optName == "cluster" {
			u.warnFn("\nwarning: found raw URL parameter \"%[1]s\"; "+
				"are you sure you did not mean to use \"options=-c%[1]s\" instead?\n\n", optName)
		}
	}
	// For tenant selection, the option is `-ccluster=`, not `-cluster=`.
	if extraOpts := opts.Get("options"); extraOpts != "" {
		opts, err := pgurl.ParseExtendedOptions(extraOpts)
		if err != nil {
			u.warnFn("\nwarning: invalid syntax in options: %v\n\n", err)
		} else {
			if opts.Has("luster") {
				// User entered options=-cluster.
				u.warnFn("\nwarning: found \"-cluster=\" in URL \"options\" field; " +
					"are you sure you did not mean to use \"options=-ccluster=\" instead?\n\n")
			}
		}
	}
}
