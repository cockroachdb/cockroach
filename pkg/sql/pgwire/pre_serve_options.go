// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwire

import (
	"context"
	"encoding/base64"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// parseClientProvidedSessionParameters reads the incoming k/v pairs
// in the startup message into a sql.SessionArgs struct.
//
// This code is tenant-independent, and cannot use configuration
// specific to one tenant. To perform tenant-specific adjustments to
// the session configuration, pass the necessary input in the
// tenantIndependentClientParameters struct then add logic to
// finalizeClientParameters() accordingly.
func parseClientProvidedSessionParameters(
	ctx context.Context,
	buf *pgwirebase.ReadBuffer,
	origRemoteAddr net.Addr,
	trustClientProvidedRemoteAddr bool,
	acceptTenantName bool,
	acceptSystemIdentityOption bool,
) (args tenantIndependentClientParameters, err error) {
	args.SessionArgs = sql.SessionArgs{
		SessionDefaults:             make(map[string]string),
		CustomOptionSessionDefaults: make(map[string]string),
		RemoteAddr:                  origRemoteAddr,
	}

	hasTenantSelectOption := false
	for {
		// Read a key-value pair from the client.
		key, err := buf.GetString()
		if err != nil {
			return args, pgerror.Wrap(
				err, pgcode.ProtocolViolation,
				"error reading option key",
			)
		}
		if len(key) == 0 {
			// End of parameter list.
			break
		}
		value, err := buf.GetString()
		if err != nil {
			return args, pgerror.Wrapf(
				err, pgcode.ProtocolViolation,
				"error reading option value for key %q", key,
			)
		}

		// Case-fold for the key for easier comparison.
		key = strings.ToLower(key)

		// Load the parameter.
		switch key {
		case "user":
			// In CockroachDB SQL, unlike in PostgreSQL, usernames are
			// case-insensitive. Therefore we need to normalize the username
			// here, so that further lookups for authentication have the correct
			// identifier.
			args.User, _ = username.MakeSQLUsernameFromUserInput(value, username.PurposeValidation)
			// IsSuperuser will get updated later when we load the user's session
			// initialization information.
			args.IsSuperuser = args.User.IsRootUser()

		case "crdb:session_revival_token_base64":
			token, err := base64.StdEncoding.DecodeString(value)
			if err != nil {
				return args, pgerror.Wrapf(
					err, pgcode.ProtocolViolation,
					"%s", key,
				)
			}
			args.SessionRevivalToken = token

		case "results_buffer_size":
			if args.ConnResultsBufferSize, err = humanizeutil.ParseBytes(value); err != nil {
				return args, errors.WithSecondaryError(
					pgerror.Newf(pgcode.ProtocolViolation,
						"error parsing results_buffer_size option value '%s' as bytes", value), err)
			}
			if args.ConnResultsBufferSize < 0 {
				return args, pgerror.Newf(pgcode.ProtocolViolation,
					"results_buffer_size option value '%s' cannot be negative", value)
			}
			args.foundBufferSize = true

		case "crdb:remote_addr":
			if !trustClientProvidedRemoteAddr {
				return args, pgerror.Newf(pgcode.ProtocolViolation,
					"server not configured to accept remote address override (requested: %q)",
					value)
			}
			hostS, portS, err := net.SplitHostPort(value)
			if err != nil {
				return args, pgerror.Wrap(
					err, pgcode.ProtocolViolation,
					"invalid address format",
				)
			}
			port, err := strconv.Atoi(portS)
			if err != nil {
				return args, pgerror.Wrap(
					err, pgcode.ProtocolViolation,
					"remote port is not numeric",
				)
			}
			ip := net.ParseIP(hostS)
			if ip == nil {
				return args, pgerror.New(pgcode.ProtocolViolation,
					"remote address is not numeric")
			}
			args.RemoteAddr = &net.TCPAddr{IP: ip, Port: port}

		case "options":
			opts, err := parseOptions(value)
			if err != nil {
				return args, err
			}
			for _, opt := range opts {
				// crdb:jwt_auth_enabled must be passed as an option in order for us to support non-CRDB
				// clients. jwt_auth_enabled is not a session variable. We extract it separately here.
				switch strings.ToLower(opt.key) {
				case "crdb:jwt_auth_enabled":
					b, err := strconv.ParseBool(opt.value)
					if err != nil {
						return args, pgerror.Wrapf(err, pgcode.InvalidParameterValue, "crdb:jwt_auth_enabled")
					}
					args.JWTAuthEnabled = b
					continue

				case "system_identity":
					if !acceptSystemIdentityOption {
						return args, pgerror.Newf(pgcode.InvalidParameterValue,
							"cannot specify system identity via options")
					}
					args.SystemIdentity, _ = username.MakeSQLUsernameFromUserInput(opt.value, username.PurposeValidation)
					continue

				case "cluster":
					if !acceptTenantName {
						return args, pgerror.Newf(pgcode.InvalidParameterValue,
							"tenant selection is not available on this server")
					}
					// The syntax after '.' will be extended in later versions.
					// The period itself cannot occur in a tenant name.
					parts := strings.SplitN(opt.value, ".", 2)
					args.tenantName = parts[0]
					hasTenantSelectOption = true
					continue
				}
				err = loadParameter(ctx, opt.key, opt.value, &args.SessionArgs)
				if err != nil {
					return args, pgerror.Wrapf(err, pgerror.GetPGCode(err), "options")
				}
			}
		default:
			err = loadParameter(ctx, key, value, &args.SessionArgs)
			if err != nil {
				return args, err
			}
		}
	}

	if !hasTenantSelectOption && acceptTenantName {
		// NB: we only inspect the database name if the special option was
		// not specified. See the tenant routing RFC for the rationale.
		if match := tenantSelectionRe.FindStringSubmatch(args.SessionDefaults["database"]); match != nil {
			args.tenantName = match[1]
			dbName := match[2]
			if dbName != "" {
				args.SessionDefaults["database"] = dbName
			} else {
				// Tenant name was specified in dbname position, but nothing
				// afterwards. This means the db name was not really specified.
				// We'll fall back on the case below.
				delete(args.SessionDefaults, "database")
			}
		}
	}

	// TODO(richardjcai): When connecting to the database, we'll want to
	// check for CONNECT privilege on the database. #59875.
	if _, ok := args.SessionDefaults["database"]; !ok {
		// CockroachDB-specific behavior: if no database is specified,
		// default to "defaultdb". In PostgreSQL this would be "postgres".
		args.SessionDefaults["database"] = catalogkeys.DefaultDatabaseName
	}

	// The client might override the application name,
	// which would prevent it from being counted in telemetry.
	// We've decided that this noise in the data is acceptable.
	if appName, ok := args.SessionDefaults["application_name"]; ok {
		if appName == catconstants.ReportableAppNamePrefix+catconstants.InternalSQLAppName {
			telemetry.Inc(sqltelemetry.CockroachShellCounter)
		}
	}

	return args, nil
}

// tenantSelectionRe is the regular expression applied to the start of the
// client-provided database name to find the target tenant.
// See the tenant routing RFC for details.
var tenantSelectionRe = regexp.MustCompile(`^cluster:([^/]*)(?:/|$)(.*)`)

func loadParameter(ctx context.Context, key, value string, args *sql.SessionArgs) error {
	key = strings.ToLower(key)
	exists, configurable := sql.IsSessionVariableConfigurable(key)

	switch {
	case exists && configurable:
		args.SessionDefaults[key] = value
	case sql.IsCustomOptionSessionVariable(key):
		args.CustomOptionSessionDefaults[key] = value
	case !exists:
		if _, ok := sql.UnsupportedVars[key]; ok {
			counter := sqltelemetry.UnimplementedClientStatusParameterCounter(key)
			telemetry.Inc(counter)
		}
		log.Warningf(ctx, "unknown configuration parameter: %q", key)

	case !configurable:
		return pgerror.Newf(pgcode.CantChangeRuntimeParam,
			"parameter %q cannot be changed", key)
	}
	return nil
}

// option represents an option argument passed in the connection URL.
type option struct {
	key   string
	value string
}

// parseOptions parses the given string into the options. The options must be
// separated by space and have one of the following patterns:
// '-c key=value', '-ckey=value', '--key=value'
func parseOptions(optionsString string) ([]option, error) {
	var res []option
	optionsRaw, err := url.QueryUnescape(optionsString)
	if err != nil {
		return nil, pgerror.Newf(pgcode.ProtocolViolation, "failed to unescape options %q", optionsString)
	}

	lastWasDashC := false
	opts := splitOptions(optionsRaw)

	for i := 0; i < len(opts); i++ {
		prefix := ""
		if len(opts[i]) > 1 {
			prefix = opts[i][:2]
		}

		switch {
		case opts[i] == "-c":
			lastWasDashC = true
			continue
		case lastWasDashC:
			lastWasDashC = false
			// if the last option was '-c' parse current option with no regard to
			// the prefix
			prefix = ""
		case prefix == "--" || prefix == "-c":
			lastWasDashC = false
		default:
			return nil, pgerror.Newf(pgcode.ProtocolViolation,
				"option %q is invalid, must have prefix '-c' or '--'", opts[i])
		}

		opt, err := splitOption(opts[i], prefix)
		if err != nil {
			return nil, err
		}
		res = append(res, opt)
	}
	return res, nil
}

// splitOptions slices the given string into substrings separated by space
// unless the space is escaped using backslashes '\\'. It also skips multiple
// subsequent spaces.
func splitOptions(options string) []string {
	var res []string
	var sb strings.Builder
	i := 0
	for i < len(options) {
		sb.Reset()
		// skip leading space
		for i < len(options) && unicode.IsSpace(rune(options[i])) {
			i++
		}
		if i == len(options) {
			break
		}

		lastWasEscape := false

		for i < len(options) {
			if unicode.IsSpace(rune(options[i])) && !lastWasEscape {
				break
			}
			if !lastWasEscape && options[i] == '\\' {
				lastWasEscape = true
			} else {
				lastWasEscape = false
				sb.WriteByte(options[i])
			}
			i++
		}

		res = append(res, sb.String())
	}

	return res
}

// splitOption splits the given opt argument into substrings separated by '='.
// It returns an error if the given option does not comply with the pattern
// "key=value" and the number of elements in the result is not two.
// splitOption removes the prefix from the key and replaces '-' with '_' so
// "--option-name=value" becomes [option_name, value].
func splitOption(opt, prefix string) (option, error) {
	kv := strings.Split(opt, "=")

	if len(kv) != 2 {
		return option{}, pgerror.Newf(pgcode.ProtocolViolation,
			"option %q is invalid, check '='", opt)
	}

	kv[0] = strings.TrimPrefix(kv[0], prefix)

	return option{key: strings.ReplaceAll(kv[0], "-", "_"), value: kv[1]}, nil
}

// Note: Usage of an env var here makes it possible to unconditionally
// enable this feature when cluster settings do not work reliably,
// e.g. in multi-tenant setups in v20.2. This override mechanism can
// be removed after all of CC is moved to use v21.1 or a version which
// supports cluster settings.
var trustClientProvidedRemoteAddrOverride = envutil.EnvOrDefaultBool("COCKROACH_TRUST_CLIENT_PROVIDED_SQL_REMOTE_ADDR", false)

// TestingSetTrustClientProvidedRemoteAddr is used in tests.
func (s *PreServeConnHandler) TestingSetTrustClientProvidedRemoteAddr(b bool) func() {
	prev := s.trustClientProvidedRemoteAddr.Get()
	s.trustClientProvidedRemoteAddr.Set(b)
	return func() { s.trustClientProvidedRemoteAddr.Set(prev) }
}

// TestingAcceptSystemIdentityOption is used in tests.
func (s *PreServeConnHandler) TestingAcceptSystemIdentityOption(b bool) func() {
	prev := s.acceptSystemIdentityOption.Get()
	s.acceptSystemIdentityOption.Set(b)
	return func() { s.acceptSystemIdentityOption.Set(prev) }
}
