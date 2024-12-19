// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire

import (
	"context"
	"encoding/base64"
	"net"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
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
		// Note: GetSafeString is used since the key/value will live well past the
		// life of the message.
		key, err := buf.GetSafeString()
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
		value, err := buf.GetSafeString()
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

		case "replication":
			// We chose to make the "replication" connection parameter to be
			// represented by a session variable.
			if err := loadParameter(ctx, key, value, &args.SessionArgs); err != nil {
				return args, pgerror.Wrapf(err, pgerror.GetPGCode(err), "replication parameter")
			}
			// Cache the value into session args.
			args.ReplicationMode, err = sql.ReplicationModeFromString(args.SessionArgs.SessionDefaults["replication"])
			if err != nil {
				return args, err
			}

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
			opts, err := pgurl.ParseExtendedOptions(value)
			if err != nil {
				return args, pgerror.WithCandidateCode(err, pgcode.ProtocolViolation)
			}
			for opt, values := range opts {
				var optvalue string
				if len(values) > 0 {
					optvalue = values[0]
				}
				// crdb:jwt_auth_enabled must be passed as an option in order for us to support non-CRDB
				// clients. jwt_auth_enabled is not a session variable. We extract it separately here.
				switch strings.ToLower(opt) {
				case "crdb:jwt_auth_enabled":
					b, err := strconv.ParseBool(optvalue)
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
					args.SystemIdentity = optvalue
					continue

				case "cluster":
					if !acceptTenantName {
						return args, pgerror.Newf(pgcode.InvalidParameterValue,
							"tenant selection is not available on this server")
					}
					// The syntax after '.' will be extended in later versions.
					// The period itself cannot occur in a tenant name.
					parts := strings.SplitN(optvalue, ".", 2)
					args.tenantName = parts[0]
					hasTenantSelectOption = true
					continue
				case "results_buffer_size":
					if args.ConnResultsBufferSize, err = humanizeutil.ParseBytes(optvalue); err != nil {
						return args, errors.WithSecondaryError(
							pgerror.Newf(pgcode.ProtocolViolation,
								"error parsing results_buffer_size option value '%s' as bytes", optvalue), err)
					}
					if args.ConnResultsBufferSize < 0 {
						return args, pgerror.Newf(pgcode.ProtocolViolation,
							"results_buffer_size option value '%s' cannot be negative", value)
					}
					args.foundBufferSize = true
					continue
				}
				err = loadParameter(ctx, opt, optvalue, &args.SessionArgs)
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

// Note: Usage of an env var here makes it possible to unconditionally
// enable this feature when cluster settings do not work reliably,
// e.g. in multi-tenant setups in v20.2. This override mechanism can
// be removed after all of CC is moved to use v21.1 or a version which
// supports cluster settings.
var trustClientProvidedRemoteAddrOverride = envutil.EnvOrDefaultBool("COCKROACH_TRUST_CLIENT_PROVIDED_SQL_REMOTE_ADDR", false)

// TestingSetTrustClientProvidedRemoteAddr is used in tests.
func (s *PreServeConnHandler) TestingSetTrustClientProvidedRemoteAddr(b bool) func() {
	prev := s.trustClientProvidedRemoteAddr.Load()
	s.trustClientProvidedRemoteAddr.Store(b)
	return func() { s.trustClientProvidedRemoteAddr.Store(prev) }
}

// TestingAcceptSystemIdentityOption is used in tests.
func (s *PreServeConnHandler) TestingAcceptSystemIdentityOption(b bool) func() {
	prev := s.acceptSystemIdentityOption.Load()
	s.acceptSystemIdentityOption.Store(b)
	return func() { s.acceptSystemIdentityOption.Store(prev) }
}
