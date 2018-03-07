// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

const (
	// PgServerVersion is the latest version of postgres that we claim to support.
	PgServerVersion = "9.5.0"
	// PgServerVersionNum is the latest version of postgres that we claim to support in the numeric format of "server_version_num".
	PgServerVersionNum = "90500"
)

// sessionVar provides a unified interface for performing operations on
// variables such as the selected database, or desired syntax.
type sessionVar struct {
	// Set performs mutations to effect the change desired by SET commands.
	Set func(
		ctx context.Context, m *sessionDataMutator,
		evalCtx *extendedEvalContext, values []tree.TypedExpr,
	) error

	// Get returns a string representation of a given variable to be used
	// either by SHOW or in the pg_catalog table.
	Get func(evalCtx *extendedEvalContext) string

	// Reset performs mutations to effect the change desired by RESET commands.
	Reset func(m *sessionDataMutator) error
}

// nopVar is a placeholder for a number of settings sent by various client
// drivers which we do not support, but should simply ignore rather than
// throwing an error when trying to SET or SHOW them.
var nopVar = sessionVar{
	Set:   func(context.Context, *sessionDataMutator, *extendedEvalContext, []tree.TypedExpr) error { return nil },
	Get:   func(*extendedEvalContext) string { return "" },
	Reset: func(*sessionDataMutator) error { return nil },
}

func formatBoolAsPostgresSetting(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

// varGen is the main definition array for all session variables.
// Note to maintainers: try to keep this sorted in the source code.
var varGen = map[string]sessionVar{
	// Set by clients to improve query logging.
	// See https://www.postgresql.org/docs/10/static/runtime-config-logging.html#GUC-APPLICATION-NAME
	`application_name`: {
		Set: func(
			_ context.Context, m *sessionDataMutator,
			evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) error {
			s, err := getStringVal(&evalCtx.EvalContext, `application_name`, values)
			if err != nil {
				return err
			}
			m.SetApplicationName(s)

			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.ApplicationName
		},
		Reset: func(m *sessionDataMutator) error {
			m.SetApplicationName(m.defaults.applicationName)
			return nil
		},
	},

	// Supported for PG compatibility only.
	// Controls returned message verbosity. We don't support this.
	// See https://www.postgresql.org/docs/9.6/static/runtime-config-compatible.html
	`client_min_messages`: nopVar,

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/9.6/static/multibyte.html
	// Also aliased to SET NAMES.
	`client_encoding`: {
		Get: func(_ *extendedEvalContext) string {
			return "UTF8"
		},
		Set: func(
			_ context.Context, m *sessionDataMutator,
			evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) error {
			s, err := getStringVal(&evalCtx.EvalContext, `client_encoding`, values)
			if err != nil {
				return err
			}
			upper := strings.ToUpper(s)
			if upper != "UTF8" && upper != "UNICODE" {
				return fmt.Errorf("non-UTF8 encoding %s not supported", s)
			}
			return nil
		},
		Reset: func(_ *sessionDataMutator) error { return nil },
	},

	// CockroachDB extension.
	// TODO(knz): may need to be replaced by 1st element of search_path for
	// pg compatibility.
	`database`: {
		Set: func(
			ctx context.Context, m *sessionDataMutator,
			evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) error {
			dbName, err := getStringVal(&evalCtx.EvalContext, `database`, values)
			if err != nil {
				return err
			}

			if len(dbName) != 0 {
				// Verify database descriptor exists.
				if _, err := evalCtx.schemaAccessors.logical.GetDatabaseDesc(dbName,
					DatabaseLookupFlags{ctx: ctx, txn: evalCtx.Txn, required: true}); err != nil {
					return err
				}
			}
			m.SetDatabase(dbName)

			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string { return evalCtx.SessionData.Database },
		Reset: func(m *sessionDataMutator) error {
			m.SetDatabase(m.defaults.database)
			return nil
		},
	},

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-DATESTYLE
	`datestyle`: {
		Get: func(_ *extendedEvalContext) string {
			return "ISO"
		},
		Set: func(
			_ context.Context, m *sessionDataMutator,
			evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) error {
			s, err := getStringVal(&evalCtx.EvalContext, `datestyle`, values)
			if err != nil {
				return err
			}
			if strings.ToUpper(s) != "ISO" {
				return fmt.Errorf("non-ISO date style %s not supported", s)
			}
			return nil
		},
		Reset: func(_ *sessionDataMutator) error { return nil },
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-DEFAULT-TRANSACTION-ISOLATION
	`default_transaction_isolation`: {
		Set: func(
			_ context.Context, m *sessionDataMutator,
			evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) error {
			// It's unfortunate that clients want us to support both SET
			// SESSION CHARACTERISTICS AS TRANSACTION ..., which takes the
			// isolation level as keywords/identifiers (e.g. JDBC), and SET
			// DEFAULT_TRANSACTION_ISOLATION TO '...', which takes an
			// expression (e.g. psycopg2). But that's how it is.  Just ensure
			// this code keeps in sync with SetSessionCharacteristics() in set.go.
			s, err := getStringVal(&evalCtx.EvalContext, `default_transaction_isolation`, values)
			if err != nil {
				return err
			}
			switch strings.ToUpper(s) {
			case `READ UNCOMMITTED`, `READ COMMITTED`, `SNAPSHOT`:
				m.SetDefaultIsolationLevel(enginepb.SNAPSHOT)
			case `REPEATABLE READ`, `SERIALIZABLE`:
				m.SetDefaultIsolationLevel(enginepb.SERIALIZABLE)
			default:
				return fmt.Errorf("set default_transaction_isolation: unknown isolation level: %q", s)
			}

			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.DefaultIsolationLevel.ToLowerCaseString()
		},
		Reset: func(m *sessionDataMutator) error {
			m.SetDefaultIsolationLevel(enginepb.IsolationType(0))
			return nil
		},
	},

	// See https://www.postgresql.org/docs/9.3/static/runtime-config-client.html#GUC-DEFAULT-TRANSACTION-READ-ONLY
	`default_transaction_read_only`: {
		Set: func(
			_ context.Context, m *sessionDataMutator,
			evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) error {
			s, err := getSingleBool("default_transaction_read_only", evalCtx, values)
			if err != nil {
				return err
			}
			m.SetDefaultReadOnly(bool(*s))

			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.DefaultReadOnly)
		},
		Reset: func(m *sessionDataMutator) error {
			m.SetDefaultReadOnly(false)
			return nil
		},
	},

	// CockroachDB extension.
	`distsql`: {
		Set: func(
			_ context.Context, m *sessionDataMutator,
			evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) error {
			s, err := getStringVal(&evalCtx.EvalContext, `distsql`, values)
			if err != nil {
				return err
			}
			mode, ok := sessiondata.DistSQLExecModeFromString(s)
			if !ok {
				return fmt.Errorf("set distsql: \"%s\" not supported", s)
			}
			m.SetDistSQLMode(mode)

			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.DistSQLMode.String()
		},
		Reset: func(m *sessionDataMutator) error {
			m.SetDistSQLMode(sessiondata.DistSQLExecMode(
				DistSQLClusterExecMode.Get(&m.settings.SV)),
			)
			return nil
		},
	},

	// CockroachDB extension.
	`experimental_force_lookup_join`: {
		Set: func(
			_ context.Context, m *sessionDataMutator,
			evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) error {
			s, err := getSingleBool("experimental_force_lookup_join", evalCtx, values)
			if err != nil {
				return err
			}
			m.SetLookupJoinEnabled(bool(*s))

			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.LookupJoinEnabled)
		},
		Reset: func(m *sessionDataMutator) error {
			m.SetLookupJoinEnabled(false)
			return nil
		},
	},

	// CockroachDB extension.
	`experimental_opt`: {
		Set: func(
			_ context.Context, m *sessionDataMutator,
			evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) error {
			s, err := getStringVal(&evalCtx.EvalContext, `experimental_opt`, values)
			if err != nil {
				return err
			}
			mode, ok := sessiondata.OptimizerModeFromString(s)
			if !ok {
				return fmt.Errorf("set experimental_opt: \"%s\" not supported", s)
			}
			m.SetOptimizerMode(mode)

			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.DistSQLMode.String()
		},
		Reset: func(m *sessionDataMutator) error {
			m.SetDistSQLMode(sessiondata.DistSQLExecMode(
				DistSQLClusterExecMode.Get(&m.settings.SV)))
			return nil
		},
	},

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html
	`extra_float_digits`: nopVar,

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html
	`intervalstyle`: {
		Get: func(_ *extendedEvalContext) string {
			return "postgres"
		},
		Set: func(
			_ context.Context, m *sessionDataMutator,
			evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) error {
			s, err := getStringVal(&evalCtx.EvalContext, `intervalstyle`, values)
			if err != nil {
				return err
			}
			if strings.ToLower(s) != "postgres" {
				return fmt.Errorf("non-postgres interval style %s not supported", s)
			}
			return nil
		},
		Reset: func(_ *sessionDataMutator) error { return nil },
	},

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html#GUC-MAX-INDEX-KEYS
	`max_index_keys`: {
		Get: func(evalCtx *extendedEvalContext) string { return "32" },
	},

	// CockroachDB extension.
	`node_id`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return fmt.Sprintf("%d", evalCtx.NodeID)
		},
	},

	// CockroachDB extension (inspired by MySQL).
	// See https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_sql_safe_updates
	`sql_safe_updates`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return strconv.FormatBool(evalCtx.SessionData.SafeUpdates)
		},
		Set: func(
			_ context.Context, m *sessionDataMutator,
			evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) error {
			b, err := getSingleBool("sql_safe_updates", evalCtx, values)
			if err != nil {
				return err
			}
			m.SetSafeUpdates(b == tree.DBoolTrue)
			return nil
		},
	},

	// See https://www.postgresql.org/docs/10/static/ddl-schemas.html#DDL-SCHEMAS-PATH
	`search_path`: {
		Set: func(
			_ context.Context, m *sessionDataMutator,
			evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) error {
			// https://www.postgresql.org/docs/9.6/static/runtime-config-client.html
			paths := make([]string, len(values))
			for i, v := range values {
				s, err := datumAsString(&evalCtx.EvalContext, "search_path", v)
				if err != nil {
					return err
				}
				paths[i] = s
			}
			m.SetSearchPath(sessiondata.MakeSearchPath(paths))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.SearchPath.String()
		},
		Reset: func(m *sessionDataMutator) error {
			m.SetSearchPath(sqlbase.DefaultSearchPath)
			return nil
		},
	},

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html#GUC-SERVER-VERSION
	`server_version`: {
		Get: func(evalCtx *extendedEvalContext) string { return PgServerVersion },
	},

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html#GUC-SERVER-VERSION-NUM
	`server_version_num`: {
		Get: func(evalCtx *extendedEvalContext) string { return PgServerVersionNum },
	},

	// CockroachDB extension.
	// In PG this is a pseudo-function used with SELECT, not SHOW.
	// See https://www.postgresql.org/docs/10/static/functions-info.html
	`session_user`: {
		Get: func(evalCtx *extendedEvalContext) string { return evalCtx.SessionData.User },
	},

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/10/static/runtime-config-compatible.html#GUC-STANDARD-CONFORMING-STRINGS
	`standard_conforming_strings`: {
		Set: func(
			_ context.Context, m *sessionDataMutator,
			evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) error {
			// If true, escape backslash literals in strings. We do this by default,
			// and we do not support the opposite behavior.
			s, err := getStringVal(&evalCtx.EvalContext, `standard_conforming_strings`, values)
			if err != nil {
				return err
			}
			if strings.ToLower(s) != "on" {
				return fmt.Errorf("set standard_conforming_strings: \"%s\" not supported", s)
			}

			return nil
		},
		Get:   func(_ *extendedEvalContext) string { return "on" },
		Reset: func(_ *sessionDataMutator) error { return nil },
	},

	`statement_timeout`: {
		Set: setStmtTimeout,
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.StmtTimeout.String()
		},
		Reset: func(m *sessionDataMutator) error {
			m.SetStmtTimeout(0)
			return nil
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-TIMEZONE
	`timezone`: {
		Get: func(evalCtx *extendedEvalContext) string {
			// If the time zone is a "fixed offset" one, initialized from an offset
			// and not a standard name, then we use a magic format in the Location's
			// name. We attempt to parse that here and retrieve the original offset
			// specified by the user.
			_, origRepr, parsed := timeutil.ParseFixedOffsetTimeZone(evalCtx.SessionData.Location.String())
			if parsed {
				return origRepr
			}
			return evalCtx.SessionData.Location.String()
		},
		Set: setTimeZone,
		Reset: func(m *sessionDataMutator) error {
			m.SetLocation(time.UTC)
			return nil
		},
	},

	// This is not directly documented in PG's docs but does indeed behave this way.
	// See https://github.com/postgres/postgres/blob/REL_10_STABLE/src/backend/utils/misc/guc.c#L3401-L3409
	`transaction_isolation`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.Txn.Isolation().ToLowerCaseString()
		},
		Set: func(
			_ context.Context, _ *sessionDataMutator,
			evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) error {
			s, err := getStringVal(&evalCtx.EvalContext, `transaction_isolation`, values)
			if err != nil {
				return err
			}
			isolationLevel, ok := tree.IsolationLevelMap[s]
			if !ok {
				return pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError,
					"unsupported isolation level \"%s\"", s)
			}
			return evalCtx.TxnModesSetter.setTransactionModes(
				tree.TransactionModes{
					Isolation: isolationLevel,
				})
		},
	},

	// CockroachDB extension.
	`transaction_priority`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.Txn.UserPriority().String()
		},
	},

	// CockroachDB extension.
	`transaction_status`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.TxnState
		},
	},

	// See https://www.postgresql.org/docs/10/static/hot-standby.html#HOT-STANDBY-USERS
	`transaction_read_only`: {
		Set: func(
			_ context.Context, m *sessionDataMutator,
			evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) error {
			s, err := getSingleBool("transaction_read_only", evalCtx, values)
			if err != nil {
				return err
			}
			m.SetReadOnly(bool(*s))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.TxnReadOnly)
		},
	},

	// CockroachDB extension.
	`tracing`: {
		Get: func(evalCtx *extendedEvalContext) string {
			sessTracing := evalCtx.Tracing
			if sessTracing.Enabled() {
				val := "on"
				if sessTracing.RecordingType() == tracing.SingleNodeRecording {
					val += ", local"
				}
				if sessTracing.KVTracingEnabled() {
					val += ", kv"
				}
				return val
			}
			return "off"
		},
		Reset: func(m *sessionDataMutator) error {
			if !m.sessionTracing.Enabled() {
				// Tracing is not active. Nothing to do.
				return nil
			}
			return stopTracing(m)
		},
		Set: func(
			_ context.Context, m *sessionDataMutator,
			evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) error {
			return enableTracing(&evalCtx.EvalContext, m, values)
		},
	},
}

func enableTracing(
	evalCtx *tree.EvalContext, m *sessionDataMutator, values []tree.TypedExpr,
) error {
	traceKV := false
	recordingType := tracing.SnowballRecording
	enableMode := true

	for _, v := range values {
		s, err := datumAsString(evalCtx, "trace", v)
		if err != nil {
			return err
		}

		switch strings.ToLower(s) {
		case "on":
			enableMode = true
		case "off":
			enableMode = false
		case "kv":
			traceKV = true
		case "local":
			recordingType = tracing.SingleNodeRecording
		case "cluster":
			recordingType = tracing.SnowballRecording
		default:
			return errors.Errorf("set tracing: unknown mode %q", s)
		}
	}
	if !enableMode {
		return stopTracing(m)
	}
	return m.StartSessionTracing(recordingType, traceKV)
}

func stopTracing(m *sessionDataMutator) error {
	if err := m.StopSessionTracing(); err != nil {
		return errors.Wrapf(err, "error stopping tracing")
	}
	return nil
}

var varNames = func() []string {
	res := make([]string, 0, len(varGen))
	for vName := range varGen {
		res = append(res, vName)
	}
	sort.Strings(res)
	return res
}()

func getSingleBool(
	name string, evalCtx *extendedEvalContext, values []tree.TypedExpr,
) (*tree.DBool, error) {
	if len(values) != 1 {
		return nil, fmt.Errorf("set %s requires a single argument", name)
	}
	val, err := values[0].Eval(&evalCtx.EvalContext)
	if err != nil {
		return nil, err
	}
	b, ok := val.(*tree.DBool)
	if !ok {
		return nil, fmt.Errorf("set %s requires a boolean value: %s is a %s",
			name, values[0], val.ResolvedType())
	}
	return b, nil
}
