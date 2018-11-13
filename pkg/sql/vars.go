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
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

const (
	// PgServerVersion is the latest version of postgres that we claim to support.
	PgServerVersion = "9.5.0"
	// PgServerVersionNum is the latest version of postgres that we claim to support in the numeric format of "server_version_num".
	PgServerVersionNum = "90500"
)

type getStringValFn = func(
	ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
) (string, error)

// sessionVar provides a unified interface for performing operations on
// variables such as the selected database, or desired syntax.
type sessionVar struct {
	// Get returns a string representation of a given variable to be used
	// either by SHOW or in the pg_catalog table.
	Get func(evalCtx *extendedEvalContext) string

	// GetStringVal converts the provided Expr to a string suitable
	// for Set() or RuntimeSet().
	// If this method is not provided,
	//   `getStringVal(evalCtx, varName, values)`
	// will be used instead.
	//
	// The reason why variable sets work in two phases like this is that
	// the Set() method has to operate on strings, because it can be
	// invoked at a point where there is no evalContext yet (e.g.
	// upon session initialization in pgwire).
	GetStringVal getStringValFn

	// Set performs mutations to effect the change desired by SET commands.
	// This method should be provided for variables that can be overridden
	// in pgwire.
	Set func(ctx context.Context, m *sessionDataMutator, val string) error

	// RuntimeSet is like Set except it can only be used in sessions
	// that are already running (i.e. not during session
	// initialization).  Currently only used for transaction_isolation.
	RuntimeSet func(_ context.Context, evalCtx *extendedEvalContext, s string) error

	// GlobalDefault is the string value to use as default for RESET or
	// during session initialization when no default value was provided
	// by the client.
	GlobalDefault func(sv *settings.Values) string
}

func formatBoolAsPostgresSetting(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func parsePostgresBool(s string) (bool, error) {
	s = strings.ToLower(s)
	switch s {
	case "on":
		s = "true"
	case "off":
		s = "false"
	}
	return strconv.ParseBool(s)
}

// varGen is the main definition array for all session variables.
// Note to maintainers: try to keep this sorted in the source code.
var varGen = map[string]sessionVar{
	// Set by clients to improve query logging.
	// See https://www.postgresql.org/docs/10/static/runtime-config-logging.html#GUC-APPLICATION-NAME
	`application_name`: {
		Set: func(
			_ context.Context, m *sessionDataMutator, s string,
		) error {
			m.SetApplicationName(s)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.ApplicationName
		},
		GlobalDefault: func(_ *settings.Values) string { return "" },
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html
	// and https://www.postgresql.org/docs/10/static/datatype-binary.html
	`bytea_output`: {
		Set: func(
			_ context.Context, m *sessionDataMutator, s string,
		) error {
			mode, ok := sessiondata.BytesEncodeFormatFromString(s)
			if !ok {
				return newVarValueError(`bytea_output`, s, "hex", "escape", "base64")
			}
			m.SetBytesEncodeFormat(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.DataConversion.BytesEncodeFormat.String()
		},
		GlobalDefault: func(sv *settings.Values) string { return sessiondata.BytesEncodeHex.String() },
	},

	// Supported for PG compatibility only.
	// Controls returned message verbosity. We don't support this.
	// See https://www.postgresql.org/docs/9.6/static/runtime-config-compatible.html
	`client_min_messages`: makeCompatStringVar(`client_min_messages`, `notice`, `debug5`, `debug4`, `debug3`, `debug2`, `debug1`, `debug`, `log`, `warning`, `error`, `fatal`, `panic`),

	// See https://www.postgresql.org/docs/9.6/static/multibyte.html
	// Also aliased to SET NAMES.
	`client_encoding`: {
		Set: func(
			_ context.Context, m *sessionDataMutator, s string,
		) error {
			encoding := builtins.CleanEncodingName(s)
			switch encoding {
			case "utf8", "unicode", "cp65001":
				return nil
			default:
				return pgerror.Unimplemented("client_encoding "+encoding,
					"unimplemented client encoding: %q", encoding)
			}
		},
		Get:           func(evalCtx *extendedEvalContext) string { return "UTF8" },
		GlobalDefault: func(_ *settings.Values) string { return "UTF8" },
	},

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/9.6/static/multibyte.html
	`server_encoding`: makeReadOnlyVar("UTF8"),

	// CockroachDB extension.
	`database`: {
		GetStringVal: func(
			ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) (string, error) {
			dbName, err := getStringVal(&evalCtx.EvalContext, `database`, values)
			if err != nil {
				return "", err
			}

			if len(dbName) == 0 && evalCtx.SessionData.SafeUpdates {
				return "", pgerror.NewDangerousStatementErrorf("SET database to empty string")
			}

			if len(dbName) != 0 {
				// Verify database descriptor exists.
				if _, err := evalCtx.schemaAccessors.logical.GetDatabaseDesc(dbName,
					DatabaseLookupFlags{ctx: ctx, txn: evalCtx.Txn, required: true}); err != nil {
					return "", err
				}
			}
			return dbName, nil
		},
		Set: func(
			ctx context.Context, m *sessionDataMutator, dbName string,
		) error {
			m.SetDatabase(dbName)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string { return evalCtx.SessionData.Database },
		GlobalDefault: func(_ *settings.Values) string {
			// The "defaultdb" value is set as session default in the pgwire
			// connection code. The global default is the empty string,
			// which is what internal connections should pick up.
			return ""
		},
	},

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-DATESTYLE
	`datestyle`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			s = strings.ToLower(s)
			parts := strings.Split(s, ",")
			if strings.TrimSpace(parts[0]) != "iso" ||
				(len(parts) == 2 && strings.TrimSpace(parts[1]) != "mdy") ||
				len(parts) > 2 {
				return newVarValueError("DateStyle", s, "ISO", "ISO, MDY").SetDetailf(
					"this parameter is currently recognized only for compatibility and has no effect in CockroachDB.")
			}
			return nil
		},
		Get:           func(evalCtx *extendedEvalContext) string { return "ISO, MDY" },
		GlobalDefault: func(_ *settings.Values) string { return "ISO, MDY" },
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-DEFAULT-TRANSACTION-ISOLATION
	`default_transaction_isolation`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			switch strings.ToUpper(s) {
			case `READ UNCOMMITTED`, `READ COMMITTED`, `SNAPSHOT`, `REPEATABLE READ`, `SERIALIZABLE`, `DEFAULT`:
				m.SetDefaultIsolationLevel(enginepb.SERIALIZABLE)
			default:
				return newVarValueError(`default_transaction_isolation`, s, "serializable")
			}

			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.DefaultIsolationLevel.ToLowerCaseString()
		},
		GlobalDefault: func(sv *settings.Values) string { return "default" },
	},

	// See https://www.postgresql.org/docs/9.3/static/runtime-config-client.html#GUC-DEFAULT-TRANSACTION-READ-ONLY
	`default_transaction_read_only`: {
		GetStringVal: makeBoolGetStringValFn("default_transaction_read_only"),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetDefaultReadOnly(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.DefaultReadOnly)
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`distsql`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			mode, ok := sessiondata.DistSQLExecModeFromString(s)
			if !ok {
				return newVarValueError(`distsql`, s, "on", "off", "auto", "always", "2.0-auto", "2.0-off")
			}
			m.SetDistSQLMode(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.DistSQLMode.String()
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondata.DistSQLExecMode(DistSQLClusterExecMode.Get(sv)).String()
		},
	},

	// CockroachDB extension.
	`experimental_force_lookup_join`: {
		GetStringVal: makeBoolGetStringValFn(`experimental_force_lookup_join`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetLookupJoinEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.LookupJoinEnabled)
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`experimental_force_split_at`: {
		GetStringVal: makeBoolGetStringValFn(`experimental_force_split_at`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetForceSplitAt(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.ForceSplitAt)
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`experimental_force_zigzag_join`: {
		GetStringVal: makeBoolGetStringValFn(`experimental_force_zigzag_join`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetZigzagJoinEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.ZigzagJoinEnabled)
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`experimental_vectorize`: {
		GetStringVal: makeBoolGetStringValFn(`experimental_vectorize`),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetVectorize(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.Vectorize)
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(VectorizeClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`optimizer`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			mode, ok := sessiondata.OptimizerModeFromString(s)
			if !ok {
				return newVarValueError(`optimizer`, s, "on", "off", "local", "always")
			}
			m.SetOptimizerMode(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.OptimizerMode.String()
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondata.OptimizerMode(
				OptimizerClusterMode.Get(sv)).String()
		},
	},

	// CockroachDB extension.
	`experimental_serial_normalization`: {
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			mode, ok := sessiondata.SerialNormalizationModeFromString(s)
			if !ok {
				return newVarValueError(`experimental_serial_normalization`, s,
					"rowid", "virtual_sequence", "sql_sequence")
			}
			m.SetSerialNormalizationMode(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.SerialNormalizationMode.String()
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondata.SerialNormalizationMode(
				SerialNormalizationMode.Get(sv)).String()
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html
	`extra_float_digits`: {
		GetStringVal: func(
			ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) (string, error) {
			// TODO(knz): extract this code into a common makeIntGetStringValFn when
			// there are multiple integer parameters.
			s, err := getIntVal(&evalCtx.EvalContext, `extra_float_digits`, values)
			if err != nil {
				return "", err
			}
			return strconv.FormatInt(s, 10), nil
		},
		Set: func(
			_ context.Context, m *sessionDataMutator, s string,
		) error {
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return wrapSetVarError("extra_float_digits", s, "%v", err)
			}
			// Note: this is the range allowed by PostgreSQL.
			// See also the documentation around (DataConversionConfig).GetFloatPrec()
			// in session_data.go.
			if i < -15 || i > 3 {
				return pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError,
					`%d is outside the valid range for parameter "extra_float_digits" (-15 .. 3)`, i)
			}
			m.SetExtraFloatDigits(int(i))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return fmt.Sprintf("%d", evalCtx.SessionData.DataConversion.ExtraFloatDigits)
		},
		GlobalDefault: func(sv *settings.Values) string { return "0" },
	},
	// CockroachDB extension. See docs on SessionData.ForceSavepointRestart.
	// https://github.com/cockroachdb/cockroach/issues/30588
	`force_savepoint_restart`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.SessionData.ForceSavepointRestart)
		},
		GetStringVal: makeBoolGetStringValFn("force_savepoint_restart"),
		Set: func(_ context.Context, m *sessionDataMutator, val string) error {
			b, err := parsePostgresBool(val)
			if err != nil {
				return err
			}
			if b {
				telemetry.Count("sql.force_savepoint_restart")
			}
			m.SetForceSavepointRestart(b)
			return nil
		},
		GlobalDefault: globalFalse,
	},
	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html
	`integer_datetimes`: makeReadOnlyVar("on"),

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-INTERVALSTYLE
	`intervalstyle`: makeCompatStringVar(`IntervalStyle`, "postgres"),

	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html#GUC-MAX-INDEX-KEYS
	`max_index_keys`: makeReadOnlyVar("32"),

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
			return formatBoolAsPostgresSetting(evalCtx.SessionData.SafeUpdates)
		},
		GetStringVal: makeBoolGetStringValFn("sql_safe_updates"),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetSafeUpdates(b)
			return nil
		},
		GlobalDefault: globalFalse,
	},

	// See https://www.postgresql.org/docs/10/static/ddl-schemas.html#DDL-SCHEMAS-PATH
	// https://www.postgresql.org/docs/9.6/static/runtime-config-client.html
	`search_path`: {
		GetStringVal: func(
			_ context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
		) (string, error) {
			comma := ""
			var buf bytes.Buffer
			for _, v := range values {
				s, err := datumAsString(&evalCtx.EvalContext, "search_path", v)
				if err != nil {
					return "", err
				}
				if strings.Contains(s, ",") {
					// TODO(knz): if/when we want to support this, we'll need to change
					// the interface between GetStringVal() and Set() to take string
					// arrays instead of a single string.
					return "", pgerror.Unimplemented("schema names containing commas in search_path",
						"schema name %q not supported in search_path", s)
				}
				buf.WriteString(comma)
				buf.WriteString(s)
				comma = ","
			}
			return buf.String(), nil
		},
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			paths := strings.Split(s, ",")
			m.SetSearchPath(sessiondata.MakeSearchPath(paths))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.SessionData.SearchPath.String()
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sqlbase.DefaultSearchPath.String()
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html#GUC-SERVER-VERSION
	`server_version`: makeReadOnlyVar(PgServerVersion),

	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html#GUC-SERVER-VERSION-NUM
	`server_version_num`: makeReadOnlyVar(PgServerVersionNum),

	// CockroachDB extension.
	`crdb_version`: makeReadOnlyVar(build.GetInfo().Short()),

	// CockroachDB extension.
	// In PG this is a pseudo-function used with SELECT, not SHOW.
	// See https://www.postgresql.org/docs/10/static/functions-info.html
	`session_user`: {
		Get: func(evalCtx *extendedEvalContext) string { return evalCtx.SessionData.User },
	},

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/10/static/runtime-config-compatible.html#GUC-STANDARD-CONFORMING-STRINGS
	`standard_conforming_strings`: makeCompatStringVar(`standard_conforming_strings`, "on"),

	`statement_timeout`: {
		GetStringVal: stmtTimeoutVarGetStringVal,
		Set:          stmtTimeoutVarSet,
		Get: func(evalCtx *extendedEvalContext) string {
			ms := evalCtx.SessionData.StmtTimeout.Nanoseconds() / int64(time.Millisecond)
			return strconv.FormatInt(ms, 10)
		},
		GlobalDefault: func(sv *settings.Values) string { return "0" },
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-TIMEZONE
	`timezone`: {
		Get: func(evalCtx *extendedEvalContext) string {
			// If the time zone is a "fixed offset" one, initialized from an offset
			// and not a standard name, then we use a magic format in the Location's
			// name. We attempt to parse that here and retrieve the original offset
			// specified by the user.
			locStr := evalCtx.SessionData.DataConversion.Location.String()
			_, origRepr, parsed := timeutil.ParseFixedOffsetTimeZone(locStr)
			if parsed {
				return origRepr
			}
			return locStr
		},
		GetStringVal:  timeZoneVarGetStringVal,
		Set:           timeZoneVarSet,
		GlobalDefault: func(_ *settings.Values) string { return "UTC" },
	},

	// This is not directly documented in PG's docs but does indeed behave this way.
	// See https://github.com/postgres/postgres/blob/REL_10_STABLE/src/backend/utils/misc/guc.c#L3401-L3409
	`transaction_isolation`: {
		Get: func(evalCtx *extendedEvalContext) string {
			return evalCtx.Txn.Isolation().ToLowerCaseString()
		},
		RuntimeSet: func(_ context.Context, evalCtx *extendedEvalContext, s string) error {
			isolationLevel, ok := tree.IsolationLevelMap[s]
			if !ok {
				return newVarValueError(`transaction_isolation`, s, "serializable")
			}
			return evalCtx.TxnModesSetter.setTransactionModes(
				tree.TransactionModes{
					Isolation: isolationLevel,
				})
		},
		GlobalDefault: func(_ *settings.Values) string { return "serializable" },
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
		GetStringVal: makeBoolGetStringValFn("transaction_read_only"),
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			b, err := parsePostgresBool(s)
			if err != nil {
				return err
			}
			m.SetReadOnly(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) string {
			return formatBoolAsPostgresSetting(evalCtx.TxnReadOnly)
		},
		GlobalDefault: globalFalse,
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
		// Setting is done by the SetTracing statement.
	},
}

func makeBoolGetStringValFn(varName string) getStringValFn {
	return func(
		ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
	) (string, error) {
		s, err := getSingleBool(varName, evalCtx, values)
		if err != nil {
			return "", err
		}
		return strconv.FormatBool(bool(*s)), nil
	}
}

func makeReadOnlyVar(value string) sessionVar {
	return sessionVar{
		Get:           func(_ *extendedEvalContext) string { return value },
		GlobalDefault: func(_ *settings.Values) string { return value },
	}
}

func globalFalse(_ *settings.Values) string { return formatBoolAsPostgresSetting(false) }

func makeCompatStringVar(varName, displayValue string, extraAllowed ...string) sessionVar {
	allowedVals := append(extraAllowed, strings.ToLower(displayValue))
	return sessionVar{
		Get: func(_ *extendedEvalContext) string {
			return displayValue
		},
		Set: func(_ context.Context, m *sessionDataMutator, s string) error {
			enc := strings.ToLower(s)
			for _, a := range allowedVals {
				if enc == a {
					return nil
				}
			}
			return newVarValueError(varName, s, allowedVals...).SetDetailf(
				"this parameter is currently recognized only for compatibility and has no effect in CockroachDB.")
		},
		GlobalDefault: func(sv *settings.Values) string { return displayValue },
	}
}

// IsSessionVariableConfigurable returns true iff there is a session
// variable with the given name and it is settable by a client
// (e.g. in pgwire).
func IsSessionVariableConfigurable(varName string) (exists, configurable bool) {
	v, exists := varGen[varName]
	return exists, v.Set != nil
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
		return nil, newSingleArgVarError(name)
	}
	val, err := values[0].Eval(&evalCtx.EvalContext)
	if err != nil {
		return nil, err
	}
	b, ok := val.(*tree.DBool)
	if !ok {
		return nil, pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError,
			"parameter %q requires a Boolean value", name).SetDetailf(
			"%s is a %s", values[0], val.ResolvedType())
	}
	return b, nil
}
