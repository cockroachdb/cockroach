// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/delegate"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
)

const (
	// PgServerVersion is the latest version of postgres that we claim to support.
	PgServerVersion = "13.0.0"
	// PgServerVersionNum is the latest version of postgres that we claim to support in the numeric format of "server_version_num".
	PgServerVersionNum = "130000"
	// PgCompatLocale is the locale string we advertise in `LC_*` session
	// variables. C.UTF-8 is the only locale that is allowed in CREATE DATABASE
	// at the time of writing.
	// See https://www.postgresql.org/docs/14/locale.html
	PgCompatLocale = "C.UTF-8"
)

type getStringValFn = func(
	ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
) (string, error)

// sessionVar provides a unified interface for performing operations on
// variables such as the selected database, or desired syntax.
type sessionVar struct {
	// Hidden indicates that the variable should not show up in the output of SHOW ALL.
	Hidden bool

	// Get returns a string representation of a given variable to be used
	// either by SHOW or in the pg_catalog table.
	Get func(evalCtx *extendedEvalContext) (string, error)

	// GetFromSessionData returns a string representation of a given variable to
	// be used by BufferParamStatus. This is only required if the variable
	// is expected to send updates through ParamStatusUpdate in pgwire.
	GetFromSessionData func(sd *sessiondata.SessionData) string

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
	Set func(ctx context.Context, m sessionDataMutator, val string) error

	// RuntimeSet is like Set except it can only be used in sessions
	// that are already running (i.e. not during session
	// initialization). Currently only used for transaction_isolation.
	RuntimeSet func(_ context.Context, evalCtx *extendedEvalContext, local bool, s string) error

	// SetWithPlanner is like Set except it can only be used in sessions
	// that are already running and the planner is passed in.
	// The planner can be used to check privileges before setting.
	SetWithPlanner func(_ context.Context, p *planner, local bool, s string) error

	// GlobalDefault is the string value to use as default for RESET or
	// during session initialization when no default value was provided
	// by the client.
	GlobalDefault func(sv *settings.Values) string

	// Equal returns whether the value of the given variable is equal between the
	// two SessionData references. This is only required if the variable is
	// expected to send updates through ParamStatusUpdate in pgwire.
	Equal func(a, b *sessiondata.SessionData) bool
}

func formatBoolAsPostgresSetting(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func formatFloatAsPostgresSetting(f float64) string {
	return strconv.FormatFloat(f, 'G', -1, 64)
}

// makeDummyBooleanSessionVar generates a sessionVar for a bool session setting.
// These functions allow the setting to be changed, but whose values are not used.
// They are logged to telemetry and output a notice that these are unused.
func makeDummyBooleanSessionVar(
	name string,
	getFunc func(*extendedEvalContext) (string, error),
	setFunc func(sessionDataMutator, bool),
	sv func(_ *settings.Values) string,
) sessionVar {
	return sessionVar{
		GetStringVal: makePostgresBoolGetStringValFn(name),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar(name, s)
			if err != nil {
				return err
			}
			setFunc(m, b)
			return nil
		},
		Get:           getFunc,
		GlobalDefault: sv,
	}
}

// varGen is the main definition array for all session variables.
// Note to maintainers: try to keep this sorted in the source code.
var varGen = map[string]sessionVar{
	// Set by clients to improve query logging.
	// See https://www.postgresql.org/docs/10/static/runtime-config-logging.html#GUC-APPLICATION-NAME
	`application_name`: {
		Set: func(
			_ context.Context, m sessionDataMutator, s string,
		) error {
			m.SetApplicationName(s)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return evalCtx.SessionData().ApplicationName, nil
		},
		GetFromSessionData: func(sd *sessiondata.SessionData) string {
			return sd.ApplicationName
		},
		GlobalDefault: func(_ *settings.Values) string {
			return ""
		},
		Equal: func(a, b *sessiondata.SessionData) bool {
			return a.ApplicationName == b.ApplicationName
		},
	},

	// CockroachDB extension.
	`avoid_buffering`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().AvoidBuffering), nil
		},
		GetStringVal: makePostgresBoolGetStringValFn("avoid_buffering"),
		Set: func(ctx context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar(`avoid_buffering`, s)
			if err != nil {
				return err
			}
			m.SetAvoidBuffering(b)
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return "false"
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html
	// and https://www.postgresql.org/docs/10/static/datatype-binary.html
	`bytea_output`: {
		Set: func(
			_ context.Context, m sessionDataMutator, s string,
		) error {
			mode, ok := lex.BytesEncodeFormatFromString(s)
			if !ok {
				return newVarValueError(`bytea_output`, s, "hex", "escape", "base64")
			}
			m.SetBytesEncodeFormat(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return evalCtx.SessionData().DataConversionConfig.BytesEncodeFormat.String(), nil
		},
		GlobalDefault: func(sv *settings.Values) string { return lex.BytesEncodeHex.String() },
	},

	`client_min_messages`: {
		Set: func(
			_ context.Context, m sessionDataMutator, s string,
		) error {
			severity, ok := pgnotice.ParseDisplaySeverity(s)
			if !ok {
				return errors.WithHintf(
					pgerror.Newf(
						pgcode.InvalidParameterValue,
						"%s is not supported",
						severity,
					),
					"Valid severities are: %s.",
					strings.Join(pgnotice.ValidDisplaySeverities(), ", "),
				)
			}
			m.SetNoticeDisplaySeverity(severity)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return pgnotice.DisplaySeverity(evalCtx.SessionData().NoticeDisplaySeverity).String(), nil
		},
		GlobalDefault: func(_ *settings.Values) string { return "notice" },
	},

	// See https://www.postgresql.org/docs/9.6/static/multibyte.html
	// Also aliased to SET NAMES.
	`client_encoding`: {
		Set: func(
			_ context.Context, m sessionDataMutator, s string,
		) error {
			encoding := builtins.CleanEncodingName(s)
			switch encoding {
			case "utf8", "unicode", "cp65001":
				return nil
			default:
				return unimplemented.NewWithIssueDetailf(35882,
					"client_encoding "+encoding,
					"unimplemented client encoding: %q", encoding)
			}
		},
		Get:           func(evalCtx *extendedEvalContext) (string, error) { return "UTF8", nil },
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

			if len(dbName) == 0 && evalCtx.SessionData().SafeUpdates {
				return "", pgerror.DangerousStatementf("SET database to empty string")
			}

			if len(dbName) != 0 {
				// Verify database descriptor exists.
				if _, err := evalCtx.Descs.GetImmutableDatabaseByName(
					ctx, evalCtx.Txn, dbName, tree.DatabaseLookupFlags{Required: true},
				); err != nil {
					return "", err
				}
			}
			return dbName, nil
		},
		Set: func(_ context.Context, m sessionDataMutator, dbName string) error {
			m.SetDatabase(dbName)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) { return evalCtx.SessionData().Database, nil },
		GlobalDefault: func(_ *settings.Values) string {
			// The "defaultdb" value is set as session default in the pgwire
			// connection code. The global default is the empty string,
			// which is what internal connections should pick up.
			return ""
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-DATESTYLE
	`datestyle`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			ds, err := pgdate.ParseDateStyle(s, m.data.GetDateStyle())
			if err != nil {
				return newVarValueError("DateStyle", s, pgdate.AllowedDateStyles()...)
			}
			if ds.Style != pgdate.Style_ISO {
				return unimplemented.NewWithIssue(41773, "only ISO style is supported")
			}
			if ds.Order != pgdate.Order_MDY && !m.data.DateStyleEnabled {
				return errors.WithDetailf(
					errors.WithHintf(
						pgerror.Newf(
							pgcode.FeatureNotSupported,
							"setting DateStyle is not enabled",
						),
						"You can enable DateStyle customization for all sessions with the cluster setting %s, or per session using SET datestyle_enabled = true.",
						dateStyleEnabledClusterSetting,
					),
					"Setting DateStyle changes the volatility of timestamp/timestamptz/date::string "+
						"and string::timestamp/timestamptz/date/time/timetz casts from immutable to stable. "+
						"No computed columns, partial indexes, partitions and check constraints can "+
						"use this casts. "+
						"Use to_char_with_style or parse_{timestamp,timestamptz,date,time,timetz} "+
						"instead if you need these casts to work in the aforementioned cases.",
				)
			}
			m.SetDateStyle(ds)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return evalCtx.GetDateStyle().SQLString(), nil
		},
		GetFromSessionData: func(sd *sessiondata.SessionData) string {
			return sd.GetDateStyle().SQLString()
		},
		GlobalDefault: func(sv *settings.Values) string {
			return dateStyleEnumMap[dateStyle.Get(sv)]
		},
		Equal: func(a, b *sessiondata.SessionData) bool {
			return a.GetDateStyle() == b.GetDateStyle()
		},
	},
	`datestyle_enabled`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().DateStyleEnabled), nil
		},
		GetStringVal: makePostgresBoolGetStringValFn("datestyle_enabled"),
		Set: func(ctx context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar(`datestyle_enabled`, s)
			if err != nil {
				return err
			}
			m.SetDateStyleEnabled(b)
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(dateStyleEnabled.Get(sv))
		},
	},

	// Controls the subsequent parsing of a "naked" INT type.
	// TODO(bob): Remove or no-op this in v2.4: https://github.com/cockroachdb/cockroach/issues/32844
	`default_int_size`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return strconv.FormatInt(int64(evalCtx.SessionData().DefaultIntSize), 10), nil
		},
		GetStringVal: makeIntGetStringValFn("default_int_size"),
		Set: func(ctx context.Context, m sessionDataMutator, val string) error {
			i, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return wrapSetVarError(err, "default_int_size", val)
			}
			if i != 4 && i != 8 {
				return pgerror.New(pgcode.InvalidParameterValue,
					`only 4 or 8 are supported by default_int_size`)
			}
			// Only record when the value has been changed to a non-default
			// value, since we really just want to know how useful int4-mode
			// is. If we were to record counts for size.4 and size.8
			// variables, we'd have to distinguish cases in which a session
			// was opened in int8 mode and switched to int4 mode, versus ones
			// set to int4 by a connection string.
			// TODO(bob): Change to 8 in v2.3: https://github.com/cockroachdb/cockroach/issues/32534
			if i == 4 {
				telemetry.Inc(sqltelemetry.DefaultIntSize4Counter)
			}
			m.SetDefaultIntSize(int32(i))
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(defaultIntSize.Get(sv), 10)
		},
	},

	// See https://www.postgresql.org/docs/10/runtime-config-client.html.
	// Supported only for pg compatibility - CockroachDB has no notion of
	// tablespaces.
	`default_tablespace`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			if s != "" {
				return newVarValueError(`default_tablespace`, s, "")
			}
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return "", nil
		},
		GlobalDefault: func(sv *settings.Values) string { return "" },
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-DEFAULT-TRANSACTION-ISOLATION
	`default_transaction_isolation`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			switch strings.ToUpper(s) {
			case `READ UNCOMMITTED`, `READ COMMITTED`, `SNAPSHOT`, `REPEATABLE READ`, `SERIALIZABLE`, `DEFAULT`:
				// Do nothing. All transactions execute with serializable isolation.
			default:
				return newVarValueError(`default_transaction_isolation`, s, "serializable")
			}

			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return "serializable", nil
		},
		GlobalDefault: func(sv *settings.Values) string { return "default" },
	},

	// CockroachDB extension.
	`default_transaction_priority`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			pri, ok := tree.UserPriorityFromString(s)
			if !ok {
				return newVarValueError(`default_transaction_isolation`, s, "low", "normal", "high")
			}
			m.SetDefaultTransactionPriority(pri)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			pri := tree.UserPriority(evalCtx.SessionData().DefaultTxnPriority)
			if pri == tree.UnspecifiedUserPriority {
				pri = tree.Normal
			}
			return strings.ToLower(pri.String()), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strings.ToLower(tree.Normal.String())
		},
	},

	// See https://www.postgresql.org/docs/9.3/static/runtime-config-client.html#GUC-DEFAULT-TRANSACTION-READ-ONLY
	`default_transaction_read_only`: {
		GetStringVal: makePostgresBoolGetStringValFn("default_transaction_read_only"),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("default_transaction_read_only", s)
			if err != nil {
				return err
			}
			m.SetDefaultTransactionReadOnly(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().DefaultTxnReadOnly), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`default_transaction_use_follower_reads`: {
		GetStringVal: makePostgresBoolGetStringValFn("default_transaction_use_follower_reads"),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("default_transaction_use_follower_reads", s)
			if err != nil {
				return err
			}
			m.SetDefaultTransactionUseFollowerReads(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().DefaultTxnUseFollowerReads), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`disable_plan_gists`: {
		GetStringVal: makePostgresBoolGetStringValFn(`disable_plan_gists`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("disable_plan_gists", s)
			if err != nil {
				return err
			}
			m.SetDisablePlanGists(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().DisablePlanGists), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`index_recommendations_enabled`: {
		GetStringVal: makePostgresBoolGetStringValFn(`index_recommendations_enabled`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("index_recommendations_enabled", s)
			if err != nil {
				return err
			}
			m.SetIndexRecommendationsEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().IndexRecommendationsEnabled), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`distsql`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			mode, ok := sessiondatapb.DistSQLExecModeFromString(s)
			if !ok {
				return newVarValueError(`distsql`, s, "on", "off", "auto", "always", "2.0-auto", "2.0-off")
			}
			m.SetDistSQLMode(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return evalCtx.SessionData().DistSQLMode.String(), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondatapb.DistSQLExecMode(DistSQLClusterExecMode.Get(sv)).String()
		},
	},

	// CockroachDB extension.
	`distsql_workmem`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			limit, err := humanizeutil.ParseBytes(s)
			if err != nil {
				return err
			}
			if limit <= 0 {
				return errors.New("distsql_workmem can only be set to a positive value")
			}
			m.SetDistSQLWorkMem(limit)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return string(humanizeutil.IBytes(evalCtx.SessionData().WorkMemLimit)), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return string(humanizeutil.IBytes(settingWorkMemBytes.Get(sv)))
		},
	},

	// CockroachDB extension.
	`experimental_distsql_planning`: {
		GetStringVal: makePostgresBoolGetStringValFn(`experimental_distsql_planning`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			mode, ok := sessiondatapb.ExperimentalDistSQLPlanningModeFromString(s)
			if !ok {
				return newVarValueError(`experimental_distsql_planning`, s,
					"off", "on", "always")
			}
			m.SetExperimentalDistSQLPlanning(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return evalCtx.SessionData().ExperimentalDistSQLPlanningMode.String(), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondatapb.ExperimentalDistSQLPlanningMode(experimentalDistSQLPlanningClusterMode.Get(sv)).String()
		},
	},

	// CockroachDB extension.
	`disable_partially_distributed_plans`: {
		GetStringVal: makePostgresBoolGetStringValFn(`disable_partially_distributed_plans`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("disable_partially_distributed_plans", s)
			if err != nil {
				return err
			}
			m.SetPartiallyDistributedPlansDisabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().PartiallyDistributedPlansDisabled), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`enable_zigzag_join`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enable_zigzag_join`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("enable_zigzag_join", s)
			if err != nil {
				return err
			}
			m.SetZigzagJoinEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().ZigzagJoinEnabled), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(zigzagJoinClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`reorder_joins_limit`: {
		GetStringVal: makeIntGetStringValFn(`reorder_joins_limit`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if b < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set reorder_joins_limit to a negative value: %d", b)
			}
			m.SetReorderJoinsLimit(int(b))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return strconv.FormatInt(evalCtx.SessionData().ReorderJoinsLimit, 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(ReorderJoinsLimitClusterValue.Get(sv), 10)
		},
	},

	// CockroachDB extension.
	`require_explicit_primary_keys`: {
		GetStringVal: makePostgresBoolGetStringValFn(`require_explicit_primary_keys`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("require_explicit_primary_key", s)
			if err != nil {
				return err
			}
			m.SetRequireExplicitPrimaryKeys(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().RequireExplicitPrimaryKeys), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(requireExplicitPrimaryKeysClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`vectorize`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			mode, ok := sessiondatapb.VectorizeExecModeFromString(s)
			if !ok {
				return newVarValueError(`vectorize`, s,
					"off", "on", "experimental_always")
			}
			m.SetVectorize(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return evalCtx.SessionData().VectorizeMode.String(), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondatapb.VectorizeExecMode(
				VectorizeClusterMode.Get(sv)).String()
		},
	},

	// CockroachDB extension.
	`testing_vectorize_inject_panics`: {
		GetStringVal: makePostgresBoolGetStringValFn(`testing_vectorize_inject_panics`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("testing_vectorize_inject_panics", s)
			if err != nil {
				return err
			}
			m.SetTestingVectorizeInjectPanics(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().TestingVectorizeInjectPanics), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	// This is deprecated; the only allowable setting is "on".
	`optimizer`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			if strings.ToUpper(s) != "ON" {
				return newVarValueError(`optimizer`, s, "on")
			}
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return "on", nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return "on"
		},
	},

	// CockroachDB extension.
	`foreign_key_cascades_limit`: {
		GetStringVal: makeIntGetStringValFn(`foreign_key_cascades_limit`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if b < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set foreign_key_cascades_limit to a negative value: %d", b)
			}
			m.SetOptimizerFKCascadesLimit(int(b))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return strconv.FormatInt(evalCtx.SessionData().OptimizerFKCascadesLimit, 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(optDrivenFKCascadesClusterLimit.Get(sv), 10)
		},
	},

	// CockroachDB extension.
	`optimizer_use_histograms`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_histograms`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_histograms", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUseHistograms(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseHistograms), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(optUseHistogramsClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`optimizer_use_multicol_stats`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_multicol_stats`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_multicol_stats", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUseMultiColStats(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseMultiColStats), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(optUseMultiColStatsClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`locality_optimized_partitioned_index_scan`: {
		GetStringVal: makePostgresBoolGetStringValFn(`locality_optimized_partitioned_index_scan`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar(`locality_optimized_partitioned_index_scan`, s)
			if err != nil {
				return err
			}
			m.SetLocalityOptimizedSearch(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().LocalityOptimizedSearch), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(localityOptimizedSearchMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`enable_implicit_select_for_update`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enable_implicit_select_for_update`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("enabled_implicit_select_for_update", s)
			if err != nil {
				return err
			}
			m.SetImplicitSelectForUpdate(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().ImplicitSelectForUpdate), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(implicitSelectForUpdateClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`enable_insert_fast_path`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enable_insert_fast_path`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("enable_insert_fast_path", s)
			if err != nil {
				return err
			}
			m.SetInsertFastPath(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().InsertFastPath), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(insertFastPathClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`serial_normalization`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			mode, ok := sessiondatapb.SerialNormalizationModeFromString(s)
			if !ok {
				return newVarValueError(`serial_normalization`, s,
					"rowid", "virtual_sequence", "sql_sequence", "sql_sequence_cached")
			}
			m.SetSerialNormalizationMode(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return evalCtx.SessionData().SerialNormalizationMode.String(), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondatapb.SerialNormalizationMode(
				SerialNormalizationMode.Get(sv)).String()
		},
	},

	// CockroachDB extension.
	`stub_catalog_tables`: {
		GetStringVal: makePostgresBoolGetStringValFn(`stub_catalog_tables`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("stub_catalog_tables", s)
			if err != nil {
				return err
			}
			m.SetStubCatalogTablesEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().StubCatalogTablesEnabled), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(stubCatalogTablesEnabledClusterValue.Get(sv))
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html
	`extra_float_digits`: {
		GetStringVal: makeIntGetStringValFn(`extra_float_digits`),
		Set: func(
			_ context.Context, m sessionDataMutator, s string,
		) error {
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return wrapSetVarError(err, "extra_float_digits", s)
			}
			// Note: this is the range allowed by PostgreSQL.
			// See also the documentation around (DataConversionConfig).GetFloatPrec()
			// in session_data.go.
			if i < -15 || i > 3 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					`%d is outside the valid range for parameter "extra_float_digits" (-15 .. 3)`, i)
			}
			m.SetExtraFloatDigits(int32(i))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return fmt.Sprintf("%d", evalCtx.SessionData().DataConversionConfig.ExtraFloatDigits), nil
		},
		GlobalDefault: func(sv *settings.Values) string { return "0" },
	},

	// CockroachDB extension. See docs on SessionData.ForceSavepointRestart.
	// https://github.com/cockroachdb/cockroach/issues/30588
	`force_savepoint_restart`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().ForceSavepointRestart), nil
		},
		GetStringVal: makePostgresBoolGetStringValFn("force_savepoint_restart"),
		Set: func(_ context.Context, m sessionDataMutator, val string) error {
			b, err := paramparse.ParseBoolVar("force_savepoint_restart", val)
			if err != nil {
				return err
			}
			if b {
				telemetry.Inc(sqltelemetry.ForceSavepointRestartCounter)
			}
			m.SetForceSavepointRestart(b)
			return nil
		},
		GlobalDefault: globalFalse,
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html
	`integer_datetimes`: makeReadOnlyVar("on"),

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-INTERVALSTYLE
	`intervalstyle`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			styleVal, ok := duration.IntervalStyle_value[strings.ToUpper(s)]
			if !ok {
				validIntervalStyles := make([]string, 0, len(duration.IntervalStyle_value))
				for k := range duration.IntervalStyle_value {
					validIntervalStyles = append(validIntervalStyles, strings.ToLower(k))
				}
				return newVarValueError(`IntervalStyle`, s, validIntervalStyles...)
			}
			style := duration.IntervalStyle(styleVal)
			if style != duration.IntervalStyle_POSTGRES &&
				!m.data.IntervalStyleEnabled {
				return errors.WithDetailf(
					errors.WithHintf(
						pgerror.Newf(
							pgcode.FeatureNotSupported,
							"setting IntervalStyle is not enabled",
						),
						"You can enable IntervalStyle customization for all sessions with the cluster setting %s, or per session using SET intervalstyle_enabled = true.",
						intervalStyleEnabledClusterSetting,
					),
					"Setting IntervalStyle changes the volatility of string::interval or interval::string "+
						"casts from immutable to stable. No computed columns, partial indexes, partitions "+
						"and check constraints can use this casts. "+
						"Use to_char_with_style or parse_interval instead if you need these casts to work "+
						"in the aforementioned cases.",
				)
			}
			m.SetIntervalStyle(style)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return strings.ToLower(evalCtx.SessionData().GetIntervalStyle().String()), nil
		},
		GetFromSessionData: func(sd *sessiondata.SessionData) string {
			return strings.ToLower(sd.GetIntervalStyle().String())
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strings.ToLower(duration.IntervalStyle_name[int32(intervalStyle.Get(sv))])
		},
		Equal: func(a, b *sessiondata.SessionData) bool {
			return a.GetIntervalStyle() == b.GetIntervalStyle()
		},
	},
	`intervalstyle_enabled`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().IntervalStyleEnabled), nil
		},
		GetStringVal: makePostgresBoolGetStringValFn("intervalstyle_enabled"),
		Set: func(ctx context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar(`intervalstyle_enabled`, s)
			if err != nil {
				return err
			}
			m.SetIntervalStyleEnabled(b)
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(intervalStyleEnabled.Get(sv))
		},
	},

	`is_superuser`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().IsSuperuser), nil
		},
		GetFromSessionData: func(sd *sessiondata.SessionData) string {
			return formatBoolAsPostgresSetting(sd.IsSuperuser)
		},
		GetStringVal:  makePostgresBoolGetStringValFn("is_superuser"),
		GlobalDefault: globalFalse,
		Equal: func(a, b *sessiondata.SessionData) bool {
			return a.IsSuperuser == b.IsSuperuser
		},
	},

	// CockroachDB extension.
	`large_full_scan_rows`: {
		GetStringVal: makeFloatGetStringValFn(`large_full_scan_rows`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return err
			}
			m.SetLargeFullScanRows(f)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatFloatAsPostgresSetting(evalCtx.SessionData().LargeFullScanRows), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatFloatAsPostgresSetting(largeFullScanRows.Get(sv))
		},
	},

	// CockroachDB extension.
	`locality`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return evalCtx.Locality.String(), nil
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-LOC-TIMEOUT
	`lock_timeout`: {
		GetStringVal: makeTimeoutVarGetter(`lock_timeout`),
		Set:          lockTimeoutVarSet,
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			ms := evalCtx.SessionData().LockTimeout.Nanoseconds() / int64(time.Millisecond)
			return strconv.FormatInt(ms, 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return clusterLockTimeout.String(sv)
		},
	},

	// See https://www.postgresql.org/docs/12/runtime-config-client.html.
	`default_table_access_method`: makeCompatStringVar(`default_table_access_method`, `heap`),

	// See https://www.postgresql.org/docs/13/runtime-config-compatible.html
	// CockroachDB only supports safe_encoding for now. If `client_encoding` is updated to
	// allow encodings other than UTF8, then the default value of `backslash_quote` should
	// be changed to `on`.
	`backslash_quote`: makeCompatStringVar(`backslash_quote`, `safe_encoding`),

	// See https://www.postgresql.org/docs/9.5/runtime-config-compatible.html
	`default_with_oids`: makeCompatBoolVar(`default_with_oids`, false, false),

	// See https://www.postgresql.org/docs/current/datatype-xml.html.
	`xmloption`: makeCompatStringVar(`xmloption`, `content`),

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/10/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
	`max_identifier_length`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) { return "128", nil },
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html#GUC-MAX-INDEX-KEYS
	`max_index_keys`: makeReadOnlyVar("32"),

	// CockroachDB extension.
	`node_id`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			nodeID, _ := evalCtx.NodeID.OptionalNodeID() // zero if unavailable
			return fmt.Sprintf("%d", nodeID), nil
		},
	},

	// CockroachDB extension.
	// TODO(dan): This should also work with SET.
	`results_buffer_size`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return strconv.FormatInt(evalCtx.SessionData().ResultsBufferSize, 10), nil
		},
	},

	// CockroachDB extension (inspired by MySQL).
	// See https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_sql_safe_updates
	`sql_safe_updates`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().SafeUpdates), nil
		},
		GetStringVal: makePostgresBoolGetStringValFn("sql_safe_updates"),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("sql_safe_updates", s)
			if err != nil {
				return err
			}
			m.SetSafeUpdates(b)
			return nil
		},
		GlobalDefault: globalFalse,
	},

	// See https://www.postgresql.org/docs/13/runtime-config-client.html.
	`check_function_bodies`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().CheckFunctionBodies), nil
		},
		GetStringVal: makePostgresBoolGetStringValFn("check_function_bodies"),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("check_function_bodies", s)
			if err != nil {
				return err
			}
			m.SetCheckFunctionBodies(b)
			return nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`prefer_lookup_joins_for_fks`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().PreferLookupJoinsForFKs), nil
		},
		GetStringVal: makePostgresBoolGetStringValFn("prefer_lookup_joins_for_fks"),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("prefer_lookup_joins_for_fks", s)
			if err != nil {
				return err
			}
			m.SetPreferLookupJoinsForFKs(b)
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(preferLookupJoinsForFKs.Get(sv))
		},
	},

	// See https://www.postgresql.org/docs/current/sql-set-role.html.
	`role`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			if evalCtx.SessionData().SessionUserProto == "" {
				return security.NoneRole, nil
			}
			return evalCtx.SessionData().User().Normalized(), nil
		},
		// SetWithPlanner is defined in init(), as otherwise there is a circular
		// initialization loop with the planner.
		GlobalDefault: func(sv *settings.Values) string {
			return security.NoneRole
		},
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
				s, err := paramparse.DatumAsString(&evalCtx.EvalContext, "search_path", v)
				if err != nil {
					return "", err
				}
				if strings.Contains(s, ",") {
					// TODO(knz): if/when we want to support this, we'll need to change
					// the interface between GetStringVal() and Set() to take string
					// arrays instead of a single string.
					return "",
						errors.WithHintf(unimplemented.NewWithIssuef(53971,
							`schema name %q has commas so is not supported in search_path.`, s),
							`Did you mean to omit quotes? SET search_path = %s`, s)
				}
				buf.WriteString(comma)
				buf.WriteString(s)
				comma = ","
			}
			return buf.String(), nil
		},
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			paths := strings.Split(s, ",")
			m.UpdateSearchPath(paths)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return evalCtx.SessionData().SearchPath.SQLIdentifiers(), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondata.DefaultSearchPath.String()
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html#GUC-SERVER-VERSION
	`server_version`: makeReadOnlyVar(PgServerVersion),

	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html#GUC-SERVER-VERSION-NUM
	`server_version_num`: makeReadOnlyVar(PgServerVersionNum),

	// This is read-only in Postgres also.
	// See https://www.postgresql.org/docs/14/sql-show.html and
	// https://www.postgresql.org/docs/14/locale.html
	`lc_collate`: makeReadOnlyVar(PgCompatLocale),

	// This is read-only in Postgres also.
	// See https://www.postgresql.org/docs/14/sql-show.html and
	// https://www.postgresql.org/docs/14/locale.html
	`lc_ctype`: makeReadOnlyVar(PgCompatLocale),

	// See https://www.postgresql.org/docs/14/sql-show.html and
	// https://www.postgresql.org/docs/14/locale.html
	`lc_messages`: makeCompatStringVar("lc_messages", PgCompatLocale),

	// See https://www.postgresql.org/docs/14/sql-show.html and
	// https://www.postgresql.org/docs/14/locale.html
	`lc_monetary`: makeCompatStringVar("lc_monetary", PgCompatLocale),

	// See https://www.postgresql.org/docs/14/sql-show.html and
	// https://www.postgresql.org/docs/14/locale.html
	`lc_numeric`: makeCompatStringVar("lc_numeric", PgCompatLocale),

	// See https://www.postgresql.org/docs/14/sql-show.html and
	// https://www.postgresql.org/docs/14/locale.html
	`lc_time`: makeCompatStringVar("lc_time", PgCompatLocale),

	// See https://www.postgresql.org/docs/9.4/runtime-config-connection.html
	`ssl_renegotiation_limit`: {
		Hidden:        true,
		GetStringVal:  makeIntGetStringValFn(`ssl_renegotiation_limit`),
		Get:           func(_ *extendedEvalContext) (string, error) { return "0", nil },
		GlobalDefault: func(_ *settings.Values) string { return "0" },
		Set: func(_ context.Context, _ sessionDataMutator, s string) error {
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return wrapSetVarError(err, "ssl_renegotiation_limit", s)
			}
			if i != 0 {
				// See pg src/backend/utils/misc/guc.c: non-zero values are not to be supported.
				return newVarValueError("ssl_renegotiation_limit", s, "0")
			}
			return nil
		},
	},

	// CockroachDB extension.
	`crdb_version`: makeReadOnlyVarWithFn(func() string {
		return build.GetInfo().Short()
	}),

	// CockroachDB extension
	`session_id`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) { return evalCtx.SessionID.String(), nil },
	},

	// CockroachDB extension.
	// In PG this is a pseudo-function used with SELECT, not SHOW.
	// See https://www.postgresql.org/docs/10/static/functions-info.html
	`session_user`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return evalCtx.SessionData().User().Normalized(), nil
		},
	},

	// See pg sources src/backend/utils/misc/guc.c. The variable is defined
	// but is hidden from SHOW ALL.
	`session_authorization`: {
		Hidden: true,
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return evalCtx.SessionData().User().Normalized(), nil
		},
	},

	// See https://www.postgresql.org/docs/current/runtime-config-connection.html#GUC-PASSWORD-ENCRYPTION
	// We only support reading this setting in clients: it is not desirable to let clients choose
	// their own password hash algorithm.
	`password_encryption`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return security.GetConfiguredPasswordHashMethod(evalCtx.Ctx(), &evalCtx.Settings.SV).String(), nil
		},
	},

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/10/static/runtime-config-compatible.html#GUC-STANDARD-CONFORMING-STRINGS
	// If this gets properly implemented, we will need to re-evaluate how escape_string_warning is implemented
	`standard_conforming_strings`: makeCompatBoolVar(`standard_conforming_strings`, true, false /* anyAllowed */),

	// See https://www.postgresql.org/docs/10/runtime-config-compatible.html#GUC-ESCAPE-STRING-WARNING
	// Supported for PG compatibility only.
	// If this gets properly implemented, we will need to re-evaluate how standard_conforming_strings is implemented
	`escape_string_warning`: makeCompatBoolVar(`escape_string_warning`, true, true /* anyAllowed */),

	// See https://www.postgresql.org/docs/10/static/runtime-config-compatible.html#GUC-SYNCHRONIZE-SEQSCANS
	// The default in pg is "on" but the behavior in CockroachDB is "off". As this does not affect
	// results received by clients, we accept both values.
	`synchronize_seqscans`: makeCompatBoolVar(`synchronize_seqscans`, true, true /* anyAllowed */),

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-ROW-SECURITY
	// The default in pg is "on" but row security is not supported in CockroachDB.
	// We blindly accept both values because as long as there are now row security policies defined,
	// either value produces the same query results in PostgreSQL. That is, as long as CockroachDB
	// does not support row security, accepting either "on" and "off" but ignoring the result
	// is postgres-compatible.
	// If/when CockroachDB is extended to support row security, the default and allowed values
	// should be modified accordingly.
	`row_security`: makeCompatBoolVar(`row_security`, false, true /* anyAllowed */),

	`statement_timeout`: {
		GetStringVal: makeTimeoutVarGetter(`statement_timeout`),
		Set:          stmtTimeoutVarSet,
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			ms := evalCtx.SessionData().StmtTimeout.Nanoseconds() / int64(time.Millisecond)
			return strconv.FormatInt(ms, 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return clusterStatementTimeout.String(sv)
		},
	},

	`idle_in_session_timeout`: {
		GetStringVal: makeTimeoutVarGetter(`idle_in_session_timeout`),
		Set:          idleInSessionTimeoutVarSet,
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			ms := evalCtx.SessionData().IdleInSessionTimeout.Nanoseconds() / int64(time.Millisecond)
			return strconv.FormatInt(ms, 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return clusterIdleInSessionTimeout.String(sv)
		},
	},

	`idle_in_transaction_session_timeout`: {
		GetStringVal: makeTimeoutVarGetter(`idle_in_transaction_session_timeout`),
		Set:          idleInTransactionSessionTimeoutVarSet,
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			ms := evalCtx.SessionData().IdleInTransactionSessionTimeout.Nanoseconds() / int64(time.Millisecond)
			return strconv.FormatInt(ms, 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return clusterIdleInTransactionSessionTimeout.String(sv)
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-TIMEZONE
	`timezone`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return sessionDataTimeZoneFormat(evalCtx.SessionData().GetLocation()), nil
		},
		GetFromSessionData: func(sd *sessiondata.SessionData) string {
			return sessionDataTimeZoneFormat(sd.GetLocation())
		},
		GetStringVal:  timeZoneVarGetStringVal,
		Set:           timeZoneVarSet,
		GlobalDefault: func(_ *settings.Values) string { return "UTC" },
		Equal: func(a, b *sessiondata.SessionData) bool {
			// NB: (*time.Location).String does not heap allocate.
			return a.GetLocation().String() == b.GetLocation().String()
		},
	},

	// This is not directly documented in PG's docs but does indeed behave this way.
	// See https://github.com/postgres/postgres/blob/REL_10_STABLE/src/backend/utils/misc/guc.c#L3401-L3409
	`transaction_isolation`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return "serializable", nil
		},
		RuntimeSet: func(_ context.Context, evalCtx *extendedEvalContext, local bool, s string) error {
			_, ok := tree.IsolationLevelMap[s]
			if !ok {
				return newVarValueError(`transaction_isolation`, s, "serializable")
			}
			return nil
		},
		GlobalDefault: func(_ *settings.Values) string { return "serializable" },
	},

	// CockroachDB extension.
	`transaction_priority`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return evalCtx.Txn.UserPriority().String(), nil
		},
	},

	// CockroachDB extension.
	`transaction_status`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return evalCtx.TxnState, nil
		},
	},

	// See https://www.postgresql.org/docs/10/static/hot-standby.html#HOT-STANDBY-USERS
	`transaction_read_only`: {
		GetStringVal: makePostgresBoolGetStringValFn("transaction_read_only"),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("transaction_read_only", s)
			if err != nil {
				return err
			}
			m.SetReadOnly(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.TxnReadOnly), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`tracing`: {
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			sessTracing := evalCtx.Tracing
			if sessTracing.Enabled() {
				val := "on"
				if sessTracing.KVTracingEnabled() {
					val += ", kv"
				}
				return val, nil
			}
			return "off", nil
		},
		// Setting is done by the SetTracing statement.
	},

	// CockroachDB extension.
	`allow_prepare_as_opt_plan`: {
		Hidden: true,
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().AllowPrepareAsOptPlan), nil
		},
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("allow_prepare_as_opt_plan", s)
			if err != nil {
				return err
			}
			m.SetAllowPrepareAsOptPlan(b)
			return nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`save_tables_prefix`: {
		Hidden: true,
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return evalCtx.SessionData().SaveTablesPrefix, nil
		},
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			m.SetSaveTablesPrefix(s)
			return nil
		},
		GlobalDefault: func(_ *settings.Values) string { return "" },
	},

	// CockroachDB extension.
	`experimental_enable_temp_tables`: {
		GetStringVal: makePostgresBoolGetStringValFn(`experimental_enable_temp_tables`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("experimental_enable_temp_tables", s)
			if err != nil {
				return err
			}
			m.SetTempTablesEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().TempTablesEnabled), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(temporaryTablesEnabledClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`enable_multiregion_placement_policy`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enable_multiregion_placement_policy`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("enable_multiregion_placement_policy", s)
			if err != nil {
				return err
			}
			m.SetPlacementEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().PlacementEnabled), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(placementEnabledClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`experimental_enable_auto_rehoming`: {
		GetStringVal: makePostgresBoolGetStringValFn(`experimental_enable_auto_rehoming`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("experimental_enable_auto_rehoming", s)
			if err != nil {
				return err
			}
			m.SetAutoRehomingEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().AutoRehomingEnabled), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(autoRehomingEnabledClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`on_update_rehome_row_enabled`: {
		GetStringVal: makePostgresBoolGetStringValFn(`on_update_rehome_row_enabled`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("on_update_rehome_row_enabled", s)
			if err != nil {
				return err
			}
			m.SetOnUpdateRehomeRowEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OnUpdateRehomeRowEnabled), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(onUpdateRehomeRowEnabledClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`experimental_enable_implicit_column_partitioning`: {
		GetStringVal: makePostgresBoolGetStringValFn(`experimental_enable_implicit_column_partitioning`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("experimental_enable_implicit_column_partitioning", s)
			if err != nil {
				return err
			}
			m.SetImplicitColumnPartitioningEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().ImplicitColumnPartitioningEnabled), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(implicitColumnPartitioningEnabledClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	// This is only kept for backwards compatibility and no longer has any effect.
	`enable_drop_enum_value`: {
		Hidden:       true,
		GetStringVal: makePostgresBoolGetStringValFn(`enable_drop_enum_value`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			_, err := paramparse.ParseBoolVar("enable_drop_enum_value", s)
			return err
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(true), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`override_multi_region_zone_config`: {
		GetStringVal: makePostgresBoolGetStringValFn(`override_multi_region_zone_config`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("override_multi_region_zone_config", s)
			if err != nil {
				return err
			}
			m.SetOverrideMultiRegionZoneConfigEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OverrideMultiRegionZoneConfigEnabled), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(overrideMultiRegionZoneConfigClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`disallow_full_table_scans`: {
		GetStringVal: makePostgresBoolGetStringValFn(`disallow_full_table_scans`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar(`disallow_full_table_scans`, s)
			if err != nil {
				return err
			}
			m.SetDisallowFullTableScans(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().DisallowFullTableScans), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(disallowFullTableScans.Get(sv))
		},
	},

	// CockroachDB extension.
	`enable_experimental_alter_column_type_general`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enable_experimental_alter_column_type_general`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("enable_experimental_alter_column_type_general", s)
			if err != nil {
				return err
			}
			m.SetAlterColumnTypeGeneral(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().AlterColumnTypeGeneralEnabled), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(experimentalAlterColumnTypeGeneralMode.Get(sv))
		},
	},

	// TODO(rytaft): remove this once unique without index constraints are fully
	// supported.
	`experimental_enable_unique_without_index_constraints`: {
		GetStringVal: makePostgresBoolGetStringValFn(`experimental_enable_unique_without_index_constraints`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar(`experimental_enable_unique_without_index_constraints`, s)
			if err != nil {
				return err
			}
			m.SetUniqueWithoutIndexConstraints(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().EnableUniqueWithoutIndexConstraints), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(experimentalUniqueWithoutIndexConstraintsMode.Get(sv))
		},
	},

	`use_declarative_schema_changer`: {
		GetStringVal: makePostgresBoolGetStringValFn(`use_declarative_schema_changer`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			mode, ok := sessiondatapb.NewSchemaChangerModeFromString(s)
			if !ok {
				return newVarValueError(`use_declarative_schema_changer`, s,
					"off", "on", "unsafe", "unsafe_always")
			}
			m.SetUseNewSchemaChanger(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return evalCtx.SessionData().NewSchemaChangerMode.String(), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondatapb.NewSchemaChangerMode(experimentalUseNewSchemaChanger.Get(sv)).String()
		},
	},

	`enable_experimental_stream_replication`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enable_experimental_stream_replication`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar(`enable_experimental_stream_replication`, s)
			if err != nil {
				return err
			}
			m.SetStreamReplicationEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().EnableStreamReplication), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(experimentalStreamReplicationEnabled.Get(sv))
		},
	},

	// CockroachDB extension. See experimentalComputedColumnRewrites or
	// ParseComputedColumnRewrites for a description of the format.
	`experimental_computed_column_rewrites`: {
		Hidden:       true,
		GetStringVal: makePostgresBoolGetStringValFn(`experimental_computed_column_rewrites`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			_, err := schemaexpr.ParseComputedColumnRewrites(s)
			if err != nil {
				return err
			}
			m.SetExperimentalComputedColumnRewrites(s)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return evalCtx.SessionData().ExperimentalComputedColumnRewrites, nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return experimentalComputedColumnRewrites.Get(sv)
		},
	},

	`null_ordered_last`: {
		GetStringVal: makePostgresBoolGetStringValFn(`null_ordered_last`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar(`null_ordered_last`, s)
			if err != nil {
				return err
			}
			m.SetNullOrderedLast(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().NullOrderedLast), nil
		},
		GlobalDefault: globalFalse,
	},

	`propagate_input_ordering`: {
		GetStringVal: makePostgresBoolGetStringValFn(`propagate_input_ordering`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar(`propagate_input_ordering`, s)
			if err != nil {
				return err
			}
			m.SetPropagateInputOrdering(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().PropagateInputOrdering), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(propagateInputOrdering.Get(sv))
		},
	},

	// CockroachDB extension.
	`transaction_rows_written_log`: {
		GetStringVal: makeIntGetStringValFn(`transaction_rows_written_log`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if b < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set transaction_rows_written_log to a negative value: %d", b)
			}
			m.SetTxnRowsWrittenLog(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return strconv.FormatInt(evalCtx.SessionData().TxnRowsWrittenLog, 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(txnRowsWrittenLog.Get(sv), 10)
		},
	},

	// CockroachDB extension.
	`transaction_rows_written_err`: {
		GetStringVal: makeIntGetStringValFn(`transaction_rows_written_err`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if b < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set transaction_rows_written_err to a negative value: %d", b)
			}
			m.SetTxnRowsWrittenErr(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return strconv.FormatInt(evalCtx.SessionData().TxnRowsWrittenErr, 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(txnRowsWrittenErr.Get(sv), 10)
		},
	},

	// CockroachDB extension.
	`transaction_rows_read_log`: {
		GetStringVal: makeIntGetStringValFn(`transaction_rows_read_log`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if b < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set transaction_rows_read_log to a negative value: %d", b)
			}
			m.SetTxnRowsReadLog(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return strconv.FormatInt(evalCtx.SessionData().TxnRowsReadLog, 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(txnRowsReadLog.Get(sv), 10)
		},
	},

	// CockroachDB extension.
	`transaction_rows_read_err`: {
		GetStringVal: makeIntGetStringValFn(`transaction_rows_read_err`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if b < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set transaction_rows_read_err to a negative value: %d", b)
			}
			m.SetTxnRowsReadErr(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return strconv.FormatInt(evalCtx.SessionData().TxnRowsReadErr, 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(txnRowsReadErr.Get(sv), 10)
		},
	},

	// CockroachDB extension. Allows for testing of transaction retry logic
	// using the cockroach_restart savepoint.
	`inject_retry_errors_enabled`: {
		GetStringVal: makePostgresBoolGetStringValFn(`retry_errors_enabled`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("inject_retry_errors_enabled", s)
			if err != nil {
				return err
			}
			m.SetInjectRetryErrorsEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().InjectRetryErrorsEnabled), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`join_reader_ordering_strategy_batch_size`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			size, err := humanizeutil.ParseBytes(s)
			if err != nil {
				return err
			}
			if size <= 0 {
				return errors.New("join_reader_ordering_strategy_batch_size can only be set to a positive value")
			}
			m.SetJoinReaderOrderingStrategyBatchSize(size)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return string(humanizeutil.IBytes(evalCtx.SessionData().JoinReaderOrderingStrategyBatchSize)), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return string(humanizeutil.IBytes(rowexec.JoinReaderOrderingStrategyBatchSize.Get(sv)))
		},
	},

	// CockroachDB extension.
	`parallelize_multi_key_lookup_joins_enabled`: {
		GetStringVal: makePostgresBoolGetStringValFn(`parallelize_multi_key_lookup_joins_enabled`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("parallelize_multi_key_lookup_joins_enabled", s)
			if err != nil {
				return err
			}
			m.SetParallelizeMultiKeyLookupJoinsEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().ParallelizeMultiKeyLookupJoinsEnabled), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return rowexec.ParallelizeMultiKeyLookupJoinsEnabled.String(sv)
		},
	},

	// TODO(harding): Remove this when costing scans based on average column size
	// is fully supported.
	// CockroachDB extension.
	`cost_scans_with_default_col_size`: {
		GetStringVal: makePostgresBoolGetStringValFn(`cost_scans_with_default_col_size`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar(`cost_scans_with_default_col_size`, s)
			if err != nil {
				return err
			}
			m.SetCostScansWithDefaultColSize(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().CostScansWithDefaultColSize), nil
		},
		GlobalDefault: globalFalse,
	},
	`default_transaction_quality_of_service`: {
		GetStringVal: makePostgresBoolGetStringValFn(`default_transaction_quality_of_service`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			qosLevel, ok := sessiondatapb.ParseQoSLevelFromString(s)
			if !ok {
				return newVarValueError(`default_transaction_quality_of_service`, s,
					sessiondatapb.NormalName, sessiondatapb.UserHighName, sessiondatapb.UserLowName)
			}
			m.SetQualityOfService(qosLevel)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return evalCtx.QualityOfService().String(), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondatapb.Normal.String()
		},
	},
	`opt_split_scan_limit`: {
		GetStringVal: makeIntGetStringValFn(`opt_split_scan_limit`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if b < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set opt_split_scan_limit to a negative value: %d", b)
			}
			if b > math.MaxInt32 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set opt_split_scan_limit to a value greater than %d", math.MaxInt32)
			}

			m.SetOptSplitScanLimit(int32(b))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext) (string, error) {
			return strconv.FormatInt(int64(evalCtx.SessionData().OptSplitScanLimit), 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(int64(tabledesc.MaxBucketAllowed), 10)
		},
	},
}

const compatErrMsg = "this parameter is currently recognized only for compatibility and has no effect in CockroachDB."

func init() {
	// SetWithPlanner must be initialized in init() to avoid a circular
	// initialization loop.
	for _, p := range []struct {
		name string
		fn   func(ctx context.Context, p *planner, local bool, s string) error
	}{
		{
			name: `role`,
			fn: func(ctx context.Context, p *planner, local bool, s string) error {
				u, err := security.MakeSQLUsernameFromUserInput(s, security.UsernameValidation)
				if err != nil {
					return err
				}
				return p.setRole(ctx, local, u)
			},
		},
	} {
		v := varGen[p.name]
		v.SetWithPlanner = p.fn
		varGen[p.name] = v
	}
	for k, v := range DummyVars {
		varGen[k] = v
	}

	// Alias `idle_session_timeout` to match the PG 14 name.
	// We create `idle_in_session_timeout` before its existence.
	varGen[`idle_session_timeout`] = varGen[`idle_in_session_timeout`]

	// Initialize delegate.ValidVars.
	for v := range varGen {
		delegate.ValidVars[v] = struct{}{}
	}
	// Initialize varNames.
	varNames = func() []string {
		res := make([]string, 0, len(varGen))
		for vName := range varGen {
			res = append(res, vName)
			if strings.Contains(vName, ".") {
				panic(fmt.Sprintf(`no session variables with "." can be created as they are reserved for custom options, found %s`, vName))
			}
		}
		sort.Strings(res)
		return res
	}()
}

// makePostgresBoolGetStringValFn returns a function that evaluates and returns
// a string representation of the first argument value.
func makePostgresBoolGetStringValFn(varName string) getStringValFn {
	return func(
		ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
	) (string, error) {
		if len(values) != 1 {
			return "", newSingleArgVarError(varName)
		}
		val, err := values[0].Eval(&evalCtx.EvalContext)
		if err != nil {
			return "", err
		}
		if s, ok := val.(*tree.DString); ok {
			return string(*s), nil
		}
		s, err := paramparse.GetSingleBool(varName, val)
		if err != nil {
			return "", err
		}
		return strconv.FormatBool(bool(*s)), nil
	}
}

func makeReadOnlyVar(value string) sessionVar {
	return sessionVar{
		Get:           func(_ *extendedEvalContext) (string, error) { return value, nil },
		GlobalDefault: func(_ *settings.Values) string { return value },
	}
}

func makeReadOnlyVarWithFn(fn func() string) sessionVar {
	return sessionVar{
		Get:           func(_ *extendedEvalContext) (string, error) { return fn(), nil },
		GlobalDefault: func(_ *settings.Values) string { return fn() },
	}
}

func displayPgBool(val bool) func(_ *settings.Values) string {
	strVal := formatBoolAsPostgresSetting(val)
	return func(_ *settings.Values) string { return strVal }
}

var globalFalse = displayPgBool(false)
var globalTrue = displayPgBool(true)

// sessionDataTimeZoneFormat returns the appropriate timezone format
// to output when the `timezone` is required output.
// If the time zone is a "fixed offset" one, initialized from an offset
// and not a standard name, then we use a magic format in the Location's
// name. We attempt to parse that here and retrieve the original offset
// specified by the user.
func sessionDataTimeZoneFormat(loc *time.Location) string {
	locStr := loc.String()
	_, origRepr, parsed := timeutil.ParseTimeZoneOffset(locStr, timeutil.TimeZoneStringToLocationISO8601Standard)
	if parsed {
		return origRepr
	}
	return locStr
}

func makeCompatBoolVar(varName string, displayValue, anyValAllowed bool) sessionVar {
	displayValStr := formatBoolAsPostgresSetting(displayValue)
	return sessionVar{
		Get: func(_ *extendedEvalContext) (string, error) { return displayValStr, nil },
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar(varName, s)
			if err != nil {
				return err
			}
			if anyValAllowed || b == displayValue {
				return nil
			}
			telemetry.Inc(sqltelemetry.UnimplementedSessionVarValueCounter(varName, s))
			allowedVals := []string{displayValStr}
			if anyValAllowed {
				allowedVals = append(allowedVals, formatBoolAsPostgresSetting(!displayValue))
			}
			err = newVarValueError(varName, s, allowedVals...)
			err = errors.WithDetail(err, compatErrMsg)
			return err
		},
		GlobalDefault: func(sv *settings.Values) string { return displayValStr },
		GetStringVal:  makePostgresBoolGetStringValFn(varName),
	}
}

func makeCompatIntVar(varName string, displayValue int, extraAllowed ...int) sessionVar {
	displayValueStr := strconv.Itoa(displayValue)
	extraAllowedStr := make([]string, len(extraAllowed))
	for i, v := range extraAllowed {
		extraAllowedStr[i] = strconv.Itoa(v)
	}
	varObj := makeCompatStringVar(varName, displayValueStr, extraAllowedStr...)
	varObj.GetStringVal = makeIntGetStringValFn(varName)
	return varObj
}

// Silence unused warning. This may be useful in the future, so keep it around.
var _ = makeCompatIntVar

func makeCompatStringVar(varName, displayValue string, extraAllowed ...string) sessionVar {
	allowedVals := append(extraAllowed, strings.ToLower(displayValue))
	return sessionVar{
		Get: func(_ *extendedEvalContext) (string, error) {
			return displayValue, nil
		},
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			enc := strings.ToLower(s)
			for _, a := range allowedVals {
				if enc == a {
					return nil
				}
			}
			telemetry.Inc(sqltelemetry.UnimplementedSessionVarValueCounter(varName, s))
			err := newVarValueError(varName, s, allowedVals...)
			err = errors.WithDetail(err, compatErrMsg)
			return err
		},
		GlobalDefault: func(sv *settings.Values) string { return displayValue },
	}
}

// makeIntGetStringValFn returns a getStringValFn which allows
// the user to provide plain integer values to a SET variable.
func makeIntGetStringValFn(name string) getStringValFn {
	return func(ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr) (string, error) {
		s, err := getIntVal(&evalCtx.EvalContext, name, values)
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(s, 10), nil
	}
}

// makeFloatGetStringValFn returns a getStringValFn which allows
// the user to provide plain float values to a SET variable.
func makeFloatGetStringValFn(name string) getStringValFn {
	return func(ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr) (string, error) {
		f, err := getFloatVal(&evalCtx.EvalContext, name, values)
		if err != nil {
			return "", err
		}
		return formatFloatAsPostgresSetting(f), nil
	}
}

// IsSessionVariableConfigurable returns true iff there is a session
// variable with the given name and it is settable by a client
// (e.g. in pgwire).
func IsSessionVariableConfigurable(varName string) (exists, configurable bool) {
	v, exists := varGen[varName]
	return exists, v.Set != nil
}

// IsCustomOptionSessionVariable returns whether the given varName is a custom
// session variable.
func IsCustomOptionSessionVariable(varName string) bool {
	_, isCustom := getCustomOptionSessionVar(varName)
	return isCustom
}

// CheckSessionVariableValueValid returns an error if the value is not valid
// for the given variable. It also returns an error if there is no variable with
// the given name or if the variable is not configurable.
func CheckSessionVariableValueValid(
	ctx context.Context, settings *cluster.Settings, varName, varValue string,
) error {
	_, sVar, err := getSessionVar(varName, false)
	if err != nil {
		return err
	}
	if sVar.Set == nil {
		return pgerror.Newf(pgcode.CantChangeRuntimeParam,
			"parameter %q cannot be changed", varName)
	}
	fakeSessionMutator := sessionDataMutator{
		data: &sessiondata.SessionData{},
		sessionDataMutatorBase: sessionDataMutatorBase{
			defaults: SessionDefaults(map[string]string{}),
			settings: settings,
		},
		sessionDataMutatorCallbacks: sessionDataMutatorCallbacks{},
	}
	return sVar.Set(ctx, fakeSessionMutator, varValue)
}

var varNames []string

func getSessionVar(name string, missingOk bool) (bool, sessionVar, error) {
	if _, ok := UnsupportedVars[name]; ok {
		return false, sessionVar{}, unimplemented.Newf("set."+name,
			"the configuration setting %q is not supported", name)
	}

	v, ok := varGen[name]
	if !ok {
		if vCustom, isCustom := getCustomOptionSessionVar(name); isCustom {
			return true, vCustom, nil
		}
		if missingOk {
			return false, sessionVar{}, nil
		}
		return false, sessionVar{}, pgerror.Newf(pgcode.UndefinedObject,
			"unrecognized configuration parameter %q", name)
	}
	return true, v, nil
}

func getCustomOptionSessionVar(varName string) (sv sessionVar, isCustom bool) {
	if strings.Contains(varName, ".") {
		return sessionVar{
			Get: func(evalCtx *extendedEvalContext) (string, error) {
				v, ok := evalCtx.SessionData().CustomOptions[varName]
				if !ok {
					return "", pgerror.Newf(pgcode.UndefinedObject,
						"unrecognized configuration parameter %q", varName)
				}
				return v, nil
			},
			Set: func(ctx context.Context, m sessionDataMutator, val string) error {
				// TODO(#72026): do some memory accounting.
				m.SetCustomOption(varName, val)
				return nil
			},
			GlobalDefault: func(sv *settings.Values) string {
				return ""
			},
		}, true
	}
	return sessionVar{}, false
}

// GetSessionVar implements the EvalSessionAccessor interface.
func (p *planner) GetSessionVar(
	_ context.Context, varName string, missingOk bool,
) (bool, string, error) {
	name := strings.ToLower(varName)
	ok, v, err := getSessionVar(name, missingOk)
	if err != nil || !ok {
		return ok, "", err
	}
	val, err := v.Get(&p.extendedEvalCtx)
	return true, val, err
}

// SetSessionVar implements the EvalSessionAccessor interface.
func (p *planner) SetSessionVar(ctx context.Context, varName, newVal string, isLocal bool) error {
	name := strings.ToLower(varName)
	_, v, err := getSessionVar(name, false /* missingOk */)
	if err != nil {
		return err
	}

	if v.Set == nil && v.RuntimeSet == nil && v.SetWithPlanner == nil {
		return newCannotChangeParameterError(name)
	}

	// Note for RuntimeSet and SetWithPlanner we do not use the sessionDataMutator
	// as the callers need items that are only accessible by higher level
	// objects - and some of the computation potentially expensive so should be
	// batched instead of performing the computation on each mutator.
	// It is their responsibility to set LOCAL or SESSION after
	// doing the computation.
	if v.RuntimeSet != nil {
		return v.RuntimeSet(ctx, p.ExtendedEvalContext(), isLocal, newVal)
	}
	if v.SetWithPlanner != nil {
		return v.SetWithPlanner(ctx, p, isLocal, newVal)
	}
	return p.applyOnSessionDataMutators(
		ctx,
		isLocal,
		func(m sessionDataMutator) error {
			return v.Set(ctx, m, newVal)
		},
	)
}
