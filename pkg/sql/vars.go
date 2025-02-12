// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/colfetcher"
	"github.com/cockroachdb/cockroach/pkg/sql/delegate"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/tsearch"
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
	ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr, kv *kv.Txn,
) (string, error)

// sessionVar provides a unified interface for performing operations on
// variables such as the selected database, or desired syntax.
type sessionVar struct {
	// Hidden indicates that the variable should not show up in the output of SHOW ALL.
	Hidden bool

	// Get returns a string representation of a given variable to be used
	// either by SHOW or in the pg_catalog table.
	Get func(evalCtx *extendedEvalContext, kv *kv.Txn) (string, error)

	// Exists returns true if this custom session option exists in the current
	// context. It's needed to support the current_setting builtin for custom
	// options. It is only defined for custom options.
	Exists func(evalCtx *extendedEvalContext, kv *kv.Txn) bool

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
	getFunc func(*extendedEvalContext, *kv.Txn) (string, error),
	setFunc func(sessionDataMutator, bool),
	globalDefault func(_ *settings.Values) string,
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
		GlobalDefault: globalDefault,
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		GlobalDefault: globalFalse,
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get:           func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) { return "UTF8", nil },
		GlobalDefault: func(_ *settings.Values) string { return "UTF8" },
	},

	// Supported for PG compatibility only.
	// See https://www.postgresql.org/docs/9.6/static/multibyte.html
	`server_encoding`: makeReadOnlyVar("UTF8"),

	// CockroachDB extension.
	`database`: {
		GetStringVal: func(
			ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr, txn *kv.Txn,
		) (string, error) {
			dbName, err := getStringVal(ctx, &evalCtx.Context, `database`, values)
			if err != nil {
				return "", err
			}

			if len(dbName) == 0 && evalCtx.SessionData().SafeUpdates {
				return "", pgerror.DangerousStatementf("SET database to empty string")
			}

			if len(dbName) != 0 {
				// Verify database descriptor exists.
				if _, err := evalCtx.Descs.ByNameWithLeased(txn).Get().Database(ctx, dbName); err != nil {
					return "", err
				}
			}
			return dbName, nil
		},
		Set: func(_ context.Context, m sessionDataMutator, dbName string) error {
			m.SetDatabase(dbName)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return evalCtx.SessionData().Database, nil
		},
		GlobalDefault: func(_ *settings.Values) string {
			// The "defaultdb" value is set as session default in the pgwire
			// connection code. The global default is the empty string,
			// which is what internal connections should pick up.
			return ""
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-DATESTYLE
	`datestyle`: {
		Set: func(ctx context.Context, m sessionDataMutator, s string) error {
			ds, err := pgdate.ParseDateStyle(s, m.data.GetDateStyle())
			if err != nil {
				return newVarValueError("DateStyle", s, pgdate.AllowedDateStyles()...)
			}
			if ds.Style != pgdate.Style_ISO {
				return unimplemented.NewWithIssue(41773, "only ISO style is supported")
			}
			m.SetDateStyle(ds)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return evalCtx.GetDateStyle().SQLString(), nil
		},
		GetFromSessionData: func(sd *sessiondata.SessionData) string {
			return sd.GetDateStyle().SQLString()
		},
		GlobalDefault: func(sv *settings.Values) string {
			// Note that dateStyle.String(sv) would return the lower-case
			// strings, but we want to preserve the upper-case ones.
			return dateStyleEnumMap[dateStyle.Get(sv)]
		},
		Equal: func(a, b *sessiondata.SessionData) bool {
			return a.GetDateStyle() == b.GetDateStyle()
		},
	},

	// See https://www.postgresql.org/docs/15/datatype-datetime.html.
	// We always log in UTC.
	`log_timezone`: makeCompatStringVar(`log_timezone`, `UTC`),

	// This is only kept for backwards compatibility and no longer has any effect.
	`datestyle_enabled`: makeBackwardsCompatBoolVar(
		"datestyle_enabled", true,
	),

	// See https://www.postgresql.org/docs/10/runtime-config-locks.html#GUC-DEADLOCK-TIMEOUT
	`deadlock_timeout`: {
		GetStringVal: makeTimeoutVarGetter(`deadlock_timeout`),
		Set:          deadlockTimeoutVarSet,
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			ms := evalCtx.SessionData().DeadlockTimeout.Nanoseconds() / int64(time.Millisecond)
			return strconv.FormatInt(ms, 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return "0s"
		},
	},

	// Controls the subsequent parsing of a "naked" INT type.
	// TODO(bob): Remove or no-op this in v2.4: https://github.com/cockroachdb/cockroach/issues/32844
	`default_int_size`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return "", nil
		},
		GlobalDefault: func(sv *settings.Values) string { return "" },
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-DEFAULT-TRANSACTION-ISOLATION
	`default_transaction_isolation`: {
		Set: func(ctx context.Context, m sessionDataMutator, s string) error {
			allowReadCommitted := allowReadCommittedIsolation.Get(&m.settings.SV)
			allowRepeatableRead := allowRepeatableReadIsolation.Get(&m.settings.SV)
			hasLicense := base.CCLDistributionAndEnterpriseEnabled(m.settings)
			var allowedValues = []string{"serializable"}
			if allowRepeatableRead {
				allowedValues = append(allowedValues, "repeatable read")
			}
			if allowReadCommitted {
				allowedValues = append(allowedValues, "read committed")
			}
			level, ok := tree.IsolationLevelMap[strings.ToLower(s)]
			if !ok {
				return newVarValueError(`default_transaction_isolation`, s, allowedValues...)
			}
			originalLevel := level
			level, upgraded, upgradedDueToLicense := level.UpgradeToEnabledLevel(
				allowReadCommitted, allowRepeatableRead, hasLicense)
			if f := m.upgradedIsolationLevel; upgraded && f != nil {
				f(ctx, originalLevel, upgradedDueToLicense)
			}
			m.SetDefaultTransactionIsolationLevel(level)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			level := tree.IsolationLevel(evalCtx.SessionData().DefaultTxnIsolationLevel)
			if level == tree.UnspecifiedIsolation {
				level = tree.SerializableIsolation
			}
			return strings.ToLower(level.String()), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strings.ToLower(tree.SerializableIsolation.String())
		},
	},

	// CockroachDB extension.
	`default_transaction_priority`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			pri, ok := tree.UserPriorityFromString(s)
			if !ok {
				return newVarValueError(`default_transaction_priority`, s, "low", "normal", "high")
			}
			m.SetDefaultTransactionPriority(pri)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().DefaultTxnUseFollowerReads), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`direct_columnar_scans_enabled`: {
		GetStringVal: makePostgresBoolGetStringValFn(`direct_columnar_scans_enabled`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar(`direct_columnar_scans_enabled`, s)
			if err != nil {
				return err
			}
			m.SetDirectColumnarScansEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().DirectColumnarScansEnabled), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(colfetcher.DirectScansEnabled.Get(sv))
		},
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().IndexRecommendationsEnabled), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`distsql`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			mode, ok := sessiondatapb.DistSQLExecModeFromString(s)
			if !ok {
				return newVarValueError(`distsql`, s, "on", "off", "auto", "always")
			}
			m.SetDistSQLMode(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return evalCtx.SessionData().DistSQLMode.String(), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return DistSQLClusterExecMode.String(sv)
		},
	},

	// CockroachDB extension.
	`distsql_workmem`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			limit, err := humanizeutil.ParseBytes(s)
			if err != nil {
				return err
			}
			if limit <= 1 {
				return errors.New("distsql_workmem can only be set to a value greater than 1")
			}
			m.SetDistSQLWorkMem(limit)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return string(humanizeutil.IBytes(evalCtx.SessionData().WorkMemLimit)), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return string(humanizeutil.IBytes(settingWorkMemBytes.Get(sv)))
		},
	},

	// CockroachDB extension.
	`experimental_distsql_planning`: {
		Hidden:       true,
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return evalCtx.SessionData().ExperimentalDistSQLPlanningMode.String(), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return experimentalDistSQLPlanningClusterMode.String(sv)
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().PartiallyDistributedPlansDisabled), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`distribute_group_by_row_count_threshold`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return strconv.FormatUint(evalCtx.SessionData().DistributeGroupByRowCountThreshold, 10), nil
		},
		GetStringVal: makeIntGetStringValFn(`distribute_group_by_row_count_threshold`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if i < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set distribute_group_by_row_count_threshold to a negative value: %d", i)
			}
			m.SetDistributeGroupByRowCountThreshold(uint64(i))
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatUint(1000, 10)
		},
	},

	// CockroachDB extension.
	`distribute_sort_row_count_threshold`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return strconv.FormatUint(evalCtx.SessionData().DistributeSortRowCountThreshold, 10), nil
		},
		GetStringVal: makeIntGetStringValFn(`distribute_sort_row_count_threshold`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if i < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set distribute_sort_row_count_threshold to a negative value: %d", i)
			}
			m.SetDistributeSortRowCountThreshold(uint64(i))
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatUint(1000, 10)
		},
	},

	// CockroachDB extension.
	`distribute_scan_row_count_threshold`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return strconv.FormatUint(evalCtx.SessionData().DistributeScanRowCountThreshold, 10), nil
		},
		GetStringVal: makeIntGetStringValFn(`distribute_scan_row_count_threshold`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if i < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set distribute_scan_row_count_threshold to a negative value: %d", i)
			}
			m.SetDistributeScanRowCountThreshold(uint64(i))
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatUint(10000, 10)
		},
	},

	// CockroachDB extension.
	`always_distribute_full_scans`: {
		GetStringVal: makePostgresBoolGetStringValFn(`always_distribute_full_scans`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("always_distribute_full_scans", s)
			if err != nil {
				return err
			}
			m.SetAlwaysDistributeFullScans(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().AlwaysDistributeFullScans), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`distribute_join_row_count_threshold`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return strconv.FormatUint(evalCtx.SessionData().DistributeJoinRowCountThreshold, 10), nil
		},
		GetStringVal: makeIntGetStringValFn(`distribute_join_row_count_threshold`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if i < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set distribute_join_row_count_threshold to a negative value: %d", i)
			}
			m.SetDistributeJoinRowCountThreshold(uint64(i))
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatUint(1000, 10)
		},
	},

	// CockroachDB extension.
	`disable_vec_union_eager_cancellation`: {
		GetStringVal: makePostgresBoolGetStringValFn(`disable_vec_union_eager_cancellation`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("disable_vec_union_eager_cancellation", s)
			if err != nil {
				return err
			}
			m.SetDisableVecUnionEagerCancellation(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().DisableVecUnionEagerCancellation), nil
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
				return newVarValueError(
					`vectorize`,
					s,
					sessiondatapb.VectorizeOff.String(),
					sessiondatapb.VectorizeOn.String(),
					sessiondatapb.VectorizeExperimentalAlways.String(),
				)
			}
			m.SetVectorize(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return evalCtx.SessionData().VectorizeMode.String(), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return VectorizeClusterMode.String(sv)
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().TestingVectorizeInjectPanics), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`testing_optimizer_inject_panics`: {
		GetStringVal: makePostgresBoolGetStringValFn(`testing_optimizer_inject_panics`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("testing_optimizer_inject_panics", s)
			if err != nil {
				return err
			}
			m.SetTestingOptimizerInjectPanics(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().TestingOptimizerInjectPanics), nil
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return "on", nil
		},
		GlobalDefault: globalTrue,
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return strconv.FormatInt(evalCtx.SessionData().OptimizerFKCascadesLimit, 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(optDrivenFKCascadesClusterLimit.Get(sv), 10)
		},
	},

	// CockroachDB extension.
	`optimizer_use_forecasts`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_forecasts`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_forecasts", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUseForecasts(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseForecasts), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`optimizer_use_merged_partial_statistics`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_merged_partial_statistics`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_merged_partial_statistics", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUseMergedPartialStatistics(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseMergedPartialStatistics), nil
		},
		GlobalDefault: globalTrue,
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseMultiColStats), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(optUseMultiColStatsClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`optimizer_use_not_visible_indexes`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_not_visible_indexes`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_not_visible_indexes", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUseNotVisibleIndexes(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseNotVisibleIndexes), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`optimizer_merge_joins_enabled`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_merge_joins_enabled`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_merge_joins_enabled", s)
			if err != nil {
				return err
			}
			m.SetOptimizerMergeJoinsEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerMergeJoinsEnabled), nil
		},
		GlobalDefault: globalTrue,
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
			b, err := paramparse.ParseBoolVar("enable_implicit_select_for_update", s)
			if err != nil {
				return err
			}
			m.SetImplicitSelectForUpdate(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
					"rowid", "virtual_sequence", "sql_sequence", "sql_sequence_cached", "sql_sequence_cached_node")
			}
			m.SetSerialNormalizationMode(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return evalCtx.SessionData().SerialNormalizationMode.String(), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return SerialNormalizationMode.String(sv)
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return fmt.Sprintf("%d", evalCtx.SessionData().DataConversionConfig.ExtraFloatDigits), nil
		},
		GlobalDefault: func(sv *settings.Values) string { return "1" },
	},

	// CockroachDB extension. See docs on SessionData.ForceSavepointRestart.
	// https://github.com/cockroachdb/cockroach/issues/30588
	`force_savepoint_restart`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Set: func(ctx context.Context, m sessionDataMutator, s string) error {
			styleVal, ok := duration.IntervalStyle_value[strings.ToUpper(s)]
			if !ok {
				validIntervalStyles := make([]string, 0, len(duration.IntervalStyle_name))
				for k := 0; k < len(duration.IntervalStyle_name); k++ {
					name := duration.IntervalStyle_name[int32(k)]
					validIntervalStyles = append(validIntervalStyles, strings.ToLower(name))
				}
				return newVarValueError(`IntervalStyle`, s, validIntervalStyles...)
			}
			style := duration.IntervalStyle(styleVal)
			m.SetIntervalStyle(style)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return strings.ToLower(evalCtx.SessionData().GetIntervalStyle().String()), nil
		},
		GetFromSessionData: func(sd *sessiondata.SessionData) string {
			return strings.ToLower(sd.GetIntervalStyle().String())
		},
		GlobalDefault: func(sv *settings.Values) string {
			return intervalStyle.String(sv)
		},
		Equal: func(a, b *sessiondata.SessionData) bool {
			return a.GetIntervalStyle() == b.GetIntervalStyle()
		},
	},
	// This is only kept for backwards compatibility and no longer has any effect.
	`intervalstyle_enabled`: makeBackwardsCompatBoolVar(
		"intervalstyle_enabled", true,
	),

	`is_superuser`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
	`system_identity`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return evalCtx.SessionData().SystemIdentity(), nil
		},
		GetFromSessionData: func(sd *sessiondata.SessionData) string {
			return sd.SystemIdentity()
		},
		GlobalDefault: func(_ *settings.Values) string { return "" },
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatFloatAsPostgresSetting(evalCtx.SessionData().LargeFullScanRows), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatFloatAsPostgresSetting(largeFullScanRows.Get(sv))
		},
	},

	// CockroachDB extension.
	`locality`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return evalCtx.Locality.String(), nil
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-LOC-TIMEOUT
	`lock_timeout`: {
		GetStringVal: makeTimeoutVarGetter(`lock_timeout`),
		Set:          lockTimeoutVarSet,
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) { return "128", nil },
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html#GUC-MAX-INDEX-KEYS
	`max_index_keys`: makeReadOnlyVar("32"),

	// Supported for PG compatibility only. MaxInt32 indicates no limit.
	// See https://www.postgresql.org/docs/10/runtime-config-resource.html#GUC-MAX-PREPARED-TRANSACTIONS
	`max_prepared_transactions`: makeReadOnlyVar(strconv.Itoa(math.MaxInt32)),

	// CockroachDB extension.
	`node_id`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			nodeID, _ := evalCtx.NodeID.OptionalNodeID() // zero if unavailable
			return fmt.Sprintf("%d", nodeID), nil
		},
	},

	// CockroachDB extension.
	// TODO(dan): This should also work with SET.
	`results_buffer_size`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return strconv.FormatInt(evalCtx.SessionData().ResultsBufferSize, 10), nil
		},
	},

	// CockroachDB extension (inspired by MySQL).
	// See https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_sql_safe_updates
	`sql_safe_updates`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			if evalCtx.SessionData().SessionUserProto == "" {
				return username.NoneRole, nil
			}
			return evalCtx.SessionData().User().Normalized(), nil
		},
		// SetWithPlanner is defined in init(), as otherwise there is a circular
		// initialization loop with the planner.
		GlobalDefault: func(sv *settings.Values) string {
			return username.NoneRole
		},
	},

	// CockroachDB extension (inspired by MySQL).
	`strict_ddl_atomicity`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().StrictDDLAtomicity), nil
		},
		GetStringVal: makePostgresBoolGetStringValFn("strict_ddl_atomicity"),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("strict_ddl_atomicity", s)
			if err != nil {
				return err
			}
			m.SetStrictDDLAtomicity(b)
			return nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension (inspired by MySQL).
	`autocommit_before_ddl`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().AutoCommitBeforeDDL), nil
		},
		GetStringVal: makePostgresBoolGetStringValFn("autocommit_before_ddl"),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("autocommit_before_ddl", s)
			if err != nil {
				return err
			}
			m.SetAutoCommitBeforeDDL(b)
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(defaultAutocommitBeforeDDL.Get(sv))
		},
	},

	// See https://www.postgresql.org/docs/10/static/ddl-schemas.html#DDL-SCHEMAS-PATH
	// https://www.postgresql.org/docs/9.6/static/runtime-config-client.html
	`search_path`: {
		GetStringVal: func(
			ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr, _ *kv.Txn,
		) (string, error) {
			paths := make([]string, len(values))
			for i, v := range values {
				s, err := paramparse.DatumAsString(ctx, &evalCtx.Context, "search_path", v)
				if err != nil {
					return "", err
				}
				paths[i] = s
			}
			return sessiondata.FormatSearchPaths(paths), nil
		},
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			paths, err := sessiondata.ParseSearchPath(s)
			if err != nil {
				return err
			}
			m.UpdateSearchPath(paths)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return evalCtx.SessionData().SearchPath.String(), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondata.DefaultSearchPath.String()
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html#GUC-SERVER-VERSION
	`server_version`: makeReadOnlyVar(PgServerVersion),

	// See https://www.postgresql.org/docs/10/static/runtime-config-preset.html#GUC-SERVER-VERSION-NUM
	`server_version_num`: makeReadOnlyVar(PgServerVersionNum),

	`pg_trgm.similarity_threshold`: {
		GetStringVal: makeFloatGetStringValFn(`pg_trgm.similarity_threshold`),
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatFloatAsPostgresSetting(evalCtx.SessionData().TrigramSimilarityThreshold), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return "0.3"
		},
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return err
			}
			if f < 0 || f > 1 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"%.2f is out of range for similarity_threshold", f)
			}
			m.SetTrigramSimilarityThreshold(f)
			return nil
		},
	},

	// CockroachDB extension.
	`troubleshooting_mode`: {
		GetStringVal: makePostgresBoolGetStringValFn(`troubleshooting_mode`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("troubleshooting_mode", s)
			if err != nil {
				return err
			}
			m.SetTroubleshootingModeEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().TroubleshootingMode), nil
		},
		GlobalDefault: globalFalse,
	},
	// CockroachDB extension.
	`transaction_timeout`: {
		GetStringVal: makeTimeoutVarGetter(`transaction_timeout`),
		Set:          transactionTimeoutVarSet,
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			ms := evalCtx.SessionData().TransactionTimeout.Nanoseconds() / int64(time.Millisecond)
			return strconv.FormatInt(ms, 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return "0s"
		},
	},

	// See https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-DEFAULT-TEXT-SEARCH-CONFIG
	`default_text_search_config`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			if err := tsearch.ValidConfig(s); err != nil {
				return err
			}
			m.SetDefaultTextSearchConfig(s)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return evalCtx.SessionData().DefaultTextSearchConfig, nil
		},
		GlobalDefault: func(c_ *settings.Values) string {
			return "pg_catalog.english"
		},
	},

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
		Get:           func(_ *extendedEvalContext, _ *kv.Txn) (string, error) { return "0", nil },
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
	// This is a read-only setting that shows the method that was used
	// to authenticate this session.
	`authentication_method`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return string(evalCtx.SessionData().AuthenticationMethod), nil
		},
	},

	// See https://www.postgresql.org/docs/9.4/runtime-config-connection.html
	`ssl`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().IsSSL), nil
		},
	},

	// CockroachDB extension.
	`crdb_version`: makeReadOnlyVarWithFn(func() string {
		return build.GetInfo().Short().StripMarkers()
	}),

	// CockroachDB extension
	`session_id`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) { return evalCtx.SessionID.String(), nil },
	},

	// CockroachDB extension.
	// In PG this is a pseudo-function used with SELECT, not SHOW.
	// See https://www.postgresql.org/docs/10/static/functions-info.html
	`session_user`: {
		Hidden: true,
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return evalCtx.SessionData().User().Normalized(), nil
		},
	},

	// See pg sources src/backend/utils/misc/guc.c. The variable is defined
	// but is hidden from SHOW ALL.
	`session_authorization`: {
		Hidden: true,
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return evalCtx.SessionData().User().Normalized(), nil
		},
	},

	// See https://www.postgresql.org/docs/current/runtime-config-connection.html#GUC-PASSWORD-ENCRYPTION
	// We only support reading this setting in clients: it is not desirable to let clients choose
	// their own password hash algorithm.
	`password_encryption`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return security.GetConfiguredPasswordHashMethod(&evalCtx.Settings.SV).String(), nil
		},
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			method := security.GetConfiguredPasswordHashMethod(&m.settings.SV)
			if s != method.String() {
				return newCannotChangeParameterError("password_encryption")
			}
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return security.GetConfiguredPasswordHashMethod(sv).String()
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			ms := evalCtx.SessionData().StmtTimeout.Nanoseconds() / int64(time.Millisecond)
			return strconv.FormatInt(ms, 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return clusterStatementTimeout.String(sv)
		},
	},

	`idle_in_session_timeout`: {
		Hidden:       true, // Superseded by `idle_session_timeout`.
		GetStringVal: makeTimeoutVarGetter(`idle_in_session_timeout`),
		Set:          idleInSessionTimeoutVarSet,
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			ms := evalCtx.SessionData().IdleInTransactionSessionTimeout.Nanoseconds() / int64(time.Millisecond)
			return strconv.FormatInt(ms, 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return clusterIdleInTransactionSessionTimeout.String(sv)
		},
	},

	// See https://www.postgresql.org/docs/10/static/runtime-config-client.html#GUC-TIMEZONE
	`timezone`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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

	// This is not directly documented in PG's docs but does indeed behave this
	// way. This variable shows the isolation level of the current transaction,
	// and also allows the isolation level to change as long as queries have not
	// been executed yet.
	// See https://github.com/postgres/postgres/blob/REL_10_STABLE/src/backend/utils/misc/guc.c#L3401-L3409
	`transaction_isolation`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			level := tree.FromKVIsoLevel(evalCtx.Txn.IsoLevel())
			return strings.ToLower(level.String()), nil
		},
		RuntimeSet: func(ctx context.Context, evalCtx *extendedEvalContext, local bool, s string) error {
			level, ok := tree.IsolationLevelMap[strings.ToLower(s)]
			if !ok {
				var allowedValues = []string{"serializable"}
				if allowRepeatableReadIsolation.Get(&evalCtx.ExecCfg.Settings.SV) {
					allowedValues = append(allowedValues, "repeatable read")
				}
				if allowReadCommittedIsolation.Get(&evalCtx.ExecCfg.Settings.SV) {
					allowedValues = append(allowedValues, "read committed")
				}
				return newVarValueError(`transaction_isolation`, s, allowedValues...)
			}
			modes := tree.TransactionModes{
				Isolation: level,
			}
			if err := evalCtx.TxnModesSetter.setTransactionModes(ctx, modes, hlc.Timestamp{} /* asOfSystemTime */); err != nil {
				return err
			}
			return nil
		},
		GlobalDefault: func(_ *settings.Values) string {
			return strings.ToLower(tree.SerializableIsolation.String())
		},
	},

	// CockroachDB extension.
	`transaction_priority`: {
		Get: func(evalCtx *extendedEvalContext, txn *kv.Txn) (string, error) {
			return txn.UserPriority().String(), nil
		},
	},

	// CockroachDB extension.
	`transaction_status`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
			return m.SetReadOnly(b)
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.TxnReadOnly), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`tracing`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
	`virtual_cluster_name`: {
		Hidden: true,
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return string(evalCtx.ExecCfg.VirtualClusterName), nil
		},
	},

	// CockroachDB extension.
	`allow_prepare_as_opt_plan`: {
		Hidden: true,
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().PlacementEnabled), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(placementEnabledClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`enable_auto_rehoming`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enable_auto_rehoming`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("enable_auto_rehoming", s)
			if err != nil {
				return err
			}
			m.SetAutoRehomingEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().ImplicitColumnPartitioningEnabled), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(implicitColumnPartitioningEnabledClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	// This is only kept for backwards compatibility and no longer has any effect.
	`enable_drop_enum_value`: makeBackwardsCompatBoolVar(
		"enable_drop_enum_value", true,
	),

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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OverrideMultiRegionZoneConfigEnabled), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(overrideMultiRegionZoneConfigClusterMode.Get(sv))
		},
	},

	// CockroachDB extension.
	// This is only kept for backwards compatibility and no longer has any effect.
	`experimental_enable_hash_sharded_indexes`: makeBackwardsCompatBoolVar("experimental_enable_hash_sharded_indexes", true),

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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().DisallowFullTableScans), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(disallowFullTableScans.Get(sv))
		},
	},

	// CockroachDB extension.
	`avoid_full_table_scans_in_mutations`: {
		GetStringVal: makePostgresBoolGetStringValFn(`avoid_full_table_scans_in_mutations`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar(`avoid_full_table_scans_in_mutations`, s)
			if err != nil {
				return err
			}
			m.SetAvoidFullTableScansInMutations(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().AvoidFullTableScansInMutations), nil
		},
		GlobalDefault: globalTrue,
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().AlterColumnTypeGeneralEnabled), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(experimentalAlterColumnTypeGeneralMode.Get(sv))
		},
	},

	// CockroachDB extension.
	`enable_row_level_security`: {
		Hidden:       true,
		GetStringVal: makePostgresBoolGetStringValFn(`enable_row_level_security`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("enable_row_level_security", s)
			if err != nil {
				return err
			}
			m.SetRowLevelSecurity(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().RowLevelSecurityEnabled), nil
		},
		GlobalDefault: globalFalse,
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return evalCtx.SessionData().NewSchemaChangerMode.String(), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return experimentalUseNewSchemaChanger.String(sv)
		},
	},

	`descriptor_validation`: {
		GetStringVal: makePostgresBoolGetStringValFn(`descriptor_validation`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			mode, ok := sessiondatapb.DescriptorValidationModeFromString(s)
			if !ok {
				return newVarValueError(`descriptor_validation`, s,
					"off", "on", "read_only")
			}
			m.SetDescriptorValidationMode(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return evalCtx.SessionData().DescriptorValidationMode.String(), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondatapb.DescriptorValidationOn.String()
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().InjectRetryErrorsEnabled), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension. Allows for testing of transaction retry logic
	// using the cockroach_restart savepoint.
	`inject_retry_errors_on_commit_enabled`: {
		GetStringVal: makePostgresBoolGetStringValFn(`inject_retry_errors_on_commit_enabled`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("inject_retry_errors_on_commit_enabled", s)
			if err != nil {
				return err
			}
			m.SetInjectRetryErrorsOnCommitEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().InjectRetryErrorsOnCommitEnabled), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension. Configures the maximum number of automatic retries
	// to perform for statements in explicit READ COMMITTED transactions that
	// see a transaction retry error.
	`max_retries_for_read_committed`: {
		GetStringVal: makeIntGetStringValFn(`max_retries_for_read_committed`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if b < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set max_retries_for_read_committed to a negative value: %d", b)
			}
			if b > math.MaxInt32 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set max_retries_for_read_committed to a value greater than %d: %d", math.MaxInt32, b)
			}

			m.SetMaxRetriesForReadCommitted(int32(b))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return strconv.FormatInt(int64(evalCtx.SessionData().MaxRetriesForReadCommitted), 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return "10"
		},
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return string(humanizeutil.IBytes(evalCtx.SessionData().JoinReaderOrderingStrategyBatchSize)), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return string(humanizeutil.IBytes(rowexec.JoinReaderOrderingStrategyBatchSize.Get(sv)))
		},
	},

	// CockroachDB extension.
	`join_reader_no_ordering_strategy_batch_size`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			size, err := humanizeutil.ParseBytes(s)
			if err != nil {
				return err
			}
			if size <= 0 {
				return errors.New("join_reader_no_ordering_strategy_batch_size can only be set to a positive value")
			}
			m.SetJoinReaderNoOrderingStrategyBatchSize(size)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return string(humanizeutil.IBytes(evalCtx.SessionData().JoinReaderNoOrderingStrategyBatchSize)), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return string(humanizeutil.IBytes(rowexec.JoinReaderNoOrderingStrategyBatchSize.Get(sv)))
		},
	},

	// CockroachDB extension.
	`join_reader_index_join_strategy_batch_size`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			size, err := humanizeutil.ParseBytes(s)
			if err != nil {
				return err
			}
			if size <= 0 {
				return errors.New("join_reader_index_join_strategy_batch_size can only be set to a positive value")
			}
			m.SetJoinReaderIndexJoinStrategyBatchSize(size)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return string(humanizeutil.IBytes(evalCtx.SessionData().JoinReaderIndexJoinStrategyBatchSize)), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return string(humanizeutil.IBytes(execinfra.JoinReaderIndexJoinStrategyBatchSize.Get(sv)))
		},
	},

	// CockroachDB extension.
	`index_join_streamer_batch_size`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			size, err := humanizeutil.ParseBytes(s)
			if err != nil {
				return err
			}
			if size <= 0 {
				return errors.New("index_join_streamer_batch_size can only be set to a positive value")
			}
			m.SetIndexJoinStreamerBatchSize(size)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return string(humanizeutil.IBytes(evalCtx.SessionData().IndexJoinStreamerBatchSize)), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return string(humanizeutil.IBytes(colfetcher.IndexJoinStreamerBatchSize.Get(sv)))
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().ParallelizeMultiKeyLookupJoinsEnabled), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(rowexec.ParallelizeMultiKeyLookupJoinsEnabled.Get(sv))
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().CostScansWithDefaultColSize), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(costScansWithDefaultColSize.Get(sv))
		},
	},

	// CockroachDB extension.
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return evalCtx.QualityOfService().String(), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondatapb.Normal.String()
		},
	},

	// CockroachDB extension.
	`copy_transaction_quality_of_service`: {
		GetStringVal: makePostgresBoolGetStringValFn(`copy_transaction_quality_of_service`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			qosLevel, ok := sessiondatapb.ParseQoSLevelFromString(s)
			if !ok {
				return newVarValueError(`copy_transaction_quality_of_service`, s,
					sessiondatapb.UserLowName, sessiondatapb.NormalName, sessiondatapb.UserHighName)
			}
			m.SetCopyQualityOfService(qosLevel)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return evalCtx.SessionData().CopyTxnQualityOfService.String(), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return sessiondatapb.UserLow.String()
		},
	},

	// CockroachDB extension.
	`copy_write_pipelining_enabled`: {
		GetStringVal: makePostgresBoolGetStringValFn(`copy_write_pipelining_enabled`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("copy_write_pipelining_enabled", s)
			if err != nil {
				return err
			}
			m.SetCopyWritePipeliningEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().CopyWritePipeliningEnabled), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`copy_num_retries_per_batch`: {
		GetStringVal: makeIntGetStringValFn(`copy_num_retries_per_batch`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if b <= 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"copy_num_retries_per_batch must be a positive value: %d", b)
			}
			if b > math.MaxInt32 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set copy_num_retries_per_batch to a value greater than %d: %d", math.MaxInt32, b)
			}
			m.SetCopyNumRetriesPerBatch(int32(b))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return strconv.FormatInt(int64(evalCtx.SessionData().CopyNumRetriesPerBatch), 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return "5"
		},
	},

	// CockroachDB extension.
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
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return strconv.FormatInt(int64(evalCtx.SessionData().OptSplitScanLimit), 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(int64(tabledesc.MaxBucketAllowed), 10)
		},
	},

	// CockroachDB extension.
	`enable_super_regions`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enable_super_regions`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("enable_super_regions", s)
			if err != nil {
				return err
			}
			m.SetEnableSuperRegions(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().EnableSuperRegions), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(enableSuperRegions.Get(sv))
		},
	},

	// CockroachDB extension.
	`alter_primary_region_super_region_override`: {
		GetStringVal: makePostgresBoolGetStringValFn(`alter_primary_region_super_region_override`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("alter_primary_region_super_region_override", s)
			if err != nil {
				return err
			}
			m.SetEnableOverrideAlterPrimaryRegionInSuperRegion(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OverrideAlterPrimaryRegionInSuperRegion), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(overrideAlterPrimaryRegionInSuperRegion.Get(sv))
		}},

	// CockroachDB extension.
	`enable_implicit_transaction_for_batch_statements`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enable_implicit_transaction_for_batch_statements`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("enable_implicit_transaction_for_batch_statements", s)
			if err != nil {
				return err
			}
			m.SetEnableImplicitTransactionForBatchStatements(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().EnableImplicitTransactionForBatchStatements), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`expect_and_ignore_not_visible_columns_in_copy`: {
		GetStringVal: makePostgresBoolGetStringValFn(`expect_and_ignore_not_visible_columns_in_copy`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("expect_and_ignore_not_visible_columns_in_copy", s)
			if err != nil {
				return err
			}
			m.SetExpectAndIgnoreNotVisibleColumnsInCopy(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().ExpectAndIgnoreNotVisibleColumnsInCopy), nil
		},
		GlobalDefault: globalFalse,
	},

	// TODO(michae2): Remove this when #70731 is fixed.
	// CockroachDB extension.
	`enable_multiple_modifications_of_table`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enable_multiple_modifications_of_table`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("enable_multiple_modifications_of_table", s)
			if err != nil {
				return err
			}
			m.SetMultipleModificationsOfTable(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().MultipleModificationsOfTable), nil
		},
		GlobalDefault: globalFalse,
	},
	// CockroachDB extension.
	`show_primary_key_constraint_on_not_visible_columns`: {
		GetStringVal: makePostgresBoolGetStringValFn(`show_primary_key_constraint_on_not_visible_columns`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("show_primary_key_constraint_on_not_visible_columns", s)
			if err != nil {
				return err
			}
			m.SetShowPrimaryKeyConstraintOnNotVisibleColumns(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().ShowPrimaryKeyConstraintOnNotVisibleColumns), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`testing_optimizer_random_seed`: {
		GetStringVal: makeIntGetStringValFn(`testing_optimizer_random_seed`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			m.SetTestingOptimizerRandomSeed(i)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return strconv.FormatInt(evalCtx.SessionData().TestingOptimizerRandomSeed, 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(0, 10)
		},
	},

	// CockroachDB extension.
	`unconstrained_non_covering_index_scan_enabled`: {
		GetStringVal: makePostgresBoolGetStringValFn(`unconstrained_non_covering_index_scan_enabled`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("unconstrained_non_covering_index_scan_enabled", s)
			if err != nil {
				return err
			}
			m.SetUnconstrainedNonCoveringIndexScanEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().UnconstrainedNonCoveringIndexScanEnabled), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`testing_optimizer_cost_perturbation`: {
		GetStringVal: makeFloatGetStringValFn(`testing_optimizer_cost_perturbation`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return err
			}
			if f < 0 {
				return pgerror.Newf(
					pgcode.InvalidParameterValue, "testing_optimizer_cost_perturbation must be non-negative",
				)
			}
			m.SetTestingOptimizerCostPerturbation(f)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatFloatAsPostgresSetting(
				evalCtx.SessionData().TestingOptimizerCostPerturbation,
			), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatFloatAsPostgresSetting(0)
		},
	},

	// CockroachDB extension.
	`testing_optimizer_disable_rule_probability`: {
		GetStringVal: makeFloatGetStringValFn(`testing_optimizer_disable_rule_probability`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return err
			}
			if f < 0 || f > 1 {
				return pgerror.Newf(
					pgcode.InvalidParameterValue,
					"testing_optimizer_disable_rule_probability must be in the range [0.0,1.0]",
				)
			}
			m.SetTestingOptimizerDisableRuleProbability(f)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatFloatAsPostgresSetting(
				evalCtx.SessionData().TestingOptimizerDisableRuleProbability,
			), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatFloatAsPostgresSetting(0)
		},
	},

	// CockroachDB extension.
	`copy_fast_path_enabled`: {
		GetStringVal: makePostgresBoolGetStringValFn(`copy_fast_path_enabled`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("copy_fast_path_enabled", s)
			if err != nil {
				return err
			}
			m.SetCopyFastPathEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().CopyFastPathEnabled), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(copyFastPathDefault)
		},
	},

	// CockroachDB extension.
	`disable_hoist_projection_in_join_limitation`: {
		GetStringVal: makePostgresBoolGetStringValFn(`disable_hoist_projection_in_join_limitation`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("disable_hoist_projection_in_join_limitation", s)
			if err != nil {
				return err
			}
			m.SetDisableHoistProjectionInJoinLimitation(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().DisableHoistProjectionInJoinLimitation), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`copy_from_atomic_enabled`: {
		GetStringVal: makePostgresBoolGetStringValFn(`copy_from_atomic_enabled`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("copy_from_atomic_enabled", s)
			if err != nil {
				return err
			}
			m.SetCopyFromAtomicEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().CopyFromAtomicEnabled), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`copy_from_retries_enabled`: {
		GetStringVal: makePostgresBoolGetStringValFn(`copy_from_retries_enabled`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("copy_from_retries_enabled", s)
			if err != nil {
				return err
			}
			m.SetCopyFromRetriesEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().CopyFromRetriesEnabled), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`declare_cursor_statement_timeout_enabled`: {
		GetStringVal: makePostgresBoolGetStringValFn(`declare_cursor_statement_timeout_enabled`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("declare_cursor_statement_timeout_enabled", s)
			if err != nil {
				return err
			}
			m.SetDeclareCursorStatementTimeoutEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().DeclareCursorStatementTimeoutEnabled), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`enforce_home_region`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enforce_home_region`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("enforce_home_region", s)
			if err != nil {
				return err
			}
			m.SetEnforceHomeRegion(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().EnforceHomeRegion), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`variable_inequality_lookup_join_enabled`: {
		GetStringVal: makePostgresBoolGetStringValFn(`variable_inequality_lookup_join_enabled`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("variable_inequality_lookup_join_enabled", s)
			if err != nil {
				return err
			}
			m.SetVariableInequalityLookupJoinEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().VariableInequalityLookupJoinEnabled), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`experimental_hash_group_join_enabled`: {
		GetStringVal: makePostgresBoolGetStringValFn(`experimental_hash_group_join_enabled`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar(`experimental_hash_group_join_enabled`, s)
			if err != nil {
				return err
			}
			m.SetExperimentalHashGroupJoinEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().ExperimentalHashGroupJoinEnabled), nil
		},
		GlobalDefault: globalFalse,
	},
	`allow_ordinal_column_references`: {
		GetStringVal: makePostgresBoolGetStringValFn(`allow_ordinal_column_references`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("allow_ordinal_column_references", s)
			if err != nil {
				return err
			}
			m.SetAllowOrdinalColumnReference(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().AllowOrdinalColumnReferences), nil
		},
		GlobalDefault: globalFalse,
	},
	`optimizer_use_improved_disjunction_stats`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_improved_disjunction_stats`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_improved_disjunction_stats", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUseImprovedDisjunctionStats(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseImprovedDisjunctionStats), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`optimizer_use_limit_ordering_for_streaming_group_by`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_limit_ordering_for_streaming_group_by`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_limit_ordering_for_streaming_group_by", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUseLimitOrderingForStreamingGroupBy(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseLimitOrderingForStreamingGroupBy), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`optimizer_use_improved_split_disjunction_for_joins`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_improved_split_disjunction_for_joins`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_improved_split_disjunction_for_joins", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUseImprovedSplitDisjunctionForJoins(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseImprovedSplitDisjunctionForJoins), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`enforce_home_region_follower_reads_enabled`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enforce_home_region_follower_reads_enabled`),
		SetWithPlanner: func(ctx context.Context, p *planner, local bool, s string) error {
			p.BufferClientNotice(ctx, pgnotice.Newf(
				"enforce_home_region_follower_reads_enabled is deprecated and will be removed in a future "+
					"release",
			))
			return p.applyOnSessionDataMutators(
				ctx,
				local,
				func(m sessionDataMutator) error {
					b, err := paramparse.ParseBoolVar("enforce_home_region_follower_reads_enabled", s)
					if err != nil {
						return err
					}
					m.SetEnforceHomeRegionFollowerReadsEnabled(b)
					return nil
				},
			)
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().EnforceHomeRegionFollowerReadsEnabled), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`optimizer_always_use_histograms`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_always_use_histograms`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_always_use_histograms", s)
			if err != nil {
				return err
			}
			m.SetOptimizerAlwaysUseHistograms(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerAlwaysUseHistograms), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`optimizer_hoist_uncorrelated_equality_subqueries`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_hoist_uncorrelated_equality_subqueries`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_hoist_uncorrelated_equality_subqueries", s)
			if err != nil {
				return err
			}
			m.SetOptimizerHoistUncorrelatedEqualitySubqueries(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerHoistUncorrelatedEqualitySubqueries), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`optimizer_use_improved_computed_column_filters_derivation`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_improved_computed_column_filters_derivation`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_improved_computed_column_filters_derivation", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUseImprovedComputedColumnFiltersDerivation(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseImprovedComputedColumnFiltersDerivation), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`enable_create_stats_using_extremes`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enable_create_stats_using_extremes`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("enable_create_stats_using_extremes", s)
			if err != nil {
				return err
			}
			m.SetEnableCreateStatsUsingExtremes(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().EnableCreateStatsUsingExtremes), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`enable_create_stats_using_extremes_bool_enum`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enable_create_stats_using_extremes_bool_enum`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("enable_create_stats_using_extremes_bool_enum", s)
			if err != nil {
				return err
			}
			m.SetEnableCreateStatsUsingExtremesBoolEnum(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().EnableCreateStatsUsingExtremesBoolEnum), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`allow_role_memberships_to_change_during_transaction`: {
		GetStringVal: makePostgresBoolGetStringValFn(`allow_role_memberships_to_change_during_transaction`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("allow_role_memberships_to_change_during_transaction", s)
			if err != nil {
				return err
			}
			m.SetAllowRoleMembershipsToChangeDuringTransaction(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().AllowRoleMembershipsToChangeDuringTransaction), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`prepared_statements_cache_size`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			limit, err := humanizeutil.ParseBytes(s)
			if err != nil {
				return err
			}
			m.SetPreparedStatementsCacheSize(limit)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return string(humanizeutil.IBytes(evalCtx.SessionData().PreparedStatementsCacheSize)), nil
		},
		GlobalDefault: func(_ *settings.Values) string {
			return string(humanizeutil.IBytes(0))
		},
	},

	// CockroachDB extension.
	`streamer_enabled`: {
		GetStringVal: makePostgresBoolGetStringValFn(`streamer_enabled`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("streamer_enabled", s)
			if err != nil {
				return err
			}
			m.SetStreamerEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().StreamerEnabled), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(execinfra.UseStreamerEnabled.Get(sv))
		},
	},

	// CockroachDB extension.
	`streamer_always_maintain_ordering`: {
		GetStringVal: makePostgresBoolGetStringValFn(`streamer_always_maintain_ordering`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("streamer_always_maintain_ordering", s)
			if err != nil {
				return err
			}
			m.SetStreamerAlwaysMaintainOrdering(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().StreamerAlwaysMaintainOrdering), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`streamer_in_order_eager_memory_usage_fraction`: {
		GetStringVal: makeFloatGetStringValFn(`streamer_in_order_eager_memory_usage_fraction`),
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatFloatAsPostgresSetting(evalCtx.SessionData().StreamerInOrderEagerMemoryUsageFraction), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return "0.5"
		},
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return err
			}
			// Note that we permit fractions above 1.0 to allow for disabling
			// the "eager memory usage" limit.
			if f < 0 {
				return pgerror.New(pgcode.InvalidParameterValue, "streamer_in_order_eager_memory_usage_fraction must be non-negative")
			}
			m.SetStreamerInOrderEagerMemoryUsageFraction(f)
			return nil
		},
	},

	// CockroachDB extension.
	`streamer_out_of_order_eager_memory_usage_fraction`: {
		GetStringVal: makeFloatGetStringValFn(`streamer_out_of_order_eager_memory_usage_fraction`),
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatFloatAsPostgresSetting(evalCtx.SessionData().StreamerOutOfOrderEagerMemoryUsageFraction), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return "0.8"
		},
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return err
			}
			// Note that we permit fractions above 1.0 to allow for disabling
			// the "eager memory usage" limit.
			if f < 0 {
				return pgerror.New(pgcode.InvalidParameterValue, "streamer_out_of_order_eager_memory_usage_fraction must be non-negative")
			}
			m.SetStreamerOutOfOrderEagerMemoryUsageFraction(f)
			return nil
		},
	},

	// CockroachDB extension.
	`streamer_head_of_line_only_fraction`: {
		GetStringVal: makeFloatGetStringValFn(`streamer_head_of_line_only_fraction`),
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatFloatAsPostgresSetting(evalCtx.SessionData().StreamerHeadOfLineOnlyFraction), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return "0.8"
		},
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return err
			}
			// Note that we permit fractions above 1.0 to allow for giving
			// head-of-the-line batch more memory that is available - this will
			// put the budget in debt.
			if f < 0 {
				return pgerror.New(pgcode.InvalidParameterValue, "streamer_head_of_line_only_fraction must be non-negative")
			}
			m.SetStreamerHeadOfLineOnlyFraction(f)
			return nil
		},
	},

	// CockroachDB extension.
	`multiple_active_portals_enabled`: {
		GetStringVal: makePostgresBoolGetStringValFn(`multiple_active_portals_enabled`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("multiple_active_portals_enabled", s)
			if err != nil {
				return err
			}
			m.SetMultipleActivePortalsEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().MultipleActivePortalsEnabled), nil
		},
		GlobalDefault: displayPgBool(metamorphic.ConstantWithTestBool("multiple_active_portals_enabled", false)),
	},

	// CockroachDB extension.
	`unbounded_parallel_scans`: {
		GetStringVal: makePostgresBoolGetStringValFn(`unbounded_parallel_scans`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("unbounded_parallel_scans", s)
			if err != nil {
				return err
			}
			m.SetUnboundedParallelScans(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().UnboundedParallelScans), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	// PostgreSQL does not use the "replication" session variable (it is only a
	// parameter on the connection string). Instead, it is represented by a
	// `am_walsender` / `am_db_walsender` bool on the connection.
	`replication`: {
		// We are hiding this for now as it is only meant for internal observability.
		// It should only be set at connection time.
		Hidden: true,
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			mode, err := ReplicationModeFromString(s)
			if err != nil {
				return err
			}
			m.SetReplicationMode(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			switch evalCtx.SessionData().ReplicationMode {
			case sessiondatapb.ReplicationMode_REPLICATION_MODE_DISABLED:
				return formatBoolAsPostgresSetting(false), nil
			case sessiondatapb.ReplicationMode_REPLICATION_MODE_ENABLED:
				return formatBoolAsPostgresSetting(true), nil
			case sessiondatapb.ReplicationMode_REPLICATION_MODE_DATABASE:
				return "database", nil
			}
			return "", errors.AssertionFailedf("unknown replication mode: %v", evalCtx.SessionData().ReplicationMode)
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`max_connections`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			maxConn := maxNumNonAdminConnections.Get(&evalCtx.ExecCfg.Settings.SV)
			return strconv.FormatInt(maxConn, 10), nil
		},
	},

	// CockroachDB extension.
	`optimizer_use_improved_join_elimination`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_improved_join_elimination`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_improved_join_elimination", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUseImprovedJoinElimination(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseImprovedJoinElimination), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`enable_implicit_fk_locking_for_serializable`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enable_implicit_fk_locking_for_serializable`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("enable_implicit_fk_locking_for_serializable", s)
			if err != nil {
				return err
			}
			m.SetImplicitFKLockingForSerializable(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().ImplicitFKLockingForSerializable), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`enable_durable_locking_for_serializable`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enable_durable_locking_for_serializable`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("enable_durable_locking_for_serializable", s)
			if err != nil {
				return err
			}
			m.SetDurableLockingForSerializable(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().DurableLockingForSerializable), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`enable_shared_locking_for_serializable`: {
		GetStringVal: makePostgresBoolGetStringValFn(`enable_shared_locking_for_serializable`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("enable_shared_locking_for_serializable", s)
			if err != nil {
				return err
			}
			m.SetSharedLockingForSerializable(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().SharedLockingForSerializable), nil
		},
		GlobalDefault: globalFalse,
	},

	interlockKeySessionVarName: {
		Hidden: true,
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			m.SetUnsafeSettingInterlockKey(s)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return evalCtx.SessionData().UnsafeSettingInterlockKey, nil
		},
		GlobalDefault: func(_ *settings.Values) string { return "" },
	},

	// CockroachDB extension.
	`optimizer_use_lock_op_for_serializable`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_lock_op_for_serializable`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_lock_op_for_serializable", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUseLockOpForSerializable(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseLockOpForSerializable), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`optimizer_use_provided_ordering_fix`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_provided_ordering_fix`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_provided_ordering_fix", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUseProvidedOrderingFix(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseProvidedOrderingFix), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`disable_changefeed_replication`: {
		GetStringVal: makePostgresBoolGetStringValFn(`disable_changefeed_replication`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar(`disable_changefeed_replication`, s)
			if err != nil {
				return err
			}
			m.SetDisableChangefeedReplication(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().DisableChangefeedReplication), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`distsql_plan_gateway_bias`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return strconv.FormatInt(evalCtx.SessionData().DistsqlPlanGatewayBias, 10), nil
		},
		GetStringVal: makeIntGetStringValFn(`distsql_plan_gateway_bias`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if i < 1 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set distsql_plan_gateway_bias to a non-positive value: %d", i)
			}
			m.SetDistSQLPlanGatewayBias(i)
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(2, 10)
		},
	},

	// CockroachDB extension (oracle compatibility).
	`close_cursors_at_commit`: {
		GetStringVal: makePostgresBoolGetStringValFn(`close_cursors_at_commit`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar(`close_cursors_at_commit`, s)
			if err != nil {
				return err
			}
			m.SetCloseCursorsAtCommit(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().CloseCursorsAtCommit), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension (oracle compatibility).
	`plpgsql_use_strict_into`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().PLpgSQLUseStrictInto), nil
		},
		GetStringVal: makePostgresBoolGetStringValFn("plpgsql_use_strict_into"),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("plpgsql_use_strict_into", s)
			if err != nil {
				return err
			}
			m.SetPLpgSQLUseStrictInto(b)
			return nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`optimizer_use_virtual_computed_column_stats`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_virtual_computed_column_stats`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_virtual_computed_column_stats", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUseVirtualComputedColumnStats(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseVirtualComputedColumnStats), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`optimizer_use_conditional_hoist_fix`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_conditional_hoist_fix`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_conditional_hoist_fix", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUseConditionalHoistFix(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseConditionalHoistFix), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`optimizer_use_trigram_similarity_optimization`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_trigram_similarity_optimization`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_trigram_similarity_optimization", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUseTrigramSimilarityOptimization(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseTrigramSimilarityOptimization), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`optimizer_use_improved_distinct_on_limit_hint_costing`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_improved_distinct_on_limit_hint_costing`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_improved_distinct_on_limit_hint_costing", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUseImprovedDistinctOnLimitHintCosting(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseImprovedDistinctOnLimitHintCosting), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`optimizer_use_improved_trigram_similarity_selectivity`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_improved_trigram_similarity_selectivity`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_improved_trigram_similarity_selectivity", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUseImprovedTrigramSimilaritySelectivity(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseImprovedTrigramSimilaritySelectivity), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`optimizer_use_improved_zigzag_join_costing`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_improved_zigzag_join_costing`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_improved_zigzag_join_costing", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUseImprovedZigzagJoinCosting(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseImprovedZigzagJoinCosting), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`optimizer_use_improved_multi_column_selectivity_estimate`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_improved_multi_column_selectivity_estimate`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_improved_multi_column_selectivity_estimate", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUseImprovedMultiColumnSelectivityEstimate(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUseImprovedMultiColumnSelectivityEstimate), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`optimizer_prove_implication_with_virtual_computed_columns`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_prove_implication_with_virtual_computed_columns`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_prove_implication_with_virtual_computed_columns", s)
			if err != nil {
				return err
			}
			m.SetOptimizerProveImplicationWithVirtualComputedColumns(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerProveImplicationWithVirtualComputedColumns), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`optimizer_push_offset_into_index_join`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_push_offset_into_index_join`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_push_offset_into_index_join", s)
			if err != nil {
				return err
			}
			m.SetOptimizerPushOffsetIntoIndexJoin(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerPushOffsetIntoIndexJoin), nil
		},
		GlobalDefault: globalTrue,
	},

	`plan_cache_mode`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			mode, ok := sessiondatapb.PlanCacheModeFromString(s)
			if !ok {
				return newVarValueError(
					`plan_cache_mode`,
					s,
					sessiondatapb.PlanCacheModeForceCustom.String(),
					sessiondatapb.PlanCacheModeForceGeneric.String(),
					sessiondatapb.PlanCacheModeAuto.String(),
				)
			}
			m.SetPlanCacheMode(mode)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return evalCtx.SessionData().PlanCacheMode.String(), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return planCacheClusterMode.String(sv)
		},
	},

	// CockroachDB extension.
	`optimizer_use_polymorphic_parameter_fix`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_use_polymorphic_parameter_fix`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_use_polymorphic_parameter_fix", s)
			if err != nil {
				return err
			}
			m.SetOptimizerUsePolymorphicParameterFix(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerUsePolymorphicParameterFix), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`optimizer_push_limit_into_project_filtered_scan`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_push_limit_into_project_filtered_scan`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_push_limit_into_project_filtered_scan", s)
			if err != nil {
				return err
			}
			m.SetOptimizerPushLimitIntoProjectFilteredScan(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerPushLimitIntoProjectFilteredScan), nil
		},
		GlobalDefault: globalTrue,
	},

	// CockroachDB extension.
	`bypass_pcr_reader_catalog_aost`: {
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("bypass_pcr_reader_catalog_aost", s)
			if err != nil {
				return err
			}
			m.SetBypassPCRReaderCatalogAOST(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().BypassPCRReaderCatalogAOST), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`unsafe_allow_triggers_modifying_cascades`: {
		GetStringVal: makePostgresBoolGetStringValFn(`unsafe_allow_triggers_modifying_cascades`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("unsafe_allow_triggers_modifying_cascades", s)
			if err != nil {
				return err
			}
			m.SetUnsafeAllowTriggersModifyingCascades(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().UnsafeAllowTriggersModifyingCascades), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`recursion_depth_limit`: {
		GetStringVal: makeIntGetStringValFn(`recursion_depth_limit`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			if b < 0 {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"cannot set recursion_depth_limit to a negative value: %d", b)
			}
			m.SetRecursionDepthLimit(int(b))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return strconv.FormatInt(evalCtx.SessionData().RecursionDepthLimit, 10), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return strconv.FormatInt(1000, 10)
		},
	},

	// CockroachDB extension.
	`legacy_varchar_typing`: {
		GetStringVal: makePostgresBoolGetStringValFn(`legacy_varchar_typing`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("legacy_varchar_typing", s)
			if err != nil {
				return err
			}
			m.SetLegacyVarcharTyping(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().LegacyVarcharTyping), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`catalog_digest_staleness_check_enabled`: {
		GetStringVal: makePostgresBoolGetStringValFn(`catalog_digest_staleness_check_enabled`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("catalog_digest_staleness_check_enabled", s)
			if err != nil {
				return err
			}
			m.SetCatalogDigestStalenessCheckEnabled(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().CatalogDigestStalenessCheckEnabled), nil
		},
		GlobalDefault: globalTrue,
		Hidden:        true,
	},

	// CockroachDB extension.
	`optimizer_prefer_bounded_cardinality`: {
		GetStringVal: makePostgresBoolGetStringValFn(`optimizer_prefer_bounded_cardinality`),
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar("optimizer_prefer_bounded_cardinality", s)
			if err != nil {
				return err
			}
			m.SetOptimizerPreferBoundedCardinality(b)
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().OptimizerPreferBoundedCardinality), nil
		},
		GlobalDefault: globalFalse,
	},

	// CockroachDB extension.
	`optimizer_min_row_count`: {
		GetStringVal: makeFloatGetStringValFn(`optimizer_min_row_count`),
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatFloatAsPostgresSetting(evalCtx.SessionData().OptimizerMinRowCount), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return "0"
		},
		Set: func(_ context.Context, m sessionDataMutator, s string) error {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return err
			}
			if f < 0 {
				return pgerror.New(pgcode.InvalidParameterValue, "optimizer_min_row_count must be non-negative")
			}
			m.SetOptimizerMinRowCount(f)
			return nil
		},
	},

	// CockroachDB extension.
	`kv_transaction_buffered_writes_enabled`: {
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatBoolAsPostgresSetting(evalCtx.SessionData().BufferedWritesEnabled), nil
		},
		GetStringVal: makePostgresBoolGetStringValFn("kv_transaction_buffered_writes_enabled"),
		Set: func(ctx context.Context, m sessionDataMutator, s string) error {
			b, err := paramparse.ParseBoolVar(`kv_transaction_buffered_writes_enabled`, s)
			if err != nil {
				return err
			}
			m.SetBufferedWritesEnabled(b)
			return nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return formatBoolAsPostgresSetting(kvcoord.BufferedWritesEnabled.Get(sv))
		},
	},

	// CockroachDB extension.
	`optimizer_check_input_min_row_count`: {
		GetStringVal: makeFloatGetStringValFn(`optimizer_check_input_min_row_count`),
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return formatFloatAsPostgresSetting(evalCtx.SessionData().OptimizerCheckInputMinRowCount), nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return "1"
		},
		Set: func(ctx context.Context, m sessionDataMutator, s string) error {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return err
			}
			if f < 0 {
				return pgerror.New(pgcode.InvalidParameterValue, "optimizer_check_input_min_row_count must be non-negative")
			}
			m.SetOptimizerCheckInputMinRowCount(f)
			return nil
		},
	},
}

func ReplicationModeFromString(s string) (sessiondatapb.ReplicationMode, error) {
	if strings.ToLower(s) == "database" {
		return sessiondatapb.ReplicationMode_REPLICATION_MODE_DATABASE, nil
	}
	b, err := paramparse.ParseBoolVar("replication", s)
	if err != nil {
		return 0, pgerror.Newf(
			pgcode.InvalidParameterValue,
			`parameter "replication" requires a boolean value or "database"`,
		)
	}
	if b {
		return sessiondatapb.ReplicationMode_REPLICATION_MODE_ENABLED, nil
	}
	return sessiondatapb.ReplicationMode_REPLICATION_MODE_DISABLED, nil
}

// We want test coverage for this on and off so make it metamorphic.
var copyFastPathDefault bool = metamorphic.ConstantWithTestBool("copy-fast-path-enabled-default", true)

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
				u, err := username.MakeSQLUsernameFromUserInput(s, username.PurposeValidation)
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
	it := varGen[`idle_in_session_timeout`]
	it.Hidden = false
	varGen[`idle_session_timeout`] = it

	// Compatibility with a previous version of CockroachDB.
	ah := varGen[`enable_auto_rehoming`]
	ah.Hidden = true
	varGen[`experimental_enable_auto_rehoming`] = ah

	// Initialize delegate.ValidVars.
	for v := range varGen {
		delegate.ValidVars[v] = struct{}{}
	}
	// Initialize varNames.
	varNames = func() []string {
		res := make([]string, 0, len(varGen))
		for vName := range varGen {
			res = append(res, vName)
		}
		sort.Strings(res)
		return res
	}()
}

// TestingSetSessionVariable sets a new value for session setting `varName` in the
// session settings owned by `evalCtx`, returning an error if not successful.
// This function should only be used for testing. For general-purpose code,
// please use SessionAccessor.SetSessionVar instead.
func TestingSetSessionVariable(
	ctx context.Context, evalCtx eval.Context, varName, varValue string,
) (err error) {
	err = CheckSessionVariableValueValid(ctx, evalCtx.Settings, varName, varValue)
	if err != nil {
		return err
	}
	sdMutatorBase := sessionDataMutatorBase{
		settings: evalCtx.Settings,
	}
	sdMutator := sessionDataMutator{
		data:                        evalCtx.SessionData(),
		sessionDataMutatorBase:      sdMutatorBase,
		sessionDataMutatorCallbacks: sessionDataMutatorCallbacks{},
	}
	_, sVar, err := getSessionVar(varName, false)
	if err != nil {
		return err
	}

	return sVar.Set(ctx, sdMutator, varValue)
}

// TestingResetSessionVariables resets all session settings in evalCtx to their
// global default, if they have a global default.
func TestingResetSessionVariables(ctx context.Context, evalCtx eval.Context) (err error) {
	sdMutatorBase := sessionDataMutatorBase{
		settings: evalCtx.Settings,
	}
	sdMutator := sessionDataMutator{
		data:                        evalCtx.SessionData(),
		sessionDataMutatorBase:      sdMutatorBase,
		sessionDataMutatorCallbacks: sessionDataMutatorCallbacks{},
	}
	for _, v := range varGen {
		if v.Set == nil || v.GlobalDefault == nil {
			continue
		}
		if err := v.Set(ctx, sdMutator, v.GlobalDefault(&evalCtx.Settings.SV)); err != nil {
			return err
		}
	}
	return nil
}

// makePostgresBoolGetStringValFn returns a function that evaluates and returns
// a string representation of the first argument value.
func makePostgresBoolGetStringValFn(varName string) getStringValFn {
	return func(
		ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr, _ *kv.Txn,
	) (string, error) {
		if len(values) != 1 {
			return "", newSingleArgVarError(varName)
		}
		val, err := eval.Expr(ctx, &evalCtx.Context, values[0])
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
		Get:           func(_ *extendedEvalContext, _ *kv.Txn) (string, error) { return value, nil },
		GlobalDefault: func(_ *settings.Values) string { return value },
	}
}

func makeReadOnlyVarWithFn(fn func() string) sessionVar {
	return sessionVar{
		Get:           func(_ *extendedEvalContext, _ *kv.Txn) (string, error) { return fn(), nil },
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
		Get: func(_ *extendedEvalContext, _ *kv.Txn) (string, error) { return displayValStr, nil },
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
		Get: func(_ *extendedEvalContext, _ *kv.Txn) (string, error) {
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

func makeBackwardsCompatBoolVar(varName string, displayValue bool) sessionVar {
	displayValStr := formatBoolAsPostgresSetting(displayValue)
	return sessionVar{
		Hidden:       true,
		GetStringVal: makePostgresBoolGetStringValFn(varName),
		SetWithPlanner: func(ctx context.Context, p *planner, local bool, s string) error {
			p.BufferClientNotice(ctx, pgnotice.Newf("%s no longer has any effect", varName))
			return nil
		},
		Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
			return displayValStr, nil
		},
		GlobalDefault: func(sv *settings.Values) string {
			return displayValStr
		},
	}
}

// makeIntGetStringValFn returns a getStringValFn which allows
// the user to provide plain integer values to a SET variable.
func makeIntGetStringValFn(name string) getStringValFn {
	return func(ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr, _ *kv.Txn) (string, error) {
		s, err := getIntVal(ctx, &evalCtx.Context, name, values)
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(s, 10), nil
	}
}

// makeFloatGetStringValFn returns a getStringValFn which allows
// the user to provide plain float values to a SET variable.
func makeFloatGetStringValFn(name string) getStringValFn {
	return func(ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr, txn *kv.Txn) (string, error) {
		f, err := getFloatVal(ctx, &evalCtx.Context, name, values)
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
			Get: func(evalCtx *extendedEvalContext, _ *kv.Txn) (string, error) {
				v, ok := evalCtx.SessionData().CustomOptions[varName]
				if !ok {
					return "", pgerror.Newf(pgcode.UndefinedObject,
						"unrecognized configuration parameter %q", varName)
				}
				return v, nil
			},
			Exists: func(evalCtx *extendedEvalContext, _ *kv.Txn) bool {
				_, ok := evalCtx.SessionData().CustomOptions[varName]
				return ok
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

// GetSessionVar implements the eval.SessionAccessor interface.
func (p *planner) GetSessionVar(
	_ context.Context, varName string, missingOk bool,
) (bool, string, error) {
	name := strings.ToLower(varName)
	ok, v, err := getSessionVar(name, missingOk)
	if err != nil || !ok {
		return ok, "", err
	}
	if existsFn := v.Exists; existsFn != nil {
		if missingOk && !existsFn(&p.extendedEvalCtx, p.Txn()) {
			return false, "", nil
		}
	}
	val, err := v.Get(&p.extendedEvalCtx, p.Txn())
	return true, val, err
}

// SetSessionVar implements the eval.SessionAccessor interface.
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
