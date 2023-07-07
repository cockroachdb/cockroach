// Copyright 2015 The Cockroach Authors.
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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/inspectz/inspectzpb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/obs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/acprovider"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contention"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob/gcjobnotifier"
	"github.com/cockroachdb/cockroach/pkg/sql/idxusage"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/optionalnodeliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirecancel"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/scheduledlogging"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/asof"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilegecache"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/collector"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// ClusterOrganization is the organization name.
var ClusterOrganization = settings.RegisterStringSetting(
	settings.TenantWritable,
	"cluster.organization",
	"organization name",
	"",
).WithPublic()

// ClusterIsInternal returns true if the cluster organization contains
// "Cockroach Labs", indicating an internal cluster.
func ClusterIsInternal(sv *settings.Values) bool {
	return strings.Contains(ClusterOrganization.Get(sv), "Cockroach Labs")
}

// ClusterSecret is a cluster specific secret. This setting is
// non-reportable.
var ClusterSecret = func() *settings.StringSetting {
	s := settings.RegisterStringSetting(
		settings.TenantWritable,
		"cluster.secret",
		"cluster specific secret",
		"",
	)
	// Even though string settings are non-reportable by default, we
	// still mark them explicitly in case a future code change flips the
	// default.
	s.SetReportable(false)
	return s
}()

// defaultIntSize controls how a "naked" INT type will be parsed.
// TODO(bob): Change this to 4 in v2.3; https://github.com/cockroachdb/cockroach/issues/32534
// TODO(bob): Remove or n-op this in v2.4: https://github.com/cockroachdb/cockroach/issues/32844
var defaultIntSize = func() *settings.IntSetting {
	s := settings.RegisterIntSetting(
		settings.TenantWritable,
		"sql.defaults.default_int_size",
		"the size, in bytes, of an INT type", 8, func(i int64) error {
			if i != 4 && i != 8 {
				return errors.New("only 4 or 8 are valid values")
			}
			return nil
		}).WithPublic()
	s.SetVisibility(settings.Public)
	return s
}()

const allowCrossDatabaseFKsSetting = "sql.cross_db_fks.enabled"

var allowCrossDatabaseFKs = settings.RegisterBoolSetting(
	settings.TenantWritable,
	allowCrossDatabaseFKsSetting,
	"if true, creating foreign key references across databases is allowed",
	false,
).WithPublic()

const allowCrossDatabaseViewsSetting = "sql.cross_db_views.enabled"

var allowCrossDatabaseViews = settings.RegisterBoolSetting(
	settings.TenantWritable,
	allowCrossDatabaseViewsSetting,
	"if true, creating views that refer to other databases is allowed",
	false,
).WithPublic()

const allowCrossDatabaseSeqOwnerSetting = "sql.cross_db_sequence_owners.enabled"

var allowCrossDatabaseSeqOwner = settings.RegisterBoolSetting(
	settings.TenantWritable,
	allowCrossDatabaseSeqOwnerSetting,
	"if true, creating sequences owned by tables from other databases is allowed",
	false,
).WithPublic()

const allowCrossDatabaseSeqReferencesSetting = "sql.cross_db_sequence_references.enabled"

var allowCrossDatabaseSeqReferences = settings.RegisterBoolSetting(
	settings.TenantWritable,
	allowCrossDatabaseSeqReferencesSetting,
	"if true, sequences referenced by tables from other databases are allowed",
	false,
).WithPublic()

// SecondaryTenantZoneConfigsEnabled controls if secondary tenants are allowed
// to set zone configurations. It has no effect for the system tenant.
//
// This setting has no effect on zone configurations that have already been set.
var SecondaryTenantZoneConfigsEnabled = settings.RegisterBoolSetting(
	settings.TenantReadOnly,
	"sql.zone_configs.allow_for_secondary_tenant.enabled",
	"allow secondary tenants to set zone configurations; does not affect the system tenant",
	false,
)

// SecondaryTenantSplitAtEnabled controls if secondary tenants are allowed to
// run ALTER TABLE/INDEX ... SPLIT AT statements. It has no effect for the
// system tenant.
var SecondaryTenantSplitAtEnabled = settings.RegisterBoolSetting(
	settings.TenantReadOnly,
	"sql.split_at.allow_for_secondary_tenant.enabled",
	"allow secondary tenants to run ALTER TABLE/INDEX ... SPLIT AT commands; does not affect the system tenant",
	false,
)

// SecondaryTenantScatterEnabled controls if secondary tenants are allowed to
// run ALTER TABLE/INDEX ... SCATTER statements. It has no effect for the
// system tenant.
var SecondaryTenantScatterEnabled = settings.RegisterBoolSetting(
	settings.TenantReadOnly,
	"sql.scatter.allow_for_secondary_tenant.enabled",
	"allow secondary tenants to run ALTER TABLE/INDEX ... SCATTER commands; does not affect the system tenant",
	false,
)

// traceTxnThreshold can be used to log SQL transactions that take
// longer than duration to complete. For example, traceTxnThreshold=1s
// will log the trace for any transaction that takes 1s or longer. To
// log traces for all transactions use traceTxnThreshold=1ns. Note
// that any positive duration will enable tracing and will slow down
// all execution because traces are gathered for all transactions even
// if they are not output.
var traceTxnThreshold = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.trace.txn.enable_threshold",
	"enables tracing on all transactions; transactions open for longer than "+
		"this duration will have their trace logged (set to 0 to disable); "+
		"note that enabling this may have a negative performance impact; "+
		"this setting is coarser-grained than sql.trace.stmt.enable_threshold "+
		"because it applies to all statements within a transaction as well as "+
		"client communication (e.g. retries)", 0,
).WithPublic()

// TraceStmtThreshold is identical to traceTxnThreshold except it applies to
// individual statements in a transaction. The motivation for this setting is
// to be able to reduce the noise associated with a larger transaction (e.g.
// round trips to client).
var TraceStmtThreshold = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.trace.stmt.enable_threshold",
	"enables tracing on all statements; statements executing for longer than "+
		"this duration will have their trace logged (set to 0 to disable); "+
		"note that enabling this may have a negative performance impact; "+
		"this setting applies to individual statements within a transaction and "+
		"is therefore finer-grained than sql.trace.txn.enable_threshold",
	0,
).WithPublic()

// traceSessionEventLogEnabled can be used to enable the event log
// that is normally kept for every SQL connection. The event log has a
// non-trivial performance impact and also reveals SQL statements
// which may be a privacy concern.
var traceSessionEventLogEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.trace.session_eventlog.enabled",
	"set to true to enable session tracing; "+
		"note that enabling this may have a negative performance impact",
	false,
).WithPublic()

// ReorderJoinsLimitClusterSettingName is the name of the cluster setting for
// the maximum number of joins to reorder.
const ReorderJoinsLimitClusterSettingName = "sql.defaults.reorder_joins_limit"

// ReorderJoinsLimitClusterValue controls the cluster default for the maximum
// number of joins reordered.
var ReorderJoinsLimitClusterValue = settings.RegisterIntSetting(
	settings.TenantWritable,
	ReorderJoinsLimitClusterSettingName,
	"default number of joins to reorder",
	opt.DefaultJoinOrderLimit,
	func(limit int64) error {
		if limit < 0 || limit > opt.MaxReorderJoinsLimit {
			return pgerror.Newf(pgcode.InvalidParameterValue,
				"cannot set %s to a value less than 0 or greater than %v",
				ReorderJoinsLimitClusterSettingName,
				opt.MaxReorderJoinsLimit,
			)
		}
		return nil
	},
).WithPublic()

var requireExplicitPrimaryKeysClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.require_explicit_primary_keys.enabled",
	"default value for requiring explicit primary keys in CREATE TABLE statements",
	false,
).WithPublic()

var placementEnabledClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.multiregion_placement_policy.enabled",
	"default value for enable_multiregion_placement_policy;"+
		" allows for use of PLACEMENT RESTRICTED",
	false,
)

var autoRehomingEnabledClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.experimental_auto_rehoming.enabled",
	"default value for experimental_enable_auto_rehoming;"+
		" allows for rows in REGIONAL BY ROW tables to be auto-rehomed on UPDATE",
	false,
)

var onUpdateRehomeRowEnabledClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.on_update_rehome_row.enabled",
	"default value for on_update_rehome_row;"+
		" enables ON UPDATE rehome_row() expressions to trigger on updates",
	true,
).WithPublic()

var temporaryTablesEnabledClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.experimental_temporary_tables.enabled",
	"default value for experimental_enable_temp_tables; allows for use of temporary tables by default",
	false,
).WithPublic()

var implicitColumnPartitioningEnabledClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.experimental_implicit_column_partitioning.enabled",
	"default value for experimental_enable_temp_tables; allows for the use of implicit column partitioning",
	false,
).WithPublic()

var overrideMultiRegionZoneConfigClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.override_multi_region_zone_config.enabled",
	"default value for override_multi_region_zone_config; "+
		"allows for overriding the zone configs of a multi-region table or database",
	false,
).WithPublic()

var maxHashShardedIndexRangePreSplit = settings.RegisterIntSetting(
	settings.SystemOnly,
	"sql.hash_sharded_range_pre_split.max",
	"max pre-split ranges to have when adding hash sharded index to an existing table",
	16,
	settings.PositiveInt,
).WithPublic()

var zigzagJoinClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.zigzag_join.enabled",
	"default value for enable_zigzag_join session setting; allows use of zig-zag join by default",
	true,
).WithPublic()

var optDrivenFKCascadesClusterLimit = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.defaults.foreign_key_cascades_limit",
	"default value for foreign_key_cascades_limit session setting; limits the number of cascading operations that run as part of a single query",
	10000,
	settings.NonNegativeInt,
).WithPublic()

var preferLookupJoinsForFKs = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.prefer_lookup_joins_for_fks.enabled",
	"default value for prefer_lookup_joins_for_fks session setting; causes foreign key operations to use lookup joins when possible",
	false,
).WithPublic()

// optUseHistogramsClusterMode controls the cluster default for whether
// histograms are used by the optimizer for cardinality estimation.
// Note that it does not control histogram collection; regardless of the
// value of this setting, the optimizer cannot use histograms if they
// haven't been collected. Collection of histograms is controlled by the
// cluster setting sql.stats.histogram_collection.enabled.
var optUseHistogramsClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.optimizer_use_histograms.enabled",
	"default value for optimizer_use_histograms session setting; enables usage of histograms in the optimizer by default",
	true,
).WithPublic()

// optUseMultiColStatsClusterMode controls the cluster default for whether
// multi-column stats are used by the optimizer for cardinality estimation.
// Note that it does not control collection of multi-column stats; regardless
// of the value of this setting, the optimizer cannot use multi-column stats
// if they haven't been collected. Collection of multi-column stats is
// controlled by the cluster setting sql.stats.multi_column_collection.enabled.
var optUseMultiColStatsClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.optimizer_use_multicol_stats.enabled",
	"default value for optimizer_use_multicol_stats session setting; enables usage of multi-column stats in the optimizer by default",
	true,
).WithPublic()

// localityOptimizedSearchMode controls the cluster default for the use of
// locality optimized search. If enabled, the optimizer will try to plan scans
// and lookup joins in which local nodes (i.e., nodes in the gateway region) are
// searched for matching rows before remote nodes, in the hope that the
// execution engine can avoid visiting remote nodes.
var localityOptimizedSearchMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.locality_optimized_partitioned_index_scan.enabled",
	"default value for locality_optimized_partitioned_index_scan session setting; "+
		"enables searching for rows in the current region before searching remote regions",
	true,
).WithPublic()

var implicitSelectForUpdateClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.implicit_select_for_update.enabled",
	"default value for enable_implicit_select_for_update session setting; enables FOR UPDATE locking during the row-fetch phase of mutation statements",
	true,
).WithPublic()

var insertFastPathClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.insert_fast_path.enabled",
	"default value for enable_insert_fast_path session setting; enables a specialized insert path",
	true,
).WithPublic()

var experimentalAlterColumnTypeGeneralMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.experimental_alter_column_type.enabled",
	"default value for experimental_alter_column_type session setting; "+
		"enables the use of ALTER COLUMN TYPE for general conversions",
	false,
).WithPublic()

var clusterStatementTimeout = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.defaults.statement_timeout",
	"default value for the statement_timeout; "+
		"default value for the statement_timeout session setting; controls the "+
		"duration a query is permitted to run before it is canceled; if set to 0, "+
		"there is no timeout",
	0,
	settings.NonNegativeDuration,
).WithPublic()

var clusterLockTimeout = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.defaults.lock_timeout",
	"default value for the lock_timeout; "+
		"default value for the lock_timeout session setting; controls the "+
		"duration a query is permitted to wait while attempting to acquire "+
		"a lock on a key or while blocking on an existing lock in order to "+
		"perform a non-locking read on a key; if set to 0, there is no timeout",
	0,
	settings.NonNegativeDuration,
).WithPublic()

var clusterIdleInSessionTimeout = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.defaults.idle_in_session_timeout",
	"default value for the idle_in_session_timeout; "+
		"default value for the idle_in_session_timeout session setting; controls the "+
		"duration a session is permitted to idle before the session is terminated; "+
		"if set to 0, there is no timeout",
	0,
	settings.NonNegativeDuration,
).WithPublic()

var clusterIdleInTransactionSessionTimeout = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.defaults.idle_in_transaction_session_timeout",
	"default value for the idle_in_transaction_session_timeout; controls the "+
		"duration a session is permitted to idle in a transaction before the "+
		"session is terminated; if set to 0, there is no timeout",
	0,
	settings.NonNegativeDuration,
).WithPublic()

// TODO(rytaft): remove this once unique without index constraints are fully
// supported.
var experimentalUniqueWithoutIndexConstraintsMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.experimental_enable_unique_without_index_constraints.enabled",
	"default value for experimental_enable_unique_without_index_constraints session setting;"+
		"disables unique without index constraints by default",
	false,
).WithPublic()

var experimentalUseNewSchemaChanger = settings.RegisterEnumSetting(
	settings.TenantWritable,
	"sql.defaults.use_declarative_schema_changer",
	"default value for use_declarative_schema_changer session setting;"+
		"disables new schema changer by default",
	"on",
	map[int64]string{
		int64(sessiondatapb.UseNewSchemaChangerOff):          "off",
		int64(sessiondatapb.UseNewSchemaChangerOn):           "on",
		int64(sessiondatapb.UseNewSchemaChangerUnsafe):       "unsafe",
		int64(sessiondatapb.UseNewSchemaChangerUnsafeAlways): "unsafe_always",
	},
).WithPublic()

var stubCatalogTablesEnabledClusterValue = settings.RegisterBoolSetting(
	settings.TenantWritable,
	`sql.defaults.stub_catalog_tables.enabled`,
	`default value for stub_catalog_tables session setting`,
	true,
).WithPublic()

var experimentalComputedColumnRewrites = settings.RegisterValidatedStringSetting(
	settings.TenantWritable,
	"sql.defaults.experimental_computed_column_rewrites",
	"allows rewriting computed column expressions in CREATE TABLE and ALTER TABLE statements; "+
		"the format is: '(before expression) -> (after expression) [, (before expression) -> (after expression) ...]'",
	"", /* defaultValue */
	func(_ *settings.Values, val string) error {
		_, err := schemaexpr.ParseComputedColumnRewrites(val)
		return err
	},
)

var propagateInputOrdering = settings.RegisterBoolSetting(
	settings.TenantWritable,
	`sql.defaults.propagate_input_ordering.enabled`,
	`default value for the experimental propagate_input_ordering session variable`,
	false,
)

// settingWorkMemBytes is a cluster setting that determines the maximum amount
// of RAM that a processor can use.
var settingWorkMemBytes = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	"sql.distsql.temp_storage.workmem",
	"maximum amount of memory in bytes a processor can use before falling back to temp storage",
	execinfra.DefaultMemoryLimit, /* 64MiB */
	func(v int64) error {
		if v <= 1 {
			return errors.Errorf("can only be set to a value greater than 1: %d", v)
		}
		return nil
	},
).WithPublic()

// ExperimentalDistSQLPlanningClusterSettingName is the name for the cluster
// setting that controls experimentalDistSQLPlanningClusterMode below.
const ExperimentalDistSQLPlanningClusterSettingName = "sql.defaults.experimental_distsql_planning"

// experimentalDistSQLPlanningClusterMode can be used to enable
// optimizer-driven DistSQL planning that sidesteps intermediate planNode
// transition when going from opt.Expr to DistSQL processor specs.
var experimentalDistSQLPlanningClusterMode = settings.RegisterEnumSetting(
	settings.TenantWritable,
	ExperimentalDistSQLPlanningClusterSettingName,
	"default experimental_distsql_planning mode; enables experimental opt-driven DistSQL planning",
	"off",
	map[int64]string{
		int64(sessiondatapb.ExperimentalDistSQLPlanningOff): "off",
		int64(sessiondatapb.ExperimentalDistSQLPlanningOn):  "on",
	},
).WithPublic()

// VectorizeClusterSettingName is the name for the cluster setting that controls
// the VectorizeClusterMode below.
const VectorizeClusterSettingName = "sql.defaults.vectorize"

// VectorizeClusterMode controls the cluster default for when automatic
// vectorization is enabled.
var VectorizeClusterMode = settings.RegisterEnumSetting(
	settings.TenantWritable,
	VectorizeClusterSettingName,
	"default vectorize mode",
	"on",
	map[int64]string{
		int64(sessiondatapb.VectorizeUnset):              "on",
		int64(sessiondatapb.VectorizeOn):                 "on",
		int64(sessiondatapb.VectorizeExperimentalAlways): "experimental_always",
		int64(sessiondatapb.VectorizeOff):                "off",
	},
).WithPublic()

// DistSQLClusterExecMode controls the cluster default for when DistSQL is used.
var DistSQLClusterExecMode = settings.RegisterEnumSetting(
	settings.TenantWritable,
	"sql.defaults.distsql",
	"default distributed SQL execution mode",
	"auto",
	map[int64]string{
		int64(sessiondatapb.DistSQLOff):    "off",
		int64(sessiondatapb.DistSQLAuto):   "auto",
		int64(sessiondatapb.DistSQLOn):     "on",
		int64(sessiondatapb.DistSQLAlways): "always",
	},
).WithPublic()

// SerialNormalizationMode controls how the SERIAL type is interpreted in table
// definitions.
var SerialNormalizationMode = settings.RegisterEnumSetting(
	settings.TenantWritable,
	"sql.defaults.serial_normalization",
	"default handling of SERIAL in table definitions",
	"rowid",
	map[int64]string{
		int64(sessiondatapb.SerialUsesRowID):              "rowid",
		int64(sessiondatapb.SerialUsesUnorderedRowID):     "unordered_rowid",
		int64(sessiondatapb.SerialUsesVirtualSequences):   "virtual_sequence",
		int64(sessiondatapb.SerialUsesSQLSequences):       "sql_sequence",
		int64(sessiondatapb.SerialUsesCachedSQLSequences): "sql_sequence_cached",
	},
).WithPublic()

var disallowFullTableScans = settings.RegisterBoolSetting(
	settings.TenantWritable,
	`sql.defaults.disallow_full_table_scans.enabled`,
	"setting to true rejects queries that have planned a full table scan",
	false,
).WithPublic()

// intervalStyle controls intervals representation.
var intervalStyle = settings.RegisterEnumSetting(
	settings.TenantWritable,
	"sql.defaults.intervalstyle",
	"default value for IntervalStyle session setting",
	strings.ToLower(duration.IntervalStyle_POSTGRES.String()),
	func() map[int64]string {
		ret := make(map[int64]string, len(duration.IntervalStyle_name))
		for k, v := range duration.IntervalStyle_name {
			ret[int64(k)] = strings.ToLower(v)
		}
		return ret
	}(),
).WithPublic()

var dateStyleEnumMap = map[int64]string{
	0: "ISO, MDY",
	1: "ISO, DMY",
	2: "ISO, YMD",
}

// dateStyle controls dates representation.
var dateStyle = settings.RegisterEnumSetting(
	settings.TenantWritable,
	"sql.defaults.datestyle",
	"default value for DateStyle session setting",
	pgdate.DefaultDateStyle().SQLString(),
	dateStyleEnumMap,
).WithPublic()

var txnRowsWrittenLog = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.defaults.transaction_rows_written_log",
	"the threshold for the number of rows written by a SQL transaction "+
		"which - once exceeded - will trigger a logging event to SQL_PERF (or "+
		"SQL_INTERNAL_PERF for internal transactions); use 0 to disable",
	0,
	settings.NonNegativeInt,
).WithPublic()

var txnRowsWrittenErr = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.defaults.transaction_rows_written_err",
	"the limit for the number of rows written by a SQL transaction which - "+
		"once exceeded - will fail the transaction (or will trigger a logging "+
		"event to SQL_INTERNAL_PERF for internal transactions); use 0 to disable",
	0,
	settings.NonNegativeInt,
).WithPublic()

var txnRowsReadLog = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.defaults.transaction_rows_read_log",
	"the threshold for the number of rows read by a SQL transaction "+
		"which - once exceeded - will trigger a logging event to SQL_PERF (or "+
		"SQL_INTERNAL_PERF for internal transactions); use 0 to disable",
	0,
	settings.NonNegativeInt,
).WithPublic()

var txnRowsReadErr = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.defaults.transaction_rows_read_err",
	"the limit for the number of rows read by a SQL transaction which - "+
		"once exceeded - will fail the transaction (or will trigger a logging "+
		"event to SQL_INTERNAL_PERF for internal transactions); use 0 to disable",
	0,
	settings.NonNegativeInt,
).WithPublic()

// This is a float setting (rather than an int setting) because the optimizer
// uses floating point for calculating row estimates.
var largeFullScanRows = settings.RegisterFloatSetting(
	settings.TenantWritable,
	"sql.defaults.large_full_scan_rows",
	"default value for large_full_scan_rows session setting which determines "+
		"the maximum table size allowed for a full scan when disallow_full_table_scans "+
		"is set to true",
	1000.0,
).WithPublic()

var costScansWithDefaultColSize = settings.RegisterBoolSetting(
	settings.TenantWritable,
	`sql.defaults.cost_scans_with_default_col_size.enabled`,
	"setting to true uses the same size for all columns to compute scan cost",
	false,
).WithPublic()

var enableSuperRegions = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.super_regions.enabled",
	"default value for enable_super_regions; "+
		"allows for the usage of super regions",
	false,
).WithPublic()

var overrideAlterPrimaryRegionInSuperRegion = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.override_alter_primary_region_in_super_region.enabled",
	"default value for override_alter_primary_region_in_super_region; "+
		"allows for altering the primary region even if the primary region is a "+
		"member of a super region",
	false,
).WithPublic()

var errNoTransactionInProgress = errors.New("there is no transaction in progress")
var errTransactionInProgress = errors.New("there is already a transaction in progress")

const sqlTxnName string = "sql txn"
const metricsSampleInterval = 10 * time.Second

// enableDropVirtualCluster (or rather, its inverted boolean value) defines
// the default value for the session var "disable_drop_virtual_cluster".
//
// Note:
//   - We use a cluster setting here instead of a default role option
//     because we need this to be settable also for the 'admin' role.
//   - The cluster setting is named "enable" because boolean cluster
//     settings are all ".enabled" -- we do not have ".disabled"
//     settings anywhere.
//   - The session var is named "disable_" because we want the Go
//     default value (false) to mean that tenant deletion is enabled.
//     This is needed for backward-compatibility with Cockroach Cloud.
var enableDropVirtualCluster = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"sql.drop_virtual_cluster.enabled",
	"default value (inverted) for the disable_virtual_cluster session setting",
	true,
)

// Fully-qualified names for metrics.
var (
	MetaSQLExecLatency = metric.Metadata{
		Name:        "sql.exec.latency",
		Help:        "Latency of SQL statement execution",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaSQLServiceLatency = metric.Metadata{
		Name:        "sql.service.latency",
		Help:        "Latency of SQL request execution",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaSQLOptFallback = metric.Metadata{
		Name:        "sql.optimizer.fallback.count",
		Help:        "Number of statements which the cost-based optimizer was unable to plan",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLOptPlanCacheHits = metric.Metadata{
		Name:        "sql.optimizer.plan_cache.hits",
		Help:        "Number of non-prepared statements for which a cached plan was used",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLOptPlanCacheMisses = metric.Metadata{
		Name:        "sql.optimizer.plan_cache.misses",
		Help:        "Number of non-prepared statements for which a cached plan was not used",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaDistSQLSelect = metric.Metadata{
		Name:        "sql.distsql.select.count",
		Help:        "Number of DistSQL SELECT statements",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaDistSQLExecLatency = metric.Metadata{
		Name:        "sql.distsql.exec.latency",
		Help:        "Latency of DistSQL statement execution",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaDistSQLServiceLatency = metric.Metadata{
		Name:        "sql.distsql.service.latency",
		Help:        "Latency of DistSQL request execution",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaTxnAbort = metric.Metadata{
		Name:        "sql.txn.abort.count",
		Help:        "Number of SQL transaction abort errors",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaFailure = metric.Metadata{
		Name:        "sql.failure.count",
		Help:        "Number of statements resulting in a planning or runtime error",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLTxnLatency = metric.Metadata{
		Name:        "sql.txn.latency",
		Help:        "Latency of SQL transactions",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaSQLTxnsOpen = metric.Metadata{
		Name:        "sql.txns.open",
		Help:        "Number of currently open user SQL transactions",
		Measurement: "Open SQL Transactions",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLActiveQueries = metric.Metadata{
		Name:        "sql.statements.active",
		Help:        "Number of currently active user SQL statements",
		Measurement: "Active Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaFullTableOrIndexScan = metric.Metadata{
		Name:        "sql.full.scan.count",
		Help:        "Number of full table or index scans",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}

	// Below are the metadata for the statement started counters.
	MetaQueryStarted = metric.Metadata{
		Name:        "sql.query.started.count",
		Help:        "Number of SQL queries started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaTxnBeginStarted = metric.Metadata{
		Name:        "sql.txn.begin.started.count",
		Help:        "Number of SQL transaction BEGIN statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaTxnCommitStarted = metric.Metadata{
		Name:        "sql.txn.commit.started.count",
		Help:        "Number of SQL transaction COMMIT statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaTxnRollbackStarted = metric.Metadata{
		Name:        "sql.txn.rollback.started.count",
		Help:        "Number of SQL transaction ROLLBACK statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLTxnContended = metric.Metadata{
		Name:        "sql.txn.contended.count",
		Help:        "Number of SQL transactions experienced contention",
		Measurement: "Contention",
		Unit:        metric.Unit_COUNT,
	}
	MetaSelectStarted = metric.Metadata{
		Name:        "sql.select.started.count",
		Help:        "Number of SQL SELECT statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaUpdateStarted = metric.Metadata{
		Name:        "sql.update.started.count",
		Help:        "Number of SQL UPDATE statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaInsertStarted = metric.Metadata{
		Name:        "sql.insert.started.count",
		Help:        "Number of SQL INSERT statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaDeleteStarted = metric.Metadata{
		Name:        "sql.delete.started.count",
		Help:        "Number of SQL DELETE statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaSavepointStarted = metric.Metadata{
		Name:        "sql.savepoint.started.count",
		Help:        "Number of SQL SAVEPOINT statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaReleaseSavepointStarted = metric.Metadata{
		Name:        "sql.savepoint.release.started.count",
		Help:        "Number of `RELEASE SAVEPOINT` statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaRollbackToSavepointStarted = metric.Metadata{
		Name:        "sql.savepoint.rollback.started.count",
		Help:        "Number of `ROLLBACK TO SAVEPOINT` statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaRestartSavepointStarted = metric.Metadata{
		Name:        "sql.restart_savepoint.started.count",
		Help:        "Number of `SAVEPOINT cockroach_restart` statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaReleaseRestartSavepointStarted = metric.Metadata{
		Name:        "sql.restart_savepoint.release.started.count",
		Help:        "Number of `RELEASE SAVEPOINT cockroach_restart` statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaRollbackToRestartSavepointStarted = metric.Metadata{
		Name:        "sql.restart_savepoint.rollback.started.count",
		Help:        "Number of `ROLLBACK TO SAVEPOINT cockroach_restart` statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaDdlStarted = metric.Metadata{
		Name:        "sql.ddl.started.count",
		Help:        "Number of SQL DDL statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaCopyStarted = metric.Metadata{
		Name:        "sql.copy.started.count",
		Help:        "Number of COPY SQL statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaCopyNonAtomicStarted = metric.Metadata{
		Name:        "sql.copy.nonatomic.started.count",
		Help:        "Number of non-atomic COPY SQL statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaMiscStarted = metric.Metadata{
		Name:        "sql.misc.started.count",
		Help:        "Number of other SQL statements started",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}

	// Below are the metadata for the statement executed counters.
	MetaQueryExecuted = metric.Metadata{
		Name:        "sql.query.count",
		Help:        "Number of SQL queries executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaTxnBeginExecuted = metric.Metadata{
		Name:        "sql.txn.begin.count",
		Help:        "Number of SQL transaction BEGIN statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaTxnCommitExecuted = metric.Metadata{
		Name:        "sql.txn.commit.count",
		Help:        "Number of SQL transaction COMMIT statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaTxnRollbackExecuted = metric.Metadata{
		Name:        "sql.txn.rollback.count",
		Help:        "Number of SQL transaction ROLLBACK statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaSelectExecuted = metric.Metadata{
		Name:        "sql.select.count",
		Help:        "Number of SQL SELECT statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaUpdateExecuted = metric.Metadata{
		Name:        "sql.update.count",
		Help:        "Number of SQL UPDATE statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaInsertExecuted = metric.Metadata{
		Name:        "sql.insert.count",
		Help:        "Number of SQL INSERT statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaDeleteExecuted = metric.Metadata{
		Name:        "sql.delete.count",
		Help:        "Number of SQL DELETE statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaSavepointExecuted = metric.Metadata{
		Name:        "sql.savepoint.count",
		Help:        "Number of SQL SAVEPOINT statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaReleaseSavepointExecuted = metric.Metadata{
		Name:        "sql.savepoint.release.count",
		Help:        "Number of `RELEASE SAVEPOINT` statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaRollbackToSavepointExecuted = metric.Metadata{
		Name:        "sql.savepoint.rollback.count",
		Help:        "Number of `ROLLBACK TO SAVEPOINT` statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaRestartSavepointExecuted = metric.Metadata{
		Name:        "sql.restart_savepoint.count",
		Help:        "Number of `SAVEPOINT cockroach_restart` statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaReleaseRestartSavepointExecuted = metric.Metadata{
		Name:        "sql.restart_savepoint.release.count",
		Help:        "Number of `RELEASE SAVEPOINT cockroach_restart` statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaRollbackToRestartSavepointExecuted = metric.Metadata{
		Name:        "sql.restart_savepoint.rollback.count",
		Help:        "Number of `ROLLBACK TO SAVEPOINT cockroach_restart` statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaDdlExecuted = metric.Metadata{
		Name:        "sql.ddl.count",
		Help:        "Number of SQL DDL statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaCopyExecuted = metric.Metadata{
		Name:        "sql.copy.count",
		Help:        "Number of COPY SQL statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaCopyNonAtomicExecuted = metric.Metadata{
		Name:        "sql.copy.nonatomic.count",
		Help:        "Number of non-atomic COPY SQL statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaMiscExecuted = metric.Metadata{
		Name:        "sql.misc.count",
		Help:        "Number of other SQL statements successfully executed",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLStatsMemMaxBytes = metric.Metadata{
		Name:        "sql.stats.mem.max",
		Help:        "Memory usage for fingerprint storage",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	MetaSQLStatsMemCurBytes = metric.Metadata{
		Name:        "sql.stats.mem.current",
		Help:        "Current memory usage for fingerprint storage",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	MetaReportedSQLStatsMemMaxBytes = metric.Metadata{
		Name:        "sql.stats.reported.mem.max",
		Help:        "Memory usage for reported fingerprint storage",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	MetaReportedSQLStatsMemCurBytes = metric.Metadata{
		Name:        "sql.stats.reported.mem.current",
		Help:        "Current memory usage for reported fingerprint storage",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	MetaDiscardedSQLStats = metric.Metadata{
		Name:        "sql.stats.discarded.current",
		Help:        "Number of fingerprint statistics being discarded",
		Measurement: "Discarded SQL Stats",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLStatsFlushStarted = metric.Metadata{
		Name:        "sql.stats.flush.count",
		Help:        "Number of times SQL Stats are flushed to persistent storage",
		Measurement: "SQL Stats Flush",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLStatsFlushFailure = metric.Metadata{
		Name:        "sql.stats.flush.error",
		Help:        "Number of errors encountered when flushing SQL Stats",
		Measurement: "SQL Stats Flush",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLStatsFlushDuration = metric.Metadata{
		Name:        "sql.stats.flush.duration",
		Help:        "Time took to in nanoseconds to complete SQL Stats flush",
		Measurement: "SQL Stats Flush",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaSQLStatsRemovedRows = metric.Metadata{
		Name:        "sql.stats.cleanup.rows_removed",
		Help:        "Number of stale statistics rows that are removed",
		Measurement: "SQL Stats Cleanup",
		Unit:        metric.Unit_COUNT,
	}
	MetaSQLTxnStatsCollectionOverhead = metric.Metadata{
		Name:        "sql.stats.txn_stats_collection.duration",
		Help:        "Time took in nanoseconds to collect transaction stats",
		Measurement: "SQL Transaction Stats Collection Overhead",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaTxnRowsWrittenLog = metric.Metadata{
		Name:        "sql.guardrails.transaction_rows_written_log.count",
		Help:        "Number of transactions logged because of transaction_rows_written_log guardrail",
		Measurement: "Logged transactions",
		Unit:        metric.Unit_COUNT,
	}
	MetaTxnRowsWrittenErr = metric.Metadata{
		Name:        "sql.guardrails.transaction_rows_written_err.count",
		Help:        "Number of transactions errored because of transaction_rows_written_err guardrail",
		Measurement: "Errored transactions",
		Unit:        metric.Unit_COUNT,
	}
	MetaTxnRowsReadLog = metric.Metadata{
		Name:        "sql.guardrails.transaction_rows_read_log.count",
		Help:        "Number of transactions logged because of transaction_rows_read_log guardrail",
		Measurement: "Logged transactions",
		Unit:        metric.Unit_COUNT,
	}
	MetaTxnRowsReadErr = metric.Metadata{
		Name:        "sql.guardrails.transaction_rows_read_err.count",
		Help:        "Number of transactions errored because of transaction_rows_read_err guardrail",
		Measurement: "Errored transactions",
		Unit:        metric.Unit_COUNT,
	}
	MetaFullTableOrIndexScanRejected = metric.Metadata{
		Name:        "sql.guardrails.full_scan_rejected.count",
		Help:        "Number of full table or index scans that have been rejected because of `disallow_full_table_scans` guardrail",
		Measurement: "SQL Statements",
		Unit:        metric.Unit_COUNT,
	}
)

func getMetricMeta(meta metric.Metadata, internal bool) metric.Metadata {
	if internal {
		meta.Name += ".internal"
		meta.Help += " (internal queries)"
		meta.Measurement = "SQL Internal Statements"
	}
	return meta
}

// NodeInfo contains metadata about the executing node and cluster.
type NodeInfo struct {
	// LogicalClusterID is the cluster ID of the tenant, unique per
	// tenant.
	LogicalClusterID func() uuid.UUID
	// NodeID is either the SQL instance ID or node ID, depending on
	// circumstances.
	// TODO(knz): Split this across node ID and instance ID. Likely,
	// the SQL layer only needs instance ID.
	NodeID *base.SQLIDContainer
	// AdminURL is the URL of the DB Console for this server.
	AdminURL func() *url.URL
	// PGURL is the SQL connection URL for this server.
	PGURL func(*url.Userinfo) (*pgurl.URL, error)
}

// nodeStatusGenerator is a limited portion of the status.MetricsRecorder
// struct, to avoid having to import all of status in sql.
type nodeStatusGenerator interface {
	GenerateNodeStatus(ctx context.Context) *statuspb.NodeStatus
}

// SystemTenantOnly wraps an object in the ExecutorConfig that is only
// available when accessed by the system tenant.
type SystemTenantOnly[T any] interface {
	// Get returns either the wrapped object if accessed by the system tenant
	// or an error if accessed by a secondary tenant.
	Get(op string) (t T, err error)
}

type systemTenantOnly[T any] struct {
	wrapped T
}

// Get implements SystemTenantOnly.
func (s *systemTenantOnly[T]) Get(string) (t T, err error) {
	return s.wrapped, nil
}

// MakeSystemTenantOnly returns a SystemTenantOnly where SystemTenantOnly.Get
// returns t.
func MakeSystemTenantOnly[T any](t T) SystemTenantOnly[T] {
	return &systemTenantOnly[T]{wrapped: t}
}

type emptySystemTenantOnly[T any] struct{}

// Get implements SystemTenantOnly.
func (emptySystemTenantOnly[T]) Get(op string) (t T, err error) {
	err = errors.Newf("operation %s supported only by system tenant", op)
	return
}

var empty = &emptySystemTenantOnly[any]{}

// EmptySystemTenantOnly returns a SystemTenantOnly where SystemTenantOnly.Get
// returns an error.
func EmptySystemTenantOnly[T any]() SystemTenantOnly[T] {
	return (*emptySystemTenantOnly[T])(empty)
}

// An ExecutorConfig encompasses the auxiliary objects and configuration
// required to create an executor.
// All fields holding a pointer or an interface are required to create
// an Executor; the rest will have sane defaults set if omitted.
type ExecutorConfig struct {
	Settings          *cluster.Settings
	NodeInfo          NodeInfo
	Codec             keys.SQLCodec
	DefaultZoneConfig *zonepb.ZoneConfig
	Locality          roachpb.Locality
	AmbientCtx        log.AmbientContext
	DB                *kv.DB
	Gossip            gossip.OptionalGossip
	NodeLiveness      optionalnodeliveness.Container
	SystemConfig      config.SystemConfigProvider
	DistSender        *kvcoord.DistSender
	RPCContext        *rpc.Context
	LeaseManager      *lease.Manager
	Clock             *hlc.Clock
	DistSQLSrv        *distsql.ServerImpl
	// NodesStatusServer gives access to the NodesStatus service and is only
	// available when running as a system tenant.
	NodesStatusServer serverpb.OptionalNodesStatusServer
	// SQLStatusServer gives access to a subset of the Status service and is
	// available when not running as a system tenant.
	SQLStatusServer    serverpb.SQLStatusServer
	TenantStatusServer serverpb.TenantStatusServer
	MetricsRecorder    nodeStatusGenerator
	SessionRegistry    *SessionRegistry
	ClosedSessionCache *ClosedSessionCache
	SQLLiveness        sqlliveness.Provider
	JobRegistry        *jobs.Registry
	VirtualSchemas     *VirtualSchemaHolder
	DistSQLPlanner     *DistSQLPlanner
	TableStatsCache    *stats.TableStatisticsCache
	StatsRefresher     *stats.Refresher
	QueryCache         *querycache.C

	SchemaChangerMetrics *SchemaChangerMetrics
	FeatureFlagMetrics   *featureflag.DenialMetrics
	RowMetrics           *rowinfra.Metrics
	InternalRowMetrics   *rowinfra.Metrics

	TestingKnobs                         ExecutorTestingKnobs
	UpgradeTestingKnobs                  *upgradebase.TestingKnobs
	PGWireTestingKnobs                   *PGWireTestingKnobs
	SchemaChangerTestingKnobs            *SchemaChangerTestingKnobs
	DeclarativeSchemaChangerTestingKnobs *scexec.TestingKnobs
	TypeSchemaChangerTestingKnobs        *TypeSchemaChangerTestingKnobs
	GCJobTestingKnobs                    *GCJobTestingKnobs
	DistSQLRunTestingKnobs               *execinfra.TestingKnobs
	EvalContextTestingKnobs              eval.TestingKnobs
	TenantTestingKnobs                   *TenantTestingKnobs
	TTLTestingKnobs                      *TTLTestingKnobs
	SchemaTelemetryTestingKnobs          *SchemaTelemetryTestingKnobs
	BackupRestoreTestingKnobs            *BackupRestoreTestingKnobs
	StreamingTestingKnobs                *StreamingTestingKnobs
	SQLStatsTestingKnobs                 *sqlstats.TestingKnobs
	TelemetryLoggingTestingKnobs         *TelemetryLoggingTestingKnobs
	SpanConfigTestingKnobs               *spanconfig.TestingKnobs
	CaptureIndexUsageStatsKnobs          *scheduledlogging.CaptureIndexUsageStatsTestingKnobs
	UnusedIndexRecommendationsKnobs      *idxusage.UnusedIndexRecommendationTestingKnobs
	ExternalConnectionTestingKnobs       *externalconn.TestingKnobs
	EventLogTestingKnobs                 *EventLogTestingKnobs

	// HistogramWindowInterval is (server.Config).HistogramWindowInterval.
	HistogramWindowInterval time.Duration

	// RangeDescriptorCache is updated by DistSQL when it finds out about
	// misplanned spans.
	RangeDescriptorCache *rangecache.RangeCache

	// Role membership cache.
	RoleMemberCache *MembershipCache

	// Client cert expiration cache.
	ClientCertExpirationCache *security.ClientCertExpirationCache

	// SessionInitCache cache; contains information used during authentication
	// and per-role default settings.
	SessionInitCache *sessioninit.Cache

	// ProtectedTimestampProvider encapsulates the protected timestamp subsystem.
	ProtectedTimestampProvider protectedts.Provider

	// StmtDiagnosticsRecorder deals with recording statement diagnostics.
	StmtDiagnosticsRecorder *stmtdiagnostics.Registry

	ExternalIODirConfig base.ExternalIODirConfig

	GCJobNotifier *gcjobnotifier.Notifier

	RangeFeedFactory *rangefeed.Factory

	// VersionUpgradeHook is called after validating a `SET CLUSTER SETTING
	// version` but before executing it. It can carry out arbitrary upgrades
	// that allow us to eventually remove legacy code.
	VersionUpgradeHook VersionUpgradeHook

	// UpgradeJobDeps is used to drive upgrades.
	UpgradeJobDeps upgrade.JobDeps

	// IndexBackfiller is used to backfill indexes. It is another rather circular
	// object which mostly just holds on to an ExecConfig.
	IndexBackfiller *IndexBackfillPlanner

	// IndexMerger is also used to backfill indexes and is also rather circular.
	IndexMerger *IndexBackfillerMergePlanner

	// IndexSpanSplitter is used to split and scatter indexes before backfill.
	IndexSpanSplitter scexec.IndexSpanSplitter

	// Validator is used to validate indexes and check constraints.
	Validator scexec.Validator

	// ProtectedTimestampManager provides protected timestamp
	// for jobs only installed after some percentage of the GC interval passes.
	ProtectedTimestampManager scexec.ProtectedTimestampManager

	// ContentionRegistry is a node-level registry of contention events used for
	// contention observability.
	ContentionRegistry *contention.Registry

	// RootMemoryMonitor is the root memory monitor of the entire server. Do not
	// use this for normal purposes. It is to be used to establish any new
	// root-level memory accounts that are not related to a user session.
	RootMemoryMonitor *mon.BytesMonitor

	// CompactEngineSpanFunc is used to inform a storage engine of the need to
	// perform compaction over a key span.
	CompactEngineSpanFunc eval.CompactEngineSpanFunc

	// CompactionConcurrencyFunc is used to inform a storage engine to change its
	// compaction concurrency.
	CompactionConcurrencyFunc eval.SetCompactionConcurrencyFunc

	// GetTableMetricsFunc is used to gather information about sstables that
	// overlap with a key range for a specified node and store.
	GetTableMetricsFunc eval.GetTableMetricsFunc

	// TraceCollector is used to contact all live nodes in the cluster, and
	// collect trace spans from their inflight node registries.
	TraceCollector *collector.TraceCollector

	// TenantUsageServer is used to implement configuration APIs for tenant cost
	// control.
	TenantUsageServer multitenant.TenantUsageServer

	// KVStoresIterator is used by various crdb_internal builtins to directly
	// access stores on this node.
	KVStoresIterator kvserverbase.StoresIterator

	// InspectzServer is used to power various crdb_internal vtables, exposing
	// the equivalent of /inspectz but through SQL.
	InspectzServer inspectzpb.InspectzServer

	// RangeDescIteratorFactory is used to construct Iterators over range
	// descriptors.
	RangeDescIteratorFactory rangedesc.IteratorFactory

	// CollectionFactory is used to construct a descs.Collection.
	CollectionFactory *descs.CollectionFactory

	// SystemTableIDResolver is used to obtain dynamic IDs for system tables.
	SystemTableIDResolver catalog.SystemTableIDResolver

	// SpanConfigReconciler is used to drive the span config reconciliation job
	// and related upgrades.
	SpanConfigReconciler spanconfig.Reconciler

	// SpanConfigSplitter is used during upgrades to seed system.span_count with
	// the right number of tenant spans.
	SpanConfigSplitter spanconfig.Splitter

	// SpanConfigLimiter is used to limit how many span configs installed.
	SpanConfigLimiter spanconfig.Limiter

	// SpanConfigKVAccessor is used when creating and deleting tenant
	// records.
	SpanConfigKVAccessor spanconfig.KVAccessor

	// InternalDB is used to create an isql.Executor bound with SessionData and
	// other ExtraTxnState.
	InternalDB *InternalDB

	// SpanStatsConsumer is used by the key visualizer job.
	SpanStatsConsumer keyvisualizer.SpanStatsConsumer

	// ConsistencyChecker is to generate the results in calls to
	// crdb_internal.check_consistency.
	ConsistencyChecker eval.ConsistencyCheckRunner

	// RangeProber is used in calls to crdb_internal.probe_ranges.
	RangeProber eval.RangeProber

	// DescIDGenerator generates unique descriptor IDs.
	DescIDGenerator eval.DescIDGenerator

	// SyntheticPrivilegeCache stores synthetic privileges in an in-memory cache.
	SyntheticPrivilegeCache *syntheticprivilegecache.Cache

	// RangeStatsFetcher is used to fetch RangeStats.
	RangeStatsFetcher eval.RangeStatsFetcher

	// EventsExporter is the client for the Observability Service.
	EventsExporter obs.EventsExporterInterface

	// NodeDescs stores {Store,Node}Descriptors in an in-memory cache.
	NodeDescs kvcoord.NodeDescStore

	TenantCapabilitiesReader SystemTenantOnly[tenantcapabilities.Reader]

	// AutoConfigProvider informs the auto config runner job of new
	// tasks to run.
	AutoConfigProvider acprovider.Provider
}

// UpdateVersionSystemSettingHook provides a callback that allows us
// update the cluster version inside the system.settings table. This hook
// is aimed at mainly updating tenant pods, which will currently skip over
// the existing upgrade logic for bumping version numbers (this logic is
// stubbed out for them). As a result there is a potential danger of upgrades
// partially being completed without the version number being persisted to storage
// for tenants. This hook allows the version number to bumped and saved at
// each step.
type UpdateVersionSystemSettingHook func(
	ctx context.Context,
	version clusterversion.ClusterVersion,
	validate func(ctx context.Context) error,
) error

// VersionUpgradeHook is used to run upgrades starting in v21.1.
type VersionUpgradeHook func(
	ctx context.Context,
	user username.SQLUsername,
	from, to clusterversion.ClusterVersion,
	updateSystemVersionSetting UpdateVersionSystemSettingHook,
) error

// Organization returns the value of cluster.organization.
func (cfg *ExecutorConfig) Organization() string {
	return ClusterOrganization.Get(&cfg.Settings.SV)
}

// GetFeatureFlagMetrics returns the value of the FeatureFlagMetrics struct.
func (cfg *ExecutorConfig) GetFeatureFlagMetrics() *featureflag.DenialMetrics {
	return cfg.FeatureFlagMetrics
}

// GenerateID generates a unique ID based on the SQL instance ID and its current
// HLC timestamp. These IDs are either scoped at the query level or at the
// session level.
func (cfg *ExecutorConfig) GenerateID() clusterunique.ID {
	return clusterunique.GenerateID(cfg.Clock.Now(), cfg.NodeInfo.NodeID.SQLInstanceID())
}

// SV returns the setting values.
func (cfg *ExecutorConfig) SV() *settings.Values {
	return &cfg.Settings.SV
}

func (cfg *ExecutorConfig) JobsKnobs() *jobs.TestingKnobs {
	knobs, _ := cfg.DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs)
	return knobs
}

var _ base.ModuleTestingKnobs = &ExecutorTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*ExecutorTestingKnobs) ModuleTestingKnobs() {}

// StatementFilter is the type of callback that
// ExecutorTestingKnobs.StatementFilter takes.
type StatementFilter func(context.Context, *sessiondata.SessionData, string, error)

// ExecutorTestingKnobs is part of the context used to control parts of the
// system during testing.
type ExecutorTestingKnobs struct {
	// StatementFilter can be used to trap execution of SQL statements and
	// optionally change their results. The filter function is invoked after each
	// statement has been executed.
	StatementFilter StatementFilter

	// BeforePrepare can be used to trap execution of SQL statement preparation.
	// If a nil error is returned, planning continues as usual.
	BeforePrepare func(ctx context.Context, stmt string, txn *kv.Txn) error

	// BeforeExecute is called by the Executor before plan execution. It is useful
	// for synchronizing statement execution.
	BeforeExecute func(ctx context.Context, stmt string, descriptors *descs.Collection)

	// AfterExecute is like StatementFilter, but it runs in the same goroutine of the
	// statement.
	AfterExecute func(ctx context.Context, stmt string, err error)

	// AfterExecCmd is called after successful execution of any command.
	AfterExecCmd func(ctx context.Context, cmd Command, buf *StmtBuf)

	// BeforeRestart is called before a transaction restarts.
	BeforeRestart func(ctx context.Context, reason error)

	// DisableAutoCommitDuringExec, if set, disables the auto-commit functionality
	// of some SQL statements. That functionality allows some statements to commit
	// directly when they're executed in an implicit SQL txn, without waiting for
	// the Executor to commit the implicit txn.
	// This has to be set in tests that need to abort such statements using a
	// StatementFilter; otherwise, the statement commits at the same time as
	// execution so there'll be nothing left to abort by the time the filter runs.
	DisableAutoCommitDuringExec bool

	// BeforeAutoCommit is called when the Executor is about to commit the KV
	// transaction after running a statement in an implicit transaction, allowing
	// tests to inject errors into that commit.
	// If an error is returned, that error will be considered the result of
	// txn.Commit(), and the txn.Commit() call will not actually be
	// made. If no error is returned, txn.Commit() is called normally.
	//
	// Note that this is not called if the SQL statement representing the implicit
	// transaction has committed the KV txn itself (e.g. if it used the 1-PC
	// optimization). This is only called when the Executor is the one doing the
	// committing.
	BeforeAutoCommit func(ctx context.Context, stmt string) error

	// DisableTempObjectsCleanupOnSessionExit disables cleaning up temporary schemas
	// and tables when a session is closed.
	DisableTempObjectsCleanupOnSessionExit bool
	// TempObjectsCleanupCh replaces the time.Ticker.C channel used for scheduling
	// a cleanup on every temp object in the cluster. If this is set, the job
	// will now trigger when items come into this channel.
	TempObjectsCleanupCh chan time.Time
	// OnTempObjectsCleanupDone will trigger when the temporary objects cleanup
	// job is done.
	OnTempObjectsCleanupDone func()

	// WithStatementTrace is called after the statement is executed in
	// execStmtInOpenState.
	WithStatementTrace func(trace tracingpb.Recording, stmt string)

	// RunAfterSCJobsCacheLookup is called after the SchemaChangeJobCache is checked for
	// a given table id.
	RunAfterSCJobsCacheLookup func(record *jobs.Record)

	// TestingSaveFlows, if set, will be called with the given stmt. The resulting
	// function will be called with the physical plan of that statement's main
	// query (i.e. no subqueries). The physical plan is only safe for use for the
	// lifetime of this function. Note that returning a nil function is
	// unsupported and will lead to a panic.
	TestingSaveFlows func(stmt string) func(map[base.SQLInstanceID]*execinfrapb.FlowSpec, execopnode.OpChains, bool) error

	// DeterministicExplain, if set, will result in overriding fields in EXPLAIN
	// and EXPLAIN ANALYZE that can vary between runs (like elapsed times).
	//
	// TODO(radu): this flag affects EXPLAIN and EXPLAIN ANALYZE differently. It
	// hides the vectorization, distribution, and cluster nodes in EXPLAIN ANALYZE
	// but not in EXPLAIN. This is just a consequence of how the tests we have are
	// written. We should replace this knob with a session setting that allows
	// exact control of the redaction flags (and have each test set it as
	// necessary).
	DeterministicExplain bool

	// ForceRealTracingSpans, if set, forces the use of real (i.e. not no-op)
	// tracing spans for every statement.
	ForceRealTracingSpans bool

	// DistSQLReceiverPushCallbackFactory, if set, will be called every time a
	// DistSQLReceiver is created for a new query execution, and it should
	// return, possibly nil, a callback that will be called every time
	// DistSQLReceiver.Push or DistSQLReceiver.PushBatch is called.
	DistSQLReceiverPushCallbackFactory func(query string) func(rowenc.EncDatumRow, coldata.Batch, *execinfrapb.ProducerMetadata)

	// OnTxnRetry, if set, will be called if there is a transaction retry.
	OnTxnRetry func(autoRetryReason error, evalCtx *eval.Context)

	// BeforeTxnStatsRecorded, if set, will be called before the statistics
	// of a transaction is being recorded.
	BeforeTxnStatsRecorded func(
		sessionData *sessiondata.SessionData,
		txnID uuid.UUID,
		txnFingerprintID appstatspb.TransactionFingerprintID,
	)

	// AfterBackupCheckpoint if set will be called after a BACKUP-CHECKPOINT
	// is written.
	AfterBackupCheckpoint func()

	// OnRecordTxnFinish, if set, will be called as we record a transaction
	// finishing.
	OnRecordTxnFinish func(isInternal bool, phaseTimes *sessionphase.Times, stmt string)

	// UseTransactionDescIDGenerator is used to force descriptor ID generation
	// to use a transaction, and, in doing so, more deterministically allocate
	// descriptor IDs at the cost of decreased parallelism.
	UseTransactionalDescIDGenerator bool

	// BeforeCopyFromInsert, if set, will be called during a COPY FROM insert statement.
	BeforeCopyFromInsert func() error

	// CopyFromInsertRetry, if set, will be called when a COPY FROM insert statement is retried.
	CopyFromInsertRetry func() error

	// ForceSQLLivenessSession will force the use of a sqlliveness session for
	// transaction deadlines even in the system tenant.
	ForceSQLLivenessSession bool
}

// PGWireTestingKnobs contains knobs for the pgwire module.
type PGWireTestingKnobs struct {
	// CatchPanics causes the pgwire.conn to recover from panics in its execution
	// thread and return them as errors to the client, closing the connection
	// afterward.
	CatchPanics bool

	// AuthHook is used to override the normal authentication handling on new
	// connections.
	AuthHook func(context.Context) error

	// AfterReadMsgTestingKnob is called after reading a message from the
	// pgwire read buffer.
	AfterReadMsgTestingKnob func(context.Context) error
}

var _ base.ModuleTestingKnobs = &PGWireTestingKnobs{}

// ModuleTestingKnobs implements the base.ModuleTestingKnobs interface.
func (*PGWireTestingKnobs) ModuleTestingKnobs() {}

// TenantTestingKnobs contains knobs for tenant behavior.
type TenantTestingKnobs struct {
	// TenantIDCodecOverride overrides the tenant ID used to construct the SQL
	// server's codec, but nothing else (e.g. its certs).
	TenantIDCodecOverride roachpb.TenantID

	// OverrideTokenBucketProvider allows a test-only TokenBucketProvider (which
	// can optionally forward requests to the real provider).
	OverrideTokenBucketProvider func(origProvider kvtenant.TokenBucketProvider) kvtenant.TokenBucketProvider

	// BeforeCheckingForDescriptorIDSequence, if set, is called before
	// the connExecutor checks for the presence of system.descriptor_id_seq after
	// handling a system tenant descriptor ID generator migration error.
	BeforeCheckingForDescriptorIDSequence func(ctx context.Context)

	// EnableTenantIDReuse avoids using the tenant ID sequence to ensure
	// that automatic tenant ID allocation is always monotonic. This can
	// be used in tests that wish to control the tenant ID.
	EnableTenantIDReuse bool
}

var _ base.ModuleTestingKnobs = &TenantTestingKnobs{}

// ModuleTestingKnobs implements the base.ModuleTestingKnobs interface.
func (*TenantTestingKnobs) ModuleTestingKnobs() {}

// TTLTestingKnobs contains testing knobs for TTL deletion.
type TTLTestingKnobs struct {
	// AOSTDuration changes the AOST timestamp duration to add to the
	// current time.
	AOSTDuration *time.Duration
	// ExpectedNumSpanPartitions causes the TTL job to fail if it does not match
	// the number of DistSQL processors.
	ExpectedNumSpanPartitions int
	// ReturnStatsError causes stats errors to be returned instead of logged as
	// warnings.
	ReturnStatsError bool
	// PreDeleteChangeTableVersion is a flag to change the table descriptor
	// during a delete.
	PreDeleteChangeTableVersion bool
	// PreSelectStatement runs before the start of the TTL select-delete
	// loop.
	PreSelectStatement string
}

// ModuleTestingKnobs implements the base.ModuleTestingKnobs interface.
func (*TTLTestingKnobs) ModuleTestingKnobs() {}

// SchemaTelemetryTestingKnobs contains testing knobs for schema telemetry.
type SchemaTelemetryTestingKnobs struct {
	// AOSTDuration changes the AOST timestamp duration to add to the
	// current time.
	AOSTDuration *time.Duration
}

// ModuleTestingKnobs implements the base.ModuleTestingKnobs interface.
func (*SchemaTelemetryTestingKnobs) ModuleTestingKnobs() {}

// BackupRestoreTestingKnobs contains knobs for backup and restore behavior.
type BackupRestoreTestingKnobs struct {
	// CaptureResolvedTableDescSpans allows for intercepting the spans which are
	// resolved during backup planning, and will eventually be backed up during
	// execution.
	CaptureResolvedTableDescSpans func([]roachpb.Span)

	// RunAfterSplitAndScatteringEntry allows blocking the RESTORE job after a
	// single RestoreSpanEntry has been split and scattered.
	RunAfterSplitAndScatteringEntry func(ctx context.Context)

	// RunAfterProcessingRestoreSpanEntry allows blocking the RESTORE job after a
	// single RestoreSpanEntry has been processed and added to the SSTBatcher.
	RunAfterProcessingRestoreSpanEntry func(ctx context.Context, entry *execinfrapb.RestoreSpanEntry)

	// RunAfterExportingSpanEntry allows blocking the BACKUP job after a single
	// span has been exported.
	RunAfterExportingSpanEntry func(ctx context.Context, response *kvpb.ExportResponse)

	// BackupMonitor is used to overwrite the monitor used by backup during
	// testing. This is typically the bulk mem monitor if not
	// specified here.
	BackupMemMonitor *mon.BytesMonitor
}

var _ base.ModuleTestingKnobs = &BackupRestoreTestingKnobs{}

// ModuleTestingKnobs implements the base.ModuleTestingKnobs interface.
func (*BackupRestoreTestingKnobs) ModuleTestingKnobs() {}

// StreamingTestingKnobs contains knobs for streaming behavior.
type StreamingTestingKnobs struct {
	// RunAfterReceivingEvent allows blocking the stream ingestion processor after
	// a single event has been received.
	RunAfterReceivingEvent func(ctx context.Context) error

	// BeforeClientSubscribe allows observation of parameters about to be passed
	// to a streaming client
	BeforeClientSubscribe func(addr string, token string, startTime hlc.Timestamp)

	// BeforeIngestionStart allows blocking the stream ingestion job
	// before a stream ingestion happens.
	BeforeIngestionStart func(ctx context.Context) error

	// AfterReplicationFlowPlan allows the caller to inspect the ingestion and
	// frontier specs generated for the replication job.
	AfterReplicationFlowPlan func([]*execinfrapb.StreamIngestionDataSpec,
		*execinfrapb.StreamIngestionFrontierSpec)

	// OverrideRevertRangeBatchSize allows overriding the `MaxSpanRequestKeys`
	// used when sending a RevertRange request.
	OverrideRevertRangeBatchSize int64

	// AfterCutoverStarted allows blocking after the cutover has started.
	AfterCutoverStarted func()

	// OnCutoverProgressUpdate is called on every progress update
	// call during the cutover process.
	OnCutoverProgressUpdate func(remainingSpans roachpb.Spans)

	// CutoverProgressShouldUpdate overrides the standard logic
	// for whether the job record is updated on a progress update.
	CutoverProgressShouldUpdate func() bool

	DistSQLRetryPolicy *retry.Options

	AfterRetryIteration func(err error)
}

var _ base.ModuleTestingKnobs = &StreamingTestingKnobs{}

// ModuleTestingKnobs implements the base.ModuleTestingKnobs interface.
func (*StreamingTestingKnobs) ModuleTestingKnobs() {}

func shouldDistributeGivenRecAndMode(
	rec distRecommendation, mode sessiondatapb.DistSQLExecMode,
) bool {
	switch mode {
	case sessiondatapb.DistSQLOff:
		return false
	case sessiondatapb.DistSQLAuto:
		return rec == shouldDistribute
	case sessiondatapb.DistSQLOn, sessiondatapb.DistSQLAlways:
		return rec != cannotDistribute
	}
	panic(errors.AssertionFailedf("unhandled distsql mode %v", mode))
}

// getPlanDistribution returns the PlanDistribution that plan will have. If
// plan already has physical representation, then the stored PlanDistribution
// is reused, but if plan has logical representation (i.e. it is a planNode
// tree), then we traverse that tree in order to determine the distribution of
// the plan.
// WARNING: in some cases when this method returns
// physicalplan.FullyDistributedPlan, the plan might actually run locally. This
// is the case when
// - the plan ends up with a single flow on the gateway, or
// - during the plan finalization (in DistSQLPlanner.finalizePlanWithRowCount)
// we decide that it is beneficial to move the single flow of the plan from the
// remote node to the gateway.
// TODO(yuzefovich): this will be easy to solve once the DistSQL spec factory is
// completed but is quite annoying to do at the moment.
func getPlanDistribution(
	ctx context.Context,
	txnHasUncommittedTypes bool,
	distSQLMode sessiondatapb.DistSQLExecMode,
	plan planMaybePhysical,
) physicalplan.PlanDistribution {
	if plan.isPhysicalPlan() {
		return plan.physPlan.Distribution
	}

	// If this transaction has modified or created any types, it is not safe to
	// distribute due to limitations around leasing descriptors modified in the
	// current transaction.
	if txnHasUncommittedTypes {
		return physicalplan.LocalPlan
	}

	if distSQLMode == sessiondatapb.DistSQLOff {
		return physicalplan.LocalPlan
	}

	// Don't try to run empty nodes (e.g. SET commands) with distSQL.
	if _, ok := plan.planNode.(*zeroNode); ok {
		return physicalplan.LocalPlan
	}

	rec, err := checkSupportForPlanNode(plan.planNode)
	if err != nil {
		// Don't use distSQL for this request.
		log.VEventf(ctx, 1, "query not supported for distSQL: %s", err)
		return physicalplan.LocalPlan
	}

	if shouldDistributeGivenRecAndMode(rec, distSQLMode) {
		return physicalplan.FullyDistributedPlan
	}
	return physicalplan.LocalPlan
}

// golangFillQueryArguments transforms Go values into datums.
// Some of the args can be datums (in which case the transformation is a no-op).
//
// TODO: This does not support arguments of the SQL 'Date' type, as there is not
// an equivalent type in Go's standard library. It's not currently needed by any
// of our internal tables.
func golangFillQueryArguments(args ...interface{}) (tree.Datums, error) {
	res := make(tree.Datums, len(args))
	for i, arg := range args {
		if arg == nil {
			res[i] = tree.DNull
			continue
		}

		// A type switch to handle a few explicit types with special semantics:
		// - Datums are passed along as is.
		// - Time datatypes get special representation in the database.
		// - Usernames are assumed pre-normalized for lookup and validation.
		var d tree.Datum
		switch t := arg.(type) {
		case tree.Datum:
			d = t
		case time.Time:
			var err error
			d, err = tree.MakeDTimestamp(t, time.Microsecond)
			if err != nil {
				return nil, err
			}
		case time.Duration:
			d = &tree.DInterval{Duration: duration.MakeDuration(t.Nanoseconds(), 0, 0)}
		case bitarray.BitArray:
			d = &tree.DBitArray{BitArray: t}
		case *apd.Decimal:
			dd := &tree.DDecimal{}
			dd.Set(t)
			d = dd
		case username.SQLUsername:
			d = tree.NewDString(t.Normalized())
		}
		if d == nil {
			// Handle all types which have an underlying type that can be stored in the
			// database.
			// Note: if this reflection becomes a performance concern in the future,
			// commonly used types could be added explicitly into the type switch above
			// for a performance gain.
			val := reflect.ValueOf(arg)
			switch val.Kind() {
			case reflect.Bool:
				d = tree.MakeDBool(tree.DBool(val.Bool()))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				d = tree.NewDInt(tree.DInt(val.Int()))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				d = tree.NewDInt(tree.DInt(val.Uint()))
			case reflect.Float32, reflect.Float64:
				d = tree.NewDFloat(tree.DFloat(val.Float()))
			case reflect.String:
				d = tree.NewDString(val.String())
			case reflect.Slice:
				switch {
				case val.IsNil():
					d = tree.DNull
				case val.Type().Elem().Kind() == reflect.String:
					a := tree.NewDArray(types.String)
					for v := 0; v < val.Len(); v++ {
						if err := a.Append(tree.NewDString(val.Index(v).String())); err != nil {
							return nil, err
						}
					}
					d = a
				case val.Type().Elem().Kind() == reflect.Int:
					a := tree.NewDArray(types.Int)
					for v := 0; v < val.Len(); v++ {
						if err := a.Append(tree.NewDInt(tree.DInt(val.Index(v).Int()))); err != nil {
							return nil, err
						}
					}
					d = a
				case val.Type().Elem().Kind() == reflect.Uint8:
					d = tree.NewDBytes(tree.DBytes(val.Bytes()))
				}
			}
			if d == nil {
				panic(errors.AssertionFailedf("unexpected type %T", arg))
			}
		}
		res[i] = d
	}
	return res, nil
}

// checkResultType verifies that a table result can be returned to the
// client.
func checkResultType(typ *types.T) error {
	// Compare all types that can rely on == equality.
	switch typ.Family() {
	case types.UnknownFamily:
	case types.BitFamily:
	case types.BoolFamily:
	case types.IntFamily:
	case types.FloatFamily:
	case types.DecimalFamily:
	case types.BytesFamily:
	case types.Box2DFamily:
	case types.GeographyFamily:
	case types.GeometryFamily:
	case types.StringFamily:
	case types.CollatedStringFamily:
	case types.DateFamily:
	case types.TimestampFamily:
	case types.TimeFamily:
	case types.TimeTZFamily:
	case types.TimestampTZFamily:
	case types.TSQueryFamily:
	case types.TSVectorFamily:
	case types.IntervalFamily:
	case types.JsonFamily:
	case types.UuidFamily:
	case types.INetFamily:
	case types.OidFamily:
	case types.PGLSNFamily:
	case types.TupleFamily:
	case types.EnumFamily:
	case types.VoidFamily:
	case types.ArrayFamily:
		if typ.ArrayContents().Family() == types.ArrayFamily {
			// Technically we could probably return arrays of arrays to a
			// client (the encoding exists) but we don't want to give
			// mixed signals -- that nested arrays appear to be supported
			// in this case, and not in other cases (eg. CREATE). So we
			// reject them in every case instead.
			return unimplemented.NewWithIssueDetail(32552,
				"result", "arrays cannot have arrays as element type")
		}
	case types.AnyFamily:
		// Placeholder case.
		return errors.Errorf("could not determine data type of %s", typ)
	default:
		return errors.Errorf("unsupported result type: %s", typ)
	}
	return nil
}

// EvalAsOfTimestamp evaluates and returns the timestamp from an AS OF SYSTEM
// TIME clause.
func (p *planner) EvalAsOfTimestamp(
	ctx context.Context, asOfClause tree.AsOfClause, opts ...asof.EvalOption,
) (eval.AsOfSystemTime, error) {
	asOf, err := asof.Eval(ctx, asOfClause, &p.semaCtx, p.EvalContext(), opts...)
	if err != nil {
		return eval.AsOfSystemTime{}, err
	}
	ts := asOf.Timestamp
	if now := p.execCfg.Clock.Now(); now.Less(ts) && !ts.Synthetic {
		return eval.AsOfSystemTime{}, errors.Errorf(
			"AS OF SYSTEM TIME: cannot specify timestamp in the future (%s > %s)", ts, now)
	}
	return asOf, nil
}

// isAsOf analyzes a statement to bypass the logic in newPlan(), since
// that requires the transaction to be started already. If the returned
// timestamp is not nil, it is the timestamp to which a transaction
// should be set. The statements that will be checked are Select,
// ShowTrace (of a Select statement), Scrub, Export, and CreateStats.
func (p *planner) isAsOf(ctx context.Context, stmt tree.Statement) (*eval.AsOfSystemTime, error) {
	var asOf tree.AsOfClause
	switch s := stmt.(type) {
	case *tree.Select:
		selStmt := s.Select
		var parenSel *tree.ParenSelect
		var ok bool
		for parenSel, ok = selStmt.(*tree.ParenSelect); ok; parenSel, ok = selStmt.(*tree.ParenSelect) {
			selStmt = parenSel.Select.Select
		}

		sc, ok := selStmt.(*tree.SelectClause)
		if !ok {
			return nil, nil
		}
		if sc.From.AsOf.Expr == nil {
			return nil, nil
		}

		asOf = sc.From.AsOf
	case *tree.Scrub:
		if s.AsOf.Expr == nil {
			return nil, nil
		}
		asOf = s.AsOf
	case *tree.Export:
		return p.isAsOf(ctx, s.Query)
	case *tree.CreateStats:
		if s.Options.AsOf.Expr == nil {
			return nil, nil
		}
		asOf = s.Options.AsOf
	case *tree.Explain:
		return p.isAsOf(ctx, s.Statement)
	default:
		return nil, nil
	}
	asOfRet, err := p.EvalAsOfTimestamp(ctx, asOf, asof.OptionAllowBoundedStaleness)
	if err != nil {
		return nil, err
	}
	return &asOfRet, err
}

// isSavepoint returns true if ast is a SAVEPOINT statement.
func isSavepoint(ast tree.Statement) bool {
	_, isSavepoint := ast.(*tree.Savepoint)
	return isSavepoint
}

// isSetTransaction returns true if ast is a "SET TRANSACTION ..." statement.
func isSetTransaction(ast tree.Statement) bool {
	_, isSet := ast.(*tree.SetTransaction)
	return isSet
}

// queryPhase represents a phase during a query's execution.
type queryPhase int

const (
	// The phase before start of execution (includes parsing, building a plan).
	preparing queryPhase = 0

	// Execution phase.
	executing queryPhase = 1
)

// queryMeta stores metadata about a query. Stored as reference in
// session.mu.ActiveQueries.
type queryMeta struct {
	// The ID of the transaction that this query is running within.
	txnID uuid.UUID

	// The timestamp when this query began execution.
	start time.Time

	// The SQL statement being executed.
	stmt statements.Statement[tree.Statement]

	// The placeholders that the query was executed with if any.
	placeholders *tree.PlaceholderInfo

	// States whether this query is distributed. Note that all queries,
	// including those that are distributed, have this field set to false until
	// start of execution; only at that point can we can actually determine whether
	// this query will be distributed. Use the phase variable below
	// to determine whether this query has entered execution yet.
	isDistributed bool

	// States whether this query is a full scan. As with isDistributed, this field
	// will be set to false for all queries until the start of execution, at which
	// point we can determine whether a full scan will occur. Use the phase variable
	// to determine whether this query has entered execution.
	isFullScan bool

	// Current phase of execution of query.
	phase queryPhase

	// Cancellation function for the context associated with this query's
	// statement.
	cancelQuery context.CancelFunc

	// If set, this query will not be reported as part of SHOW QUERIES. This is
	// set based on the statement implementing tree.HiddenFromShowQueries.
	hidden bool

	progressAtomic uint64

	// The compressed plan for this query. This can converted  back into the
	// logical plan. This field will only be populated in the EXECUTING phase.
	planGist string

	// The database the statement was executed on.
	database string
}

// SessionDefaults mirrors fields in Session, for restoring default
// configuration values in SET ... TO DEFAULT (or RESET ...) statements.
type SessionDefaults map[string]string

// SessionArgs contains arguments for serving a client connection.
type SessionArgs struct {
	User                        username.SQLUsername
	IsSuperuser                 bool
	IsSSL                       bool
	SystemIdentity              username.SQLUsername
	SessionDefaults             SessionDefaults
	CustomOptionSessionDefaults SessionDefaults
	// RemoteAddr is the client's address. This is nil iff this is an internal
	// client.
	RemoteAddr            net.Addr
	ConnResultsBufferSize int64
	// SessionRevivalToken may contain a token generated from a different session
	// that can be used to authenticate this session. If it is set, all other
	// authentication is skipped. Once the token is used to authenticate, this
	// value should be zeroed out.
	SessionRevivalToken []byte
	// JWTAuthEnabled indicates if the customer is passing a JWT token in the
	// password field.
	JWTAuthEnabled bool
}

// SessionRegistry stores a set of all sessions on this node.
// Use register() and deregister() to modify this registry.
type SessionRegistry struct {
	mu struct {
		syncutil.RWMutex
		sessionsByID        map[clusterunique.ID]RegistrySession
		sessionsByCancelKey map[pgwirecancel.BackendKeyData]RegistrySession
	}
}

// NewSessionRegistry creates a new SessionRegistry with an empty set
// of sessions.
func NewSessionRegistry() *SessionRegistry {
	r := SessionRegistry{}
	r.mu.sessionsByID = make(map[clusterunique.ID]RegistrySession)
	r.mu.sessionsByCancelKey = make(map[pgwirecancel.BackendKeyData]RegistrySession)
	return &r
}

func (r *SessionRegistry) GetSessionByID(sessionID clusterunique.ID) (RegistrySession, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	session, ok := r.mu.sessionsByID[sessionID]
	return session, ok
}

func (r *SessionRegistry) GetSessionByQueryID(queryID clusterunique.ID) (RegistrySession, bool) {
	for _, session := range r.getSessions() {
		if session.hasQuery(queryID) {
			return session, true
		}
	}
	return nil, false
}

func (r *SessionRegistry) GetSessionByCancelKey(
	cancelKey pgwirecancel.BackendKeyData,
) (RegistrySession, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	session, ok := r.mu.sessionsByCancelKey[cancelKey]
	return session, ok
}

func (r *SessionRegistry) getSessions() []RegistrySession {
	r.mu.RLock()
	defer r.mu.RUnlock()
	sessions := make([]RegistrySession, 0, len(r.mu.sessionsByID))
	for _, session := range r.mu.sessionsByID {
		sessions = append(sessions, session)
	}
	return sessions
}

func (r *SessionRegistry) register(
	id clusterunique.ID, queryCancelKey pgwirecancel.BackendKeyData, s RegistrySession,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.sessionsByID[id] = s
	r.mu.sessionsByCancelKey[queryCancelKey] = s
}

func (r *SessionRegistry) deregister(
	id clusterunique.ID, queryCancelKey pgwirecancel.BackendKeyData,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.mu.sessionsByID, id)
	delete(r.mu.sessionsByCancelKey, queryCancelKey)
}

type RegistrySession interface {
	// SessionUser returns the session user's username.
	SessionUser() username.SQLUsername
	hasQuery(queryID clusterunique.ID) bool
	// CancelQuery cancels the query specified by queryID if it exists.
	CancelQuery(queryID clusterunique.ID) bool
	// CancelActiveQueries cancels all currently active queries.
	CancelActiveQueries() bool
	// CancelSession cancels the session.
	CancelSession()
	// serialize serializes a Session into a serverpb.Session
	// that can be served over RPC.
	serialize() serverpb.Session
}

// SerializeAll returns a slice of all sessions in the registry converted to
// serverpb.Sessions.
func (r *SessionRegistry) SerializeAll() []serverpb.Session {
	sessions := r.getSessions()
	response := make([]serverpb.Session, 0, len(sessions))
	for _, s := range sessions {
		response = append(response, s.serialize())
	}
	return response
}

// MaxSQLBytes is the maximum length in bytes of SQL statements serialized
// into a serverpb.Session. Exported for testing.
const MaxSQLBytes = 1000

// truncateStatementStringForTelemetry truncates the string
// representation of a statement to a maximum length, so as to not
// create unduly large logging and error payloads.
func truncateStatementStringForTelemetry(stmt string) string {
	// panicLogOutputCutoiffChars is the maximum length of the copy of the
	// current statement embedded in telemetry reports and panic errors in
	// logs.
	const panicLogOutputCutoffChars = 10000
	if len(stmt) > panicLogOutputCutoffChars {
		stmt = stmt[:len(stmt)-6] + " [...]"
	}
	return stmt
}

// hideNonVirtualTableNameFunc returns a function that can be used with
// FmtCtx.SetReformatTableNames. It hides all table names that are not virtual
// tables.
func hideNonVirtualTableNameFunc(
	vt VirtualTabler, ns eval.ClientNoticeSender,
) func(ctx *tree.FmtCtx, name *tree.TableName) {
	reformatFn := func(ctx *tree.FmtCtx, tn *tree.TableName) {
		virtual, err := vt.getVirtualTableEntry(tn, ns)

		if err != nil || virtual == nil {
			// Current table is non-virtual and therefore needs to be scrubbed (for statement stats) or redacted (for logs).
			if ctx.HasFlags(tree.FmtMarkRedactionNode) {
				// The redaction flag is set, redact the table name.

				// Individually format the table name's fields for individual field redaction.
				ctx.FormatNode(&tn.CatalogName)
				ctx.WriteByte('.')

				// Check if the table's schema name is 'public', we do not redact the 'public' schema.
				if tn.ObjectNamePrefix.SchemaName == "public" {
					ctx.WithFlags(tree.FmtParsable, func() {
						ctx.FormatNode(&tn.ObjectNamePrefix.SchemaName)
					})
				} else {
					// The table's schema name is not 'public', format schema name normally for redaction.
					ctx.FormatNode(&tn.ObjectNamePrefix.SchemaName)
				}

				ctx.WriteByte('.')
				ctx.FormatNode(&tn.ObjectName)
			} else {
				// The redaction flag is not set, this means that we are scrubbing the table for statement stats.
				// Scrub the table name with '_'.
				ctx.WriteByte('_')
			}
			return
		}
		// Virtual table: we want to keep the name; however
		// we need to scrub the database name prefix.
		newTn := *tn
		newTn.CatalogName = "_"

		ctx.WithFlags(tree.FmtParsable, func() {
			ctx.WithReformatTableNames(nil, func() {
				ctx.FormatNode(&newTn)
			})
		})
	}
	return reformatFn
}

func anonymizeStmtAndConstants(
	stmt tree.Statement, vt VirtualTabler, ns eval.ClientNoticeSender,
) string {
	// Re-format to remove most names.
	fmtFlags := tree.FmtAnonymize | tree.FmtHideConstants
	var f *tree.FmtCtx
	if vt != nil {
		f = tree.NewFmtCtx(
			fmtFlags,
			tree.FmtReformatTableNames(hideNonVirtualTableNameFunc(vt, ns)),
		)
	} else {
		f = tree.NewFmtCtx(fmtFlags)
	}
	f.FormatNode(stmt)
	return f.CloseAndGetString()
}

// WithAnonymizedStatement attaches the anonymized form of a statement
// to an error object.
func WithAnonymizedStatement(
	err error, stmt tree.Statement, vt VirtualTabler, ns eval.ClientNoticeSender,
) error {
	anonStmtStr := anonymizeStmtAndConstants(stmt, vt, ns)
	anonStmtStr = truncateStatementStringForTelemetry(anonStmtStr)
	return errors.WithSafeDetails(err,
		"while executing: %s", errors.Safe(anonStmtStr))
}

// SessionTracing holds the state used by SET TRACING statements in the context
// of one SQL session.
// It holds the current trace being collected (or the last trace collected, if
// tracing is not currently ongoing).
//
// SessionTracing and its interactions with the connExecutor are thread-safe;
// tracing can be turned on at any time.
type SessionTracing struct {
	// enabled is set at times when "session enabled" is active - i.e. when
	// transactions are being recorded.
	enabled bool

	// kvTracingEnabled is set at times when KV tracing is active. When
	// KV tracing is enabled, the SQL/KV interface logs individual K/V
	// operators to the current context.
	kvTracingEnabled bool

	// showResults, when set, indicates that the result rows produced by
	// the execution statement must be reported in the
	// trace. showResults can be set manually by SET TRACING = ...,
	// results
	showResults bool

	// If recording==true, recordingType indicates the type of the current
	// recording.
	recordingType tracingpb.RecordingType

	// ex is the connExecutor to which this SessionTracing is tied.
	ex *connExecutor

	// connSpan is the connection's span. This is recording. It is finished and
	// unset in StopTracing.
	connSpan *tracing.Span

	// lastRecording will collect the recording when stopping tracing.
	lastRecording []traceRow
}

// getSessionTrace returns the session trace. If we're not currently tracing,
// this will be the last recorded trace. If we are currently tracing, we'll
// return whatever was recorded so far.
func (st *SessionTracing) getSessionTrace() ([]traceRow, error) {
	if !st.enabled {
		return st.lastRecording, nil
	}

	return generateSessionTraceVTable(st.connSpan.GetRecording(tracingpb.RecordingVerbose))
}

// StartTracing starts "session tracing". From this moment on, everything
// happening on both the connection's context and the current txn's context (if
// any) will be traced.
// StopTracing() needs to be called to finish this trace.
//
// There's two contexts on which we must record:
// 1) If we're inside a txn, we start recording on the txn's span. We assume
// that the txn's ctx has a recordable span on it.
// 2) Regardless of whether we're in a txn or not, we need to record the
// connection's context. This context generally does not have a span, so we
// "hijack" it with one that does. Whatever happens on that context, plus
// whatever happens in future derived txn contexts, will be recorded.
//
// Args:
// kvTracingEnabled: If set, the traces will also include "KV trace" messages -
//
//	verbose messages around the interaction of SQL with KV. Some of the messages
//	are per-row.
//
// showResults: If set, result rows are reported in the trace.
func (st *SessionTracing) StartTracing(
	recType tracingpb.RecordingType, kvTracingEnabled, showResults bool,
) error {
	if st.enabled {
		// We're already tracing. Only treat as no-op if the same options
		// are requested.
		if kvTracingEnabled != st.kvTracingEnabled ||
			showResults != st.showResults ||
			recType != st.recordingType {
			var desiredOptions bytes.Buffer
			comma := ""
			if kvTracingEnabled {
				desiredOptions.WriteString("kv")
				comma = ", "
			}
			if showResults {
				fmt.Fprintf(&desiredOptions, "%sresults", comma)
				comma = ", "
			}
			recOption := "cluster"
			fmt.Fprintf(&desiredOptions, "%s%s", comma, recOption)

			err := pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"tracing is already started with different options")
			return errors.WithHintf(err,
				"reset with SET tracing = off; SET tracing = %s", desiredOptions.String())
		}

		return nil
	}

	// Hijack the conn's ctx with one that has a recording span. All future
	// transactions will inherit from this span, so they'll all be recorded.
	var newConnCtx context.Context
	{
		connCtx := st.ex.ctxHolder.connCtx
		opName := "session recording"
		newConnCtx, st.connSpan = tracing.EnsureChildSpan(
			connCtx,
			st.ex.server.cfg.AmbientCtx.Tracer,
			opName,
			tracing.WithForceRealSpan(),
		)
		st.connSpan.SetRecordingType(tracingpb.RecordingVerbose)
		st.ex.ctxHolder.hijack(newConnCtx)
	}

	// If we're inside a transaction, hijack the txn's ctx with one that has a
	// recording span.
	if _, ok := st.ex.machine.CurState().(stateNoTxn); !ok {
		txnCtx := st.ex.state.Ctx
		sp := tracing.SpanFromContext(txnCtx)
		if sp == nil {
			return errors.Errorf("no txn span for SessionTracing")
		}
		// We're hijacking this span and we're never going to un-hijack it, so it's
		// up to us to finish it.
		sp.Finish()

		st.ex.state.Ctx, _ = tracing.EnsureChildSpan(
			newConnCtx, st.ex.server.cfg.AmbientCtx.Tracer, "session tracing")
	}

	st.enabled = true
	st.kvTracingEnabled = kvTracingEnabled
	st.showResults = showResults
	st.recordingType = recType

	return nil
}

// StopTracing stops the trace that was started with StartTracing().
func (st *SessionTracing) StopTracing() error {
	if !st.enabled {
		// We're not currently tracing. No-op.
		return nil
	}
	st.enabled = false
	st.kvTracingEnabled = false
	st.showResults = false
	st.recordingType = tracingpb.RecordingOff

	// Accumulate all recordings and finish the tracing spans.
	rec := st.connSpan.GetRecording(tracingpb.RecordingVerbose)
	// We're about to finish this span, but there might be a child that remains
	// open - the child corresponding to the current transaction. We don't want
	// that span to be recording any more.
	st.connSpan.SetRecordingType(tracingpb.RecordingOff)
	st.connSpan.Finish()
	st.connSpan = nil
	st.ex.ctxHolder.unhijack()

	var err error
	st.lastRecording, err = generateSessionTraceVTable(rec)
	return err
}

// KVTracingEnabled checks whether KV tracing is currently enabled.
func (st *SessionTracing) KVTracingEnabled() bool {
	return st.kvTracingEnabled
}

// Enabled checks whether session tracing is currently enabled.
func (st *SessionTracing) Enabled() bool {
	return st.enabled
}

// TracePlanStart conditionally emits a trace message at the moment
// logical planning starts.
func (st *SessionTracing) TracePlanStart(ctx context.Context, stmtTag string) {
	if st.enabled {
		log.VEventf(ctx, 2, "planning starts: %s", stmtTag)
	}
}

// TracePlanEnd conditionally emits a trace message at the moment
// logical planning ends.
func (st *SessionTracing) TracePlanEnd(ctx context.Context, err error) {
	log.VEventfDepth(ctx, 2, 1, "planning ends")
	if err != nil {
		log.VEventfDepth(ctx, 2, 1, "planning error: %v", err)
	}
}

// TracePlanCheckStart conditionally emits a trace message at the
// moment the test of which execution engine to use starts.
func (st *SessionTracing) TracePlanCheckStart(ctx context.Context) {
	log.VEventfDepth(ctx, 2, 1, "checking distributability")
}

// TracePlanCheckEnd conditionally emits a trace message at the moment
// the engine check ends.
func (st *SessionTracing) TracePlanCheckEnd(ctx context.Context, err error, dist bool) {
	if err != nil {
		log.VEventfDepth(ctx, 2, 1, "distributability check error: %v", err)
	} else {
		log.VEventfDepth(ctx, 2, 1, "will distribute plan: %v", dist)
	}
}

// TraceRetryInformation conditionally emits a trace message for retry information.
func (st *SessionTracing) TraceRetryInformation(ctx context.Context, retries int, err error) {
	log.VEventfDepth(ctx, 2, 1, "executing after %d retries, last retry reason: %v", retries, err)
}

// TraceExecStart conditionally emits a trace message at the moment
// plan execution starts.
func (st *SessionTracing) TraceExecStart(ctx context.Context, engine redact.SafeString) {
	log.VEventfDepth(ctx, 2, 1, "execution starts: %s engine", engine)
}

// TraceExecConsume creates a context for TraceExecRowsResult below.
func (st *SessionTracing) TraceExecConsume(ctx context.Context) (context.Context, func()) {
	if st.enabled {
		consumeCtx, sp := tracing.ChildSpan(ctx, "consuming rows")
		return consumeCtx, sp.Finish
	}
	return ctx, func() {}
}

// TraceExecRowsResult conditionally emits a trace message for a single output row.
func (st *SessionTracing) TraceExecRowsResult(ctx context.Context, values tree.Datums) {
	if st.showResults {
		log.VEventfDepth(ctx, 2, 1, "output row: %s", values)
	}
}

// TraceExecBatchResult conditionally emits a trace message for a single batch.
func (st *SessionTracing) TraceExecBatchResult(ctx context.Context, batch coldata.Batch) {
	if st.showResults {
		outputRows := coldata.VecsToStringWithRowPrefix(batch.ColVecs(), batch.Length(), batch.Selection(), "output row: ")
		for _, row := range outputRows {
			log.VEventfDepth(ctx, 2, 1, "%s", row)
		}
	}
}

// TraceExecEnd conditionally emits a trace message at the moment
// plan execution completes.
func (st *SessionTracing) TraceExecEnd(ctx context.Context, err error, count int) {
	log.VEventfDepth(ctx, 2, 1, "execution ends")
	if err != nil {
		log.VEventfDepth(ctx, 2, 1, "execution failed after %d rows: %v", count, err)
	} else {
		log.VEventfDepth(ctx, 2, 1, "rows affected: %d", count)
	}
}

const (
	// span_idx    INT NOT NULL,        -- The span's index.
	traceSpanIdxCol = iota
	// message_idx INT NOT NULL,        -- The message's index within its span.
	_
	// timestamp   TIMESTAMPTZ NOT NULL,-- The message's timestamp.
	traceTimestampCol
	// duration    INTERVAL,            -- The span's duration.
	//                                  -- NULL if the span was not finished at the time
	//                                  -- the trace has been collected.
	traceDurationCol
	// operation   STRING NULL,         -- The span's operation.
	traceOpCol
	// loc         STRING NOT NULL,     -- The file name / line number prefix, if any.
	traceLocCol
	// tag         STRING NOT NULL,     -- The logging tag, if any.
	traceTagCol
	// message     STRING NOT NULL,     -- The logged message.
	traceMsgCol
	// age         INTERVAL NOT NULL    -- The age of the message.
	traceAgeCol
	// traceNumCols must be the last item in the enumeration.
	traceNumCols
)

// traceRow is the type of a single row in the session_trace vtable.
type traceRow [traceNumCols]tree.Datum

// A regular expression to split log messages.
// It has three parts:
//   - the (optional) code location, with at least one forward slash and a period
//     in the file name:
//     ((?:[^][ :]+/[^][ :]+\.[^][ :]+:[0-9]+)?)
//   - the (optional) tag: ((?:\[(?:[^][]|\[[^]]*\])*\])?)
//   - the message itself: the rest.
var logMessageRE = regexp.MustCompile(
	`(?s:^((?:[^][ :]+/[^][ :]+\.[^][ :]+:[0-9]+)?) *((?:\[(?:[^][]|\[[^]]*\])*\])?) *(.*))`)

// generateSessionTraceVTable generates the rows of said table by using the log
// messages from the session's trace (i.e. the ongoing trace, if any, or the
// last one recorded).
//
// All the log messages from the current recording are returned, in
// the order in which they should be presented in the crdb_internal.session_info
// virtual table. Messages from child spans are inserted as a block in between
// messages from the parent span. Messages from sibling spans are not
// interleaved.
//
// Here's a drawing showing the order in which messages from different spans
// will be interleaved. Each box is a span; inner-boxes are child spans. The
// numbers indicate the order in which the log messages will appear in the
// virtual table.
//
// +-----------------------+
// |           1           |
// | +-------------------+ |
// | |         2         | |
// | |  +----+           | |
// | |  |    | +----+    | |
// | |  | 3  | | 4  |    | |
// | |  |    | |    |  5 | |
// | |  |    | |    | ++ | |
// | |  |    | |    |    | |
// | |  +----+ |    |    | |
// | |         +----+    | |
// | |                   | |
// | |          6        | |
// | +-------------------+ |
// |            7          |
// +-----------------------+
//
// Note that what's described above is not the order in which SHOW TRACE FOR SESSION
// displays the information: SHOW TRACE will sort by the age column.
func generateSessionTraceVTable(spans []tracingpb.RecordedSpan) ([]traceRow, error) {
	// Get all the log messages, in the right order.
	var allLogs []logRecordRow

	// NOTE: The spans are recorded in the order in which they are started.
	seenSpans := make(map[tracingpb.SpanID]struct{})
	for spanIdx, span := range spans {
		if _, ok := seenSpans[span.SpanID]; ok {
			continue
		}
		spanWithIndex := spanWithIndex{
			RecordedSpan: &spans[spanIdx],
			index:        spanIdx,
		}
		msgs, err := getMessagesForSubtrace(spanWithIndex, spans, seenSpans)
		if err != nil {
			return nil, err
		}
		allLogs = append(allLogs, msgs...)
	}

	// Transform the log messages into table rows.
	// We need to populate "operation" later because it is only
	// set for the first row in each span.
	opMap := make(map[tree.DInt]*tree.DString)
	durMap := make(map[tree.DInt]*tree.DInterval)
	var res []traceRow
	var minTimestamp, zeroTime time.Time
	for _, lrr := range allLogs {
		// The "operation" column is only set for the first row in span.
		// We'll populate the rest below.
		if lrr.index == 0 {
			spanIdx := tree.DInt(lrr.span.index)
			opMap[spanIdx] = tree.NewDString(lrr.span.Operation)
			if lrr.span.Duration != 0 {
				durMap[spanIdx] = &tree.DInterval{
					Duration: duration.MakeDuration(lrr.span.Duration.Nanoseconds(), 0, 0),
				}
			}
		}

		// We'll need the lowest timestamp to compute ages below.
		if minTimestamp == zeroTime || lrr.timestamp.Before(minTimestamp) {
			minTimestamp = lrr.timestamp
		}

		// Split the message into component parts.
		//
		// The result of FindStringSubmatchIndex is a 1D array of pairs
		// [start, end) of positions in the input string.  The first pair
		// identifies the entire match; the 2nd pair corresponds to the
		// 1st parenthetized expression in the regexp, and so on.
		loc := logMessageRE.FindStringSubmatchIndex(lrr.msg)
		if loc == nil {
			return nil, fmt.Errorf("unable to split trace message: %q", lrr.msg)
		}

		tsDatum, err := tree.MakeDTimestampTZ(lrr.timestamp, time.Nanosecond)
		if err != nil {
			return nil, err
		}

		row := traceRow{
			tree.NewDInt(tree.DInt(lrr.span.index)), // span_idx
			tree.NewDInt(tree.DInt(lrr.index)),      // message_idx
			tsDatum,                                 // timestamp
			tree.DNull,                              // duration, will be populated below
			tree.DNull,                              // operation, will be populated below
			tree.NewDString(lrr.msg[loc[2]:loc[3]]), // location
			tree.NewDString(lrr.msg[loc[4]:loc[5]]), // tag
			tree.NewDString(lrr.msg[loc[6]:loc[7]]), // message
			tree.DNull,                              // age, will be populated below
		}
		res = append(res, row)
	}

	if len(res) == 0 {
		// Nothing to do below. Shortcut.
		return res, nil
	}

	// Populate the operation and age columns.
	for i := range res {
		spanIdx := res[i][traceSpanIdxCol]

		if opStr, ok := opMap[*(spanIdx.(*tree.DInt))]; ok {
			res[i][traceOpCol] = opStr
		}

		if dur, ok := durMap[*(spanIdx.(*tree.DInt))]; ok {
			res[i][traceDurationCol] = dur
		}

		ts := res[i][traceTimestampCol].(*tree.DTimestampTZ)
		res[i][traceAgeCol] = &tree.DInterval{
			Duration: duration.MakeDuration(ts.Sub(minTimestamp).Nanoseconds(), 0, 0),
		}
	}

	return res, nil
}

// getOrderedChildSpans returns all the spans in allSpans that are children of
// spanID. It assumes the input is ordered by start time, in which case the
// output is also ordered.
func getOrderedChildSpans(
	spanID tracingpb.SpanID, allSpans []tracingpb.RecordedSpan,
) []spanWithIndex {
	children := make([]spanWithIndex, 0)
	for i := range allSpans {
		if allSpans[i].ParentSpanID == spanID {
			children = append(
				children,
				spanWithIndex{
					RecordedSpan: &allSpans[i],
					index:        i,
				})
		}
	}
	return children
}

// getMessagesForSubtrace takes a span and interleaves its log messages with
// those from its children (recursively). The order is the one defined in the
// comment on generateSessionTraceVTable().
//
// seenSpans is modified to record all the spans that are part of the subtrace
// rooted at span.
func getMessagesForSubtrace(
	span spanWithIndex, allSpans []tracingpb.RecordedSpan, seenSpans map[tracingpb.SpanID]struct{},
) ([]logRecordRow, error) {
	if _, ok := seenSpans[span.SpanID]; ok {
		return nil, errors.Errorf("duplicate span %d", span.SpanID)
	}
	var allLogs []logRecordRow

	// This message marks the beginning of the span, to indicate the start time
	// and duration of the span.
	allLogs = append(
		allLogs,
		logRecordRow{
			timestamp: span.StartTime,
			msg:       fmt.Sprintf("=== SPAN START: %s ===", span.Operation),
			span:      span,
			index:     0,
		},
	)

	seenSpans[span.SpanID] = struct{}{}
	childSpans := getOrderedChildSpans(span.SpanID, allSpans)
	var i, j int
	// Sentinel value - year 6000.
	maxTime := time.Date(6000, 0, 0, 0, 0, 0, 0, time.UTC)
	// Merge the logs with the child spans.
	for i < len(span.Logs) || j < len(childSpans) {
		logTime := maxTime
		childTime := maxTime
		if i < len(span.Logs) {
			logTime = span.Logs[i].Time
		}
		if j < len(childSpans) {
			childTime = childSpans[j].StartTime
		}

		if logTime.Before(childTime) {
			allLogs = append(allLogs,
				logRecordRow{
					timestamp: logTime,
					msg:       span.Logs[i].Msg().StripMarkers(),
					span:      span,
					// Add 1 to the index to account for the first dummy message in a
					// span.
					index: i + 1,
				})
			i++
		} else {
			// Recursively append messages from the trace rooted at the child.
			childMsgs, err := getMessagesForSubtrace(childSpans[j], allSpans, seenSpans)
			if err != nil {
				return nil, err
			}
			allLogs = append(allLogs, childMsgs...)
			j++
		}
	}
	return allLogs, nil
}

// logRecordRow is used to temporarily hold on to log messages and their
// metadata while flattening a trace.
type logRecordRow struct {
	timestamp time.Time
	msg       string
	span      spanWithIndex
	// index of the log message within its span.
	index int
}

type spanWithIndex struct {
	*tracingpb.RecordedSpan
	index int
}

// paramStatusUpdater is a subset of RestrictedCommandResult which allows sending
// status updates. Ensure all updatable settings are in bufferableParamStatusUpdates.
type paramStatusUpdater interface {
	BufferParamStatusUpdate(string, string)
}

type bufferableParamStatusUpdate struct {
	name      string
	lowerName string
}

// bufferableParamStatusUpdates contains all vars which can be sent through
// ParamStatusUpdates.
var bufferableParamStatusUpdates = func() []bufferableParamStatusUpdate {
	params := []string{
		"application_name",
		"DateStyle",
		"IntervalStyle",
		"is_superuser",
		"TimeZone",
	}
	ret := make([]bufferableParamStatusUpdate, len(params))
	for i, param := range params {
		ret[i] = bufferableParamStatusUpdate{
			name:      param,
			lowerName: strings.ToLower(param),
		}
	}
	return ret
}()

// sessionDataMutatorBase contains elements in a sessionDataMutator
// which is the same across all SessionData elements in the sessiondata.Stack.
type sessionDataMutatorBase struct {
	defaults SessionDefaults
	settings *cluster.Settings
}

// sessionDataMutatorCallbacks contains elements in a sessionDataMutator
// which are only populated when mutating the "top" sessionData element.
// It is intended for functions which should only be called once per SET
// (e.g. param status updates, which only should be sent once within
// a transaction where there may be two or more SessionData elements in
// the stack)
type sessionDataMutatorCallbacks struct {
	// paramStatusUpdater is called when there is a ParamStatusUpdate.
	// It can be nil, in which case nothing triggers on execution.
	paramStatusUpdater paramStatusUpdater
	// setCurTxnReadOnly is called when we execute SET transaction_read_only = ...
	// It can be nil, in which case nothing triggers on execution.
	setCurTxnReadOnly func(val bool)
	// onTempSchemaCreation is called when the temporary schema is set
	// on the search path (the first and only time).
	// It can be nil, in which case nothing triggers on execution.
	onTempSchemaCreation func()
	// onDefaultIntSizeChange is called when default_int_size changes. It is
	// needed because the pgwire connection's read buffer needs to be aware
	// of the default int size in order to be able to parse the unqualified
	// INT type correctly.
	onDefaultIntSizeChange func(int32)
	// onApplicationName is called when the application_name changes. It is
	// needed because the stats writer needs to be notified of changes to the
	// application name.
	onApplicationNameChange func(string)
}

// sessionDataMutatorIterator generates sessionDataMutators which allow
// the changing of SessionData on some element inside the sessiondata Stack.
type sessionDataMutatorIterator struct {
	sessionDataMutatorBase
	sds *sessiondata.Stack
	sessionDataMutatorCallbacks
}

func makeSessionDataMutatorIterator(
	sds *sessiondata.Stack, defaults SessionDefaults, settings *cluster.Settings,
) *sessionDataMutatorIterator {
	return &sessionDataMutatorIterator{
		sds: sds,
		sessionDataMutatorBase: sessionDataMutatorBase{
			defaults: defaults,
			settings: settings,
		},
		sessionDataMutatorCallbacks: sessionDataMutatorCallbacks{},
	}
}

// mutator returns a mutator for the given sessionData.
func (it *sessionDataMutatorIterator) mutator(
	applyCallbacks bool, sd *sessiondata.SessionData,
) sessionDataMutator {
	ret := sessionDataMutator{
		data:                   sd,
		sessionDataMutatorBase: it.sessionDataMutatorBase,
	}
	// We usually apply callbacks on the first element in the stack, as the txn
	// rollback will always reset to the first element we touch in the stack,
	// in which case it should be up-to-date by default.
	if applyCallbacks {
		ret.sessionDataMutatorCallbacks = it.sessionDataMutatorCallbacks
	}
	return ret
}

// SetSessionDefaultIntSize sets the default int size for the session.
// It is exported for use in import which is a CCL package.
func (it *sessionDataMutatorIterator) SetSessionDefaultIntSize(size int32) {
	it.applyOnEachMutator(func(m sessionDataMutator) {
		m.SetDefaultIntSize(size)
	})
}

// applyOnTopMutator applies the given function on the mutator for the top
// element on the sessiondata Stack only.
func (it *sessionDataMutatorIterator) applyOnTopMutator(
	applyFunc func(m sessionDataMutator) error,
) error {
	return applyFunc(it.mutator(true /* applyCallbacks */, it.sds.Top()))
}

// applyOnEachMutator iterates over each mutator over all SessionData elements
// in the stack and applies the given function to them.
// It is the equivalent of SET SESSION x = y.
func (it *sessionDataMutatorIterator) applyOnEachMutator(applyFunc func(m sessionDataMutator)) {
	elems := it.sds.Elems()
	for i, sd := range elems {
		applyFunc(it.mutator(i == 0, sd))
	}
}

// applyOnEachMutatorError is the same as applyOnEachMutator, but takes in a function
// that can return an error, erroring if any of applications error.
func (it *sessionDataMutatorIterator) applyOnEachMutatorError(
	applyFunc func(m sessionDataMutator) error,
) error {
	elems := it.sds.Elems()
	for i, sd := range elems {
		if err := applyFunc(it.mutator(i == 0, sd)); err != nil {
			return err
		}
	}
	return nil
}

// sessionDataMutator is the object used by sessionVars to change the session
// state. It mostly mutates the session's SessionData, but not exclusively (e.g.
// see curTxnReadOnly).
type sessionDataMutator struct {
	data *sessiondata.SessionData
	sessionDataMutatorBase
	sessionDataMutatorCallbacks
}

func (m *sessionDataMutator) bufferParamStatusUpdate(param string, status string) {
	if m.paramStatusUpdater != nil {
		m.paramStatusUpdater.BufferParamStatusUpdate(param, status)
	}
}

// SetApplicationName sets the application name.
func (m *sessionDataMutator) SetApplicationName(appName string) {
	oldName := m.data.ApplicationName
	m.data.ApplicationName = appName
	if m.onApplicationNameChange != nil {
		m.onApplicationNameChange(appName)
	}
	if oldName != appName {
		m.bufferParamStatusUpdate("application_name", appName)
	}
}

// SetAvoidBuffering sets avoid buffering option.
func (m *sessionDataMutator) SetAvoidBuffering(b bool) {
	m.data.AvoidBuffering = b
}

func (m *sessionDataMutator) SetBytesEncodeFormat(val lex.BytesEncodeFormat) {
	m.data.DataConversionConfig.BytesEncodeFormat = val
}

func (m *sessionDataMutator) SetExtraFloatDigits(val int32) {
	m.data.DataConversionConfig.ExtraFloatDigits = val
}

func (m *sessionDataMutator) SetDatabase(dbName string) {
	m.data.Database = dbName
}

func (m *sessionDataMutator) SetTemporarySchemaName(scName string) {
	if m.onTempSchemaCreation != nil {
		m.onTempSchemaCreation()
	}
	m.data.SearchPath = m.data.SearchPath.WithTemporarySchemaName(scName)
}

func (m *sessionDataMutator) SetTemporarySchemaIDForDatabase(dbID uint32, tempSchemaID uint32) {
	if m.data.DatabaseIDToTempSchemaID == nil {
		m.data.DatabaseIDToTempSchemaID = make(map[uint32]uint32)
	}
	m.data.DatabaseIDToTempSchemaID[dbID] = tempSchemaID
}

func (m *sessionDataMutator) SetDefaultIntSize(size int32) {
	m.data.DefaultIntSize = size
	if m.onDefaultIntSizeChange != nil {
		m.onDefaultIntSizeChange(size)
	}
}

func (m *sessionDataMutator) SetDefaultTransactionPriority(val tree.UserPriority) {
	m.data.DefaultTxnPriority = int64(val)
}

func (m *sessionDataMutator) SetDefaultTransactionIsolationLevel(val tree.IsolationLevel) {
	m.data.DefaultTxnIsolationLevel = int64(val)
}

func (m *sessionDataMutator) SetDefaultTransactionReadOnly(val bool) {
	m.data.DefaultTxnReadOnly = val
}

func (m *sessionDataMutator) SetDefaultTransactionUseFollowerReads(val bool) {
	m.data.DefaultTxnUseFollowerReads = val
}

func (m *sessionDataMutator) SetEnableSeqScan(val bool) {
	m.data.EnableSeqScan = val
}

func (m *sessionDataMutator) SetSynchronousCommit(val bool) {
	m.data.SynchronousCommit = val
}

func (m *sessionDataMutator) SetDirectColumnarScansEnabled(b bool) {
	m.data.DirectColumnarScansEnabled = b
}

func (m *sessionDataMutator) SetDisablePlanGists(val bool) {
	m.data.DisablePlanGists = val
}

func (m *sessionDataMutator) SetDistSQLMode(val sessiondatapb.DistSQLExecMode) {
	m.data.DistSQLMode = val
}

func (m *sessionDataMutator) SetDistSQLWorkMem(val int64) {
	m.data.WorkMemLimit = val
}

func (m *sessionDataMutator) SetForceSavepointRestart(val bool) {
	m.data.ForceSavepointRestart = val
}

func (m *sessionDataMutator) SetZigzagJoinEnabled(val bool) {
	m.data.ZigzagJoinEnabled = val
}

func (m *sessionDataMutator) SetIndexRecommendationsEnabled(val bool) {
	m.data.IndexRecommendationsEnabled = val
}

func (m *sessionDataMutator) SetExperimentalDistSQLPlanning(
	val sessiondatapb.ExperimentalDistSQLPlanningMode,
) {
	m.data.ExperimentalDistSQLPlanningMode = val
}

func (m *sessionDataMutator) SetPartiallyDistributedPlansDisabled(val bool) {
	m.data.PartiallyDistributedPlansDisabled = val
}

func (m *sessionDataMutator) SetRequireExplicitPrimaryKeys(val bool) {
	m.data.RequireExplicitPrimaryKeys = val
}

func (m *sessionDataMutator) SetReorderJoinsLimit(val int) {
	m.data.ReorderJoinsLimit = int64(val)
}

func (m *sessionDataMutator) SetVectorize(val sessiondatapb.VectorizeExecMode) {
	m.data.VectorizeMode = val
}

func (m *sessionDataMutator) SetTestingVectorizeInjectPanics(val bool) {
	m.data.TestingVectorizeInjectPanics = val
}

func (m *sessionDataMutator) SetTestingOptimizerInjectPanics(val bool) {
	m.data.TestingOptimizerInjectPanics = val
}

func (m *sessionDataMutator) SetOptimizerFKCascadesLimit(val int) {
	m.data.OptimizerFKCascadesLimit = int64(val)
}

func (m *sessionDataMutator) SetOptimizerUseForecasts(val bool) {
	m.data.OptimizerUseForecasts = val
}

func (m *sessionDataMutator) SetOptimizerUseHistograms(val bool) {
	m.data.OptimizerUseHistograms = val
}

func (m *sessionDataMutator) SetOptimizerUseMultiColStats(val bool) {
	m.data.OptimizerUseMultiColStats = val
}

func (m *sessionDataMutator) SetOptimizerUseNotVisibleIndexes(val bool) {
	m.data.OptimizerUseNotVisibleIndexes = val
}

func (m *sessionDataMutator) SetLocalityOptimizedSearch(val bool) {
	m.data.LocalityOptimizedSearch = val
}

func (m *sessionDataMutator) SetImplicitSelectForUpdate(val bool) {
	m.data.ImplicitSelectForUpdate = val
}

func (m *sessionDataMutator) SetInsertFastPath(val bool) {
	m.data.InsertFastPath = val
}

func (m *sessionDataMutator) SetSerialNormalizationMode(val sessiondatapb.SerialNormalizationMode) {
	m.data.SerialNormalizationMode = val
}

func (m *sessionDataMutator) SetSafeUpdates(val bool) {
	m.data.SafeUpdates = val
}

func (m *sessionDataMutator) SetDisableDropVirtualCluster(val bool) {
	m.data.DisableDropVirtualCluster = val
}

func (m *sessionDataMutator) SetCheckFunctionBodies(val bool) {
	m.data.CheckFunctionBodies = val
}

func (m *sessionDataMutator) SetPreferLookupJoinsForFKs(val bool) {
	m.data.PreferLookupJoinsForFKs = val
}

func (m *sessionDataMutator) UpdateSearchPath(paths []string) {
	m.data.SearchPath = m.data.SearchPath.UpdatePaths(paths)
}

func (m *sessionDataMutator) SetLocation(loc *time.Location) {
	oldLocation := sessionDataTimeZoneFormat(m.data.Location)
	m.data.Location = loc
	if formatted := sessionDataTimeZoneFormat(loc); oldLocation != formatted {
		m.bufferParamStatusUpdate("TimeZone", formatted)
	}
}

func (m *sessionDataMutator) SetCustomOption(name, val string) {
	if m.data.CustomOptions == nil {
		m.data.CustomOptions = make(map[string]string)
	}
	m.data.CustomOptions[name] = val
}

func (m *sessionDataMutator) SetReadOnly(val bool) {
	// The read-only state is special; it's set as a session variable (SET
	// transaction_read_only=<>), but it represents per-txn state, not
	// per-session. There's no field for it in the SessionData struct. Instead, we
	// call into the connEx, which modifies its TxnState.
	// NOTE(andrei): I couldn't find good documentation on transaction_read_only,
	// but I've tested its behavior in Postgres 11.
	if m.setCurTxnReadOnly != nil {
		m.setCurTxnReadOnly(val)
	}
}

func (m *sessionDataMutator) SetStmtTimeout(timeout time.Duration) {
	m.data.StmtTimeout = timeout
}

func (m *sessionDataMutator) SetLockTimeout(timeout time.Duration) {
	m.data.LockTimeout = timeout
}

func (m *sessionDataMutator) SetIdleInSessionTimeout(timeout time.Duration) {
	m.data.IdleInSessionTimeout = timeout
}

func (m *sessionDataMutator) SetIdleInTransactionSessionTimeout(timeout time.Duration) {
	m.data.IdleInTransactionSessionTimeout = timeout
}

func (m *sessionDataMutator) SetTransactionTimeout(timeout time.Duration) {
	m.data.TransactionTimeout = timeout
}

func (m *sessionDataMutator) SetAllowPrepareAsOptPlan(val bool) {
	m.data.AllowPrepareAsOptPlan = val
}

func (m *sessionDataMutator) SetSaveTablesPrefix(prefix string) {
	m.data.SaveTablesPrefix = prefix
}

func (m *sessionDataMutator) SetPlacementEnabled(val bool) {
	m.data.PlacementEnabled = val
}

func (m *sessionDataMutator) SetAutoRehomingEnabled(val bool) {
	m.data.AutoRehomingEnabled = val
}

func (m *sessionDataMutator) SetOnUpdateRehomeRowEnabled(val bool) {
	m.data.OnUpdateRehomeRowEnabled = val
}

func (m *sessionDataMutator) SetTempTablesEnabled(val bool) {
	m.data.TempTablesEnabled = val
}

func (m *sessionDataMutator) SetImplicitColumnPartitioningEnabled(val bool) {
	m.data.ImplicitColumnPartitioningEnabled = val
}

func (m *sessionDataMutator) SetOverrideMultiRegionZoneConfigEnabled(val bool) {
	m.data.OverrideMultiRegionZoneConfigEnabled = val
}

func (m *sessionDataMutator) SetDisallowFullTableScans(val bool) {
	m.data.DisallowFullTableScans = val
}

func (m *sessionDataMutator) SetAlterColumnTypeGeneral(val bool) {
	m.data.AlterColumnTypeGeneralEnabled = val
}

func (m *sessionDataMutator) SetEnableSuperRegions(val bool) {
	m.data.EnableSuperRegions = val
}

func (m *sessionDataMutator) SetEnableOverrideAlterPrimaryRegionInSuperRegion(val bool) {
	m.data.OverrideAlterPrimaryRegionInSuperRegion = val
}

// TODO(rytaft): remove this once unique without index constraints are fully
// supported.
func (m *sessionDataMutator) SetUniqueWithoutIndexConstraints(val bool) {
	m.data.EnableUniqueWithoutIndexConstraints = val
}

func (m *sessionDataMutator) SetUseNewSchemaChanger(val sessiondatapb.NewSchemaChangerMode) {
	m.data.NewSchemaChangerMode = val
}

func (m *sessionDataMutator) SetDescriptorValidationMode(
	val sessiondatapb.DescriptorValidationMode,
) {
	m.data.DescriptorValidationMode = val
}

func (m *sessionDataMutator) SetQualityOfService(val sessiondatapb.QoSLevel) {
	m.data.DefaultTxnQualityOfService = val.Validate()
}

func (m *sessionDataMutator) SetOptSplitScanLimit(val int32) {
	m.data.OptSplitScanLimit = val
}

func (m *sessionDataMutator) SetStreamReplicationEnabled(val bool) {
	m.data.EnableStreamReplication = val
}

// RecordLatestSequenceVal records that value to which the session incremented
// a sequence.
func (m *sessionDataMutator) RecordLatestSequenceVal(seqID uint32, val int64) {
	m.data.SequenceState.RecordValue(seqID, val)
}

// SetNoticeDisplaySeverity sets the NoticeDisplaySeverity for the given session.
func (m *sessionDataMutator) SetNoticeDisplaySeverity(severity pgnotice.DisplaySeverity) {
	m.data.NoticeDisplaySeverity = uint32(severity)
}

// initSequenceCache creates an empty sequence cache instance for the session.
func (m *sessionDataMutator) initSequenceCache() {
	m.data.SequenceCache = sessiondatapb.SequenceCache{}
}

// SetIntervalStyle sets the IntervalStyle for the given session.
func (m *sessionDataMutator) SetIntervalStyle(style duration.IntervalStyle) {
	oldStyle := m.data.DataConversionConfig.IntervalStyle
	m.data.DataConversionConfig.IntervalStyle = style
	if oldStyle != style {
		m.bufferParamStatusUpdate("IntervalStyle", strings.ToLower(style.String()))
	}
}

// SetDateStyle sets the DateStyle for the given session.
func (m *sessionDataMutator) SetDateStyle(style pgdate.DateStyle) {
	oldStyle := m.data.DataConversionConfig.DateStyle
	m.data.DataConversionConfig.DateStyle = style
	if oldStyle != style {
		m.bufferParamStatusUpdate("DateStyle", style.SQLString())
	}
}

// SetStubCatalogTablesEnabled sets default value for stub_catalog_tables.
func (m *sessionDataMutator) SetStubCatalogTablesEnabled(enabled bool) {
	m.data.StubCatalogTablesEnabled = enabled
}

func (m *sessionDataMutator) SetExperimentalComputedColumnRewrites(val string) {
	m.data.ExperimentalComputedColumnRewrites = val
}

func (m *sessionDataMutator) SetNullOrderedLast(b bool) {
	m.data.NullOrderedLast = b
}

func (m *sessionDataMutator) SetPropagateInputOrdering(b bool) {
	m.data.PropagateInputOrdering = b
}

func (m *sessionDataMutator) SetTxnRowsWrittenLog(val int64) {
	m.data.TxnRowsWrittenLog = val
}

func (m *sessionDataMutator) SetTxnRowsWrittenErr(val int64) {
	m.data.TxnRowsWrittenErr = val
}

func (m *sessionDataMutator) SetTxnRowsReadLog(val int64) {
	m.data.TxnRowsReadLog = val
}

func (m *sessionDataMutator) SetTxnRowsReadErr(val int64) {
	m.data.TxnRowsReadErr = val
}

func (m *sessionDataMutator) SetLargeFullScanRows(val float64) {
	m.data.LargeFullScanRows = val
}

func (m *sessionDataMutator) SetInjectRetryErrorsEnabled(val bool) {
	m.data.InjectRetryErrorsEnabled = val
}

func (m *sessionDataMutator) SetJoinReaderOrderingStrategyBatchSize(val int64) {
	m.data.JoinReaderOrderingStrategyBatchSize = val
}

func (m *sessionDataMutator) SetJoinReaderNoOrderingStrategyBatchSize(val int64) {
	m.data.JoinReaderNoOrderingStrategyBatchSize = val
}

func (m *sessionDataMutator) SetJoinReaderIndexJoinStrategyBatchSize(val int64) {
	m.data.JoinReaderIndexJoinStrategyBatchSize = val
}

func (m *sessionDataMutator) SetIndexJoinStreamerBatchSize(val int64) {
	m.data.IndexJoinStreamerBatchSize = val
}

func (m *sessionDataMutator) SetParallelizeMultiKeyLookupJoinsEnabled(val bool) {
	m.data.ParallelizeMultiKeyLookupJoinsEnabled = val
}

// TODO(harding): Remove this when costing scans based on average column size
// is fully supported.
func (m *sessionDataMutator) SetCostScansWithDefaultColSize(val bool) {
	m.data.CostScansWithDefaultColSize = val
}

func (m *sessionDataMutator) SetEnableImplicitTransactionForBatchStatements(val bool) {
	m.data.EnableImplicitTransactionForBatchStatements = val
}

func (m *sessionDataMutator) SetExpectAndIgnoreNotVisibleColumnsInCopy(val bool) {
	m.data.ExpectAndIgnoreNotVisibleColumnsInCopy = val
}

func (m *sessionDataMutator) SetMultipleModificationsOfTable(val bool) {
	m.data.MultipleModificationsOfTable = val
}

func (m *sessionDataMutator) SetShowPrimaryKeyConstraintOnNotVisibleColumns(val bool) {
	m.data.ShowPrimaryKeyConstraintOnNotVisibleColumns = val
}

func (m *sessionDataMutator) SetTestingOptimizerRandomSeed(val int64) {
	m.data.TestingOptimizerRandomSeed = val
}

func (m *sessionDataMutator) SetTestingOptimizerCostPerturbation(val float64) {
	m.data.TestingOptimizerCostPerturbation = val
}

func (m *sessionDataMutator) SetTestingOptimizerDisableRuleProbability(val float64) {
	m.data.TestingOptimizerDisableRuleProbability = val
}

func (m *sessionDataMutator) SetTrigramSimilarityThreshold(val float64) {
	m.data.TrigramSimilarityThreshold = val
}

func (m *sessionDataMutator) SetUnconstrainedNonCoveringIndexScanEnabled(val bool) {
	m.data.UnconstrainedNonCoveringIndexScanEnabled = val
}

func (m *sessionDataMutator) SetDisableHoistProjectionInJoinLimitation(val bool) {
	m.data.DisableHoistProjectionInJoinLimitation = val
}

func (m *sessionDataMutator) SetTroubleshootingModeEnabled(val bool) {
	m.data.TroubleshootingMode = val
}

func (m *sessionDataMutator) SetCopyFastPathEnabled(val bool) {
	m.data.CopyFastPathEnabled = val
}

func (m *sessionDataMutator) SetCopyFromAtomicEnabled(val bool) {
	m.data.CopyFromAtomicEnabled = val
}

func (m *sessionDataMutator) SetCopyFromRetriesEnabled(val bool) {
	m.data.CopyFromRetriesEnabled = val
}

func (m *sessionDataMutator) SetDeclareCursorStatementTimeoutEnabled(val bool) {
	m.data.DeclareCursorStatementTimeoutEnabled = val
}

func (m *sessionDataMutator) SetEnforceHomeRegion(val bool) {
	m.data.EnforceHomeRegion = val
}

func (m *sessionDataMutator) SetVariableInequalityLookupJoinEnabled(val bool) {
	m.data.VariableInequalityLookupJoinEnabled = val
}

func (m *sessionDataMutator) SetExperimentalHashGroupJoinEnabled(val bool) {
	m.data.ExperimentalHashGroupJoinEnabled = val
}

func (m *sessionDataMutator) SetAllowOrdinalColumnReference(val bool) {
	m.data.AllowOrdinalColumnReferences = val
}

func (m *sessionDataMutator) SetOptimizerUseImprovedDisjunctionStats(val bool) {
	m.data.OptimizerUseImprovedDisjunctionStats = val
}

func (m *sessionDataMutator) SetOptimizerUseLimitOrderingForStreamingGroupBy(val bool) {
	m.data.OptimizerUseLimitOrderingForStreamingGroupBy = val
}

func (m *sessionDataMutator) SetOptimizerUseImprovedSplitDisjunctionForJoins(val bool) {
	m.data.OptimizerUseImprovedSplitDisjunctionForJoins = val
}

func (m *sessionDataMutator) SetInjectRetryErrorsOnCommitEnabled(val bool) {
	m.data.InjectRetryErrorsOnCommitEnabled = val
}

func (m *sessionDataMutator) SetEnforceHomeRegionFollowerReadsEnabled(val bool) {
	m.data.EnforceHomeRegionFollowerReadsEnabled = val
}

func (m *sessionDataMutator) SetOptimizerAlwaysUseHistograms(val bool) {
	m.data.OptimizerAlwaysUseHistograms = val
}

func (m *sessionDataMutator) SetOptimizerHoistUncorrelatedEqualitySubqueries(val bool) {
	m.data.OptimizerHoistUncorrelatedEqualitySubqueries = val
}

func (m *sessionDataMutator) SetOptimizerUseImprovedComputedColumnFiltersDerivation(val bool) {
	m.data.OptimizerUseImprovedComputedColumnFiltersDerivation = val
}

func (m *sessionDataMutator) SetEnableCreateStatsUsingExtremes(val bool) {
	m.data.EnableCreateStatsUsingExtremes = val
}

func (m *sessionDataMutator) SetAllowRoleMembershipsToChangeDuringTransaction(val bool) {
	m.data.AllowRoleMembershipsToChangeDuringTransaction = val
}

func (m *sessionDataMutator) SetDefaultTextSearchConfig(val string) {
	m.data.DefaultTextSearchConfig = val
}

func (m *sessionDataMutator) SetPreparedStatementsCacheSize(val int64) {
	m.data.PreparedStatementsCacheSize = val
}

func (m *sessionDataMutator) SetStreamerEnabled(val bool) {
	m.data.StreamerEnabled = val
}

func (m *sessionDataMutator) SetMultipleActivePortalsEnabled(val bool) {
	m.data.MultipleActivePortalsEnabled = val
}

func (m *sessionDataMutator) SetUnboundedParallelScans(val bool) {
	m.data.UnboundedParallelScans = val
}

func (m *sessionDataMutator) SetReplicationMode(val sessiondatapb.ReplicationMode) {
	m.data.ReplicationMode = val
}

// Utility functions related to scrubbing sensitive information on SQL Stats.

// quantizeCounts ensures that the Count field in the
// appstatspb.StatementStatistics is bucketed to the order of magnitude base 10s
// and recomputes the squared differences using the new Count value.
func quantizeCounts(d *appstatspb.StatementStatistics) {
	oldCount := d.Count
	newCount := telemetry.Bucket10(oldCount)
	d.Count = newCount
	// The SquaredDiffs values are meant to enable computing the variance
	// via the formula variance = squareddiffs / (count - 1).
	// Since we're adjusting the count, we must re-compute a value
	// for SquaredDiffs that keeps the same variance with the new count.
	oldCountMinusOne := float64(oldCount - 1)
	newCountMinusOne := float64(newCount - 1)
	d.NumRows.SquaredDiffs = (d.NumRows.SquaredDiffs / oldCountMinusOne) * newCountMinusOne
	d.ParseLat.SquaredDiffs = (d.ParseLat.SquaredDiffs / oldCountMinusOne) * newCountMinusOne
	d.PlanLat.SquaredDiffs = (d.PlanLat.SquaredDiffs / oldCountMinusOne) * newCountMinusOne
	d.RunLat.SquaredDiffs = (d.RunLat.SquaredDiffs / oldCountMinusOne) * newCountMinusOne
	d.ServiceLat.SquaredDiffs = (d.ServiceLat.SquaredDiffs / oldCountMinusOne) * newCountMinusOne
	d.OverheadLat.SquaredDiffs = (d.OverheadLat.SquaredDiffs / oldCountMinusOne) * newCountMinusOne

	d.MaxRetries = telemetry.Bucket10(d.MaxRetries)

	d.FirstAttemptCount = int64((float64(d.FirstAttemptCount) / float64(oldCount)) * float64(newCount))
}

func scrubStmtStatKey(vt VirtualTabler, key string, ns eval.ClientNoticeSender) (string, bool) {
	// Re-parse the statement to obtain its AST.
	stmt, err := parser.ParseOne(key)
	if err != nil {
		return "", false
	}

	// Re-format to remove most names.
	f := tree.NewFmtCtx(
		tree.FmtAnonymize,
		tree.FmtReformatTableNames(hideNonVirtualTableNameFunc(vt, ns)),
	)
	f.FormatNode(stmt.AST)
	return f.CloseAndGetString(), true
}

// formatStmtKeyAsRedactableString given an AST node this function will fully
// qualify names using annotations to format it out into a redactable string.
func formatStmtKeyAsRedactableString(
	vt VirtualTabler,
	rootAST tree.Statement,
	ann *tree.Annotations,
	fs tree.FmtFlags,
	ns eval.ClientNoticeSender,
) redact.RedactableString {
	f := tree.NewFmtCtx(
		tree.FmtAlwaysQualifyTableNames|tree.FmtMarkRedactionNode|fs,
		tree.FmtAnnotations(ann),
		tree.FmtReformatTableNames(hideNonVirtualTableNameFunc(vt, ns)))
	f.FormatNode(rootAST)
	formattedRedactableStatementString := f.CloseAndGetString()
	return redact.RedactableString(formattedRedactableStatementString)
}

// FailedHashedValue is used as a default return value for when HashForReporting
// cannot hash a value correctly.
const FailedHashedValue = "unknown"

// HashForReporting 1-way hashes values for use in stat reporting. The secret
// should be the cluster.secret setting.
func HashForReporting(secret, appName string) string {
	// If no secret is provided, we cannot irreversibly hash the value, so return
	// a default value.
	if len(secret) == 0 {
		return FailedHashedValue
	}
	hash := hmac.New(sha256.New, []byte(secret))
	if _, err := hash.Write([]byte(appName)); err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err,
			`"It never returns an error." -- https://golang.org/pkg/hash`))
	}
	return hex.EncodeToString(hash.Sum(nil)[:4])
}

// formatStatementHideConstants formats the statement using
// tree.FmtHideConstants. It does *not* anonymize the statement, since
// the result will still contain names and identifiers.
func formatStatementHideConstants(ast tree.Statement) string {
	if ast == nil {
		return ""
	}
	return tree.AsStringWithFlags(ast, tree.FmtHideConstants)
}

// formatStatementSummary formats the statement using tree.FmtSummary
// and tree.FmtHideConstants. This returns a summarized version of the
// query. It does *not* anonymize the statement, since the result will
// still contain names and identifiers.
func formatStatementSummary(ast tree.Statement) string {
	if ast == nil {
		return ""
	}
	fmtFlags := tree.FmtSummary | tree.FmtHideConstants
	return tree.AsStringWithFlags(ast, fmtFlags)
}

// DescsTxn is a convenient method for running a transaction on descriptors
// when you have an ExecutorConfig.
//
// TODO(ajwerner): Remove this now that it is such a thin shim.
func DescsTxn(
	ctx context.Context,
	execCfg *ExecutorConfig,
	f func(ctx context.Context, txn isql.Txn, col *descs.Collection) error,
) error {
	return execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		return f(ctx, txn, txn.Descriptors())
	})
}

// TestingDescsTxn is a convenience function for running a transaction on
// descriptors when you have a serverutils.TestServerInterface.
func TestingDescsTxn(
	ctx context.Context,
	s serverutils.TestServerInterface,
	f func(ctx context.Context, txn isql.Txn, col *descs.Collection) error,
) error {
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	return DescsTxn(ctx, &execCfg, f)
}

// NewRowMetrics creates a rowinfra.Metrics struct for either internal or user
// queries.
func NewRowMetrics(internal bool) rowinfra.Metrics {
	return rowinfra.Metrics{
		MaxRowSizeLogCount: metric.NewCounter(getMetricMeta(rowinfra.MetaMaxRowSizeLog, internal)),
		MaxRowSizeErrCount: metric.NewCounter(getMetricMeta(rowinfra.MetaMaxRowSizeErr, internal)),
	}
}

// GetRowMetrics returns the proper rowinfra.Metrics for either internal or user
// queries.
func (cfg *ExecutorConfig) GetRowMetrics(internal bool) *rowinfra.Metrics {
	if internal {
		return cfg.InternalRowMetrics
	}
	return cfg.RowMetrics
}

// RequireSystemTenantOrClusterSetting returns a setting disabled error if
// executed from inside a secondary tenant that does not have the specified
// cluster setting.
func (cfg *ExecutorConfig) RequireSystemTenantOrClusterSetting(
	setting *settings.BoolSetting,
) error {
	if cfg.Codec.ForSystemTenant() || setting.Get(&cfg.Settings.SV) {
		return nil
	}
	return errors.Newf("tenant cluster setting %s disabled", setting.Key())
}
