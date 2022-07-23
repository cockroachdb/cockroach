// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clustermode

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
)

// RequireExplicitPrimaryKeysClusterMode is a cluster mode setting about
// requiring explicit primary keys in CREATE TABLE statements.
var RequireExplicitPrimaryKeysClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.require_explicit_primary_keys.enabled",
	"default value for requiring explicit primary keys in CREATE TABLE statements",
	false,
).WithPublic()

// PlacementEnabledClusterMode is a cluster mode setting about
//  allowing the use of PLACEMENT RESTRICTED.
var PlacementEnabledClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.multiregion_placement_policy.enabled",
	"default value for enable_multiregion_placement_policy;"+
		" allows for use of PLACEMENT RESTRICTED",
	false,
)

// AutoRehomingEnabledClusterMode is a cluster mode setting about
// allowing rows in REGIONAL BY ROW tables to be auto-rehomed on UPDATE.
var AutoRehomingEnabledClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.experimental_auto_rehoming.enabled",
	"default value for experimental_enable_auto_rehoming;"+
		" allows for rows in REGIONAL BY ROW tables to be auto-rehomed on UPDATE",
	false,
).WithPublic()

// OnUpdateRehomeRowEnabledClusterMode is a cluster mode setting about
// enabling ON UPDATE rehome_row() expressions to trigger on updates.
var OnUpdateRehomeRowEnabledClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.on_update_rehome_row.enabled",
	"default value for on_update_rehome_row;"+
		" enables ON UPDATE rehome_row() expressions to trigger on updates",
	true,
).WithPublic()

// TemporaryTablesEnabledClusterMode is a cluster mode setting about
// allowing use of temporary tables by default.
var TemporaryTablesEnabledClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.experimental_temporary_tables.enabled",
	"default value for experimental_enable_temp_tables; allows for use of temporary tables by default",
	false,
).WithPublic()

// ImplicitColumnPartitioningEnabledClusterMode is a cluster mode setting about
// allowing the use of implicit column partitioning.
var ImplicitColumnPartitioningEnabledClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.experimental_implicit_column_partitioning.enabled",
	"default value for experimental_enable_temp_tables; allows for the use of implicit column partitioning",
	false,
).WithPublic()

// OverrideMultiRegionZoneConfigClusterMode is a cluster mode setting about
// allowing overriding the zone configs of a multi-region table or database.
var OverrideMultiRegionZoneConfigClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.override_multi_region_zone_config.enabled",
	"default value for override_multi_region_zone_config; "+
		"allows for overriding the zone configs of a multi-region table or database",
	false,
).WithPublic()

// ZigzagJoinClusterMode is a cluster mode setting about allowing use of
// zig-zag join by default.
var ZigzagJoinClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.zigzag_join.enabled",
	"default value for enable_zigzag_join session setting; allows use of zig-zag join by default",
	true,
).WithPublic()

// OptUseHistogramsClusterMode controls the cluster default for whether
// histograms are used by the optimizer for cardinality estimation.
// Note that it does not control histogram collection; regardless of the
// value of this setting, the optimizer cannot use histograms if they
// haven't been collected. Collection of histograms is controlled by the
// cluster setting sql.stats.histogram_collection.enabled.
var OptUseHistogramsClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.optimizer_use_histograms.enabled",
	"default value for optimizer_use_histograms session setting; enables usage of histograms in the optimizer by default",
	true,
).WithPublic()

// OptUseMultiColStatsClusterMode controls the cluster default for whether
// multi-column stats are used by the optimizer for cardinality estimation.
// Note that it does not control collection of multi-column stats; regardless
// of the value of this setting, the optimizer cannot use multi-column stats
// if they haven't been collected. Collection of multi-column stats is
// controlled by the cluster setting sql.stats.multi_column_collection.enabled.
var OptUseMultiColStatsClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.optimizer_use_multicol_stats.enabled",
	"default value for optimizer_use_multicol_stats session setting; enables usage of multi-column stats in the optimizer by default",
	true,
).WithPublic()

// LocalityOptimizedSearchMode controls the cluster default for the use of
// locality optimized search. If enabled, the optimizer will try to plan scans
// and lookup joins in which local nodes (i.e., nodes in the gateway region) are
// searched for matching rows before remote nodes, in the hope that the
// execution engine can avoid visiting remote nodes.
var LocalityOptimizedSearchMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.locality_optimized_partitioned_index_scan.enabled",
	"default value for locality_optimized_partitioned_index_scan session setting; "+
		"enables searching for rows in the current region before searching remote regions",
	true,
).WithPublic()

// ImplicitSelectForUpdateClusterMode is a cluster mode setting about
// enabling FOR UPDATE locking during the row-fetch phase of mutation statements.
var ImplicitSelectForUpdateClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.implicit_select_for_update.enabled",
	"default value for enable_implicit_select_for_update session setting; enables FOR UPDATE locking during the row-fetch phase of mutation statements",
	true,
).WithPublic()

// InsertFastPathClusterMode is a cluster mode setting about enabling a
// specialized insert path
var InsertFastPathClusterMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.insert_fast_path.enabled",
	"default value for enable_insert_fast_path session setting; enables a specialized insert path",
	true,
).WithPublic()

// ExperimentalAlterColumnTypeGeneralMode is a cluster mode setting about
// enabling the use of ALTER COLUMN TYPE for general conversions.
var ExperimentalAlterColumnTypeGeneralMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.experimental_alter_column_type.enabled",
	"default value for experimental_alter_column_type session setting; "+
		"enables the use of ALTER COLUMN TYPE for general conversions",
	false,
).WithPublic()

// ClusterStatementTimeout is a setting about controlling the duration a query
// is permitted to run before it is canceled; if set to 0, there is no timeout.
var ClusterStatementTimeout = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.defaults.statement_timeout",
	"default value for the statement_timeout; "+
		"default value for the statement_timeout session setting; controls the "+
		"duration a query is permitted to run before it is canceled; if set to 0, "+
		"there is no timeout",
	0,
	settings.NonNegativeDuration,
).WithPublic()

// ClusterLockTimeout is a setting about controlling the duration a query is
// permitted to wait while attempting to acquire a lock on a key or while
// blocking on an existing lock in order to perform a non-locking read on a key;
// if set to 0, there is no timeout.
var ClusterLockTimeout = settings.RegisterDurationSetting(
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

// ClusterIdleInSessionTimeout is a setting about controlling the duration a
// session is permitted to idle before the session is terminated; if set to 0,
// there is no timeout.
var ClusterIdleInSessionTimeout = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.defaults.idle_in_session_timeout",
	"default value for the idle_in_session_timeout; "+
		"default value for the idle_in_session_timeout session setting; controls the "+
		"duration a session is permitted to idle before the session is terminated; "+
		"if set to 0, there is no timeout",
	0,
	settings.NonNegativeDuration,
).WithPublic()

// ClusterIdleInTransactionSessionTimeout is a setting about controlling the
// duration a session is permitted to idle in a transaction before the session
// is terminated; if set to 0, there is no timeout.
var ClusterIdleInTransactionSessionTimeout = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.defaults.idle_in_transaction_session_timeout",
	"default value for the idle_in_transaction_session_timeout; controls the "+
		"duration a session is permitted to idle in a transaction before the "+
		"session is terminated; if set to 0, there is no timeout",
	0,
	settings.NonNegativeDuration,
).WithPublic()

// ExperimentalUniqueWithoutIndexConstraintsMode is a setting about
// disabling unique without index constraints by default.
// TODO(rytaft): remove this once unique without index constraints are fully
// supported.
var ExperimentalUniqueWithoutIndexConstraintsMode = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.defaults.experimental_enable_unique_without_index_constraints.enabled",
	"default value for experimental_enable_unique_without_index_constraints session setting;"+
		"disables unique without index constraints by default",
	false,
).WithPublic()

// ExperimentalDistSQLPlanningClusterMode can be used to enable
// optimizer-driven DistSQL planning that sidesteps intermediate planNode
// transition when going from opt.Expr to DistSQL processor specs.
var ExperimentalDistSQLPlanningClusterMode = settings.RegisterEnumSetting(
	settings.TenantWritable,
	ExperimentalDistSQLPlanningClusterSettingName,
	"default experimental_distsql_planning mode; enables experimental opt-driven DistSQL planning",
	"off",
	map[int64]string{
		int64(sessiondatapb.ExperimentalDistSQLPlanningOff): "off",
		int64(sessiondatapb.ExperimentalDistSQLPlanningOn):  "on",
	},
).WithPublic()

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

// ExperimentalDistSQLPlanningClusterSettingName is the name for the cluster
// setting that controls experimentalDistSQLPlanningClusterMode below.
const ExperimentalDistSQLPlanningClusterSettingName = "sql.defaults.experimental_distsql_planning"

// VectorizeClusterSettingName is the name for the cluster setting that controls
// the VectorizeClusterMode below.
const VectorizeClusterSettingName = "sql.defaults.vectorize"
