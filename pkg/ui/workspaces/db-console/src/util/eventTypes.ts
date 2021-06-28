// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// NOTE: This file is kept in sync manually with sql/event_log.go

import _ from "lodash";

import * as protos from "src/js/protos";

type Event = protos.cockroach.server.serverpb.EventsResponse.Event;

// Recorded when a database is created.
export const CREATE_DATABASE = "create_database";
// Recorded when a database is dropped.
export const DROP_DATABASE = "drop_database";
// Recorded when a database is renamed.
export const RENAME_DATABASE = "rename_database";
// Recorded when a database's owner is changed.
export const ALTER_DATABASE_OWNER = "alter_database_owner";
// Recorded when a region is added to a database.
export const ALTER_DATABASE_ADD_REGION = "alter_database_add_region";
// Recorded when a region is dropped from a database.
export const ALTER_DATABASE_DROP_REGION = "alter_database_drop_region";
// Recorded when the primary region of a database is altered.
export const ALTER_DATABASE_PRIMARY_REGION = "alter_database_primary_region";
// Recorded when the survival goal of a database is altered.
export const ALTER_DATABASE_SURVIVAL_GOAL = "alter_database_survival_goal";
// Recorded when a table's owner is changed.
export const ALTER_TABLE_OWNER = "alter_table_owner";
// Recorded when a table is created.
export const CREATE_TABLE = "create_table";
// Recorded when a table is dropped.
export const DROP_TABLE = "drop_table";
// Recorded when a table is renamed.
export const RENAME_TABLE = "rename_table";
// Recorded when a table is truncated.
export const TRUNCATE_TABLE = "truncate_table";
// Recorded when a table is altered.
export const ALTER_TABLE = "alter_table";
// Recorded when an index is created.
export const CREATE_INDEX = "create_index";
// Recorded when an index is dropped.
export const DROP_INDEX = "drop_index";
// Recorded when an index is altered.
export const ALTER_INDEX = "alter_index";
// Recorded when a view is created.
export const CREATE_VIEW = "create_view";
// Recorded when a view is dropped.
export const DROP_VIEW = "drop_view";
// Recorded when a type is created.
export const CREATE_TYPE = "create_type";
// Recorded when a type is altered.
export const ALTER_TYPE = "alter_type";
// Recorded when a type's owner is changed.
export const ALTER_TYPE_OWNER = "alter_type_owner";
// Recorded when a database's comment is changed.
export const COMMENT_ON_DATABASE = "comment_on_database";
// Recorded when a table's comment is changed.
export const COMMENT_ON_TABLE = "comment_on_table";
// Recorded when a index's comment is changed.
export const COMMENT_ON_INDEX = "comment_on_index";
// Recorded when a column's comment is changed.
export const COMMENT_ON_COLUMN = "comment_on_column";
// Recorded when a type is dropped.
export const DROP_TYPE = "drop_type";
// Recorded when a type is renamed.
export const RENAME_TYPE = "rename_type";
// Recorded when a sequence is created.
export const CREATE_SEQUENCE = "create_sequence";
// Recorded when a sequence is altered.
export const ALTER_SEQUENCE = "alter_sequence";
// Recorded when a sequence is dropped.
export const DROP_SEQUENCE = "drop_sequence";
// Recorded when an in-progress schema change encounters a problem and is
// reversed.
export const REVERSE_SCHEMA_CHANGE = "reverse_schema_change";
// Recorded when a previously initiated schema change has completed.
export const FINISH_SCHEMA_CHANGE = "finish_schema_change";
// Recorded when a schema change rollback has completed.
export const FINISH_SCHEMA_CHANGE_ROLLBACK = "finish_schema_change_rollback";
// Recorded when a node joins the cluster.
export const NODE_JOIN = "node_join";
// Recorded when an existing node rejoins the cluster after being offline.
export const NODE_RESTART = "node_restart";
// Recorded when a node is marked for decommissioning.
export const NODE_DECOMMISSIONING = "node_decommissioning";
// Recorded when a node is marked as decommissioned.
export const NODE_DECOMMISSIONED = "node_decommissioned";
// Recorded when a decommissioning node is recommissioned.
export const NODE_RECOMMISSIONED = "node_recommissioned";
// Recorded when a cluster setting is changed.
export const SET_CLUSTER_SETTING = "set_cluster_setting";
// Recorded when a zone config is changed.
export const SET_ZONE_CONFIG = "set_zone_config";
// Recorded when a zone config is removed.
export const REMOVE_ZONE_CONFIG = "remove_zone_config";
// Recorded when statistics are collected for a table.
export const CREATE_STATISTICS = "create_statistics";
// Recorded when privileges are added to a user.
export const CHANGE_DATABASE_PRIVILEGE = "change_database_privilege";
// Recorded when privileges are added to a user.
export const CHANGE_TABLE_PRIVILEGE = "change_table_privilege";
// Recorded when privileges are added to a user.
export const CHANGE_SCHEMA_PRIVILEGE = "change_schema_privilege";
// Recorded when privileges are added to a user.
export const CHANGE_TYPE_PRIVILEGE = "change_type_privilege";
// Recorded when a schema is set.
export const SET_SCHEMA = "set_schema";
// Recorded when a schema is created.
export const CREATE_SCHEMA = "create_schema";
// Recorded when a schema is dropped.
export const DROP_SCHEMA = "drop_schema";
// Recorded when a schema is renamed.
export const RENAME_SCHEMA = "rename_schema";
// Recorded when a schema's owner is changed.
export const ALTER_SCHEMA_OWNER = "alter_schema_owner";
// Recorded when a database is converted to a schema.
export const CONVERT_TO_SCHEMA = "convert_to_schema";
// Recorded when a role is created.
export const CREATE_ROLE = "create_role";
// Recorded when a role is dropped.
export const DROP_ROLE = "drop_role";
// Recorded when a role is altered.
export const ALTER_ROLE = "alter_role";
// Recorded when an import job is in different stages of execution.
export const IMPORT = "import";
// Recorded when a restore job is in different stages of execution.
export const RESTORE = "restore";
// Recorded when crdb_internal.unsafe_delete_descriptor is executed.
export const UNSAFE_DELETE_DESCRIPTOR = "unsafe_delete_descriptor";
// Recorded when crdb_internal.unsafe_delete_namespace_entry is executed.
export const UNSAFE_DELETE_NAMESPACE_ENTRY = "unsafe_delete_namespace_entry";
// Recorded when crdb_internal.unsafe_upsert_descriptor is executed.
export const UNSAFE_UPSERT_DESCRIPTOR = "unsafe_upsert_descriptor";
// Recorded when crdb_internal.unsafe_upsert_namespace_entry is executed.
export const UNSAFE_UPSERT_NAMESPACE_ENTRY = "unsafe_upsert_namespace_entry";

// Node Event Types
export const nodeEvents = [
  NODE_JOIN,
  NODE_RESTART,
  NODE_DECOMMISSIONING,
  NODE_DECOMMISSIONED,
  NODE_RECOMMISSIONED,
];
export const databaseEvents = [CREATE_DATABASE, DROP_DATABASE];
export const tableEvents = [
  CREATE_TABLE,
  DROP_TABLE,
  TRUNCATE_TABLE,
  ALTER_TABLE,
  CREATE_INDEX,
  ALTER_INDEX,
  DROP_INDEX,
  CREATE_VIEW,
  DROP_VIEW,
  REVERSE_SCHEMA_CHANGE,
  FINISH_SCHEMA_CHANGE,
  FINISH_SCHEMA_CHANGE_ROLLBACK,
];
export const settingsEvents = [
  SET_CLUSTER_SETTING,
  SET_ZONE_CONFIG,
  REMOVE_ZONE_CONFIG,
];
export const jobEvents = [IMPORT, RESTORE];
export const allEvents = [
  ...nodeEvents,
  ...databaseEvents,
  ...tableEvents,
  ...settingsEvents,
  ...jobEvents,
];

const nodeEventSet = _.invert(nodeEvents);
const databaseEventSet = _.invert(databaseEvents);
const tableEventSet = _.invert(tableEvents);
const settingsEventSet = _.invert(settingsEvents);
const jobsEventSet = _.invert(jobEvents);

export function isNodeEvent(e: Event): boolean {
  return !_.isUndefined(nodeEventSet[e.event_type]);
}

export function isDatabaseEvent(e: Event): boolean {
  return !_.isUndefined(databaseEventSet[e.event_type]);
}

export function isTableEvent(e: Event): boolean {
  return !_.isUndefined(tableEventSet[e.event_type]);
}

export function isSettingsEvent(e: Event): boolean {
  return !_.isUndefined(settingsEventSet[e.event_type]);
}

export function isJobsEvent(e: Event): boolean {
  return !_.isUndefined(jobsEventSet[e.event_type]);
}
