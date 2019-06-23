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
// Recorded when a table is created.
export const CREATE_TABLE = "create_table";
// Recorded when a table is dropped.
export const DROP_TABLE = "drop_table";
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
// Recorded when a node is marked as decommissioning.
export const NODE_DECOMMISSIONED = "node_decommissioned";
// Recorded when a decommissioned node is recommissioned.
export const NODE_RECOMMISSIONED = "node_recommissioned";
// Recorded when a cluster setting is changed.
export const SET_CLUSTER_SETTING = "set_cluster_setting";
// Recorded when a zone config is changed.
export const SET_ZONE_CONFIG = "set_zone_config";
// Recorded when a zone config is removed.
export const REMOVE_ZONE_CONFIG = "remove_zone_config";
// Recorded when statistics are collected for a table.
export const CREATE_STATISTICS = "create_statistics";

// Node Event Types
export const nodeEvents = [NODE_JOIN, NODE_RESTART, NODE_DECOMMISSIONED, NODE_RECOMMISSIONED];
export const databaseEvents = [CREATE_DATABASE, DROP_DATABASE];
export const tableEvents = [
  CREATE_TABLE, DROP_TABLE, TRUNCATE_TABLE, ALTER_TABLE, CREATE_INDEX,
  ALTER_INDEX, DROP_INDEX, CREATE_VIEW, DROP_VIEW, REVERSE_SCHEMA_CHANGE,
  FINISH_SCHEMA_CHANGE, FINISH_SCHEMA_CHANGE_ROLLBACK,
];
export const settingsEvents = [SET_CLUSTER_SETTING, SET_ZONE_CONFIG, REMOVE_ZONE_CONFIG];
export const allEvents = [...nodeEvents, ...databaseEvents, ...tableEvents, ...settingsEvents];

const nodeEventSet = _.invert(nodeEvents);
const databaseEventSet = _.invert(databaseEvents);
const tableEventSet = _.invert(tableEvents);
const settingsEventSet = _.invert(settingsEvents);

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
