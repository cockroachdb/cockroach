// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as protobuf from "protobufjs/minimal";

import * as protos from "src/js/protos";
import * as eventTypes from "src/util/eventTypes";

type Event$Properties = protos.cockroach.server.serverpb.EventsResponse.IEvent;

/**
 * getEventDescription returns a short summary of an event.
 */
export function getEventDescription(e: Event$Properties): string {
  const info: EventInfo = protobuf.util.isset(e, "info") ? JSON.parse(e.info) : {};
  const targetId: number = e.target_id ? e.target_id.toNumber() : null;

  switch (e.event_type) {
    case eventTypes.CREATE_DATABASE:
      return `Database Created: User ${info.User} created database ${info.DatabaseName}`;
    case eventTypes.DROP_DATABASE:
      const tableDropText = getDroppedObjectsText(info);
      return `Database Dropped: User ${info.User} dropped database ${info.DatabaseName}. ${tableDropText}`;
    case eventTypes.CREATE_TABLE:
      return `Table Created: User ${info.User} created table ${info.TableName}`;
    case eventTypes.DROP_TABLE:
      return `Table Dropped: User ${info.User} dropped table ${info.TableName}`;
    case eventTypes.TRUNCATE_TABLE:
      return `Table Truncated: User ${info.User} truncated table ${info.TableName}`;
    case eventTypes.ALTER_TABLE:
      return `Schema Change: User ${info.User} began a schema change to alter table ${info.TableName} with ID ${info.MutationID}`;
    case eventTypes.CREATE_INDEX:
      return `Schema Change: User ${info.User} began a schema change to create an index ${info.IndexName} on table ${info.TableName} with ID ${info.MutationID}`;
    case eventTypes.DROP_INDEX:
      return `Schema Change: User ${info.User} began a schema change to drop index ${info.IndexName} on table ${info.TableName} with ID ${info.MutationID}`;
    case eventTypes.ALTER_INDEX:
      return `Schema Change: User ${info.User} began a schema change to alter index ${info.IndexName} on table ${info.TableName} with ID ${info.MutationID}`;
    case eventTypes.CREATE_VIEW:
      return `View Created: User ${info.User} created view ${info.ViewName}`;
    case eventTypes.DROP_VIEW:
      return `View Dropped: User ${info.User} dropped view ${info.ViewName}`;
    case eventTypes.CREATE_SEQUENCE:
      return `Sequence Created: User ${info.User} created sequence ${info.SequenceName}`;
    case eventTypes.ALTER_SEQUENCE:
      return `Sequence Altered: User ${info.User} altered sequence ${info.SequenceName}`;
    case eventTypes.DROP_SEQUENCE:
      return `Sequence Dropped: User ${info.User} dropped sequence ${info.SequenceName}`;
    case eventTypes.REVERSE_SCHEMA_CHANGE:
      return `Schema Change Reversed: Schema change with ID ${info.MutationID} was reversed.`;
    case eventTypes.FINISH_SCHEMA_CHANGE:
      return `Schema Change Completed: Schema change with ID ${info.MutationID} was completed.`;
    case eventTypes.FINISH_SCHEMA_CHANGE_ROLLBACK:
      return `Schema Change Rollback Completed: Rollback of schema change with ID ${info.MutationID} was completed.`;
    case eventTypes.NODE_JOIN:
      return `Node Joined: Node ${targetId} joined the cluster`;
    case eventTypes.NODE_DECOMMISSIONED:
      return `Node Decommissioned: Node ${targetId} was decommissioned`;
    case eventTypes.NODE_RECOMMISSIONED:
      return `Node Recommissioned: Node ${targetId} was recommissioned`;
    case eventTypes.NODE_RESTART:
      return `Node Rejoined: Node ${targetId} rejoined the cluster`;
    case eventTypes.SET_CLUSTER_SETTING:
      if (info.Value && info.Value.length > 0) {
        return `Cluster Setting Changed: User ${info.User} set ${info.SettingName} to ${info.Value}`;
      }
      return `Cluster Setting Changed: User ${info.User} changed ${info.SettingName}`;
    case eventTypes.SET_ZONE_CONFIG:
      return `Zone Config Changed: User ${info.User} set the zone config for ${info.Target} to ${info.Config}`;
    case eventTypes.REMOVE_ZONE_CONFIG:
      return `Zone Config Removed: User ${info.User} removed the zone config for ${info.Target}`;
    case eventTypes.CREATE_STATISTICS:
      return `Table statistics refreshed for ${info.TableName}`;
    default:
      return `Unknown Event Type: ${e.event_type}, content: ${JSON.stringify(info, null, 2)}`;
  }
}

// EventInfo corresponds to the `info` column of the `system.eventlog` table
// and the `info` field of the `server.serverpb.EventsResponse.Event` proto.
export interface EventInfo {
  User: string;
  DatabaseName?: string;
  TableName?: string;
  IndexName?: string;
  MutationID?: string;
  ViewName?: string;
  SequenceName?: string;
  SettingName?: string;
  Value?: string;
  Target?: string;
  Config?: string;
  Statement?: string;
  // The following are three names for the same key (it was renamed twice).
  // All ar included for backwards compatibility.
  DroppedTables?: string[];
  DroppedTablesAndViews?: string[];
  DroppedSchemaObjects?: string[];
}

export function getDroppedObjectsText(eventInfo: EventInfo): string {
  const droppedObjects =
    eventInfo.DroppedSchemaObjects || eventInfo.DroppedTablesAndViews || eventInfo.DroppedTables;
  if (!droppedObjects) {
    return "";
  }
  if (droppedObjects.length === 0) {
    return "No schema objects were dropped.";
  } else if (droppedObjects.length === 1) {
    return `1 schema object was dropped: ${droppedObjects[0]}`;
  }
  return `${droppedObjects.length} schema objects were dropped: ${droppedObjects.join(", ")}`;
}
