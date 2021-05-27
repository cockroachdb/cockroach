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
  const info: EventInfo = protobuf.util.isset(e, "info")
    ? JSON.parse(e.info)
    : {};
  let privs = "";
  let comment = "";

  switch (e.event_type) {
    case eventTypes.CREATE_DATABASE:
      return `Database Created: User ${info.User} created database ${info.DatabaseName}`;
    case eventTypes.DROP_DATABASE: {
      const tableDropText = getDroppedObjectsText(info);
      return `Database Dropped: User ${info.User} dropped database ${info.DatabaseName}. ${tableDropText}`;
    }
    case eventTypes.RENAME_DATABASE:
      return `Database Renamed: User ${info.User} renamed database ${info.DatabaseName} to ${info.NewDatabaseName}`;
    case eventTypes.ALTER_DATABASE_OWNER:
      return `Database Owner Altered: User ${info.User} altered the owner of database ${info.DatabaseName} to ${info.Owner}`;
    case eventTypes.ALTER_TABLE_OWNER:
      return `Table Owner Altered: User ${info.User} altered the owner of the table ${info.TableName} to ${info.Owner}`;
    case eventTypes.COMMENT_ON_DATABASE:
      if (info.NullComment) {
        comment += " removed the comment ";
      } else {
        comment += ' commented "' + info.Comment + '" ';
      }
      return `Comment: User ${info.User}${comment} on database ${info.DatabaseName}`;
    case eventTypes.COMMENT_ON_TABLE:
      if (info.NullComment) {
        comment += " removed the comment ";
      } else {
        comment += ' commented "' + info.Comment + '" ';
      }
      return `Comment: User ${info.User}${comment} on table ${info.TableName}`;
    case eventTypes.COMMENT_ON_INDEX:
      if (info.NullComment) {
        comment += " removed the comment ";
      } else {
        comment += ' commented "' + info.Comment + '" ';
      }
      return `Comment: User ${info.User}${comment} on index ${info.IndexName} of table ${info.TableName}`;
    case eventTypes.COMMENT_ON_COLUMN:
      if (info.NullComment) {
        comment += " removed the comment ";
      } else {
        comment += ' commented "' + info.Comment + '" ';
      }
      return `Comment: User ${info.User}${comment} on column ${info.ColumnName} of table ${info.TableName}`;
    case eventTypes.CREATE_TABLE:
      return `Table Created: User ${info.User} created table ${info.TableName}`;
    case eventTypes.DROP_TABLE:
      return `Table Dropped: User ${info.User} dropped table ${info.TableName}`;
    case eventTypes.RENAME_TABLE:
      return `Table Renamed: User ${info.User} renamed table ${info.TableName} to ${info.NewTableName}`;
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
    case eventTypes.CREATE_TYPE:
      return `Type Created: User ${info.User} created type ${info.TypeName}`;
    case eventTypes.ALTER_TYPE:
      return `Type Altered: User ${info.User} altered type ${info.TypeName}`;
    case eventTypes.ALTER_TYPE_OWNER:
      return `Type Owner Altered: User ${info.User} altered the owner of the type ${info.TypeName} to ${info.Owner}`;
    case eventTypes.DROP_TYPE:
      return `Type Dropped: User ${info.User} dropped type ${info.TypeName}`;
    case eventTypes.RENAME_TYPE:
      return `Type Renamed: User ${info.User} renamed type ${info.TypeName} to ${info.NewTypeName}`;
    case eventTypes.CREATE_SEQUENCE:
      return `Sequence Created: User ${info.User} created sequence ${info.SequenceName}`;
    case eventTypes.ALTER_SEQUENCE:
      return `Sequence Altered: User ${info.User} altered sequence ${info.SequenceName}`;
    case eventTypes.DROP_SEQUENCE:
      return `Sequence Dropped: User ${info.User} dropped sequence ${info.SequenceName}`;
    case eventTypes.REVERSE_SCHEMA_CHANGE:
      return `Schema Change Reversed: Schema change on descriptor ${info.DescriptorID} with ID ${info.MutationID} was reversed.`;
    case eventTypes.FINISH_SCHEMA_CHANGE:
      return `Schema Change Completed: Schema change on descriptor ${info.DescriptorID} with ID ${info.MutationID} was completed.`;
    case eventTypes.FINISH_SCHEMA_CHANGE_ROLLBACK:
      return `Schema Change Rollback Completed: Rollback of schema change on descriptor ${info.DescriptorID} with ID ${info.MutationID} was completed.`;
    case eventTypes.NODE_JOIN:
      return `Node Joined: Node ${info.NodeID} joined the cluster`;
    case eventTypes.NODE_DECOMMISSIONING:
      return `Node Decommissioning: Node ${info.TargetNodeID} was marked as decommissioning`;
    case eventTypes.NODE_DECOMMISSIONED:
      return `Node Decommissioned: Node ${info.TargetNodeID} was decommissioned`;
    case eventTypes.NODE_RECOMMISSIONED:
      return `Node Recommissioned: Node ${info.TargetNodeID} was recommissioned`;
    case eventTypes.NODE_RESTART:
      return `Node Rejoined: Node ${info.NodeID} rejoined the cluster`;
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
    case eventTypes.CHANGE_TABLE_PRIVILEGE:
      if (info.GrantedPrivileges && info.GrantedPrivileges.length > 0) {
        privs += " granted " + info.GrantedPrivileges;
      }
      if (info.RevokedPrivileges && info.RevokedPrivileges.length > 0) {
        privs += " revoked " + info.GrantedPrivileges;
      }
      return `Privilege change: User ${info.User}${privs} to ${info.Grantee} on table ${info.TableName}`;
    case eventTypes.CHANGE_SCHEMA_PRIVILEGE:
      if (info.GrantedPrivileges && info.GrantedPrivileges.length > 0) {
        privs += " granted " + info.GrantedPrivileges;
      }
      if (info.RevokedPrivileges && info.RevokedPrivileges.length > 0) {
        privs += " revoked " + info.GrantedPrivileges;
      }
      return `Privilege change: User ${info.User}${privs} to ${info.Grantee} on schema ${info.SchemaName}`;
    case eventTypes.CHANGE_DATABASE_PRIVILEGE:
      if (info.GrantedPrivileges && info.GrantedPrivileges.length > 0) {
        privs += " granted " + info.GrantedPrivileges;
      }
      if (info.RevokedPrivileges && info.RevokedPrivileges.length > 0) {
        privs += " revoked " + info.GrantedPrivileges;
      }
      return `Privilege change: User ${info.User}${privs} to ${info.Grantee} on database ${info.DatabaseName}`;
    case eventTypes.CHANGE_TYPE_PRIVILEGE:
      if (info.GrantedPrivileges && info.GrantedPrivileges.length > 0) {
        privs += " granted " + info.GrantedPrivileges;
      }
      if (info.RevokedPrivileges && info.RevokedPrivileges.length > 0) {
        privs += " revoked " + info.GrantedPrivileges;
      }
      return `Privilege change: User ${info.User}${privs} to ${info.Grantee} on type ${info.TypeName}`;
    case eventTypes.SET_SCHEMA:
      return `Schema Change: User ${info.User} set the schema of ${info.DescriptorType} ${info.DescriptorName} to ${info.NewDescriptorName}`;
    case eventTypes.CREATE_SCHEMA:
      return `Schema Created: User ${info.User} created schema ${info.SchemaName} with owner ${info.Owner}`;
    case eventTypes.DROP_SCHEMA:
      return `Schema Dropped: User ${info.User} dropped schema ${info.SchemaName}`;
    case eventTypes.RENAME_SCHEMA:
      return `Schema Renamed: User ${info.User} renamed schema ${info.SchemaName} to ${info.NewSchemaName}`;
    case eventTypes.ALTER_SCHEMA_OWNER:
      return `Schema Owner Altered: User ${info.User} altered the owner of schema ${info.SchemaName} to ${info.Owner}`;
    case eventTypes.CONVERT_TO_SCHEMA:
      return `Database Converted: User ${info.User} converted database ${info.DatabaseName} to a schema with parent database ${info.NewDatabaseParent}`;
    case eventTypes.CREATE_ROLE:
      return `Role Created: User ${info.User} created role ${info.RoleName}`;
    case eventTypes.DROP_ROLE:
      return `Role Dropped: User ${info.User} dropped role ${info.RoleName}`;
    case eventTypes.ALTER_ROLE:
      return `Role Altered: User ${info.User} altered role ${info.RoleName} with options ${info.Options}`;
    case eventTypes.IMPORT:
      return `Import Job: User ${info.User} has a job ${info.JobID} running with status ${info.Status}`;
    case eventTypes.RESTORE:
      return `Restore Job: User ${info.User} has a job ${info.JobID} running with status ${info.Status}`;
    case eventTypes.ALTER_DATABASE_ADD_REGION:
      return `Add Region: User ${info.User} added region ${info.RegionName} to database ${info.DatabaseName}`;
    case eventTypes.ALTER_DATABASE_DROP_REGION:
      return `Drop Region: User ${info.User} dropped region ${info.RegionName} from database ${info.DatabaseName}`;
    case eventTypes.ALTER_DATABASE_PRIMARY_REGION:
      return `Primary Region Changed: User ${info.User} changed primary region of database ${info.DatabaseName} to ${info.PrimaryRegionName}`;
    case eventTypes.ALTER_DATABASE_SURVIVAL_GOAL:
      return `Survival Goal Changed: User ${info.User} changed survival goal of database ${info.DatabaseName} to ${info.SurvivalGoal}`;
    case (eventTypes.UNSAFE_UPSERT_NAMESPACE_ENTRY,
    eventTypes.UNSAFE_DELETE_NAMESPACE_ENTRY,
    eventTypes.UNSAFE_UPSERT_DESCRIPTOR,
    eventTypes.UNSAFE_DELETE_DESCRIPTOR):
      return `Unsafe: User ${info.User} executed crdb_internal.${
        e.event_type
      }, Info: ${JSON.stringify(info, null, 2)}`;
    default:
      return `Event: ${e.event_type}, content: ${JSON.stringify(
        info,
        null,
        2,
      )}`;
  }
}

// EventInfo corresponds to the `info` column of the `system.eventlog` table
// and the `info` field of the `server.serverpb.EventsResponse.Event` proto.
export interface EventInfo {
  CascadeDroppedViews?: string[];
  Config?: string;
  DatabaseName?: string;
  DescriptorType?: string;
  DescriptorName?: string;
  NewDescriptorName?: string;
  DescriptorID?: string;
  Grantee?: string;
  GrantedPrivileges?: string[];
  IndexName?: string;
  MutationID?: string;
  NewDatabaseName?: string;
  NewTypeName?: string;
  NewSchemaName?: string;
  NewTableName?: string;
  NodeID?: string;
  Options?: string[];
  Owner?: string;
  Comment?: string;
  NullComment?: string;
  RevokedPrivileges?: string[];
  RoleName?: string;
  SchemaName?: string;
  SequenceName?: string;
  SettingName?: string;
  Statement?: string;
  TableName?: string;
  ColumnName?: string;
  Target?: string;
  TargetNodeID?: string;
  TypeName?: string;
  User: string;
  Value?: string;
  ViewName?: string;
  // The following are three names for the same key (it was renamed twice).
  // All ar included for backwards compatibility.
  DroppedTables?: string[];
  DroppedTablesAndViews?: string[];
  DroppedSchemaObjects?: string[];
  Grantees?: string;
  NewDatabaseParent?: string;
  JobID?: string;
  Status?: string;
  RegionName?: string;
  PrimaryRegionName?: string;
  SurvivalGoal?: string;
  ParentID?: string;
  ParentSchemaID?: string;
  Name?: string;
  Force?: string;
  ForceNotice?: string;
  PreviousDescriptor?: string;
  NewDescriptor?: string;
}

export function getDroppedObjectsText(eventInfo: EventInfo): string {
  const droppedObjects =
    eventInfo.DroppedSchemaObjects ||
    eventInfo.DroppedTablesAndViews ||
    eventInfo.DroppedTables ||
    eventInfo.CascadeDroppedViews;
  if (!droppedObjects) {
    return "";
  }
  if (droppedObjects.length === 0) {
    return "No schema objects were dropped.";
  } else if (droppedObjects.length === 1) {
    return `1 schema object was dropped: ${droppedObjects[0]}`;
  }
  return `${
    droppedObjects.length
  } schema objects were dropped: ${droppedObjects.join(", ")}`;
}
