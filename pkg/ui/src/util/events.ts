import * as protobuf from "protobufjs/minimal";

import * as protos from "src/js/protos";
import * as eventTypes from "src/util/eventTypes";

type Event$Properties = protos.cockroach.server.serverpb.EventsResponse.Event$Properties;

/**
 * getEventDescription returns a short summary of an event.
 */
export function getEventDescription(e: Event$Properties): string {
  const info: {
    DatabaseName: string,
    DroppedTables: string[],
    IndexName: string,
    MutationID: string,
    TableName: string,
    User: string,
    ViewName: string,
    SequenceName: string,
    SettingName: string,
    Value: string,
  } = protobuf.util.isset(e, "info") ? JSON.parse(e.info) : {};
  const targetId: number = e.target_id ? e.target_id.toNumber() : null;

  switch (e.event_type) {
    case eventTypes.CREATE_DATABASE:
      return `Database Created: User ${info.User} created database ${info.DatabaseName}`;
    case eventTypes.DROP_DATABASE:
      info.DroppedTables = info.DroppedTables || [];
      let tableDropText: string = `${info.DroppedTables.length} tables were dropped: ${info.DroppedTables.join(", ")}`;
      if (info.DroppedTables.length === 0) {
        tableDropText = "No tables were dropped.";
      } else if (info.DroppedTables.length === 1) {
        tableDropText = `1 table was dropped: ${info.DroppedTables[0]}`;
      }
      return `Database Dropped: User ${info.User} dropped database ${info.DatabaseName}.${tableDropText}`;
    case eventTypes.CREATE_TABLE:
      return `Table Created: User ${info.User} created table ${info.TableName}`;
    case eventTypes.DROP_TABLE:
      return `Table Dropped: User ${info.User} dropped table ${info.TableName}`;
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
    default:
      return `Unknown Event Type: ${e.event_type}, content: ${JSON.stringify(info, null, 2)}`;
  }
}
