/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../models/proto.ts" />

module Models {
  "use strict";

  // API is the model used for interacting with the backend API
  // which provides access to certain SQL commands used by the UI
  export module API {
    import MithrilPromise = _mithril.MithrilPromise;
    import Moment = moment.Moment;
    import DatabaseList = Models.Proto.DatabaseList;
    import Database = Models.Proto.Database;
    import SQLTable = Models.Proto.SQLTable;
    import Users = Models.Proto.Users;
    import EventInfo = Models.Proto.EventInfo;
    import UnparsedClusterEvents = Models.Proto.UnparsedClusterEvents;
    import UnparsedClusterEvent = Models.Proto.UnparsedClusterEvent;
    import GetUIDataResponse = Models.Proto.GetUIDataResponse;

    export interface ClusterEvent {
      timestamp: Moment;
      event_type: string;
      target_id: number;
      reporting_id: number;
      info: EventInfo;
    }

    export interface ClusterEvents {
      events: ClusterEvent[];
    }

    // Timeout after 2s
    let xhrConfig = function(xhr: XMLHttpRequest): XMLHttpRequest {
      xhr.timeout = 2000;
      return xhr;
    };

    // gets a list of databases
    export function databases(): MithrilPromise<DatabaseList> {
      return m.request<DatabaseList>({
        url: "/_admin/v1/databases",
        config: xhrConfig,
      });
    }

    // gets information about a specific database
    export function database(database: string): MithrilPromise<Database> {
      return m.request<Database>({
        url: `/_admin/v1/databases/${database}`,
        config: xhrConfig,
      });
    }

    // gets information about a specific table
    export function table(database: string, table: string): MithrilPromise<SQLTable> {
      return m.request<SQLTable>({
        url: `/_admin/v1/databases/${database}/tables/${table}`,
        config: xhrConfig,
      });
    }

    // gets a list of users
    export function users(): MithrilPromise<Users> {
      return m.request<Users>({
        url: "/_admin/v1/users",
        config: xhrConfig,
      });
    }

    // TODO: parameters
    // gets a list of range events
    export function events(): MithrilPromise<ClusterEvents> {
      return m.request<UnparsedClusterEvents>({
        url: "/_admin/v1/events",
        config: xhrConfig,
      }).then((response: UnparsedClusterEvents): ClusterEvents => {
        return {
          events: _.map<UnparsedClusterEvent, ClusterEvent>(response.events, (event: UnparsedClusterEvent): ClusterEvent => {
            let timestamp: Moment = Utils.Convert.TimestampToMoment(event.timestamp);
            let info: EventInfo = null;
            try {
              info = JSON.parse(event.info);
            } catch (e) {
              info = null;
            }

            return {
              timestamp: timestamp,
              event_type: event.event_type,
              target_id: event.target_id,
              reporting_id: event.reporting_id,
              info: info,
            };
          }),
        };
      });
    }

    export function getUIData(key: string): MithrilPromise<GetUIDataResponse> {
      return m.request<GetUIDataResponse>({
        url: `/_admin/v1/uidata?key=${key}`,
        config: xhrConfig,
      });
    }

    export function setUIData(key: string, value: string): MithrilPromise<any> {
      return m.request<UnparsedClusterEvents>({
        url: `/_admin/v1/uidata`,
        config: xhrConfig,
        method: "POST",
        data: {
          key: key,
          value: value,
        },
      });
    }
  }
}
