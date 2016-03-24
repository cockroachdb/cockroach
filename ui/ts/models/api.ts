/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/browser.d.ts"/>
/// <reference path="../models/proto.ts" />
/// <reference path="../util/format.ts" />
/// <reference path="../util/http.ts" />

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

    // gets a list of databases
    export function databases(): MithrilPromise<DatabaseList> {
      return m.request<DatabaseList>({
        url: "/_admin/v1/databases",
        config: Utils.Http.XHRConfig,
      });
    }

    // gets information about a specific database
    export function database(database: string): MithrilPromise<Database> {
      return m.request<Database>({
        url: `/_admin/v1/databases/${database}`,
        config: Utils.Http.XHRConfig,
      });
    }

    // gets information about a specific table
    export function table(database: string, table: string): MithrilPromise<SQLTable> {
      return m.request<SQLTable>({
        url: `/_admin/v1/databases/${database}/tables/${table}`,
        config: Utils.Http.XHRConfig,
      });
    }

    // gets a list of users
    export function users(): MithrilPromise<Users> {
      return m.request<Users>({
        url: "/_admin/v1/users",
        config: Utils.Http.XHRConfig,
      });
    }

    // TODO: parameters
    // gets a list of range events
    export function events(): MithrilPromise<ClusterEvents> {
      return m.request<UnparsedClusterEvents>({
        url: "/_admin/v1/events",
        config: Utils.Http.XHRConfig,
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
        config: Utils.Http.XHRConfig,
        background: true,
      });
    }

    export function setUIData(keyValues: {[key: string]: string}): MithrilPromise<any> {
      return m.request<UnparsedClusterEvents>({
        url: `/_admin/v1/uidata`,
        config: Utils.Http.XHRConfig,
        method: "POST",
        data: {
          key_values: keyValues,
        },
      });
    }
  }
}
