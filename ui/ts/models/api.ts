/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../models/proto.ts" />

module Models {
  "use strict";

  // API is the model used for interacting with the backend API
  // which provides access to certain SQL commands used by the UI
  export module API {
    import MithrilPromise = _mithril.MithrilPromise;

    export interface DatabaseList {
      Databases: string[];
    }

    export interface Grant {
      Database: string;
      Privileges: string[];
      User: string;
    }

    export interface Database {
      Grants: Grant[];
      Tables: string[];
    }

    export interface SQLColumn {
      Field: string;
      Type: string;
      Null: boolean;
      Default: string;
    }

    export interface SQLIndex {
      Table: string;
      Name: string;
      Unique: boolean;
      Seq: number;
      Column: string;
      Direction: string;
      Storing: boolean;
    }

    export interface SQLTable {
      Columns: SQLColumn[];
      Index: SQLIndex[];
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
      // TODO: replace with backend endpoint
      return m.sync<any[]>([
        Models.SQLQuery.runQuery<SQLColumn[]>(`SHOW COLUMNS FROM ${database}.${table}`, true),
        Models.SQLQuery.runQuery<SQLIndex[]>(`SHOW INDEX FROM ${database}.${table}`, true),
      ]).then(function (results: [SQLColumn[], SQLIndex[]]): SQLTable {
        return {
          Columns: results[0],
          Index: results[1],
        };
      });
    }
  }
}
