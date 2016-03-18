// source: models/log.ts
/// <reference path="../models/proto.ts" />
/// <reference path="../../typings/browser.d.ts" />
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../util/chainprop.ts" />
/// <reference path="../util/format.ts" />
/// <reference path="../util/querycache.ts" />
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

/**
 * Models contains data models pulled from cockroach.
 */
module Models {
  "use strict";
  /**
   * Log package represents the logs collected by each Cockroach node.
   */
  export module Log {
    import Promise = _mithril.MithrilPromise;
    import Property = _mithril.MithrilProperty;

    export interface LogResponseSet {
      d: Proto.LogEntry[];
    }

    export class Entries {
      public allEntries: Utils.ReadOnlyProperty<Proto.LogEntry[]>;

      startTime: Property<number> = m.prop(<number>null);
      endTime: Property<number> = m.prop(<number>null);
      max: Property<number> = m.prop(<number>null);
      level: Property<string> = m.prop(<string>null);
      pattern: Property<string> = m.prop(<string>null);
      node: Property<string> = m.prop(<string>null);

      private _data: Utils.QueryCache<Proto.LogEntry[]> = new Utils.QueryCache(
          (): Promise<Proto.LogEntry[]> => {
              return m.request({ url: this._url(), method: "GET", extract: nonJsonErrors, config: function(xhr: XMLHttpRequest): void { xhr.timeout = 10000; } })
                  .then((results: LogResponseSet) => {
                      return results.d;
                  });
          },
          true
      );

      refresh: () => void = () => {
        this._data.refresh();
      };

      result: () => Proto.LogEntry[] = () => {
        return this._data.result();
      };

      nodeName: () => string = () => {
        if ((this.node() != null) && (this.node() !== "local")) {
          return this.node();
        }
        return "Local";
      };

      /**
       * getURL creates the URL based on the node id. It is designed to be
       * combined getParams to define the full route.
       */
      getURL(): string {
        let url: string = "/logs/";
        if (this.node() != null) {
          url += encodeURIComponent(this.node());
        } else {
          url += "local";
        }
        return url;
      }

      /**
       * getParams creates the query parameter object based on the current
       * state of all the properties on entries. This object can be passed to
       * m.route.buildQueryString or to m.route(url, parms).
       */
      getParams(): Object {
        let params = {
          level: this.level(),
          startTime: this.startTime(),
          endTime: this.endTime(),
          max: this.max(),
          pattern: this.pattern(),
        };
        // buildQueryString includes empty query strings when they have a null
        // value and we need to remove them.
        let keys: string[] = Object.keys(params);
        for (let i: number = 0; i < keys.length; i++) {
          if (!params[keys[i]]) {
            delete params[keys[i]];
          }
        }
        return params;
      }

      /**
       * _url return the url used for queries to the status server.
       */
      private _url(): string {
        return "/_status" + this.getURL() + "?" + m.route.buildQueryString(this.getParams());
      }

      constructor() {
        this.level(Utils.Format.Severity(2));
        this.allEntries = this._data.result;
      }
    }

    /**
     * nonJsonErrors ensures that error messages returned from the server
     * are parseable as JSON strings.
     */
    function nonJsonErrors(xhr: XMLHttpRequest, opts: _mithril.MithrilXHROptions<{}>): string {
      return xhr.status > 200 ? JSON.stringify(xhr.responseText) : xhr.responseText;
    }
  }
}
