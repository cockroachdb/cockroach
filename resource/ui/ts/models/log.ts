// source: models/log.ts
/// <reference path="../models/proto.ts" />
/// <reference path="../typings/d3/d3.d.ts" />
/// <reference path="../typings/mithriljs/mithril.d.ts" />
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
      startTime: Property<number> = m.prop(<number>null);
      endTime: Property<number> = m.prop(<number>null);
      max: Property<number> = m.prop(<number>null);
      level: Property<string> = m.prop(<string>null);

      refresh: () => void = () => {
        this._data.refresh();
      };

      result: () => Proto.LogEntry[] = () => {
        return this._data.result();
      };

      private _data: Utils.QueryCache<Proto.LogEntry[]> = new Utils.QueryCache((): Promise<Proto.LogEntry[]> => {
        return m.request({ url: this._url(), method: "GET", extract: nonJsonErrors })
          .then((results: LogResponseSet) => {
            return results.d;
          });
      });

      private _url(): string {
        let url: string = "/_status/local/log";
        if (this.level() != null) {
          url += "/" + this.level();
        }
        url += "?";
        if (this.startTime() != null) {
          url += "startTime=" + this.startTime().toString() + "&";
        }
        if (this.endTime() != null) {
          url += "entTime=" + this.endTime().toString() + "&";
        }
        if (this.max() != null) {
          url += "max=" + this.max().toString() + "&";
        }
        return url;
      }

      constructor() {
        this.level(Utils.Format.Severity(2));
        this.max(null);
        this.startTime(null);
        this.endTime(null);
      }
    }

    /**
     * nonJsonErrors ensures that error messages returned from the server
     * are parseable as JSON strings.
     */
    function nonJsonErrors(xhr: XMLHttpRequest, opts: _mithril.MithrilXHROptions): string {
      return xhr.status > 200 ? JSON.stringify(xhr.responseText) : xhr.responseText;
    }
  }
}
