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
    /**
     * Log package represents the logs collected by each Cockroach node.
     */
    export module Log {
        import promise = _mithril.MithrilPromise;

        export interface LogResponseSet {
            d: Proto.LogEntry[]
        }

        export class Entries {
            startTime = Utils.chainProp(this, <number>null);
            endTime = Utils.chainProp(this, <number>null);
            max = Utils.chainProp(this, <number>null);
            level = Utils.chainProp(this, <string>null);

            private _url(): string {
                var url = "/_status/local/log";
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

            private _innerQuery = () => {
                return m.request({ url: this._url(), method: "GET", extract: nonJsonErrors })
                    .then((results: LogResponseSet) => {
                        return results.d;
                    });
            }

            private _data = new Utils.QueryCache((): promise<Proto.LogEntry[]> => {
                return m.request({ url: this._url(), method: "GET", extract: nonJsonErrors })
                    .then((results: LogResponseSet) => {
                        return results.d;
                    });
            })

            public refresh() {
                this._data.refresh();
            }

            public result(): Proto.LogEntry[] {
                return this._data.result();
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
