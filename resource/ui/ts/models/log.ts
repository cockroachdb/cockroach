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

        // TODO(bram): Move these into css classes.
        var tableStyle = "border-collapse:collapse; border - spacing:0; border - color:#ccc";
        var thStyle = "font-family:Arial, sans-serif;font-size:14px;font-weight:normal;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;color:#333;background-color:#efefef;text-align:center";
        var tdStyleOddFirst = "font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;color:#333;background-color:#efefef;text-align:center";
        var tdStyleOdd = "font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;color:#333;background-color:#f9f9f9;text-align:center";
        var tdStyleEvenFirst = "font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;color:#333;background-color:#efefef;text-align:center";
        var tdStyleEven = "font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;color:#333;background-color:#fff;text-align:center";

        export class Entries {

            startTime = Utils.chainProp(this, <number>null);
            endTime = Utils.chainProp(this, <number>null);
            max = Utils.chainProp(this, <number>null);
            level = Utils.chainProp(this, <string>null);

            private _url():string {
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

            private _data = new Utils.QueryCache((): promise<Proto.LogEntry[]> => {
                return m.request({ url: this._url(), method: "GET", extract: nonJsonErrors })
                    .then((results: LogResponseSet) => {
                        return results.d;
                    });
            })

            public Refresh() {
                this._data.refresh();
            }

            public Entries(): Proto.LogEntry[] {
                return this._data.result();
            }

            private static _formatTags = new RegExp("%s|%d|%v|%+v", "gi")
            private static _FormatMessage(entry: Proto.LogEntry): string {
                var i = -1;
                return entry.format.replace(Entries._formatTags, function(tag) {
                    i++;
                    if (entry.args.length > i) {
                        return entry.args[i].str;
                    } else {
                        return "";
                    }
                });
            }

            private static _EntryRow(entry: Proto.LogEntry, count: number): _mithril.MithrilVirtualElement {
                var dstyle, countStyle: string;
                if (count % 2 == 0) {
                    countStyle = tdStyleEvenFirst;
                    dstyle = tdStyleEven;
                } else {
                    countStyle = tdStyleOddFirst;
                    dstyle = tdStyleOdd;
                }
                return m("tr", [
                    m("td", { style: countStyle }, (count + 1).toString()),
                    m("td", { style : dstyle },  Utils.Format.DateFromTimestamp(entry.time)),
                    m("td", { style: dstyle }, Utils.Format.SeverityFromNumber(entry.severity)),
                    m("td", { style: dstyle }, Entries._FormatMessage(entry)),
                    m("td", { style: dstyle }, entry.node_id),
                    m("td", { style: dstyle }, entry.store_id),
                    m("td", { style: dstyle }, entry.raft_id),
                    m("td", { style: dstyle }, entry.key),
                    m("td", { style: dstyle }, entry.file + ":" + entry.line),
                    m("td", { style: dstyle }, entry.method),
                    m("td", { style: dstyle }, entry.thread_id)
                ]);
            }

            public EntryRows(): _mithril.MithrilVirtualElement {
                var entries = this.Entries()
                var rows: _mithril.MithrilVirtualElement[] = []
                if (entries != null) {
                    var rows: _mithril.MithrilVirtualElement[] = [];
                    for (var i = 0; i < entries.length; i++) {
                        rows.push(Entries._EntryRow(entries[i], i));
                    };
                }

                return m("div", [
                    m("p", rows.length + " log entries retrieved"),
                    m("table", { style: tableStyle }, [
                        m("tr", [
                            m("th", { style: thStyle }, "#"),
                            m("th", { style: thStyle }, "Time"),
                            m("th", { style: thStyle }, "Severity"),
                            m("th", { style: thStyle }, "Message"),
                            m("th", { style: thStyle }, "Node"),
                            m("th", { style: thStyle }, "Store"),
                            m("th", { style: thStyle }, "Raft"),
                            m("th", { style: thStyle }, "Key"),
                            m("th", { style: thStyle }, "File:Line"),
                            m("th", { style: thStyle }, "Method"),
                            m("th", { style: thStyle }, "Thread")
                            ]),
                        rows
                    ])
                ]);
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
