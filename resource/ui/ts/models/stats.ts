// source: models/stats.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
// Author: Bram Gruneir (bram.gruneir@gmail.com)

/**
 * Models contains data models pulled from cockroach.
 */
module Models {
    export module Stats {
        export interface MVCCStats {
            live_bytes: number;
            key_bytes: number;
            val_bytes: number;
            intent_bytes: number;
            live_count: number;
            key_count: number;
            val_count: number;
            intent_count: number;
            intent_age: number;
            gc_bytes_age: number;
            sys_bytes: number;
            sys_count: number;
            last_update_nanos: number;
        }

        // This function was adapted from
        // https://stackoverflow.com/questions/10420352/converting-file-size-in-bytes-to-human-readable
        var kibi = 1024
        var units = ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
        export function FormatBytes(bytes: number): string {
            if (Math.abs(bytes) < kibi) {
                return bytes + ' B';
            }
            var u = -1;
            do {
                bytes /= kibi;
                ++u;
            } while (Math.abs(bytes) >= kibi && u < units.length - 1);
            return bytes.toFixed(1) + ' ' + units[u];
        }

        // TODO(bram): Move these into css classes.
        var tableStyle = "border-collapse:collapse; border - spacing:0; border - color:#ccc";
        var thStyle = "font-family:Arial, sans-serif;font-size:14px;font-weight:normal;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;color:#333;background-color:#efefef;text-align:center";
        var tdStyleOddFirst = "font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;color:#333;background-color:#efefef;text-align:center";
        var tdStyleOdd = "font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;color:#333;background-color:#f9f9f9;text-align:center";
        var tdStyleEvenFirst = "font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;color:#333;background-color:#efefef;text-align:center";
        var tdStyleEven = "font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;color:#333;background-color:#fff;text-align:center";

        export function CreateStatsTable(stats: MVCCStats): _mithril.MithrilVirtualElement {
            return m("div", [
                m("h3", "Statistics"),
                m("table", { style: tableStyle }, [
                    m("tr", [
                        m("th", { style: thStyle }, ""),
                        m("th", { style: thStyle }, "Key"),
                        m("th", { style: thStyle }, "Value"),
                        m("th", { style: thStyle }, "Live"),
                        m("th", { style: thStyle }, "Intent"),
                        m("th", { style: thStyle }, "System")
                    ]),
                    m("tr", [
                        m("td", { style: tdStyleOddFirst }, "Count"),
                        m("td", { style: tdStyleOdd }, stats.key_count),
                        m("td", { style: tdStyleOdd }, stats.val_count),
                        m("td", { style: tdStyleOdd }, stats.live_count),
                        m("td", { style: tdStyleOdd }, stats.intent_count),
                        m("td", { style: tdStyleOdd }, stats.sys_count)
                    ]),
                    m("tr", [
                        m("td", { style: tdStyleEvenFirst }, "Size"),
                        m("td", { style: tdStyleEven }, FormatBytes(stats.key_bytes)),
                        m("td", { style: tdStyleEven }, FormatBytes(stats.val_bytes)),
                        m("td", { style: tdStyleEven }, FormatBytes(stats.live_bytes)),
                        m("td", { style: tdStyleEven }, FormatBytes(stats.intent_bytes)),
                        m("td", { style: tdStyleEven }, FormatBytes(stats.sys_bytes))
                    ])
                ])
            ]);
        }
    }
}
