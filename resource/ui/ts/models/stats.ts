// source: models/stats.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="proto.ts" />
/// <reference path="../util/format.ts" />
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

/**
 * Models contains data models pulled from cockroach.
 */
module Models {
    "use strict";
    export module Stats {
        // TODO(bram): Move these into css classes.
        const tableStyle: string = "border-collapse:collapse; border - spacing:0; border - color:#ccc";
        const thStyle: string = "font-family:Arial, sans-serif;font-size:14px;font-weight:normal;padding:10px 5px;" +
            "border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;" +
            "color:#333;background-color:#efefef;text-align:center";
        const tdStyleOddFirst: string = "font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;" +
            "border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;" +
            "color:#333;background-color:#efefef;text-align:center";
        const tdStyleOdd: string = "font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;" +
            "border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;" +
            "color:#333;background-color:#f9f9f9;text-align:center";
        const tdStyleEvenFirst: string = "font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;" +
            "border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;" +
            "color:#333;background-color:#efefef;text-align:center";
        const tdStyleEven: string = "font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;" +
            "border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:" +
            "#ccc;color:#333;background-color:#fff;text-align:center";

        export function CreateStatsTable(stats: Proto.MVCCStats): _mithril.MithrilVirtualElement {
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
                        m("td", { style: tdStyleEven }, Utils.Format.Bytes(stats.key_bytes)),
                        m("td", { style: tdStyleEven }, Utils.Format.Bytes(stats.val_bytes)),
                        m("td", { style: tdStyleEven }, Utils.Format.Bytes(stats.live_bytes)),
                        m("td", { style: tdStyleEven }, Utils.Format.Bytes(stats.intent_bytes)),
                        m("td", { style: tdStyleEven }, Utils.Format.Bytes(stats.sys_bytes))
                    ])
                ])
            ]);
        }
    }
}
