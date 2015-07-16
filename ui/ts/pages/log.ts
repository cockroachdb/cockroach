// source: pages/log.ts
/// <reference path="../components/select.ts" />
/// <reference path="../models/log.ts" />
/// <reference path="../models/proto.ts" />
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../util/format.ts" />

// Author: Bram Gruneir (bram+code@cockroachlabs.com)

/**
 * AdminViews is the primary module for Cockroaches administrative web
 * interface.
 */
module AdminViews {
  "use strict";

  /**
   * Log is the view for exploring the logs from nodes.
   */
  export module Log {
    let entries: Models.Log.Entries = new Models.Log.Entries();

    /**
     * Page displays log entries from the current node.
     */
    export module Page {
      class Controller {
        private static _queryEveryMS: number = 10000;
        private _interval: number;

        onunload(): void {
          clearInterval(this._interval);
        }

        private _Refresh(): void {
          entries.refresh();
        }

        constructor() {
          this._Refresh();
          this._interval = setInterval(() => this._Refresh(), Controller._queryEveryMS);
        }
      };

      export function controller(): Controller {
        return new Controller();
      };

      // TODO(bram): Move these into css classes.
      const _tableStyle: string = "border-collapse:collapse; border - spacing:0; border - color:#ccc";
      const _thStyle: string = "font-family:Arial, sans-serif;font-size:14px;font-weight:normal;padding:10px 5px;" +
        "border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;color:#333;" +
        "background-color:#efefef;text-align:center";
      const _tdStyleOddFirst: string = "font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;" +
        "border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;color:#333;" +
        "background-color:#efefef;text-align:center";
      const _tdStyleOdd: string = "font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;" +
        "border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;" +
        "color:#333;background-color:#f9f9f9;text-align:center";
      const _tdStyleEvenFirst: string = "font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;" +
        "border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;" +
        "color:#333;background-color:#efefef;text-align:center";
      const _tdStyleEven: string = "font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;" +
        "border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;" +
        "color:#333;background-color:#fff;text-align:center";

      function _EntryRow(entry: Models.Proto.LogEntry, count: number): _mithril.MithrilVirtualElement {
        let dstyle: string;
        let countStyle: string;
        if (count % 2 === 0) {
          countStyle = _tdStyleEvenFirst;
          dstyle = _tdStyleEven;
        } else {
          countStyle = _tdStyleOddFirst;
          dstyle = _tdStyleOdd;
        }
        let date: Date = new Date(Utils.Convert.NanoToMilli(entry.time));
        return m("tr", [
          m("td", { style: countStyle }, (count + 1).toString()),
          m("td", { style: dstyle }, Utils.Format.Date(date)),
          m("td", { style: dstyle }, Utils.Format.Severity(entry.severity)),
          m("td", { style: dstyle }, Utils.Format.LogEntryMessage(entry)),
          m("td", { style: dstyle }, entry.node_id),
          m("td", { style: dstyle }, entry.store_id),
          m("td", { style: dstyle }, entry.raft_id),
          m("td", { style: dstyle }, entry.key),
          m("td", { style: dstyle }, entry.file + ":" + entry.line),
          m("td", { style: dstyle }, entry.method)
        ]);
      };

      const _severitySelectOptions: Components.Select.Item[] = [
        { value: Utils.Format.Severity(0), text: ">= " + Utils.Format.Severity(0) },
        { value: Utils.Format.Severity(1), text: ">= " + Utils.Format.Severity(1) },
        { value: Utils.Format.Severity(2), text: ">= " + Utils.Format.Severity(2) },
        { value: Utils.Format.Severity(3), text: Utils.Format.Severity(3) },
      ];

      function onChangeSeverity(val: string): void {
        entries.level(val);
        entries.refresh();
      };

      function onChangeMax(val: string): void {
        let result: number = parseInt(val, 10);
        if (result > 0) {
          entries.max(result);
        } else {
          entries.max(null);
        }
        entries.refresh();
      }

      function onChangePattern(val: string): void {
        entries.pattern(val);
        entries.refresh();
      }

      export function view(ctrl: Controller): _mithril.MithrilVirtualElement {
        let rows: _mithril.MithrilVirtualElement[] = [];
        if (entries.result() != null) {
          for (let i: number = 0; i < entries.result().length; i++) {
            rows.push(_EntryRow(entries.result()[i], i));
          }
        }

        return m("div", [
          m("form", [
            m.trust("Severity: "),
            m.component(Components.Select, {
              items: _severitySelectOptions,
              value: entries.level,
              onChange: onChangeSeverity
            }),
            m.trust("&nbsp;&nbsp;Max Results: "),
            m("input", { oninput: m.withAttr("value", onChangeMax), value: entries.max() }),
            m.trust("&nbsp;&nbsp;Regex Filter: "),
            m("input", { oninput: m.withAttr("value", onChangePattern), value: entries.pattern() })

          ]),
          m("p", rows.length + " log entries retrieved"),
          m("table", { style: _tableStyle }, [
            m("tr", [
              m("th", { style: _thStyle }, "#"),
              m("th", { style: _thStyle }, "Time"),
              m("th", { style: _thStyle }, "Severity"),
              m("th", { style: _thStyle }, "Message"),
              m("th", { style: _thStyle }, "Node"),
              m("th", { style: _thStyle }, "Store"),
              m("th", { style: _thStyle }, "Raft"),
              m("th", { style: _thStyle }, "Key"),
              m("th", { style: _thStyle }, "File:Line"),
              m("th", { style: _thStyle }, "Method")
            ]),
            rows
          ])
        ]);
      };
    }
  }
}
