// source: pages/log.ts
/// <reference path="../components/select.ts" />
/// <reference path="../components/table.ts" />
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
    import Table = Components.Table;
    import LogEntry = Models.Proto.LogEntry;

    let entries: Models.Log.Entries;
    /**
     * Page displays log entries from the current node.
     */
    export module Page {
      class Controller {
        private static comparisonColumns: Table.TableColumn<LogEntry>[] = [
          {
            title: "Time",
            view: (entry: LogEntry): string => {
              let date = new Date(Utils.Convert.NanoToMilli(entry.time));
              return Utils.Format.Date(date);
            },
            sortable: true
          },
          {
            title: "Severity",
            view: (entry: LogEntry): string => Utils.Format.Severity(entry.severity)
          },
          {
            title: "Message",
            view: (entry: LogEntry): string => Utils.Format.LogEntryMessage(entry)
          },
          {
            title: "Node",
            view: (entry: LogEntry): string => entry.node_id ? entry.node_id.toString() : "",
            sortable: true,
            sortValue: (entry: LogEntry): number => entry.node_id
          },
          {
            title: "Store",
            view: (entry: LogEntry): string => entry.store_id ? entry.store_id.toString() : "",
            sortable: true,
            sortValue: (entry: LogEntry): number => entry.store_id
          },
          {
            title: "Raft",
            view: (entry: LogEntry): string => entry.raft_id ? entry.raft_id.toString() : "",
            sortable: true,
            sortValue: (entry: LogEntry): number => entry.raft_id
          },
          {
            title: "Key",
            view: (entry: LogEntry): string => entry.key,
            sortable: true
          },
          {
            title: "File:Line",
            view: (entry: LogEntry): string => entry.file + ":" + entry.line,
            sortable: true
          },
          {
            title: "Method",
            view: (entry: LogEntry): string => entry.method ? entry.method.toString() : "",
            sortable: true,
            sortValue: (entry: LogEntry): number => entry.method
          }
        ];

        private static _queryEveryMS: number = 10000;

        public columns: Utils.Property<Table.TableColumn<LogEntry>[]> = Utils.Prop(Controller.comparisonColumns);
        private _interval: number;

        onunload(): void {
          clearInterval(this._interval);
        }

        private _Refresh(): void {
          entries.refresh();
        }

        constructor(nodeId?: string) {
          entries = new Models.Log.Entries(nodeId);
          this._Refresh();
          this._interval = setInterval(() => this._Refresh(), Controller._queryEveryMS);
        }
      };

      export function controller(): Controller {
        let nodeId: string = m.route.param("node_id");
        return new Controller(nodeId);
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
        let comparisonData: Table.TableData<LogEntry> = {
          columns: ctrl.columns,
          rows: entries.allEntries
        };
        let count: number;
        if (entries.allEntries()) {
          count = entries.allEntries().length;
        } else {
          count = 0;
        }

        return m("div", [
          m("h2", "Node " + entries.nodeName() + " Log"),
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
          m("p", count + " log entries retrieved"),
          m(".stats-table", Components.Table.create(comparisonData))
        ]);
      };
    }
  }
}
