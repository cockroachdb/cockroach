// source: pages/log.ts
/// <reference path="../components/select.ts" />
/// <reference path="../components/table.ts" />
/// <reference path="../models/log.ts" />
/// <reference path="../models/proto.ts" />
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../util/format.ts" />
/// <reference path="../util/convert.ts" />

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
    import MithrilContext = _mithril.MithrilContext;
    import MithrilAttributes = _mithril.MithrilAttributes;

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
            sortable: true,
          },
          {
            title: "Severity",
            view: (entry: LogEntry): string => Utils.Format.Severity(entry.severity),
          },
          {
            title: "Message",
            view: (entry: LogEntry): string => entry.format,
          },
          {
            title: "File:Line",
            view: (entry: LogEntry): string => entry.file + ":" + entry.line,
            sortable: true,
          },
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

        constructor() {
          entries = new Models.Log.Entries();
          entries.node(m.route.param("node_id") || null);
          entries.level(m.route.param("level") || Utils.Format.Severity(2));
          entries.max(parseInt(m.route.param("max"), 10) || null);
          entries.startTime(parseInt(m.route.param("startTime"), 10) || null);
          entries.endTime(parseInt(m.route.param("endTime"), 10) || null);
          entries.pattern(m.route.param("pattern") || null);
          this._Refresh();
          this._interval = window.setInterval(() => this._Refresh(), Controller._queryEveryMS);
        }
      }

      export function controller(): Controller {
        return new Controller();
      }

      const _severitySelectOptions: Components.Select.Item[] = [
        { value: Utils.Format.Severity(0), text: ">= " + Utils.Format.Severity(0) },
        { value: Utils.Format.Severity(1), text: ">= " + Utils.Format.Severity(1) },
        { value: Utils.Format.Severity(2), text: ">= " + Utils.Format.Severity(2) },
        { value: Utils.Format.Severity(3), text: Utils.Format.Severity(3) },
      ];

      function reroute(): void {
        m.route(entries.getURL(), entries.getParams(), true);
      }

      function onChangeSeverity(val: string): void {
        entries.level(val);
        reroute();
      }

      function onChangeMax(val: string): void {
        let result: number = parseInt(val, 10);
        if (result > 0) {
          entries.max(result);
        } else {
          entries.max(null);
        }
        reroute();
      }

      function onChangePattern(val: string): void {
        entries.pattern(val);
        reroute();
      }

      export function view(ctrl: Controller): _mithril.MithrilVirtualElement {
        let comparisonData: Table.TableData<LogEntry> = {
          columns: ctrl.columns,
          rows: entries.allEntries,
        };
        let count: number;
        if (entries.allEntries()) {
          count = entries.allEntries().length;
        } else {
          count = 0;
        }

        // This allows us to persist the elements on the page through reroutes, which prevents the inputs losing focus.
        // It needs to be added to every parent of the element of the input as well.
        let persistent: MithrilAttributes = {
          config: (el: Element, isInit: boolean, context: MithrilContext): void => { context.retain = true; },
        };

        return m("div", persistent, [
          m.component(Components.Topbar, {
            title: "Node " + entries.nodeName() + " Log",
            updated: Utils.Convert.MilliToNano(Date.now()),
          }),
          m(".section.logs", persistent, [
            m("form", persistent, [
              "Severity: ",
              m.component(Components.Select, {
                items: _severitySelectOptions,
                value: entries.level,
                onChange: onChangeSeverity,
              }),
              m("span.spacing", "Max Results: "),
              // We listen on keyup instead of change, because otherwise the value is reset when the health endpoint
              // triggers a redraw.
              m("input",
                {
                  config: (el: Element, isInit: boolean, context: MithrilContext): any => {
                    context.retain = true;
                    if (!isInit) {
                      el.addEventListener("keyup", m.withAttr("value", onChangeMax));
                    }
                  },
                  value: entries.max(),
                }),
              m("span.spacing", "Regex Filter: "),
              m("input",
                {
                  config: (el: Element, isInit: boolean, context: MithrilContext): any => {
                    context.retain = true;
                    if (!isInit) {
                      el.addEventListener("keyup", m.withAttr("value", onChangePattern));
                    }
                  },
                  value: entries.pattern(),
                }),
            ]),
          ]),
          m(".section.logs-table", [
            m("p", count + " log entries retrieved"),
            m(".logs-table", Components.Table.create(comparisonData)),
          ]),
        ]);
      }
    }
  }
}
