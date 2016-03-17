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
    import NavigationBar = Components.NavigationBar;

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

        public TargetSet(): NavigationBar.TargetSet {
          return {
            baseRoute: "/nodes/" + m.route.param("node_id") + "/",
            targets: Utils.Prop(AdminViews.Nodes.NodePage.Controller.nodeTabs),
            isActive: (t: NavigationBar.Target): boolean => t.route === "logs",
          };
        }

        onunload(): void {
          clearInterval(this._interval);
        }

        private _Refresh(): void {
          entries.refresh();
        }

        constructor() {
          // TODO: keep values on refresh, allow sharing via url
          entries = new Models.Log.Entries();
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

        // TODO: the log page was factored into the nodes page
        // there should probably be a parent nodes page that handles the tabbing and any common elements

        // Page title.
        let title: _mithril.MithrilVirtualElement = m("", [
          m("a", {config: m.route, href: "/nodes"}, "Nodes"),
          ": Node " + m.route.param("node_id"),
        ]);

        return m(".page", [
          m.component(Components.Topbar, {
            title: title,
            updated: Utils.Convert.MilliToNano(Date.now()),
          }),
          m.component(NavigationBar, {ts: ctrl.TargetSet()}),
          m(".section.logs", [
            m("form", [
              "Severity: ",
              m.component(Components.Select, {
                items: _severitySelectOptions,
                value: entries.level,
                onChange: (val: string): void => {
                  entries.level(val);
                  entries.refresh();
                },
              }),
              m("span.spacing", "Max Results: "),
              // We listen on keyup instead of change so that the page updates as the user types
              m("input",
                {
                  config: (el: Element, isInit: boolean, context: MithrilContext): any => {
                    if (!isInit) {
                      el.addEventListener("keyup", (e: Event): void => {
                        let num: number = parseInt((<HTMLInputElement>e.srcElement).value, 10);
                        entries.max(num > 0 ? num : null);
                        entries.refresh();
                      });
                    }
                  },
                }),
              m("span.spacing", "Regex Filter: "),
              m("input",
                {
                  config: (el: Element, isInit: boolean, context: MithrilContext): any => {
                    if (!isInit) {
                      el.addEventListener("keyup", (e: Event): void => {
                        entries.pattern((<HTMLInputElement>e.srcElement).value);
                        entries.refresh();
                      });
                    }
                  },
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
