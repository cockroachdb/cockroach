// source: pages/stores.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../models/status.ts" />
/// <reference path="../components/metrics.ts" />
/// <reference path="../components/table.ts" />
/// <reference path="../components/navbar.ts" />
/// <reference path="../util/format.ts" />

// Author: Bram Gruneir (bram+code@cockroachlabs.com)

/**
 * AdminViews is the primary module for Cockroaches administrative web
 * interface.
 */
module AdminViews {
  "use strict";

  import MithrilElement = _mithril.MithrilVirtualElement;

  /**
   * Stores is the view for exploring the status of all Stores.
   */
  export module Stores {
    import Metrics = Models.Metrics;
    import Table = Components.Table;
    import StoreStatus = Models.Proto.StoreStatus;
    let storeStatuses: Models.Status.Stores = new Models.Status.Stores();

    function _storeMetric(storeId: string, metric: string): string {
      return "cr.store." + metric + "." + storeId;
    }

    /**
     * StoresPage show a list of all the available nodes.
     */
    export module StoresPage {
      class Controller {
        private static comparisonColumns: Table.TableColumn<StoreStatus>[] = [
          {
            title: "Store ID",
            view: (status: StoreStatus): MithrilElement => {
              return m("a", {href: "/stores/" + status.desc.store_id, config: m.route}, status.desc.store_id.toString());
            },
            sortable: true,
            sortValue: (status: StoreStatus): number => status.desc.store_id
          },
          {
            title: "Node ID",
            view: (status: StoreStatus): MithrilElement => {
              return m("a", {href: "/nodes/" + status.desc.node.node_id, config: m.route}, status.desc.node.node_id.toString());
            },
            sortable: true,
            sortValue: (status: StoreStatus): number => status.desc.node.node_id
          },
          {
            title: "Address",
            view: (status: StoreStatus): string => status.desc.node.address.address,
            sortable: true
          },
          {
            title: "Started At",
            view: (status: StoreStatus): string => {
              let date = new Date(Utils.Convert.NanoToMilli(status.started_at));
              return Utils.Format.Date(date);
            },
            sortable: true
          },
          {
            title: "Live Bytes",
            view: (status: StoreStatus): string => Utils.Format.Bytes(status.stats.live_bytes),
            sortable: true,
            sortValue: (status: StoreStatus): number => status.stats.live_bytes
          }
        ];

        private static _queryEveryMS: number = 10000;
        public columns: Utils.Property<Table.TableColumn<StoreStatus>[]> = Utils.Prop(Controller.comparisonColumns);
        private _interval: number;

        public constructor(nodeId?: string) {
          this._refresh();
          this._interval = setInterval(() => this._refresh(), Controller._queryEveryMS);
        }

        public onunload(): void {
          clearInterval(this._interval);
        }

        public RenderPrimaryStats(): MithrilElement {
          let allStats: Models.Proto.Status = storeStatuses.totalStatus();
          if (allStats) {
            return m(".primary-stats", [
              m(".stat", [
                m("span.title", "Total Ranges"),
                m("span.value", allStats.range_count)
              ]),
              m(".stat", [
                m("span.title", "Total Live Bytes"),
                m("span.value", Utils.Format.Bytes(allStats.stats.live_bytes))
              ]),
              m(".stat", [
                m("span.title", "Leader Ranges"),
                m("span.value", allStats.leader_range_count)
              ]),
              m(".stat", [
                m("span.title", "Available"),
                m("span.value", Utils.Format.Percentage(allStats.available_range_count, allStats.leader_range_count))
              ]),
              m(".stat", [
                m("span.title", "Fully Replicated"),
                m("span.value", Utils.Format.Percentage(allStats.replicated_range_count, allStats.leader_range_count))
              ])
            ]);
          }
          return m(".primary-stats");
        }

        private _refresh(): void {
          storeStatuses.refresh();
        }
      }

      export function controller(): Controller {
        return new Controller();
      }

      export function view(ctrl: Controller): MithrilElement {
        let comparisonData: Table.TableData<StoreStatus> = {
          columns: ctrl.columns,
          rows: storeStatuses.allStatuses
        };
        return m(".page", [
          m(".section.primary", m("h2", "Stores Overview")),
          m(".section.primary", ctrl.RenderPrimaryStats()),
          m(".section", m(".stats-table", Components.Table.create(comparisonData)))
        ]);
      }
    }

    /**
     * StorePage show the details of a single node.
     */
    export module StorePage {
      import NavigationBar = Components.NavigationBar;

      class Controller {
        private static defaultTargets: NavigationBar.Target[] = [
          {
            title: "Overview",
            route: ""
          },
          {
            title: "Graphs",
            route: "graph"
          }
        ];

        private static isActive: (targ: NavigationBar.Target) => boolean = (t: NavigationBar.Target) => {
          return ((m.route.param("detail") || "") === t.route);
        };

        private static _queryEveryMS: number = 10000;
        exec: Metrics.Executor;
        axes: Metrics.Axis[] = [];
        private _query: Metrics.Query;
        private _interval: number;
        private _storeId: string;

        public onunload(): void {
          clearInterval(this._interval);
        }

        public constructor(storeId: string) {
          this._storeId = storeId;
          this._query = Metrics.NewQuery();
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric(storeId, "keycount"))
                .title("Key Count")
              )
              .label("Count")
            );
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric(storeId, "livecount"))
                .title("Live Value Count")
              )
              .label("Count")
            );
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric(storeId, "valcount"))
                .title("Total Value Count")
              )
              .label("Count")
            );
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric(storeId, "intentcount"))
                .title("Intent Count")
              )
              .label("Count")
            );
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric(storeId, "ranges"))
                .title("Range Count")
              )
              .label("Count")
            );
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric(storeId, "livebytes"))
                .title("Live Bytes")
              )
              .label("Bytes")
              .format(Utils.Format.Bytes)
            );

          this.exec = new Metrics.Executor(this._query);
          this._refresh();
          this._interval = setInterval(() => this._refresh(), Controller._queryEveryMS);
        }

        public RenderPrimaryStats(): MithrilElement {
          let storeStats: Models.Proto.StoreStatus = storeStatuses.GetStatus(this._storeId);
          if (storeStats) {
            return m(".primary-stats", [
              m(".stat", [
                m("span.title", "Started At"),
                m("span.value", Utils.Format.Date(new Date(Utils.Convert.NanoToMilli(storeStats.started_at))))
              ]),
              m(".stat", [
                m("span.title", "Last Updated At"),
                m("span.value", Utils.Format.Date(new Date(Utils.Convert.NanoToMilli(storeStats.updated_at))))
              ]),
              m(".stat", [
                m("span.title", "Total Ranges"),
                m("span.value", storeStats.range_count)
              ]),
              m(".stat", [
                m("span.title", "Total Live Bytes"),
                m("span.value", Utils.Format.Bytes(storeStats.stats.live_bytes))
              ]),
              m(".stat", [
                m("span.title", "Leader Ranges"),
                m("span.value", storeStats.leader_range_count)
              ]),
              m(".stat", [
                m("span.title", "Available"),
                m("span.value", Utils.Format.Percentage(storeStats.available_range_count, storeStats.leader_range_count))
              ]),
              m(".stat", [
                m("span.title", "Fully Replicated"),
                m("span.value", Utils.Format.Percentage(storeStats.replicated_range_count, storeStats.leader_range_count))
              ])
            ]);
          }
          return m(".primary-stats");
        }

        public RenderGraphs(): MithrilElement {
          return m(".charts", this.axes.map((axis: Metrics.Axis) => {
            return m("", { style: "float:left" }, [
              m("h4", axis.title()),
              Components.Metrics.LineGraph.create(this.exec, axis)
            ]);
          }));
        }

        public TargetSet(): NavigationBar.TargetSet {
          return {
            baseRoute: "/stores/" + this._storeId + "/",
            targets: Utils.Prop(Controller.defaultTargets),
            isActive: Controller.isActive
          };
        }

        public GetStoreId(): string {
          return this._storeId;
        }

        private _refresh(): void {
          storeStatuses.refresh();
          this.exec.refresh();
        }

        private _addChart(axis: Metrics.Axis): void {
          axis.selectors().forEach((s: Metrics.Select.Selector) => this._query.selectors().push(s));
          this.axes.push(axis);
        }
      }

      export function controller(): Controller {
        let storeId: string = m.route.param("store_id");
        return new Controller(storeId);
      }

      export function view(ctrl: Controller): MithrilElement {
        let detail: string = m.route.param("detail");

        // Page title. 
        let title: string = "Stores: Store " + ctrl.GetStoreId();
        if (detail === "graph") {
          title += ": Graphs";
        }

        // Primary content
        let primaryContent: MithrilElement;
        if (detail === "graph") {
          primaryContent = ctrl.RenderGraphs();
        } else {
          primaryContent = ctrl.RenderPrimaryStats();
        }

        return m(".page", [
          m(".section.primary", [
            m.component(NavigationBar, ctrl.TargetSet()),
            m("h2", title)
          ]),
          m(".section", primaryContent)
        ]);
      }
    }
  }
}
