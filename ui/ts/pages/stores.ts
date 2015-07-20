// source: pages/stores.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../models/status.ts" />
/// <reference path="../components/metrics.ts" />
/// <reference path="../components/table.ts" />

// Author: Bram Gruneir (bram+code@cockroachlabs.com)

/**
 * AdminViews is the primary module for Cockroaches administrative web
 * interface.
 */
module AdminViews {
  "use strict";

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
            data: (status: StoreStatus): _mithril.MithrilVirtualElement => {
              return m("a", {href: "/stores/" + status.desc.store_id, config: m.route}, status.desc.store_id.toString());
            }
          },
          {
            title: "Node ID",
            data: (status: StoreStatus): _mithril.MithrilVirtualElement => {
              return m("a", {href: "/nodes/" + status.desc.node.node_id, config: m.route}, status.desc.node.node_id.toString());
            }
          },
          {
            title: "Address",
            data: (status: StoreStatus): string => status.desc.node.address.address
          },
          {
            title: "Started At",
            data: (status: StoreStatus): string => {
              let date = new Date(Utils.Convert.NanoToMilli(status.started_at));
              return Utils.Format.Date(date);
            }
          },
          {
            title: "Live Bytes",
            data: (status: StoreStatus): string => Utils.Format.Bytes(status.stats.live_bytes)
          }
        ];

        private static _queryEveryMS: number = 10000;
        private _interval: number;

        public constructor(nodeId?: string) {
          this._refresh();
          this._interval = setInterval(() => this._refresh(), Controller._queryEveryMS);
        }

        public onunload(): void {
          clearInterval(this._interval);
        }

        public GetColumns(): Table.TableColumn<StoreStatus>[] {
          return Controller.comparisonColumns;
        }

        private _refresh(): void {
          storeStatuses.refresh();
        }
      }

      export function controller(): Controller {
        return new Controller();
      }

      export function view(ctrl: Controller): _mithril.MithrilVirtualElement {
        let comparisonData: Table.TableData<StoreStatus> = {
          columns: ctrl.GetColumns,
          rows: (): StoreStatus[] => storeStatuses.GetAllStatuses()
        };
        return m("div", [
          m("h2", "Stores List"),
          m(".stats-table", Components.Table.create(comparisonData)),
          storeStatuses.AllDetails()
        ]);
      }
    }

    /**
     * StorePage show the details of a single node.
     */
    export module StorePage {
      class Controller {
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

      export function view(ctrl: Controller): _mithril.MithrilVirtualElement {
        let storeId: string = m.route.param("store_id");
        return m("div", [
          m("h2", "Store Status"),
          m("div", [
            m("h3", "Store: " + storeId),
            storeStatuses.Details(storeId)
          ]),
          m(".charts", ctrl.axes.map((axis: Metrics.Axis) => {
            return m("", { style: "float:left" }, [
              m("h4", axis.title()),
              Components.Metrics.LineGraph.create(ctrl.exec, axis)
            ]);
          }))
        ]);
      }
    }
  }
}
