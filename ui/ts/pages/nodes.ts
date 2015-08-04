// source: pages/nodes.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../models/status.ts" />
/// <reference path="../components/metrics.ts" />
/// <reference path="../components/table.ts" />
/// <reference path="../util/format.ts" />

// Author: Bram Gruneir (bram+code@cockroachlabs.com)
// Author: Matt Tracy (matt@cockroachlabs.com)

/**
 * AdminViews is the primary module for Cockroaches administrative web
 * interface.
 */
module AdminViews {
  "use strict";

  /**
   * Nodes is the view for exploring the status of all nodes.
   */
  export module Nodes {
    import Metrics = Models.Metrics;
    import Table = Components.Table;
    import NodeStatus = Models.Proto.NodeStatus;

    let nodeStatuses: Models.Status.Nodes = new Models.Status.Nodes();

    function _nodeMetric(nodeId: string, metric: string): string {
      return "cr.node." + metric + "." + nodeId;
    }

    /**
     * NodesPage show a list of all the available nodes.
     */
    export module NodesPage {
      class Controller {
        private static comparisonColumns: Table.TableColumn<NodeStatus>[] = [
          {
            title: "Node ID",
            view: (status: NodeStatus): _mithril.MithrilVirtualElement =>
              m("a", {href: "/nodes/" + status.desc.node_id, config: m.route}, status.desc.node_id.toString()),
            sortable: true,
            sortValue: (status: NodeStatus): number => status.desc.node_id
          },
          {
            title: "Address",
            view: (status: NodeStatus): string => status.desc.address.address,
            sortable: true
          },
          {
            title: "Started At",
            view: (status: NodeStatus): string => {
              let date = new Date(Utils.Convert.NanoToMilli(status.started_at));
              return Utils.Format.Date(date);
            },
            sortable: true
          },
          {
            title: "Live Bytes",
            view: (status: NodeStatus): string => Utils.Format.Bytes(status.stats.live_bytes),
            sortable: true,
            sortValue: (status: NodeStatus): number => status.stats.live_bytes
          },
          {
            title: "Logs",
            view: (status: NodeStatus): _mithril.MithrilVirtualElement =>
              m("a", { href: "/logs/" + status.desc.node_id, config: m.route }, "Log")
          }
        ];

        private static _queryEveryMS: number = 10000;

        public columns: Utils.Property<Table.TableColumn<NodeStatus>[]> = Utils.Prop(Controller.comparisonColumns);
        private _interval: number;

        public constructor(nodeId?: string) {
          this._refresh();
          this._interval = setInterval(() => this._refresh(), Controller._queryEveryMS);
        }

        public onunload(): void {
          clearInterval(this._interval);
        }

        public PrimaryStats(): _mithril.MithrilVirtualElement {
          let allStats: Models.Proto.Status = nodeStatuses.totalStatus();
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
          nodeStatuses.refresh();
        }
      }

      export function controller(): Controller {
        return new Controller();
      }

      export function view(ctrl: Controller): _mithril.MithrilVirtualElement {
        let comparisonData: Table.TableData<NodeStatus> = {
          columns: ctrl.columns,
          rows: nodeStatuses.allStatuses
        };
        return m("div", [
          m("h2", "Nodes List"),
          m(".section", ctrl.PrimaryStats()),
          m(".stats-table", Components.Table.create(comparisonData))
        ]);
      }
    }

    /**
     * NodePage show the details of a single node.
     */
    export module NodePage {
      class Controller {
        private static _queryEveryMS: number = 10000;
        exec: Metrics.Executor;
        axes: Metrics.Axis[] = [];
        private _query: Metrics.Query;
        private _interval: number;
        private _nodeId: string;

        public onunload(): void {
          clearInterval(this._interval);
        }

        public constructor(nodeId: string) {
          this._nodeId = nodeId;
          this._query = Metrics.NewQuery();
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.AvgRate(_nodeMetric(nodeId, "calls.success"))
                .title("Successful Calls")
              )
              .label("Count / 10 sec."));
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.AvgRate(_nodeMetric(nodeId, "calls.error"))
                .title("Error Calls")
              )
              .label("Count / 10 sec."));

          this.exec = new Metrics.Executor(this._query);
          this._refresh();
          this._interval = setInterval(() => this._refresh(), Controller._queryEveryMS);
        }

        public PrimaryStats(): _mithril.MithrilVirtualElement {
          let nodeStats: Models.Proto.NodeStatus = nodeStatuses.GetStatus(this._nodeId);
          if (nodeStats) {
            return m(".primary-stats", [
              m(".stat", [
                m("span.title", "Started At"),
                m("span.value", Utils.Format.Date(new Date(Utils.Convert.NanoToMilli(nodeStats.started_at))))
              ]),
              m(".stat", [
                m("span.title", "Last Updated At"),
                m("span.value", Utils.Format.Date(new Date(Utils.Convert.NanoToMilli(nodeStats.updated_at))))
              ]),
              m(".stat", [
                m("span.title", "Total Ranges"),
                m("span.value", nodeStats.range_count)
              ]),
              m(".stat", [
                m("span.title", "Total Live Bytes"),
                m("span.value", Utils.Format.Bytes(nodeStats.stats.live_bytes))
              ]),
              m(".stat", [
                m("span.title", "Leader Ranges"),
                m("span.value", nodeStats.leader_range_count)
              ]),
              m(".stat", [
                m("span.title", "Available"),
                m("span.value", Utils.Format.Percentage(nodeStats.available_range_count, nodeStats.leader_range_count))
              ]),
              m(".stat", [
                m("span.title", "Fully Replicated"),
                m("span.value", Utils.Format.Percentage(nodeStats.replicated_range_count, nodeStats.leader_range_count))
              ])
            ]);
          }
          return m(".primary-stats");
        }

        private _refresh(): void {
          nodeStatuses.refresh();
          this.exec.refresh();
        }

        private _addChart(axis: Metrics.Axis): void {
          axis.selectors().forEach((s: Metrics.Select.Selector) => this._query.selectors().push(s));
          this.axes.push(axis);
        }
      }

      export function controller(): Controller {
        let nodeId: string = m.route.param("node_id");
        return new Controller(nodeId);
      }

      export function view(ctrl: Controller): _mithril.MithrilVirtualElement {
        let nodeId: string = m.route.param("node_id");
        return m("div", [
          m("h2", "Node Status"),
          m(".section", [
            m("h3", "Node: " + nodeId),
            ctrl.PrimaryStats()
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
