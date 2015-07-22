// source: pages/nodes.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../models/status.ts" />
/// <reference path="../components/metrics.ts" />
/// <reference path="../components/table.ts" />
/// <reference path="../util/format.ts" />

// Author: Bram Gruneir (bram+code@cockroachlabs.com)

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
            data: (status: NodeStatus): _mithril.MithrilVirtualElement => {
              return m("a", {href: "/nodes/" + status.desc.node_id, config: m.route}, status.desc.node_id.toString());
            }
          },
          {
            title: "Address",
            data: (status: NodeStatus): string => status.desc.address.address
          },
          {
            title: "Started At",
            data: (status: NodeStatus): string => {
              let date = new Date(Utils.Convert.NanoToMilli(status.started_at));
              return Utils.Format.Date(date);
            }
          },
          {
            title: "Live Bytes",
            data: (status: NodeStatus): string => Utils.Format.Bytes(status.stats.live_bytes)
          },
          {
            title: "Logs",
            data: (status: NodeStatus): _mithril.MithrilVirtualElement => {
              return m("a", { href: "/logs/" + status.desc.node_id, config: m.route }, "Log");
            }
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

        public GetColumns(): Table.TableColumn<NodeStatus>[] {
          return Controller.comparisonColumns;
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
          columns: ctrl.GetColumns,
          rows: (): NodeStatus[] => nodeStatuses.GetAllStatuses()
        };
        return m("div", [
          m("h2", "Nodes List"),
          m(".stats-table", Components.Table.create(comparisonData)),
          nodeStatuses.AllDetails()
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

        private _refresh(): void {
          nodeStatuses.refresh();
          this.exec.refresh();
        }

        private _addChart(axis: Metrics.Axis): void {
          axis.selectors().forEach((s: Metrics.Select.Selector) => this._query.selectors().push(s));
          this.axes.push(axis);
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
      }

      export function controller(): Controller {
        let nodeId: string = m.route.param("node_id");
        return new Controller(nodeId);
      }

      export function view(ctrl: Controller): _mithril.MithrilVirtualElement {
        let nodeId: string = m.route.param("node_id");
        return m("div", [
          m("h2", "Node Status"),
          m("div", [
            m("h3", "Node: " + nodeId),
            nodeStatuses.Details(nodeId)
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
