// source: pages/nodes.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../models/status.ts" />
/// <reference path="../components/metrics.ts" />

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
    let nodeStatuses: Models.Status.Nodes = new Models.Status.Nodes();

    function _nodeMetric(nodeId: string, metric: string): string {
      return "cr.node." + metric + "." + nodeId;
    }

    /**
     * NodesPage show a list of all the available nodes.
     */
    export module NodesPage {
      class Controller {
        private static _queryEveryMS: number = 10000;
        private _interval: number;

        public constructor(nodeId?: string) {
          this._refresh();
          this._interval = setInterval(() => this._refresh(), Controller._queryEveryMS);
        }

        public onunload(): void {
          clearInterval(this._interval);
        }

        private _refresh(): void {
          nodeStatuses.refresh();
        }
      }

      export function controller(): Controller {
        return new Controller();
      }

      function _singleNodeView(nodeId: string): _mithril.MithrilVirtualElement {
        let desc: Models.Proto.NodeDescriptor = nodeStatuses.GetDesc(nodeId);
        return m("li", { key: desc.node_id }, m("div", [
          m.trust("&nbsp;&bull;&nbsp;"),
          m("a[href=/nodes/" + desc.node_id + "]", { config: m.route }, "Node:" + desc.node_id),
          " with Address:" + desc.address.network + "-" + desc.address.address
        ]));
      }

      export function view(ctrl: Controller): _mithril.MithrilVirtualElement {
        return m("div", [
          m("h2", "Nodes List"),
          m("ul", [nodeStatuses.GetNodeIds().map(_singleNodeView)]),
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
