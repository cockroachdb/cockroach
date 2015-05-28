// source: pages/nodes.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../models/node_status.ts" />
/// <reference path="../models/timeseries.ts" />
/// <reference path="../components/metrics.ts" />

// Author: Bram Gruneir (bram.gruneir@gmail.com)

/**
 * AdminViews is the primary module for Cockroaches administrative web
 * interface.
 */
module AdminViews {
  /**
   * Nodes is the view for exploring the status of all nodes.
   */
  export module Nodes {
    // TODO(bram): Lots of duplicate code between here and stores.ts, refactor
    // it into one place.
    export interface RefreshFunction {
      f: () => _mithril.MithrilPromise<any>;
      o: any;
    }

    export interface QueryManagerSourceMap {
      [nodeId: number]: { [source: string]: Models.Metrics.QueryManager }
    }

    export var nodeStatuses: Models.NodeStatus.Nodes = new Models.NodeStatus.Nodes();
    export var queryManagers: QueryManagerSourceMap = {};

    export class Controller implements _mithril.MithrilController {
      private static _queryEveryMS = 10000;
      private _interval: number;
      private _nodeId: string;
      private _refreshFunctions: RefreshFunction[]

      private _refresh():void {
        for (var i = 0; i < this._refreshFunctions.length; i++) {
          this._refreshFunctions[i].f.call(this._refreshFunctions[i].o);
        }
      }

      private static _queryManagerBuilder(nodeId: string, agg: Models.Metrics.QueryAggregator, source:string): Models.Metrics.QueryManager {
        var query = new Models.Metrics.RecentQuery(10 * 60 * 1000, agg, "cr.node." + source + "." + nodeId);
        return new Models.Metrics.QueryManager(query);
      }

      private _addChart(agg: Models.Metrics.QueryAggregator, source:string):void {
        var name = agg + ":" + source;
        if (queryManagers[this._nodeId][name] == null) {
          queryManagers[this._nodeId][name] = Controller._queryManagerBuilder(this._nodeId, agg, source);
        }
        this._refreshFunctions.push({ f: queryManagers[this._nodeId][name].refresh, o: queryManagers[this._nodeId][name] });
      }

      public constructor(nodeId?:string) {
        this._refreshFunctions = [{
          f: nodeStatuses.Query,
          o: nodeStatuses
        }];

        if (nodeId != null) {
          this._nodeId = nodeId;

          // Add a collection of charts.
          if (queryManagers[nodeId] == null) {
            queryManagers[nodeId] = {};
          }
          this._addChart(Models.Metrics.QueryAggregator.AVG, "calls.success");
          this._addChart(Models.Metrics.QueryAggregator.AVG_RATE, "calls.success");
          this._addChart(Models.Metrics.QueryAggregator.AVG, "calls.error");
          this._addChart(Models.Metrics.QueryAggregator.AVG_RATE, "calls.error");
        } else {
          this._nodeId = null;
        }

        this._refresh();
        this._interval = setInterval(() => this._refresh(), Controller._queryEveryMS);
      }

      public onunload() {
        clearInterval(this._interval);
      }
    }

    /**
     * NodesPage show a list of all the available nodes.
     */
    export module NodesPage {
      export function controller():Controller {
        return new Controller();
      }
      export function view(ctrl: Controller) {
        return m("div", [
          m("h2", "Nodes List"),
          m("ul", [
            Object.keys(nodeStatuses.desc()).sort().map(function(nodeId) {
              var desc = nodeStatuses.desc()[nodeId];
              return m("li", { key: desc.node_id },
                m("div", [
                  m.trust("&nbsp;&bull;&nbsp;"),
                  m("a[href=/nodes/" + desc.node_id + "]", { config: m.route }, "Node:" + desc.node_id),
                  " with Address:" + desc.address.network + "-" + desc.address.address
                ]));
            }),
          ]),
        ]);
      }
    }

    /**
     * NodePage show the details of a single node.
     */
    export module NodePage {
      export function controller():Controller {
        var nodeId = m.route.param("node_id");
        return new Nodes.Controller(nodeId);
      }
      export function view(ctrl:Controller) {
        var nodeId = m.route.param("node_id");
        return m("div", [
          m("h2", "Node Status"),
          m("div", [
            m("h3", "Node: " + nodeId),
            nodeStatuses.Details(nodeId)
          ]),
          m("table", [
            m("tr", [
              m("td", [
                m("h4", "Successful Calls"),
                Components.Metrics.LineGraph.create(Nodes.queryManagers[nodeId]["1:calls.success"])
              ]),
              m("td", [
                m("h4", "Successful Calls Rate"),
                Components.Metrics.LineGraph.create(Nodes.queryManagers[nodeId]["2:calls.success"])
              ])
            ]),
            m("tr", [
              m("td", [
                m("h4", "Error Calls"),
                Components.Metrics.LineGraph.create(Nodes.queryManagers[nodeId]["1:calls.error"])
              ]),
              m("td", [
                m("h4", "Error Calls Rate"),
                Components.Metrics.LineGraph.create(Nodes.queryManagers[nodeId]["2:calls.error"])
              ])
            ])
          ])
        ]);
      }
    }
  }
}
