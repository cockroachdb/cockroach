// source: pages/nodes.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../models/node_status.ts" />

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
    export var nodeStatuses: Models.NodeStatus.Nodes = new Models.NodeStatus.Nodes();
    export class Controller {
      private static _queryEveryMS = 10000;
      private _interval: number;

      public constructor() {
        nodeStatuses.Query();
        this._interval = setInterval(() => nodeStatuses.Query(), Controller._queryEveryMS);
      }

      public onunload() {
        clearInterval(this._interval);
      }
    }

    /**
     * NodesPage show a list of all the available nodes.
     */
    export module NodesPage {
      export function controller() {
        return new Controller();
      }
      export function view(ctrl: Controller) {
        return m("div", [
          m("h2", "Nodes Status"),
          m("div", [
            m("h3", "Nodes"),
            m("ul", [
              Object.keys(nodeStatuses.desc()).sort().map(function(nodeId) {
                var desc = nodeStatuses.desc()[nodeId];
                return m("li", { key: desc.node_id },
                  m("a[href=/nodes/" + nodeId + "]",
                    { config: m.route },
                    "ID:" + nodeId + " Address:" + desc.address.network + "-" + desc.address.address));
              }),
            ]),
          ])
        ]);
      }
    }

    /**
     * NodePage show the details of a single node.
     */
    export module NodePage {
      export function controller() {
        return new Controller();
      }
      export function view(ctrl:Controller) {
        var nodeId = m.route.param("node_id");
        return m("div", [
          m("h2", "Node Status"),
          m("div", [
            m("h3", "Node: " + nodeId),
            m("p", JSON.stringify(nodeStatuses.statuses()[nodeId]))
          ])
        ]);
      }
    }
  }
}
