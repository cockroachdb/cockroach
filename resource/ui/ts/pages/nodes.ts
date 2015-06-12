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
    /**
     * Nodes is the view for exploring the status of all nodes.
     */
    export module Nodes {
        import metrics = Models.Metrics;
        var nodeStatuses = new Models.Status.Nodes();

        function _nodeMetric(nodeId: string, metric:string):string {
            return "cr.node." + metric + "." + nodeId;
        }

        /**
         * NodesPage show a list of all the available nodes.
         */
        export module NodesPage {
            class Controller {
                private static _queryEveryMS = 10000;
                private _interval: number;

                private _refresh():void {
                    nodeStatuses.refresh();
                }

                public constructor(nodeId?:string) {
                    this._refresh();
                    this._interval = setInterval(() => this._refresh(), Controller._queryEveryMS);
                }

                public onunload() {
                    clearInterval(this._interval);
                }
            }

            export function controller():Controller {
                return new Controller();
            }

            export function view(ctrl:Controller) {
                return m("div", [
                    m("h2", "Nodes List"),
                    m("ul", [
                        nodeStatuses.GetNodeIds().map(function(nodeId) {
                            var desc = nodeStatuses.GetDesc(nodeId);
                            return m("li", { key: desc.node_id },
                                m("div", [
                                    m.trust("&nbsp;&bull;&nbsp;"),
                                    m("a[href=/nodes/" + desc.node_id + "]", { config: m.route }, "Node:" + desc.node_id),
                                    " with Address:" + desc.address.network + "-" + desc.address.address
                                ]));
                        }),
                    ]),
                    nodeStatuses.AllDetails()
                ]);
            }
        }

        /**
         * NodePage show the details of a single node.
         */
        export module NodePage {
            class Controller {
                exec:metrics.Executor;
                axes: metrics.Axis[] = [];
                private _query:metrics.Query;
                private static _queryEveryMS = 10000;
                private _interval: number;
                private _nodeId: string

                private _refresh():void {
                    nodeStatuses.refresh();
                    this.exec.refresh();
                }

                private _addChart(axis:metrics.Axis):void {
                    axis.selectors().forEach((s) => this._query.selectors().push(s));
                    this.axes.push(axis);
                }

                public constructor(nodeId:string) {
                    this._nodeId = nodeId;
                    this._query = metrics.NewQuery();
                    this._addChart(
                        metrics.NewAxis(
                            metrics.select.AvgRate(_nodeMetric(nodeId, "calls.success"))
                                .title("Successful Calls")
                            )
                        .label("Count / 10 sec."));
                    this._addChart(
                        metrics.NewAxis(
                            metrics.select.AvgRate(_nodeMetric(nodeId, "calls.error"))
                                .title("Error Calls")
                            )
                        .label("Count / 10 sec."));

                    this.exec = new metrics.Executor(this._query);
                    this._refresh();
                    this._interval = setInterval(() => this._refresh(), Controller._queryEveryMS);
                }

                public onunload() {
                    clearInterval(this._interval);
                }
            }

            export function controller():Controller {
                var nodeId = m.route.param("node_id");
                return new Controller(nodeId);
            }

            export function view(ctrl:Controller) {
                var nodeId = m.route.param("node_id");
                return m("div", [
                    m("h2", "Node Status"),
                    m("div", [
                        m("h3", "Node: " + nodeId),
                        nodeStatuses.Details(nodeId)
                    ]),
                    m(".charts", ctrl.axes.map((axis:metrics.Axis) => {
                        return m("", { style : "float:left" }, [
                            m("h4", axis.title()),
                            Components.Metrics.LineGraph.create(ctrl.exec, axis)
                        ]);
                    }))
                ]);
            }
        }
    }
}
