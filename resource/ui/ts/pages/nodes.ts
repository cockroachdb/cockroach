// source: pages/nodes.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../models/status.ts" />
/// <reference path="../models/timeseries.ts" />
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

        interface QueryHolder {
            Result: Utils.QueryCache<Models.Proto.QueryResultSet>;
            Query: metrics.Query;
        }

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
                charts: QueryHolder[] = [];
                private static _queryEveryMS = 10000;
                private _interval: number;
                private _nodeId: string;

                private _refresh():void {
                    nodeStatuses.refresh();
                    for (var i = 0; i < this.charts.length; i++) {
                        this.charts[i].Result.refresh();
                    }
                }

                private _addChart(q:metrics.Query):void {
                    this.charts.push({
                        Query: q, 
                        Result: new Utils.QueryCache(q.execute),
                    });
                }

                public constructor(nodeId:string) {
                    this._nodeId = nodeId;
                    this._addChart(
                        metrics.NewQuery(
                            metrics.select.AvgRate(_nodeMetric(nodeId, "calls.success")))
                        .title("Successful Calls Rate"));
                    this._addChart(
                        metrics.NewQuery(
                            metrics.select.AvgRate(_nodeMetric(nodeId, "calls.error")))
                        .title("Error Calls Rate"));

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
                    m(".charts", ctrl.charts.map((chart:QueryHolder) => {
                        return m("", { style : "float:left" }, [
                            m("h4", chart.Query.title()),
                            Components.Metrics.LineGraph.create(chart.Result)
                        ]);
                    }))
                ]);
            }
        }
    }
}
