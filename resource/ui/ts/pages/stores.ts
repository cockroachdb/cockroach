// source: pages/stores.ts
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
     * Stores is the view for exploring the status of all Stores.
     */
    export module Stores {
        import Metrics = Models.Metrics;
        let storeStatuses: Models.Status.Stores = new Models.Status.Stores();

        function _storeMetric(storeId: string, metric: string): string {
            return "cr.store." + metric + "." + storeId;
        }

        /**
         * StoresPage show a list of all the available nodes.
         */
        export module StoresPage {
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
                    storeStatuses.refresh();
                }
            }

            export function controller(): Controller {
                return new Controller();
            }

            function _singleStoreView(storeId: string): _mithril.MithrilVirtualElement {
                let desc: Models.Proto.StoreDescriptor = storeStatuses.GetDesc(storeId);
                return m("li", { key: desc.store_id }, m("div", [
                    m.trust("&nbsp;&bull;&nbsp;"),
                    m("a[href=/stores/" + storeId + "]", { config: m.route }, "Store:" + storeId),
                    " on ",
                    m("a[href=/nodes/" + desc.node.node_id + "]", { config: m.route }, "Node:" + desc.node.node_id),
                    " with Address:" + desc.node.address.network + "-" + desc.node.address.address
                ]));
            }

            export function view(ctrl: Controller): _mithril.MithrilVirtualElement {
                return m("div", [
                    m("h2", "Nodes List"),
                    m("ul", [storeStatuses.GetStoreIds().map(_singleStoreView)]),
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
