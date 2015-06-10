// source: models/store_status.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../typings/d3/d3.d.ts" />
/// <reference path="stats.ts" />
// Author: Bram Gruneir (bram+code@cockroachlabs.com)
// Author: Matt Tracy (matt@cockroachlabs.com)

module Models {
    export module Status {
        import promise = _mithril.MithrilPromise;

        export interface StoreStatusResponseSet {
            d: Proto.StoreStatus[]
        }

        export interface StoreStatusMap {
            [storeId: string]: Proto.StoreStatus
        }

        function _availability(status: Proto.Status): string {
            if (status.leader_range_count == 0) {
                return "100%";
            }
            return Math.floor(status.available_range_count / status.leader_range_count * 100).toString() + "%";
        }

        function _replicated(status: Proto.Status): string {
            if (status.leader_range_count == 0) {
                return "100%";
            }
            return Math.floor(status.replicated_range_count / status.leader_range_count * 100).toString() + "%";
        }

        var _datetimeFormatter = d3.time.format("%Y-%m-%d %H:%M:%S");
        function _formatDate(nanos: number): string {
            var datetime = new Date(nanos / 1.0e6);
            return _datetimeFormatter(datetime);
        }

        export class Stores {
            private _data:StoreStatusMap = {};

            Query(): promise<StoreStatusResponseSet> {
                var url = "/_status/stores/";
                return m.request({ url: url, method: "GET", extract: nonJsonErrors })
                    .then((results: StoreStatusResponseSet) => {
                        results.d.forEach((status) => {
                            var storeId = status.desc.store_id;
                            this._data[storeId] = status;
                        });
                        return results;
                    });
            }

            public GetStoreIds():string[] {
                return Object.keys(this._data).sort();
            }

            public GetDesc(storeId:string):Proto.StoreDescriptor {
                return this._data[storeId].desc;
            }

            public Details(storeId:string):_mithril.MithrilVirtualElement {
                var store = this._data[storeId];
                if (store == null) {
                    return m("div", "No data present yet.")
                }
                return m("div",[
                    m("table", [
                      m("tr", [m("td", "Node Id:"), m("td", m("a[href=/nodes/" + store.desc.node.node_id + "]", { config: m.route }, store.desc.node.node_id))]),
                      m("tr", [m("td", "Node Network:"), m("td", store.desc.node.address.network)]),
                      m("tr", [m("td", "Node Address:"), m("td", store.desc.node.address.address)]),
                      m("tr", [m("td", "Started at:"), m("td", _formatDate(store.started_at))]),
                      m("tr", [m("td", "Updated at:"), m("td", _formatDate(store.updated_at))]),
                      m("tr", [m("td", "Ranges:"), m("td", store.range_count)]),
                      m("tr", [m("td", "Leader Ranges:"), m("td", store.leader_range_count)]),
                      m("tr", [m("td", "Available Ranges:"), m("td", store.available_range_count)]),
                      m("tr", [m("td", "Availablility:"), m("td", _availability(store))]),
                      m("tr", [m("td", "Under-Replicated Ranges:"), m("td", store.leader_range_count - store.replicated_range_count)]),
                      m("tr", [m("td", "Fully Replicated:"), m("td", _replicated(store))])
                    ]),
                    Stats.CreateStatsTable(store.stats)
                ]);
            }

            public AllDetails(): _mithril.MithrilVirtualElement {
                var status:Proto.Status = {
                    range_count: 0,
                    updated_at: 0,
                    started_at: 0,
                    leader_range_count: 0,
                    replicated_range_count: 0,
                    available_range_count: 0,
                    stats: Proto.NewMVCCStats()
                };

                for (var storeId in this._data) {
                    var storeStatus = this._data[storeId];
					Proto.AccumulateStatus(status, storeStatus);
                };

                return m("div", [
                    m("h2", "Details"),
                    m("table", [
                        m("tr", [m("td", "Updated at:"), m("td", _formatDate(status.updated_at))]),
                        m("tr", [m("td", "Ranges:"), m("td", status.range_count)]),
                        m("tr", [m("td", "Leader Ranges:"), m("td", status.leader_range_count)]),
                        m("tr", [m("td", "Available Ranges:"), m("td", status.available_range_count)]),
                        m("tr", [m("td", "Availablility:"), m("td", _availability(status))]),
                        m("tr", [m("td", "Under-Replicated Ranges:"), m("td", status.leader_range_count - status.replicated_range_count)]),
                        m("tr", [m("td", "Fully Replicated:"), m("td", _replicated(status))])
                    ]),
                    Stats.CreateStatsTable(status.stats)
                ]);
            }
        }

        export interface NodeStatusResponseSet {
            d: Proto.NodeStatus[]
        }

        export interface NodeStatusMap {
            [nodeId: string]: Proto.NodeStatus
        }

        export class Nodes {
            private _data:NodeStatusMap = {}

            Query(): promise<NodeStatusResponseSet> {
                var url = "/_status/nodes/";
                return m.request({ url: url, method: "GET", extract: nonJsonErrors })
                    .then((results: NodeStatusResponseSet) => {
                        results.d.forEach((status) => {
                            var nodeId = status.desc.node_id;
							this._data[nodeId] = status;
                        });
                        return results;
                    });
            }

            public GetNodeIds():string[] {
                return Object.keys(this._data).sort();
            }

            public GetDesc(nodeId:string):Proto.NodeDescriptor {
                return this._data[nodeId].desc;
            }

            public Details(nodeId: string): _mithril.MithrilVirtualElement {
                var node = this._data[nodeId];
                if (node == null) {
                    return m("div", "No data present yet.")
                }

                return m("div", [
                    m("table", [
                        m("tr", [m("td", "Stores (" + node.store_ids.length + "):"),
                            m("td", [node.store_ids.map(function(storeId) {
                                return m("div", [
                                    m("a[href=/stores/" + storeId + "]", { config: m.route }, storeId),
                                    " "]);
                            })])
                        ]),
                        m("tr", [m("td", "Network:"), m("td", node.desc.address.network)]),
                        m("tr", [m("td", "Address:"), m("td", node.desc.address.address)]),
                        m("tr", [m("td", "Started at:"), m("td", _formatDate(node.started_at))]),
                        m("tr", [m("td", "Updated at:"), m("td", _formatDate(node.updated_at))]),
                        m("tr", [m("td", "Ranges:"), m("td", node.range_count)]),
                        m("tr", [m("td", "Leader Ranges:"), m("td", node.leader_range_count)]),
                        m("tr", [m("td", "Available Ranges:"), m("td", node.available_range_count)]),
                        m("tr", [m("td", "Availablility:"), m("td", _availability(node))]),
                        m("tr", [m("td", "Under-Replicated Ranges:"), m("td", node.leader_range_count - node.replicated_range_count)]),
                        m("tr", [m("td", "Fully Replicated:"), m("td", _replicated(node))])
                    ]),
                    Stats.CreateStatsTable(node.stats)
                ]);
            }

            public AllDetails(): _mithril.MithrilVirtualElement {
                var status:Proto.NodeStatus = <Proto.NodeStatus>{
                    range_count: 0,
                    updated_at: 0,
                    leader_range_count: 0,
                    replicated_range_count: 0,
                    available_range_count: 0,
                    stats: Proto.NewMVCCStats(),
                };

                for (var nodeId in this._data) {
                    var nodeStatus = this._data[nodeId];
					Proto.AccumulateStatus(status, nodeStatus);
                };

                return m("div", [
                    m("h2", "Details"),
                    m("table", [
                        m("tr", [m("td", "Updated at:"), m("td", _formatDate(status.updated_at))]),
                        m("tr", [m("td", "Ranges:"), m("td", status.range_count)]),
                        m("tr", [m("td", "Leader Ranges:"), m("td", status.leader_range_count)]),
                        m("tr", [m("td", "Available Ranges:"), m("td", status.available_range_count)]),
                        m("tr", [m("td", "Availablility:"), m("td", _availability(status))]),
                        m("tr", [m("td", "Under-Replicated Ranges:"), m("td", status.leader_range_count - status.replicated_range_count)]),
                        m("tr", [m("td", "Fully Replicated:"), m("td", _replicated(status))])
                    ]),
                    Stats.CreateStatsTable(status.stats)
                ]);
            }
        }

        /**
         * nonJsonErrors ensures that error messages returned from the server
         * are parseable as JSON strings.
         */
        function nonJsonErrors(xhr: XMLHttpRequest, opts: _mithril.MithrilXHROptions): string {
            return xhr.status > 200 ? JSON.stringify(xhr.responseText) : xhr.responseText;
        }
    }
}
