// source: models/node_status.ts
/// <reference path="proto.ts" />
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../typings/d3/d3.d.ts" />
/// <reference path="stats.ts" />
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

module Models {
    export module NodeStatus {
        import promise = _mithril.MithrilPromise;

        export interface NodeStatusResponseSet {
            d: Proto.NodeStatus[]
        }

        export interface NodeStatusListMap {
            [nodeId: number]: Proto.NodeStatus[]
        }

        export interface NodeDescriptionMap {
            [nodeId: number]: Proto.NodeDescriptor
        }

        export interface NodeStatusMap {
            [nodeId: number]: Proto.NodeStatus
        }

        export class Nodes {
            private static _dataLimit = 100000;
            private static _dataPrunedSize = 90000;
            private _data: _mithril.MithrilProperty<NodeStatusListMap> = m.prop(<NodeStatusListMap> {});

            public desc: _mithril.MithrilProperty<NodeDescriptionMap> = m.prop(<NodeDescriptionMap> {});
            public statuses: _mithril.MithrilProperty<NodeStatusMap> = m.prop(<NodeStatusMap> {});

            Query(): _mithril.MithrilPromise<NodeStatusResponseSet> {
                var url = "/_status/nodes/";
                return m.request({ url: url, method: "GET", extract: nonJsonErrors })
                    .then((results: NodeStatusResponseSet) => {
                        results.d.forEach((status) => {
                            var nodeId = status.desc.node_id;
                            if (this._data()[nodeId] == null) {
                                this._data()[nodeId] = [];
                            }
                            var statusList = this._data()[nodeId];
                            if ((statusList.length == 0) ||
                                (statusList[statusList.length - 1].updated_at < status.updated_at)) {
                                this._data()[nodeId].push(status);
                                this.statuses()[nodeId] = status;
                            }
                        });
                        this._pruneOldEntries();
                        this._updateDescriptions();
                        return results;
                    });
            }

            private _updateDescriptions(): void {
                this.desc(<NodeDescriptionMap> {});
                for (var nodeId in this._data()) {
                    this.desc()[nodeId] = this._data()[nodeId][this._data()[nodeId].length - 1].desc;
                }
            }

            private _pruneOldEntries(): void {
                for (var nodeId in this._data()) {
                    var status = <Proto.NodeStatus[]>this._data()[nodeId];
                    if (status.length > Nodes._dataLimit) {
                        status = status.slice(status.length - Nodes._dataPrunedSize,status.length - 1)
                    }
                }
            }

            // TODO(Bram): Move to utility class.
            private static _availability(status: Proto.NodeStatus): string {
                if (status.leader_range_count == 0) {
                    return "100%";
                }
                return Math.floor(status.available_range_count / status.leader_range_count * 100).toString() + "%";
            }

            // TODO(Bram): Move to utility class.
            private static _replicated(status: Proto.NodeStatus): string {
                if (status.leader_range_count == 0) {
                    return "100%";
                }
                return Math.floor(status.replicated_range_count / status.leader_range_count * 100).toString() + "%";
            }

            // TODO(Bram): Move to utility class.
            private static _datetimeFormater = d3.time.format("%Y-%m-%d %H:%M:%S")
            private static _formatDate(nanos: number): string {
                var datetime = new Date(nanos / 1.0e6);
                return Nodes._datetimeFormater(datetime);
            }

            public Details(nodeId: string): _mithril.MithrilVirtualElement {
                var node = this.statuses()[nodeId];
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
                        m("tr", [m("td", "Started at:"), m("td", Nodes._formatDate(node.started_at))]),
                        m("tr", [m("td", "Updated at:"), m("td", Nodes._formatDate(node.updated_at))]),
                        m("tr", [m("td", "Ranges:"), m("td", node.range_count)]),
                        m("tr", [m("td", "Leader Ranges:"), m("td", node.leader_range_count)]),
                        m("tr", [m("td", "Available Ranges:"), m("td", node.available_range_count)]),
                        m("tr", [m("td", "Availablility:"), m("td", Nodes._availability(node))]),
                        m("tr", [m("td", "Under-Replicated Ranges:"), m("td", node.leader_range_count - node.replicated_range_count)]),
                        m("tr", [m("td", "Fully Replicated:"), m("td", Nodes._replicated(node))])
                    ]),
                    Stats.CreateStatsTable(node.stats)
                ]);
            }

            public AllDetails(): _mithril.MithrilVirtualElement {

                var status = <Proto.NodeStatus>{
                    range_count: 0,
                    updated_at: 0,
                    leader_range_count: 0,
                    replicated_range_count: 0,
                    available_range_count: 0,
                    stats: <Proto.MVCCStats>{
                        live_bytes: 0,
                        key_bytes: 0,
                        val_bytes: 0,
                        intent_bytes: 0,
                        live_count: 0,
                        key_count: 0,
                        val_count: 0,
                        intent_count: 0,
                        sys_bytes: 0,
                        sys_count: 0
                    }
                };

                for (var nodeId in this.statuses()) {
                    var nodeStatus = this.statuses()[nodeId];
                    status.range_count += nodeStatus.range_count;
                    status.leader_range_count += nodeStatus.leader_range_count;
                    status.replicated_range_count += nodeStatus.replicated_range_count;
                    status.available_range_count += nodeStatus.available_range_count;
                    if (nodeStatus.updated_at > status.updated_at) {
                        status.updated_at = nodeStatus.updated_at;
                    }
                    status.stats.live_bytes += nodeStatus.stats.live_bytes;
                    status.stats.key_bytes += nodeStatus.stats.key_bytes;
                    status.stats.val_bytes += nodeStatus.stats.val_bytes;
                    status.stats.intent_bytes += nodeStatus.stats.intent_bytes;
                    status.stats.live_count += nodeStatus.stats.live_count;
                    status.stats.key_count += nodeStatus.stats.key_count;
                    status.stats.val_count += nodeStatus.stats.val_count;
                    status.stats.intent_count += nodeStatus.stats.intent_count;
                    status.stats.sys_bytes += nodeStatus.stats.sys_bytes;
                    status.stats.sys_count += nodeStatus.stats.sys_count;
                };

                return m("div", [
                    m("h2", "Details"),
                    m("table", [
                        m("tr", [m("td", "Updated at:"), m("td", Nodes._formatDate(status.updated_at))]),
                        m("tr", [m("td", "Ranges:"), m("td", status.range_count)]),
                        m("tr", [m("td", "Leader Ranges:"), m("td", status.leader_range_count)]),
                        m("tr", [m("td", "Available Ranges:"), m("td", status.available_range_count)]),
                        m("tr", [m("td", "Availablility:"), m("td", Nodes._availability(status))]),
                        m("tr", [m("td", "Under-Replicated Ranges:"), m("td", status.leader_range_count - status.replicated_range_count)]),
                        m("tr", [m("td", "Fully Replicated:"), m("td", Nodes._replicated(status))])
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
