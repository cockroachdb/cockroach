// source: models/node_status.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="stats.ts" />
// Author: Bram Gruneir (bram.gruneir@gmail.com)

module Models {
    export module NodeStatus {
        import promise = _mithril.MithrilPromise;

        export interface Address {
            network: string;
            address: string;
        }

        export interface NodeDescription {
            node_id: number;
            address: Address;
            attrs: any;
        }

        export interface NodeStatus {
            desc: NodeDescription;
            store_ids: number[];
            range_count: number;
            started_at: number;
            updated_at: number;
            stats: Models.Stats.MVCCStats;
            leader_range_count: number;
            replicated_range_count: number;
            available_range_count: number;
        }

        export interface NodeStatusResponseSet {
            d: NodeStatus[]
        }

        export interface NodeStatusListMap {
            [nodeId: number]: NodeStatus[]
        }

        export interface NodeDescriptionMap {
            [nodeId: number]: NodeDescription
        }

        export interface NodeStatusMap {
            [nodeId: number]: NodeStatus
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
                var nodeId: string;
                for (nodeId in this._data()) {
                    this.desc()[nodeId] = this._data()[nodeId][this._data()[nodeId].length - 1].desc;
                }
            }

            private _pruneOldEntries(): void {
                var nodeId: string;
                for (nodeId in this._data()) {
                    var status = this._data()[nodeId];
                    if (status.length > Nodes._dataLimit) {
                        status = status.sclice(status.length - Nodes._dataPrunedSize,status.length - 1)
                    }
                }
            }

            // TODO(Bram): Move to utility class.
            private _availability(nodeId: string): string {
                var node = this.statuses()[nodeId];
                if (node.leader_range_count == 0) {
                    return "100%";
                }
                return (node.available_range_count / node.leader_range_count * 100).toString() + "%";
            }

            // TODO(Bram): Move to utility class.
            private _replicated(nodeId: string): string {
                var node = this.statuses()[nodeId];
                if (node.leader_range_count == 0) {
                    return "100%";
                }
                return (node.replicated_range_count / node.leader_range_count * 100).toString() + "%";
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
                        m("tr", [m("td", "Availablility:"), m("td", this._availability(nodeId))]),
                        m("tr", [m("td", "Under-Replicated Ranges:"), m("td", node.leader_range_count - node.replicated_range_count)]),
                        m("tr", [m("td", "Fully Replicated:"), m("td", this._replicated(nodeId))])
                    ])
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
