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
                            if (this._data()[status.desc.node_id] == null) {
                                this._data()[status.desc.node_id] = [];
                            }
                            this._data()[status.desc.node_id].push(status);
                            this.statuses()[status.desc.node_id] = status;
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
