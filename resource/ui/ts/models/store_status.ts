// source: models/store_status.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="node_status.ts" />
/// <reference path="stats.ts" />
// Author: Bram Gruneir (bram.gruneir@gmail.com)

module Models {
    export module StoreStatus {
        import promise = _mithril.MithrilPromise;

        export interface StoreDescription {
            store_id: number;
            node: Models.NodeStatus.NodeDescription;
            attrs: any;
        }

        export interface Capacity {
            Capacity: number;
            Available: number;
            RangeCount: number;
        }

        export interface StoreStatus {
            desc: StoreDescription;
            range_count: number;
            started_at: number;
            updated_at: number;
            stats: Models.Stats.MVCCStats;
            leader_range_count: number;
            replicated_range_count: number;
            available_range_count: number;
        }

        export interface StoreStatusResponseSet {
            d: StoreStatus[]
        }

        export interface StoreStatusListMap {
            [storeId: number]: StoreStatus[]
        }

        export interface StoreDescriptionMap {
            [storeId: number]: StoreDescription
        }

        export interface StoreStatusMap {
            [storeId: number]: StoreStatus
        }

        export class Stores {
            private static _dataLimit = 100000;
            private static _dataPrunedSize = 90000;
            private _data: _mithril.MithrilProperty<StoreStatusListMap> = m.prop(<StoreStatusListMap> {});

            public desc: _mithril.MithrilProperty<StoreDescriptionMap> = m.prop(<StoreDescriptionMap> {});
            public statuses: _mithril.MithrilProperty<StoreStatusMap> = m.prop(<StoreStatusMap> {});

            Query(): _mithril.MithrilPromise<StoreStatusResponseSet> {
                var url = "/_status/stores/";
                return m.request({ url: url, method: "GET", extract: nonJsonErrors })
                    .then((results: StoreStatusResponseSet) => {
                        results.d.forEach((status) => {
                            if (this._data()[status.desc.store_id] == null) {
                                this._data()[status.desc.store_id] = [];
                            }
                            this._data()[status.desc.store_id].push(status);
                            this.statuses()[status.desc.store_id] = status;
                        });
                        this._pruneOldEntries();
                        this._updateDescriptions();
                        return results;
                    });
            }

            private _updateDescriptions(): void {
                this.desc(<StoreDescriptionMap> {});
                var nodeId: string;
                for (nodeId in this._data()) {
                    this.desc()[nodeId] = this._data()[nodeId][this._data()[nodeId].length - 1].desc;
                }
            }

            private _pruneOldEntries(): void {
                var nodeId: string;
                for (nodeId in this._data()) {
                    var status = this._data()[nodeId];
                    if (status.length > Stores._dataLimit) {
                        status = status.sclice(status.length - Stores._dataPrunedSize, status.length - 1)
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
