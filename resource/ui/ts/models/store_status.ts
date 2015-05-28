// source: models/store_status.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../typings/d3/d3.d.ts" />
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
                            var storeId = status.desc.store_id;
                            if (this._data()[storeId] == null) {
                                this._data()[storeId] = [];
                            }
                            var statusList = this._data()[storeId];
                            if ((statusList.length == 0) ||
                                (statusList[statusList.length - 1].updated_at < status.updated_at)) {
                                this._data()[storeId].push(status);
                                this.statuses()[storeId] = status;
                            }
                        });
                        this._pruneOldEntries();
                        this._updateDescriptions();
                        return results;
                    });
            }

            private _updateDescriptions(): void {
                this.desc(<StoreDescriptionMap> {});
                for (var storeId in this._data()) {
                    this.desc()[storeId] = this._data()[storeId][this._data()[storeId].length - 1].desc;
                }
            }

            private _pruneOldEntries(): void {
                for (var storeId in this._data()) {
                    var status = <StoreStatus[]>this._data()[storeId];
                    if (status.length > Stores._dataLimit) {
                        status = status.slice(status.length - Stores._dataPrunedSize, status.length - 1)
                    }
                }
            }

            // TODO(Bram): Move to utility class.
            private static _availability(store: StoreStatus):string {
                if (store.leader_range_count == 0) {
                    return "100%";
                }
                return Math.floor(store.available_range_count / store.leader_range_count * 100).toString() + "%";
            }

            // TODO(Bram): Move to utility class.
            private static _replicated(store: StoreStatus): string {
                if (store.leader_range_count == 0) {
                    return "100%";
                }
                return Math.floor(store.replicated_range_count / store.leader_range_count * 100).toString() + "%";
            }

            // TODO(Bram): Move to utility class.
            private static _datetimeFormater = d3.time.format("%Y-%m-%d %H:%M:%S")
            private static _formatDate(nanos: number): string {
                var datetime = new Date(nanos / 1.0e6);
                return Stores._datetimeFormater(datetime);
            }

            public Details(storeId:string):_mithril.MithrilVirtualElement {
                var store = this.statuses()[storeId];
                if (store == null) {
                    return m("div", "No data present yet.")
                }
                return m("div",[
                    m("table", [
                      m("tr", [m("td", "Node Id:"), m("td", m("a[href=/nodes/" + store.desc.node.node_id + "]", { config: m.route }, store.desc.node.node_id))]),
                      m("tr", [m("td", "Node Network:"), m("td", store.desc.node.address.network)]),
                      m("tr", [m("td", "Node Address:"), m("td", store.desc.node.address.address)]),
                      m("tr", [m("td", "Started at:"), m("td", Stores._formatDate(store.started_at))]),
                      m("tr", [m("td", "Updated at:"), m("td", Stores._formatDate(store.updated_at))]),
                      m("tr", [m("td", "Ranges:"), m("td", store.range_count)]),
                      m("tr", [m("td", "Leader Ranges:"), m("td", store.leader_range_count)]),
                      m("tr", [m("td", "Available Ranges:"), m("td", store.available_range_count)]),
                      m("tr", [m("td", "Availablility:"), m("td", Stores._availability(store))]),
                      m("tr", [m("td", "Under-Replicated Ranges:"), m("td", store.leader_range_count - store.replicated_range_count)]),
                      m("tr", [m("td", "Fully Replicated:"), m("td", Stores._replicated(store))])
                    ]),
                    Stats.CreateStatsTable(store.stats)
                ]);
            }

            public AllDetails(): _mithril.MithrilVirtualElement {

                // TODO(Bram): This is all really ugly here. Refactor it and the
                // equivalent in node_status.ts into one place that contains
                // a better way to sum these stats.
                var status = <StoreStatus>{
                    range_count: 0,
                    updated_at: 0,
                    leader_range_count: 0,
                    replicated_range_count: 0,
                    available_range_count: 0,
                    stats: <Models.Stats.MVCCStats>{
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

                for (var storeId in this.statuses()) {
                    var storeStatus = this.statuses()[storeId];
                    status.range_count += storeStatus.range_count;
                    status.leader_range_count += storeStatus.leader_range_count;
                    status.replicated_range_count += storeStatus.replicated_range_count;
                    status.available_range_count += storeStatus.available_range_count;
                    if (storeStatus.updated_at > status.updated_at) {
                        status.updated_at = storeStatus.updated_at;
                    }
                    status.stats.live_bytes += storeStatus.stats.live_bytes;
                    status.stats.key_bytes += storeStatus.stats.key_bytes;
                    status.stats.val_bytes += storeStatus.stats.val_bytes;
                    status.stats.intent_bytes += storeStatus.stats.intent_bytes;
                    status.stats.live_count += storeStatus.stats.live_count;
                    status.stats.key_count += storeStatus.stats.key_count;
                    status.stats.val_count += storeStatus.stats.val_count;
                    status.stats.intent_count += storeStatus.stats.intent_count;
                    status.stats.sys_bytes += storeStatus.stats.sys_bytes;
                    status.stats.sys_count += storeStatus.stats.sys_count;
                };

                return m("div", [
                    m("h2", "Details"),
                    m("table", [
                        m("tr", [m("td", "Updated at:"), m("td", Stores._formatDate(status.updated_at))]),
                        m("tr", [m("td", "Ranges:"), m("td", status.range_count)]),
                        m("tr", [m("td", "Leader Ranges:"), m("td", status.leader_range_count)]),
                        m("tr", [m("td", "Available Ranges:"), m("td", status.available_range_count)]),
                        m("tr", [m("td", "Availablility:"), m("td", Stores._availability(status))]),
                        m("tr", [m("td", "Under-Replicated Ranges:"), m("td", status.leader_range_count - status.replicated_range_count)]),
                        m("tr", [m("td", "Fully Replicated:"), m("td", Stores._replicated(status))])
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
