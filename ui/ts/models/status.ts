// source: models/status.ts
/// <reference path="../../typings/browser.d.ts" />
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../util/http.ts" />
/// <reference path="../util/querycache.ts" />
/// <reference path="proto.ts" />
// Author: Bram Gruneir (bram+code@cockroachlabs.com)
// Author: Matt Tracy (matt@cockroachlabs.com)

module Models {
  "use strict";
  export module Status {
    import promise = _mithril.MithrilPromise;
    import Moment = moment.Moment;
    import MithrilPromise = _mithril.MithrilPromise;

    export interface StoreStatusResponseSet {
      d: Proto.StoreStatus[];
    }

    export interface StoreStatusMap {
      [storeId: string]: Proto.StoreStatus;
    }

    export interface NodeStatusResponseSet {
      d: Proto.NodeStatus[];
    }

    export interface NodeStatusMap {
      [nodeId: string]: Proto.NodeStatus;
    }

    /**
     * staleStatus returns the "status" of the node depending on how long ago
     * the last status update was received.
     *
     *   healthy <=1min
     *   stale   <1min & <=10min
     *   missing >10min
     *
     * @param  {Moment} lastUpdate - the last updated time received
     * @return {string} - healthy/stale/missing
     */
    export function staleStatus(lastUpdate: Moment): string {
      if (lastUpdate.isBefore(moment().subtract(10, "minutes"))) {
        return "missing";
      }
      if (lastUpdate.isBefore(moment().subtract(1, "minute"))) {
        return "stale";
      }
      return "healthy";
    }

    export interface BytesAndCount {
      bytes: number;
      count: number;
    }

    export function bytesAndCountReducer(byteAttr: string, countAttr: string, rows: (Proto.StatusMetrics)[]): BytesAndCount {
      return _.reduce(
        rows,
        function (memo: BytesAndCount, row: (Proto.StatusMetrics)): BytesAndCount {
          memo.bytes += <number>_.get(row, byteAttr);
          memo.count += <number>_.get(row, countAttr);
          return memo;
        },
        {bytes: 0, count: 0});
    }

    export function sumReducer(attr: string, rows: (Proto.StatusMetrics)[]): number {
      return _.reduce(
        rows,
        function (memo: number, row: (Proto.StatusMetrics)): number {
          return memo + <number>_.get(row, attr);
        },
        0);
    }

    export class Nodes {
      public allStatuses: Utils.ReadOnlyProperty<Proto.NodeStatus[]>;
      public totalStatus: Utils.ReadOnlyProperty<Proto.StatusMetrics>;

      private _data: Utils.QueryCache<Proto.NodeStatus[]> = new Utils.QueryCache((): promise<Proto.NodeStatus[]> => {
        return Utils.Http.Get("/_status/nodes/")
          .then((results: NodeStatusResponseSet) => {
            let statuses = results.d;
            statuses.forEach((ns: Proto.NodeStatus) => {
              // store_statuses will be null if the associated node has no
              // bootstrapped stores; this causes significant null-handling
              // problems in code. Convert "null" or "undefined" to an empty
              // object.
              if (!_.isObject(ns.store_statuses)) {
                ns.store_statuses = {};
              }
              Models.Proto.RollupStoreMetrics(ns);
            });
            return statuses;
          });
      });

      private _dataMap: Utils.ReadOnlyProperty<NodeStatusMap> = Utils.Computed(this._data.lastResult, (list: Proto.NodeStatus[]) => {
        return _.keyBy(list, (status: Proto.NodeStatus) => status.desc.node_id);
      });

      private _totalStatus: Utils.ReadOnlyProperty<Proto.StatusMetrics> = Utils.Computed(this._data.lastResult, (list: Proto.NodeStatus[]) => {
        let totalStatus: Models.Proto.StatusMetrics = {};
        if (list) {
          Models.Proto.AccumulateMetrics(totalStatus, ..._.map(list, (ns) => ns.metrics));
        }
        return totalStatus;
      });

      constructor() {
        this.allStatuses = this._data.lastResult;
        this.totalStatus = this._totalStatus;
      }

      public GetStatus(nodeId: string): Proto.NodeStatus {
        return this._dataMap()[nodeId];
      }

      public refresh(): MithrilPromise<any> {
        return this._data.refresh();
      }

      public error(): Error {
        return this._data.error();
      }
    }

    export let nodeStatusSingleton = new Nodes();
  }
}
