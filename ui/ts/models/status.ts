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
    import StoreStatus = Models.Proto.StoreStatus;
    import NodeStatus = Models.Proto.NodeStatus;

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

    export function bytesAndCountReducer(byteAttr: string, countAttr: string, rows: (NodeStatus|StoreStatus)[]): BytesAndCount {
      return _.reduce(
        rows,
        function(memo: BytesAndCount, row: (NodeStatus|StoreStatus)): BytesAndCount {
          memo.bytes += <number>_.get(row, byteAttr);
          memo.count += <number>_.get(row, countAttr);
          return memo;
        },
        {bytes: 0, count: 0});
    }

    export function sumReducer(attr: string, rows: (NodeStatus|StoreStatus)[]): number {
      return _.reduce(
        rows,
        function(memo: number, row: (NodeStatus|StoreStatus)): number {
          return memo + <number>_.get(row, attr);
        },
        0);
    }

    export class Stores {
      public allStatuses: Utils.ReadOnlyProperty<Proto.StoreStatus[]>;
      public totalStatus: Utils.ReadOnlyProperty<Proto.Status>;

      private _data: Utils.QueryCache<Proto.StoreStatus[]> = new Utils.QueryCache((): promise<Proto.StoreStatus[]> => {
        return Utils.Http.Get("/_status/stores/")
          .then((results: StoreStatusResponseSet) => {
            return results.d;
          });
      });

      private _dataMap: Utils.ReadOnlyProperty<StoreStatusMap> = Utils.Computed(this._data.result, (list: Proto.StoreStatus[]) => {
        return _.indexBy(list, (status: Proto.StoreStatus) => status.desc.node.node_id);
      });

      private _totalStatus: Utils.ReadOnlyProperty<Proto.Status> = Utils.Computed(this._data.result, (list: Proto.StoreStatus[]) => {
        let status: Proto.Status = {
          range_count: 0,
          updated_at: 0,
          started_at: 0,
          leader_range_count: 0,
          replicated_range_count: 0,
          available_range_count: 0,
          stats: Proto.NewMVCCStats(),
        };
        if (list) {
          list.forEach((storeStatus: Proto.StoreStatus) => {
            Proto.AccumulateStatus(status, storeStatus);
          });
        }
        return status;
      });

      constructor() {
        this.allStatuses = this._data.result;
        this.totalStatus = this._totalStatus;
      }

      public GetStatus(storeId: string): Proto.StoreStatus {
        return this._dataMap()[storeId];
      }

      public refresh(): void {
        this._data.refresh();
      }
    }

    export class Nodes {
      public allStatuses: Utils.ReadOnlyProperty<Proto.NodeStatus[]>;
      public totalStatus: Utils.ReadOnlyProperty<Proto.Status>;

      private _data: Utils.QueryCache<Proto.NodeStatus[]> = new Utils.QueryCache((): promise<Proto.NodeStatus[]> => {
        return Utils.Http.Get("/_status/nodes/")
          .then((results: NodeStatusResponseSet) => {
            return results.d;
          });
      });

      private _dataMap: Utils.ReadOnlyProperty<NodeStatusMap> = Utils.Computed(this._data.result, (list: Proto.NodeStatus[]) => {
        return _.indexBy(list, (status: Proto.NodeStatus) => status.desc.node_id);
      });

      private _totalStatus: Utils.ReadOnlyProperty<Proto.Status> = Utils.Computed(this._data.result, (list: Proto.NodeStatus[]) => {
        let status: Proto.Status = {
          range_count: 0,
          updated_at: 0,
          started_at: 0,
          leader_range_count: 0,
          replicated_range_count: 0,
          available_range_count: 0,
          stats: Proto.NewMVCCStats(),
        };
        if (list) {
          list.forEach((nodeStatus: Proto.NodeStatus) => {
            Proto.AccumulateStatus(status, nodeStatus);
          });
        }
        return status;
      });

      constructor() {
        this.allStatuses = this._data.result;
        this.totalStatus = this._totalStatus;
      }

      public GetStatus(nodeId: string): Proto.NodeStatus {
        return this._dataMap()[nodeId];
      }

      public refresh(): void {
        this._data.refresh();
      }
    }
  }
}
