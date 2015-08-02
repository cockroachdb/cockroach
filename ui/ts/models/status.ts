// source: models/status.ts
/// <reference path="../typings/lodash/lodash.d.ts" />
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../util/http.ts" />
/// <reference path="../util/querycache.ts" />
/// <reference path="proto.ts" />
// Author: Bram Gruneir (bram+code@cockroachlabs.com)
// Author: Matt Tracy (matt@cockroachlabs.com)

module Models {
  "use strict";
  export module Status {
    import promise = _mithril.MithrilPromise;

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
          stats: Proto.NewMVCCStats()
        };
        list.forEach((storeStatus: Proto.StoreStatus) => {
          Proto.AccumulateStatus(status, storeStatus);
        });
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
          stats: Proto.NewMVCCStats()
        };
        list.forEach((nodeStatus: Proto.NodeStatus) => {
          Proto.AccumulateStatus(status, nodeStatus);
        });
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
