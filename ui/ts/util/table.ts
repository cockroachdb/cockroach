/// <reference path="../../typings/lodash/lodash.d.ts" />
/// <reference path="../models/proto.ts" />

// source: util/table.ts
// Author: Max Lang (max@cockroachlabs.com)

/**
 * Table helper functions for rollups
 */
module Utils {
  "use strict";

  export module Table {
    import StoreStatus = Models.Proto.StoreStatus;
    import NodeStatus = Models.Proto.NodeStatus;

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
  }
}
