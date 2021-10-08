// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert } from "chai";

import { INodeStatus, rollupStoreMetrics } from "./proto";

describe("Proto utils", () => {
  describe("rollupStoreMetrics", () => {
    let nodeStatus: Partial<INodeStatus>;
    let statusWithRolledMetrics: Partial<INodeStatus>;

    beforeEach(() => {
      nodeStatus = {
        metrics: {
          a: 10,
          b: 5,
          c: 0,
          y: 15,
          z: 5,
        },
        store_statuses: [
          {
            metrics: {
              c: 25,
              d: 5,
              e: 5,
            },
          },
          {
            metrics: {
              a: 5,
              b: 100,
              x: 0,
              y: 20,
              z: 0,
            },
          },
        ],
      };
      statusWithRolledMetrics = {
        ...nodeStatus,
        metrics: {
          a: 15,
          b: 105,
          c: 25,
          d: 5,
          e: 5,
          x: 0,
          y: 35,
          z: 5,
        },
      };
    });

    it("sums up values for every metric", () => {
      assert.deepEqual(
        rollupStoreMetrics(nodeStatus),
        statusWithRolledMetrics.metrics,
      );
    });
  });
});
