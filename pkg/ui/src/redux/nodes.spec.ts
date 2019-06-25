// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert } from "chai";

import {MetricConstants, INodeStatus} from "src/util/proto";
import * as protos from "src/js/protos";

import {
  nodeDisplayNameByIDSelector,
  selectCommissionedNodeStatuses,
  selectStoreIDsByNodeID,
  LivenessStatus,
  sumNodeStats,
} from "./nodes";
import { nodesReducerObj, livenessReducerObj } from "./apiReducers";
import { createAdminUIStore } from "./state";

function makeNodesState(...addresses: { id: number, address: string, status?: LivenessStatus }[]) {
  const nodeData = addresses.map(addr => {
    return {
      desc : {
        node_id: addr.id,
        address: {
          address_field: addr.address,
        },
      },
    };
  });
  const livenessData: {statuses: {[key: string]: LivenessStatus}} = {
    statuses: {},
  };
  addresses.forEach(addr => {
    livenessData.statuses[addr.id] = addr.status || LivenessStatus.LIVE;
  });
  const store = createAdminUIStore();
  store.dispatch(nodesReducerObj.receiveData(nodeData));
  store.dispatch(livenessReducerObj.receiveData(new protos.cockroach.server.serverpb.LivenessResponse(livenessData)));
  return store.getState();
}

describe("node data selectors", function() {
  describe("display name by ID", function() {
    it("display name is node id appended to address", function() {
      const state: any = makeNodesState(
        { id: 1, address: "addressA" },
        { id: 2, address: "addressB" },
        { id: 3, address: "addressC" },
        { id: 4, address: "addressD" },
      );

      const addressesByID = nodeDisplayNameByIDSelector(state);
      assert.deepEqual(addressesByID, {
        1: "addressA (n1)",
        2: "addressB (n2)",
        3: "addressC (n3)",
        4: "addressD (n4)",
      });
    });

    it("generates unique names for re-used addresses", function() {
      const state: any = makeNodesState(
        { id: 1, address: "addressA" },
        { id: 2, address: "addressB" },
        { id: 3, address: "addressC" },
        { id: 4, address: "addressD" },
        { id: 5, address: "addressA" },
        { id: 6, address: "addressC" },
        { id: 7, address: "addressA" },
      );

      const addressesByID = nodeDisplayNameByIDSelector(state);
      assert.deepEqual(addressesByID, {
        1: "addressA (n1)",
        2: "addressB (n2)",
        3: "addressC (n3)",
        4: "addressD (n4)",
        5: "addressA (n5)",
        6: "addressC (n6)",
        7: "addressA (n7)",
      });
    });

    it("adds decommissioned flag to decommissioned nodes", function() {
      const state: any = makeNodesState(
        { id: 1, address: "addressA", status: LivenessStatus.DECOMMISSIONED },
        { id: 2, address: "addressB" },
        { id: 3, address: "addressC", status: LivenessStatus.DECOMMISSIONED },
        { id: 4, address: "addressD", status: LivenessStatus.DEAD },
        { id: 5, address: "addressA", status: LivenessStatus.DECOMMISSIONED },
        { id: 6, address: "addressC" },
        { id: 7, address: "addressA" },
        { id: 8, address: "addressE", status: LivenessStatus.DECOMMISSIONING },
        { id: 9, address: "addressF", status: LivenessStatus.UNAVAILABLE },
      );

      const addressesByID = nodeDisplayNameByIDSelector(state);
      assert.equal(addressesByID[1], "[decommissioned] addressA (n1)");
      assert.deepEqual(addressesByID, {
        1: "[decommissioned] addressA (n1)",
        2: "addressB (n2)",
        3: "[decommissioned] addressC (n3)",
        4: "addressD (n4)",
        5: "[decommissioned] addressA (n5)",
        6: "addressC (n6)",
        7: "addressA (n7)",
        8: "addressE (n8)",
        9: "addressF (n9)",
      });
    });

    it("returns empty collection for empty state", function() {
      const store = createAdminUIStore();
      assert.deepEqual(nodeDisplayNameByIDSelector(store.getState()), {});
    });
  });

  describe("store IDs by node ID", function() {
    it("correctly creates storeID map", function() {
      const data = [
        {
          desc: { node_id: 1 },
          store_statuses: [
            { desc: { store_id: 1 }},
            { desc: { store_id: 2 }},
            { desc: { store_id: 3 }},
          ],
        },
        {
          desc: { node_id: 2 },
          store_statuses: [
            { desc: { store_id: 4 }},
          ],
        },
        {
          desc: { node_id: 3 },
          store_statuses: [
            { desc: { store_id: 5 }},
            { desc: { store_id: 6 }},
          ],
        },
      ];
      const store = createAdminUIStore();
      store.dispatch(nodesReducerObj.receiveData(data));
      const state = store.getState();

      assert.deepEqual(selectStoreIDsByNodeID(state), {
        1: ["1", "2", "3"],
        2: ["4"],
        3: ["5", "6"],
      });
    });
  });
});

describe("selectCommissionedNodeStatuses", function() {
  const nodeStatuses: INodeStatus[] = [
    {
      desc: {
        node_id: 1,
      },
    },
  ];

  function makeStateForLiveness(livenessStatuses: { [id: string]: LivenessStatus }) {
    return {
      cachedData: {
        nodes: {
          data: nodeStatuses,
          inFlight: false,
          valid: true,
        },
        liveness: {
          data: {
            statuses: livenessStatuses,
          },
          inFlight: false,
          valid: true,
        },
      },
    };
  }

  it("selects all nodes when liveness status missing", function() {
    const state = makeStateForLiveness({});

    const result = selectCommissionedNodeStatuses(state);

    assert.deepEqual(result, nodeStatuses);
  });

  const testCases: [string, LivenessStatus, INodeStatus[]][] = [
    ["excludes decommissioned nodes", LivenessStatus.DECOMMISSIONED, []],
    ["includes decommissioning nodes", LivenessStatus.DECOMMISSIONING, nodeStatuses],
    ["includes live nodes", LivenessStatus.LIVE, nodeStatuses],
    ["includes unavailable nodes", LivenessStatus.UNAVAILABLE, nodeStatuses],
    ["includes dead nodes", LivenessStatus.DEAD, nodeStatuses],
  ];

  testCases.forEach(([name, status, expected]) => {
    it(name, function() {
      const state = makeStateForLiveness({ "1": status });

      const result = selectCommissionedNodeStatuses(state);

      assert.deepEqual(result, expected);
    });
  });
});

describe("sumNodeStats", function() {
  it("sums stats from an array of nodes", function() {
    // Each of these nodes only has half of its capacity "usable" for cockroach data.
    // See diagram for what these stats mean:
    // https://github.com/cockroachdb/cockroach/blob/31e4299ab73a43f539b1ba63ed86be5ee18685f6/pkg/storage/metrics.go#L145-L153
    const nodeStatuses: INodeStatus[] = [
      {
        desc: { node_id: 1 },
        metrics: {
          [MetricConstants.capacity]: 100,
          [MetricConstants.usedCapacity]: 10,
          [MetricConstants.availableCapacity]: 40,
        },
      },
      {
        desc: { node_id: 2 },
        metrics: {
          [MetricConstants.capacity]: 100,
          [MetricConstants.usedCapacity]: 10,
          [MetricConstants.availableCapacity]: 40,
        },
      },
    ];
    const livenessStatusByNodeID: { [key: string]: LivenessStatus } = {
      1: LivenessStatus.LIVE,
      2: LivenessStatus.LIVE,
    };
    const actual = sumNodeStats(nodeStatuses, livenessStatusByNodeID);
    assert.equal(actual.nodeCounts.healthy, 2);
    assert.equal(actual.capacityTotal, 200);
    assert.equal(actual.capacityUsed, 20);
    // usable = used + available.
    assert.equal(actual.capacityUsable, 100);
  });
});
