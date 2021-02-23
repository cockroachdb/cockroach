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
import { createHashHistory } from "history";

import { MetricConstants, INodeStatus } from "src/util/proto";
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

function makeNodesState(
  ...addresses: { id: number; address: string; status?: LivenessStatus }[]
) {
  const nodeData = addresses.map((addr) => {
    return {
      desc: {
        node_id: addr.id,
        address: {
          address_field: addr.address,
        },
      },
    };
  });
  const livenessData: { statuses: { [key: string]: LivenessStatus } } = {
    statuses: {},
  };
  addresses.forEach((addr) => {
    livenessData.statuses[addr.id] =
      addr.status || LivenessStatus.NODE_STATUS_LIVE;
  });
  const store = createAdminUIStore(createHashHistory());
  store.dispatch(nodesReducerObj.receiveData(nodeData));
  store.dispatch(
    livenessReducerObj.receiveData(
      new protos.cockroach.server.serverpb.LivenessResponse(livenessData),
    ),
  );
  return store.getState();
}

describe("node data selectors", function () {
  describe("display name by ID", function () {
    it("display name is node id appended to address", function () {
      const state: any = makeNodesState(
        { id: 1, address: "addressA" },
        { id: 2, address: "addressB" },
        { id: 3, address: "addressC" },
        { id: 4, address: "addressD" },
      );

      const addressesByID = nodeDisplayNameByIDSelector(state);
      assert.deepEqual(addressesByID, {
        1: "(n1) addressA",
        2: "(n2) addressB",
        3: "(n3) addressC",
        4: "(n4) addressD",
      });
    });

    it("generates unique names for re-used addresses", function () {
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
        1: "(n1) addressA",
        2: "(n2) addressB",
        3: "(n3) addressC",
        4: "(n4) addressD",
        5: "(n5) addressA",
        6: "(n6) addressC",
        7: "(n7) addressA",
      });
    });

    it("adds decommissioned flag to decommissioned nodes", function () {
      const state: any = makeNodesState(
        {
          id: 1,
          address: "addressA",
          status: LivenessStatus.NODE_STATUS_DECOMMISSIONED,
        },
        { id: 2, address: "addressB" },
        {
          id: 3,
          address: "addressC",
          status: LivenessStatus.NODE_STATUS_DECOMMISSIONED,
        },
        { id: 4, address: "addressD", status: LivenessStatus.NODE_STATUS_DEAD },
        {
          id: 5,
          address: "addressA",
          status: LivenessStatus.NODE_STATUS_DECOMMISSIONED,
        },
        { id: 6, address: "addressC" },
        { id: 7, address: "addressA" },
        {
          id: 8,
          address: "addressE",
          status: LivenessStatus.NODE_STATUS_DECOMMISSIONING,
        },
        {
          id: 9,
          address: "addressF",
          status: LivenessStatus.NODE_STATUS_UNAVAILABLE,
        },
      );

      const addressesByID = nodeDisplayNameByIDSelector(state);
      assert.equal(addressesByID[1], "[decommissioned] (n1) addressA");
      assert.deepEqual(addressesByID, {
        1: "[decommissioned] (n1) addressA",
        2: "(n2) addressB",
        3: "[decommissioned] (n3) addressC",
        4: "(n4) addressD",
        5: "[decommissioned] (n5) addressA",
        6: "(n6) addressC",
        7: "(n7) addressA",
        8: "(n8) addressE",
        9: "(n9) addressF",
      });
    });

    it("returns empty collection for empty state", function () {
      const store = createAdminUIStore(createHashHistory());
      assert.deepEqual(nodeDisplayNameByIDSelector(store.getState()), {});
    });
  });

  describe("store IDs by node ID", function () {
    it("correctly creates storeID map", function () {
      const data = [
        {
          desc: { node_id: 1 },
          store_statuses: [
            { desc: { store_id: 1 } },
            { desc: { store_id: 2 } },
            { desc: { store_id: 3 } },
          ],
        },
        {
          desc: { node_id: 2 },
          store_statuses: [{ desc: { store_id: 4 } }],
        },
        {
          desc: { node_id: 3 },
          store_statuses: [
            { desc: { store_id: 5 } },
            { desc: { store_id: 6 } },
          ],
        },
      ];
      const store = createAdminUIStore(createHashHistory());
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

describe("selectCommissionedNodeStatuses", function () {
  const nodeStatuses: INodeStatus[] = [
    {
      desc: {
        node_id: 1,
      },
    },
  ];

  function makeStateForLiveness(livenessStatuses: {
    [id: string]: LivenessStatus;
  }) {
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

  it("selects all nodes when liveness status missing", function () {
    const state = makeStateForLiveness({});

    const result = selectCommissionedNodeStatuses(state);

    assert.deepEqual(result, nodeStatuses);
  });

  const testCases: [string, LivenessStatus, INodeStatus[]][] = [
    [
      "excludes decommissioned nodes",
      LivenessStatus.NODE_STATUS_DECOMMISSIONED,
      [],
    ],
    [
      "includes decommissioning nodes",
      LivenessStatus.NODE_STATUS_DECOMMISSIONING,
      nodeStatuses,
    ],
    ["includes live nodes", LivenessStatus.NODE_STATUS_LIVE, nodeStatuses],
    [
      "includes unavailable nodes",
      LivenessStatus.NODE_STATUS_UNAVAILABLE,
      nodeStatuses,
    ],
    ["includes dead nodes", LivenessStatus.NODE_STATUS_DEAD, nodeStatuses],
  ];

  testCases.forEach(([name, status, expected]) => {
    it(name, function () {
      const state = makeStateForLiveness({ "1": status });

      const result = selectCommissionedNodeStatuses(state);

      assert.deepEqual(result, expected);
    });
  });
});

describe("sumNodeStats", function () {
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

  it("sums stats from an array of nodes", function () {
    const livenessStatusByNodeID: { [key: string]: LivenessStatus } = {
      1: LivenessStatus.NODE_STATUS_LIVE,
      2: LivenessStatus.NODE_STATUS_LIVE,
    };
    const actual = sumNodeStats(nodeStatuses, livenessStatusByNodeID);
    assert.equal(actual.nodeCounts.healthy, 2);
    assert.equal(actual.capacityTotal, 200);
    assert.equal(actual.capacityUsed, 20);
    // usable = used + available.
    assert.equal(actual.capacityUsable, 100);
  });

  it("returns empty stats if liveness statuses are not provided", () => {
    const { nodeCounts, ...restStats } = sumNodeStats(nodeStatuses, {});
    Object.entries(restStats).forEach(([_, value]) => assert.equal(value, 0));
    Object.entries(nodeCounts).forEach(([_, value]) => assert.equal(value, 0));
  });
});
