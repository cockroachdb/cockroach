import { assert } from "chai";

import {MetricConstants, NodeStatus$Properties} from "src/util/proto";

import {
  nodeDisplayNameByIDSelector,
  selectCommissionedNodeStatuses,
  LivenessStatus,
  sumNodeStats,
} from "./nodes";
import { nodesReducerObj  } from "./apiReducers";
import { createAdminUIStore } from "./state";

function makeNodesState(...addresses: { id: number, address: string }[]) {
  const data = addresses.map(addr => {
    return {
      desc : {
        node_id: addr.id,
        address: {
          address_field: addr.address,
        },
      },
    };
  });
  const store = createAdminUIStore();
  store.dispatch(nodesReducerObj.receiveData(data));
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

    it("returns empty collection for empty state", function() {
      const store = createAdminUIStore();
      assert.deepEqual(nodeDisplayNameByIDSelector(store.getState()), {});
    });
  });
});

describe("selectCommissionedNodeStatuses", function() {
  const nodeStatuses: NodeStatus$Properties[] = [
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

  const testCases: [string, LivenessStatus, NodeStatus$Properties[]][] = [
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
    const nodeStatuses: NodeStatus$Properties[] = [
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
