import { assert } from "chai";

import { nodeDisplayNameByIDSelector } from "./nodes";
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
