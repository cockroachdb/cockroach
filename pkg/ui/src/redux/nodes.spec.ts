import { assert } from "chai";
import { nodeAddressByIDSelector } from "./nodes";

function makeNodesState(...addresses: { id: number, address: string }[]): any {
  return {
    cachedData: {
      nodes: {
        data: addresses.map(addr => {
          return {
            desc : {
              node_id: addr.id,
              address: {
                address_field: addr.address,
              },
            },
          };
        }),
      },
    },
  };
}

describe("node data selectors", function() {
  describe("node address by ID", function() {
    it("does nothing for normal addresses", function() {
      const state: any = makeNodesState(
        { id: 1, address: "addressA" },
        { id: 2, address: "addressB" },
        { id: 3, address: "addressC" },
        { id: 4, address: "addressD" },
      );

      const addressesByID = nodeAddressByIDSelector(state);
      assert.deepEqual(addressesByID, {
        1: "addressA",
        2: "addressB",
        3: "addressC",
        4: "addressD",
      });
    });

    it("de-duplicates re-used addresses", function() {
      const state: any = makeNodesState(
        { id: 1, address: "addressA" },
        { id: 2, address: "addressB" },
        { id: 3, address: "addressC" },
        { id: 4, address: "addressD" },
        { id: 5, address: "addressA" },
        { id: 6, address: "addressC" },
        { id: 7, address: "addressA" },
      );

      const addressesByID = nodeAddressByIDSelector(state);
      assert.deepEqual(addressesByID, {
        1: "addressA (1)",
        2: "addressB",
        3: "addressC (3)",
        4: "addressD",
        5: "addressA (5)",
        6: "addressC (6)",
        7: "addressA (7)",
      });
    });

    it("returns empty collection for empty state", function() {
      assert.deepEqual(nodeAddressByIDSelector(makeNodesState()), {});
    });
  });
});
