// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { render } from "@testing-library/react";

import { INodeStatus } from "../util";

import {
  Nodes,
  Stores,
  getNodesByRegionString,
  getNodeIdsFromStoreIds,
  normalizePrivileges,
  normalizeRoles,
  LoadingCell,
  formatSQLTableName,
} from "./util";

describe("Getting nodes by region string", () => {
  describe("is not tenant", () => {
    it("when all nodes different regions", () => {
      const nodes: Nodes = { kind: "node", ids: [1, 2, 3] };
      const regions = {
        "1": "region1",
        "2": "region2",
        "3": "region3",
      };
      const result = getNodesByRegionString(nodes, regions, false);
      expect(result).toEqual(`region1(n1), region2(n2), region3(n3)`);
    });

    it("when all nodes same region", () => {
      const nodes: Nodes = { kind: "node", ids: [1, 2, 3] };
      const regions = {
        "1": "region1",
        "2": "region1",
        "3": "region1",
      };
      const result = getNodesByRegionString(nodes, regions, false);
      expect(result).toEqual(`region1(n1,n2,n3)`);
    });

    it("when some nodes different regions", () => {
      const nodes: Nodes = { kind: "node", ids: [1, 2, 3] };
      const regions = {
        "1": "region1",
        "2": "region1",
        "3": "region2",
      };
      const result = getNodesByRegionString(nodes, regions, false);
      expect(result).toEqual(`region1(n1,n2), region2(n3)`);
    });

    it("when region map is empty", () => {
      const nodes: Nodes = { kind: "node", ids: [1, 2, 3] };
      const regions = {};
      const result = getNodesByRegionString(nodes, regions, false);
      expect(result).toEqual("");
    });

    it("when nodes are empty", () => {
      const nodes: Nodes = { kind: "node", ids: [] };
      const regions = {
        "1": "region1",
        "2": "region1",
        "3": "region2",
      };
      const result = getNodesByRegionString(nodes, regions, false);
      expect(result).toEqual("");
    });
  });
});

describe("getNodeIdsFromStoreIds", () => {
  it("returns the correct node ids when all nodes have multiple stores", () => {
    const stores: Stores = { kind: "store", ids: [1, 3, 6, 2, 4, 5] };
    const nodeStatuses: INodeStatus[] = [
      {
        desc: {
          node_id: 1,
        },
        store_statuses: [{ desc: { store_id: 1 } }, { desc: { store_id: 2 } }],
      },
      {
        desc: {
          node_id: 2,
        },
        store_statuses: [{ desc: { store_id: 3 } }, { desc: { store_id: 5 } }],
      },
      {
        desc: {
          node_id: 3,
        },
        store_statuses: [{ desc: { store_id: 4 } }, { desc: { store_id: 6 } }],
      },
    ];
    const result = getNodeIdsFromStoreIds(stores, nodeStatuses);
    expect(result).toEqual({ kind: "node", ids: [1, 2, 3] });
  });

  it("returns an empty list when no stores ids are provided", () => {
    const stores: Stores = { kind: "store", ids: [] };
    const result = getNodeIdsFromStoreIds(stores, []);
    expect(result).toEqual({ kind: "node", ids: [] });
  });

  it("returns the correct node ids when there is one store per node", () => {
    const stores: Stores = { kind: "store", ids: [1, 3, 4] };
    const nodeStatuses: INodeStatus[] = [
      {
        desc: {
          node_id: 1,
        },
        store_statuses: [{ desc: { store_id: 1 } }],
      },
      {
        desc: {
          node_id: 2,
        },
        store_statuses: [{ desc: { store_id: 3 } }],
      },
      {
        desc: {
          node_id: 3,
        },
        store_statuses: [{ desc: { store_id: 4 } }],
      },
    ];
    const result = getNodeIdsFromStoreIds(stores, nodeStatuses);
    expect(result).toEqual({ kind: "node", ids: [1, 2, 3] });
  });
  it("returns the correct node ids when there is only one node", () => {
    const stores: Stores = { kind: "store", ids: [3] };
    const nodeStatuses: INodeStatus[] = [
      {
        desc: {
          node_id: 1,
        },
        store_statuses: [{ desc: { store_id: 3 } }],
      },
    ];
    const result = getNodeIdsFromStoreIds(stores, nodeStatuses);
    expect(result).toEqual({ kind: "node", ids: [1] });
  });
});

describe("Normalize privileges", () => {
  it("sorts correctly when input is disordered", () => {
    const privs = ["CREATE", "DELETE", "UPDATE", "ALL", "GRANT"];
    const result = normalizePrivileges(privs);
    expect(result).toEqual(["ALL", "CREATE", "GRANT", "UPDATE", "DELETE"]);
  });

  it("removes duplicates", () => {
    const privs = ["CREATE", "CREATE", "UPDATE", "ALL", "GRANT"];
    const result = normalizePrivileges(privs);
    expect(result).toEqual(["ALL", "CREATE", "GRANT", "UPDATE"]);
  });
});

describe("Normalize roles", () => {
  it("sorts correctly when input is disordered", () => {
    const roles = ["public", "root", "admin"];
    const result = normalizeRoles(roles);
    expect(result).toEqual(["root", "admin", "public"]);
  });

  it("removes duplicates", () => {
    const roles = ["public", "admin", "admin"];
    const result = normalizeRoles(roles);
    expect(result).toEqual(["admin", "public"]);
  });
});

describe("LoadingCell", () => {
  it("renders empty data", () => {
    const { getByText } = render(
      <LoadingCell requestError={null} loading={false}>
        {null}
      </LoadingCell>,
    );

    expect(getByText("No data")).not.toBeNull();
  });
  it("renders skeleton heading when loading", () => {
    const { getByRole } = render(
      <LoadingCell requestError={null} loading={true}>
        {null}
      </LoadingCell>,
    );

    expect(getByRole("heading")).not.toBeNull();
  });
  it("renders error name and status icon", () => {
    const { getByRole } = render(
      <LoadingCell
        requestError={{ name: "error name", message: "error message" }}
        loading={false}
      >
        {null}
      </LoadingCell>,
    );

    // TODO(davidh): rendering of antd Tooltip component doesn't work
    // here and hence can't be directly tested to contain the error
    // name.
    expect(getByRole("status")).not.toBeNull();
  });
});

describe("formatSQLTableName", () => {
  const tests = [
    {
      input: `"db"."table"`,
      expected: `db.table`,
    },
    {
      input: `"db"."schema"."table"`,
      expected: `db.schema.table`,
    },
    {
      input: `"a234ajf"."ojir__931a"`,
      expected: `a234ajf.ojir__931a`,
    },
    {
      input: `"public.hello.world"."table"`,
      expected: `"public.hello.world".table`,
    },
    {
      input: `"public"."my table"`,
      expected: `public."my table"`,
    },
    {
      input: `"db"."public. hello . world"."my.table"`,
      expected: `db."public. hello . world"."my.table"`,
    },
  ];
  it.each(tests)(
    `removes double quotes from table name parts unless it contains a space or period`,
    tc => {
      expect(formatSQLTableName(tc.input)).toBe(tc.expected);
    },
  );
});
