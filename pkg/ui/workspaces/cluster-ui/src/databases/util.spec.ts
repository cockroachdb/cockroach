// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  getNodesByRegionString,
  normalizePrivileges,
  normalizeRoles,
} from "./util";

describe("Getting nodes by region string", () => {
  describe("is not tenant", () => {
    it("when all nodes different regions", () => {
      const nodes = [1, 2, 3];
      const regions = {
        "1": "region1",
        "2": "region2",
        "3": "region3",
      };
      const result = getNodesByRegionString(nodes, regions, false);
      expect(result).toEqual(`region1(n1), region2(n2), region3(n3)`);
    });

    it("when all nodes same region", () => {
      const nodes = [1, 2, 3];
      const regions = {
        "1": "region1",
        "2": "region1",
        "3": "region1",
      };
      const result = getNodesByRegionString(nodes, regions, false);
      expect(result).toEqual(`region1(n1,n2,n3)`);
    });

    it("when some nodes different regions", () => {
      const nodes = [1, 2, 3];
      const regions = {
        "1": "region1",
        "2": "region1",
        "3": "region2",
      };
      const result = getNodesByRegionString(nodes, regions, false);
      expect(result).toEqual(`region1(n1,n2), region2(n3)`);
    });

    it("when region map is empty", () => {
      const nodes = [1, 2, 3];
      const regions = {};
      const result = getNodesByRegionString(nodes, regions, false);
      expect(result).toEqual("");
    });

    it("when nodes are empty", () => {
      const nodes: number[] = [];
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
