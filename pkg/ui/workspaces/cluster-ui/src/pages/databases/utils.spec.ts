// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { groupGrantsByGrantee } from "./util";

describe("groupGrantsByGrantee", () => {
  it("should group grants by grantee", () => {
    const grants = [
      { grantee: "user1", privilege: "a" },
      { grantee: "user1", privilege: "b" },
      { grantee: "user1", privilege: "c" },
      { grantee: "user2", privilege: "privilege1" },
      { grantee: "user2", privilege: "privilege2" },
      { grantee: "user3", privilege: "singlePriv" },
    ];

    const result = groupGrantsByGrantee(grants);

    expect(result).toEqual([
      { key: "user1", grantee: "user1", privileges: ["a", "b", "c"] },
      {
        key: "user2",
        grantee: "user2",
        privileges: ["privilege1", "privilege2"],
      },
      { key: "user3", grantee: "user3", privileges: ["singlePriv"] },
    ]);
  });

  it("should return empty array if the argument is empty or null", () => {
    const result = groupGrantsByGrantee([]);
    expect(result).toEqual([]);

    const resultUsingNull = groupGrantsByGrantee(null);
    expect(resultUsingNull).toEqual([]);
  });
});
