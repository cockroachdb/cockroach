// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { setBasePath, withBasePath } from "./basePath";

describe("withBasePath", () => {
  afterAll(() => {
    setBasePath("");
  });
  beforeAll(() => {
    setBasePath("");
  });

  const testCases = [
    {
      basePath: "",
      path: "",
      expected: "",
    },
    {
      basePath: "",
      path: "ppp",
      expected: "ppp",
    },
    {
      basePath: "",
      path: "/ppp",
      expectedError: `Application paths must remain compatible with relative base. Remove prefix \`/\` character.`,
    },
    {
      basePath: "dbconsole",
      path: "",
      expected: "dbconsole/",
    },
    {
      basePath: "dbconsole",
      path: "ppp",
      expected: "dbconsole/ppp",
    },
    {
      basePath: "dbconsole/",
      path: "",
      expected: "dbconsole/",
    },
    {
      basePath: "dbconsole/",
      path: "ppp",
      expected: "dbconsole/ppp",
    },
  ];

  test.each(testCases)(
    "inputs %s and %s",
    ({ path, basePath, expected, expectedError }) => {
      setBasePath(basePath);
      if (expectedError && expectedError !== "") {
        expect(() => withBasePath(path)).toThrow(expectedError);
      } else {
        expect(withBasePath(path)).toEqual(expected);
      }
    },
  );
});
