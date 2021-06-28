// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { assert } from "chai";
import { shallow } from "enzyme";
import {
  ResultsPerPageLabel,
  ResultsPerPageLabelProps,
} from "./resultsPerPageLabel";

const selectedApp = "$ cockroach demo";

describe("ResultsPerPageLabel", () => {
  const testCases: Array<{
    props: ResultsPerPageLabelProps;
    expected: string;
    description: string;
  }> = [
    {
      props: {
        pagination: { pageSize: 20, current: 1, total: 200 },
        pageName: "test",
      },
      expected: "1-20 of 200 test",
      description: "More than one page, first page is active",
    },
    {
      props: {
        pagination: { pageSize: 20, current: 1, total: 7 },
        pageName: "test",
      },
      expected: "1-7 of 7 test",
      description: "Total results count is less then page size",
    },
    {
      props: {
        pagination: { pageSize: 20, current: 1, total: 0 },
        pageName: "test",
      },
      expected: "0 test",
      description: "No results on page",
    },
    {
      props: {
        pagination: { pageSize: 10, current: 2, total: 15 },
        pageName: "test",
      },
      expected: "11-15 of 15 test",
      description: "Last page with less results count than page size",
    },
    {
      props: {
        pagination: { pageSize: 20, current: 2, total: 300 },
        pageName: "test",
        selectedApp,
      },
      expected: "21-40 of 300 results",
      description: "Selected app provided",
    },
    {
      props: {
        pagination: { pageSize: 20, current: 1, total: 64 },
        pageName: "test",
        selectedApp,
        search: "test",
      },
      expected: "1-20 of 64 results",
      description: "With selected app and search param provided",
    },
  ];

  testCases.forEach(tc => {
    it(tc.description, () => {
      assert.equal(
        shallow(<ResultsPerPageLabel {...tc.props} />).text(),
        tc.expected,
      );
    });
  });
});
