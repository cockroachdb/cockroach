// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Filters, getFiltersFromQueryString } from "./filter";

describe("Test filter functions", (): void => {
  describe("Test get filters from query string", (): void => {
    it("no values on query string", (): void => {
      const expectedFilters: Filters = {
        app: "",
        timeNumber: "0",
        timeUnit: "seconds",
        fullScan: false,
        sqlType: "",
        database: "",
        regions: "",
        sessionStatus: "",
        nodes: "",
        username: "",
      };
      const resultFilters = getFiltersFromQueryString("");
      expect(resultFilters).toEqual(expectedFilters);
    });
  });

  it("different values from default values on query string", (): void => {
    const expectedFilters: Filters = {
      app: "$ internal",
      timeNumber: "1",
      timeUnit: "milliseconds",
      fullScan: true,
      sqlType: "DML",
      database: "movr",
      regions: "us-central",
      sessionStatus: "idle",
      nodes: "n1,n2",
      username: "root",
    };
    const resultFilters = getFiltersFromQueryString(
      "app=%24+internal&timeNumber=1&timeUnit=milliseconds&fullScan=true&sqlType=DML&database=movr&sessionStatus=idle&username=root&regions=us-central&nodes=n1,n2",
    );
    expect(resultFilters).toEqual(expectedFilters);
  });

  it("testing boolean with full scan = true", (): void => {
    const expectedFilters: Filters = {
      app: "",
      timeNumber: "0",
      timeUnit: "seconds",
      fullScan: true,
      sqlType: "",
      database: "",
      regions: "",
      sessionStatus: "",
      nodes: "",
      username: "",
    };
    const resultFilters = getFiltersFromQueryString("fullScan=true");
    expect(resultFilters).toEqual(expectedFilters);
  });

  it("testing boolean with full scan = false", (): void => {
    const expectedFilters: Filters = {
      app: "",
      timeNumber: "0",
      timeUnit: "seconds",
      fullScan: false,
      sqlType: "",
      database: "",
      regions: "",
      sessionStatus: "",
      nodes: "",
      username: "",
    };
    const resultFilters = getFiltersFromQueryString("fullScan=false");
    expect(resultFilters).toEqual(expectedFilters);
  });

  it("testing open sessions", (): void => {
    const expectedFilters: Filters = {
      app: "",
      timeNumber: "0",
      timeUnit: "seconds",
      fullScan: false,
      sqlType: "",
      database: "",
      regions: "",
      sessionStatus: "open",
      nodes: "",
      username: "",
    };
    const resultFilters = getFiltersFromQueryString("sessionStatus=open");
    expect(resultFilters).toEqual(expectedFilters);
  });

  it("testing idle sessions", (): void => {
    const expectedFilters: Filters = {
      app: "",
      timeNumber: "0",
      timeUnit: "seconds",
      fullScan: false,
      sqlType: "",
      database: "",
      regions: "",
      sessionStatus: "idle",
      nodes: "",
      username: "",
    };
    const resultFilters = getFiltersFromQueryString("sessionStatus=idle");
    expect(resultFilters).toEqual(expectedFilters);
  });

  it("testing closed sessions", (): void => {
    const expectedFilters: Filters = {
      app: "",
      timeNumber: "0",
      timeUnit: "seconds",
      fullScan: false,
      sqlType: "",
      database: "",
      regions: "",
      sessionStatus: "closed",
      nodes: "",
      username: "",
    };
    const resultFilters = getFiltersFromQueryString("sessionStatus=closed");
    expect(resultFilters).toEqual(expectedFilters);
  });
});
