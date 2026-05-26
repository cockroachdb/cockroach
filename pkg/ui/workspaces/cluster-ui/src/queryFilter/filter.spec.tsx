// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Filters, getFiltersFromQueryString } from "./filter";

describe("Test filter functions", (): void => {
  describe("Test get filters from query string", (): void => {
    it("no values on query string", (): void => {
      const expectedFilters: Filters = {
        app: "",
        timeNumber: "0",
        timeUnit: "seconds",
        executionStatus: "",
        fullScan: false,
        sqlType: "",
        database: "",
        regions: "",
        schemaInsightType: "",
        sessionStatus: "",
        nodes: "",
        username: "",
        workloadInsightType: "",
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
      executionStatus: "",
      fullScan: true,
      sqlType: "DML",
      database: "movr",
      regions: "us-central",
      schemaInsightType: "Drop Unused Index",
      sessionStatus: "idle",
      nodes: "n1,n2",
      username: "root",
      workloadInsightType: "",
    };
    const resultFilters = getFiltersFromQueryString(
      "app=%24+internal&timeNumber=1&timeUnit=milliseconds&fullScan=true&sqlType=DML&database=movr&sessionStatus=idle&username=root&regions=us-central&nodes=n1,n2&schemaInsightType=Drop+Unused+Index",
    );
    expect(resultFilters).toEqual(expectedFilters);
  });

  it("testing boolean with full scan = true", (): void => {
    const expectedFilters: Filters = {
      app: "",
      timeNumber: "0",
      timeUnit: "seconds",
      executionStatus: "",
      fullScan: true,
      sqlType: "",
      database: "",
      regions: "",
      schemaInsightType: "",
      sessionStatus: "",
      nodes: "",
      username: "",
      workloadInsightType: "",
    };
    const resultFilters = getFiltersFromQueryString("fullScan=true");
    expect(resultFilters).toEqual(expectedFilters);
  });

  it("testing boolean with full scan = false", (): void => {
    const expectedFilters: Filters = {
      app: "",
      timeNumber: "0",
      timeUnit: "seconds",
      executionStatus: "",
      fullScan: false,
      sqlType: "",
      database: "",
      regions: "",
      schemaInsightType: "",
      sessionStatus: "",
      nodes: "",
      username: "",
      workloadInsightType: "",
    };
    const resultFilters = getFiltersFromQueryString("fullScan=false");
    expect(resultFilters).toEqual(expectedFilters);
  });

  it("testing open sessions", (): void => {
    const expectedFilters: Filters = {
      app: "",
      timeNumber: "0",
      timeUnit: "seconds",
      executionStatus: "",
      fullScan: false,
      sqlType: "",
      database: "",
      regions: "",
      schemaInsightType: "",
      sessionStatus: "open",
      nodes: "",
      username: "",
      workloadInsightType: "",
    };
    const resultFilters = getFiltersFromQueryString("sessionStatus=open");
    expect(resultFilters).toEqual(expectedFilters);
  });

  it("testing idle sessions", (): void => {
    const expectedFilters: Filters = {
      app: "",
      timeNumber: "0",
      timeUnit: "seconds",
      executionStatus: "",
      fullScan: false,
      sqlType: "",
      database: "",
      regions: "",
      schemaInsightType: "",
      sessionStatus: "idle",
      nodes: "",
      username: "",
      workloadInsightType: "",
    };
    const resultFilters = getFiltersFromQueryString("sessionStatus=idle");
    expect(resultFilters).toEqual(expectedFilters);
  });

  it("testing closed sessions", (): void => {
    const expectedFilters: Filters = {
      app: "",
      timeNumber: "0",
      timeUnit: "seconds",
      executionStatus: "",
      fullScan: false,
      sqlType: "",
      database: "",
      regions: "",
      schemaInsightType: "",
      sessionStatus: "closed",
      nodes: "",
      username: "",
      workloadInsightType: "",
    };
    const resultFilters = getFiltersFromQueryString("sessionStatus=closed");
    expect(resultFilters).toEqual(expectedFilters);
  });

  it("testing schemaInsightType", (): void => {
    const expectedFilters: Filters = {
      app: "",
      timeNumber: "0",
      timeUnit: "seconds",
      executionStatus: "",
      fullScan: false,
      sqlType: "",
      database: "",
      regions: "",
      schemaInsightType: "Drop Unused Index",
      sessionStatus: "",
      nodes: "",
      username: "",
      workloadInsightType: "",
    };
    const resultFilters = getFiltersFromQueryString(
      "schemaInsightType=Drop+Unused+Index",
    );
    expect(resultFilters).toEqual(expectedFilters);
  });
});
